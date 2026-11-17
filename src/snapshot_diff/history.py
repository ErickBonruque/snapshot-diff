"""Version history of a table (type 2 slowly changing dimension), stored append-only.

Each applied snapshot adds one folder with only the rows that changed::

    history/_history.json                          key columns
    history/version=2026-10-01/part-0.parquet      first snapshot: every row
    history/version=2026-10-01/_SUCCESS.json       summary of the diff
    history/version=2026-11-01/part-0.parquet      next snapshot: inserted, updated, deleted rows

Every stored row has ``_op`` (``upsert`` or ``delete``) and ``_valid_from``. The SCD2 columns
``_valid_to`` and ``_is_current`` are calculated when the history is read, with ``lead()`` over
the versions of each key, so old files never change.
"""

from __future__ import annotations

import json
import shutil
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from snapshot_diff._sql import Source, idents, literal, parquet_source, session
from snapshot_diff.diff import (
    CHANGES_TABLE,
    DiffResult,
    SnapshotDiffError,
    columns,
    compare,
)

META_FILE = "_history.json"
DATA_FILE = "part-0.parquet"
SUCCESS_FILE = "_SUCCESS.json"
VERSION_PREFIX = "version="
META_COLUMNS = ("_valid_from", "_valid_to", "_is_current")


@dataclass(frozen=True)
class Version:
    as_of: date
    path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class ApplyResult:
    as_of: date
    applied: bool  # False when this date was already applied (nothing was written)
    diff: DiffResult | None


class History:
    def __init__(self, path: Source) -> None:
        self.path = Path(path)

    @property
    def key(self) -> tuple[str, ...] | None:
        meta = self.path / META_FILE
        if not meta.exists():
            return None
        return tuple(json.loads(meta.read_text())["key"])

    def versions(self) -> list[Version]:
        found = []
        for folder in self.path.glob(f"{VERSION_PREFIX}*"):
            success = folder / SUCCESS_FILE
            if folder.is_dir() and success.exists():
                as_of = date.fromisoformat(folder.name.removeprefix(VERSION_PREFIX))
                found.append(Version(as_of, folder, json.loads(success.read_text())))
        return sorted(found, key=lambda version: version.as_of)

    def scd2_sql(self) -> str:
        """All versions of all rows, with ``_valid_from``, ``_valid_to`` and ``_is_current``."""
        key = self._require_key()
        files = literal(str(self.path / f"{VERSION_PREFIX}*" / DATA_FILE))
        return f"""
            SELECT * EXCLUDE (_op), _valid_to IS NULL AS _is_current
            FROM (
                SELECT *, lead(_valid_from) OVER (
                    PARTITION BY {idents(key)} ORDER BY _valid_from
                ) AS _valid_to
                FROM read_parquet({files}, union_by_name = true, hive_partitioning = false)
            )
            WHERE _op = 'upsert'
        """

    def as_of_sql(self, when: date) -> str:
        """The table as it was on ``when`` (rows valid on that day)."""
        day = f"DATE {literal(when.isoformat())}"
        return f"""
            SELECT * EXCLUDE ({idents(META_COLUMNS)}) FROM ({self.scd2_sql()})
            WHERE _valid_from <= {day} AND (_valid_to IS NULL OR _valid_to > {day})
        """

    def current_sql(self) -> str:
        return (
            f"SELECT * EXCLUDE ({idents(META_COLUMNS)}) FROM ({self.scd2_sql()}) WHERE _is_current"
        )

    def apply(
        self,
        snapshot: Source,
        key: Sequence[str],
        as_of: date,
        *,
        memory_limit: str | None = None,
        threads: int | None = None,
    ) -> ApplyResult:
        """Add the snapshot valid from ``as_of``. Applying the same date again does nothing."""
        key = tuple(key)
        stored = self.key
        if stored is not None and stored != key:
            raise SnapshotDiffError(f"this history uses the key {list(stored)}, not {list(key)}")
        versions = self.versions()
        if any(version.as_of == as_of for version in versions):
            return ApplyResult(as_of, applied=False, diff=None)
        if versions and as_of < versions[-1].as_of:
            raise SnapshotDiffError(
                f"{as_of} is before the last applied version ({versions[-1].as_of}); "
                "versions must be applied in date order"
            )

        self.path.mkdir(parents=True, exist_ok=True)
        if stored is None:
            (self.path / META_FILE).write_text(json.dumps({"key": list(key)}, indent=2) + "\n")

        with session(memory_limit, threads) as con:
            con.execute(f"CREATE VIEW _new_src AS SELECT * FROM {parquet_source(snapshot)}")
            current = self.current_sql() if versions else "SELECT * FROM _new_src LIMIT 0"
            con.execute(f"CREATE VIEW _old_src AS {current}")
            result = compare(con, "_old_src", "_new_src", key, compare_missing=True)

            stamp = f"DATE {literal(as_of.isoformat())} AS _valid_from"
            has_values = any(c not in key for c in columns(con, "_new_src"))
            values = ", unnest(after)" if has_values else ""
            final = self.path / f"{VERSION_PREFIX}{as_of.isoformat()}"
            staging = self.path / f"_tmp-{uuid.uuid4().hex}"
            staging.mkdir()
            try:
                con.execute(
                    f"""
                    COPY (
                        SELECT {idents(key)}{values}, 'upsert' AS _op, {stamp}
                        FROM {CHANGES_TABLE} WHERE change_type <> 'delete'
                        UNION ALL BY NAME
                        SELECT {idents(key)}, 'delete' AS _op, {stamp}
                        FROM {CHANGES_TABLE} WHERE change_type = 'delete'
                    ) TO {literal(str(staging / DATA_FILE))} (FORMAT parquet, COMPRESSION zstd)
                    """
                )
                summary = {"as_of": as_of.isoformat(), **result.to_dict()}
                (staging / SUCCESS_FILE).write_text(json.dumps(summary, indent=2) + "\n")
                if final.exists():  # left over from a run that stopped before _SUCCESS.json
                    shutil.rmtree(final)
                staging.rename(final)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
        return ApplyResult(as_of, applied=True, diff=result)

    def export(self, output: Source, when: date | None = None) -> int:
        """Write the table on ``when`` (or the current table) to Parquet; return the row count."""
        sql = self.as_of_sql(when) if when else self.current_sql()
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with session() as con:
            con.execute(
                f"COPY ({sql} ORDER BY {idents(self._require_key())}) "
                f"TO {literal(str(path))} (FORMAT parquet, COMPRESSION zstd)"
            )
            fetched = con.execute(f"SELECT count(*) FROM {parquet_source(path)}").fetchone()
        return int(fetched[0]) if fetched else 0

    def _require_key(self) -> tuple[str, ...]:
        key = self.key
        if key is None:
            raise SnapshotDiffError(f"{self.path} is not a history (no {META_FILE})")
        return key
