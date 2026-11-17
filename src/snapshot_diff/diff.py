"""Compare two snapshots of a table by key.

The comparison runs in two phases, so that the large join stays narrow:

1. For each snapshot, keep one row per key with a hash of the compared columns. A full outer
   join of these narrow tables finds the keys that were inserted, deleted or possibly updated.
2. Only for those keys, join back to the full rows and compare each column with
   ``IS DISTINCT FROM`` (two NULLs are equal). The result keeps the row before and after.

A 64-bit hash can collide, so phase 1 can in theory miss an update (about one in 10^19 per
changed row). Phase 2 never reports an update that did not happen.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from snapshot_diff._sql import Source, ident, idents, literal, parquet_source, session

CHANGES_TABLE = "changes"


class SnapshotDiffError(ValueError):
    """The snapshots or the key cannot be compared."""


@dataclass(frozen=True)
class SchemaChange:
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    retyped: tuple[tuple[str, str, str], ...] = ()  # (column, old type, new type)

    def __bool__(self) -> bool:
        return bool(self.added or self.removed or self.retyped)


@dataclass(frozen=True)
class DiffResult:
    key: tuple[str, ...]
    old_rows: int
    new_rows: int
    inserted: int
    updated: int
    deleted: int
    changed_columns: dict[str, int] = field(default_factory=dict)
    schema: SchemaChange = field(default_factory=SchemaChange)

    @property
    def unchanged(self) -> int:
        return self.new_rows - self.inserted - self.updated

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["unchanged"] = self.unchanged
        return data


def diff(
    old: Source,
    new: Source,
    key: Sequence[str],
    *,
    output: Source | None = None,
    ignore: Sequence[str] = (),
    memory_limit: str | None = None,
    threads: int | None = None,
) -> DiffResult:
    """Compare two Parquet snapshots (file, glob or folder) by ``key``.

    If ``output`` is given, the changed rows are written there as Parquet with the columns
    ``<key columns>, change_type, changed_columns, before, after``.
    """
    with session(memory_limit, threads) as con:
        con.execute(f"CREATE VIEW _old_src AS SELECT * FROM {parquet_source(old)}")
        con.execute(f"CREATE VIEW _new_src AS SELECT * FROM {parquet_source(new)}")
        result = compare(con, "_old_src", "_new_src", key, ignore=ignore)
        if output is not None:
            write_changes(con, result.key, output)
        return result


def columns(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    """Column name to DuckDB type, in table order."""
    rows = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def compare(
    con: duckdb.DuckDBPyConnection,
    old: str,
    new: str,
    key: Sequence[str],
    *,
    ignore: Sequence[str] = (),
    compare_missing: bool = False,
) -> DiffResult:
    """Compare two relations and keep the changed rows in the temporary table ``changes``.

    ``old`` and ``new`` are table or view names. With ``compare_missing``, a column that exists
    in only one snapshot is compared as if it were NULL in the other one.
    """
    key = tuple(key)
    if not key:
        raise SnapshotDiffError("at least one key column is required")
    old_cols = columns(con, old)
    new_cols = columns(con, new)
    _check_columns(key, ignore, old_cols, new_cols)

    schema = SchemaChange(
        added=tuple(c for c in new_cols if c not in old_cols),
        removed=tuple(c for c in old_cols if c not in new_cols),
        retyped=tuple(
            (c, old_cols[c], new_cols[c])
            for c in old_cols
            if c in new_cols and old_cols[c] != new_cols[c]
        ),
    )
    retyped = {name for name, _, _ in schema.retyped}
    all_cols = list(old_cols) + [c for c in new_cols if c not in old_cols]
    compared = [
        c
        for c in all_cols
        if c not in key
        and c not in ignore
        and ((c in old_cols and c in new_cols) or compare_missing)
    ]

    def value(alias: str, col: str, available: dict[str, str]) -> str:
        if col not in available:
            return "NULL"
        expr = f"{alias}.{ident(col)}"
        return f"CAST({expr} AS VARCHAR)" if col in retyped else expr

    # Phase 1: narrow tables (key + row hash) and a full outer join on the key.
    for table, relation, cols, side in (
        ("_old_h", old, old_cols, "old"),
        ("_new_h", new, new_cols, "new"),
    ):
        keys = ", ".join(f"{value('s', k, cols)} AS {ident(k)}" for k in key)
        row = ", ".join(value("s", c, cols) for c in compared)
        row_hash = f"hash(row({row}))" if compared else "0::UBIGINT"
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS "
            f"SELECT {keys}, {row_hash} AS _h FROM {relation} s"
        )
        _check_keys(con, table, key, side)

    on = " AND ".join(f"o.{ident(k)} = n.{ident(k)}" for k in key)
    coalesced = ", ".join(f"coalesce(o.{ident(k)}, n.{ident(k)}) AS {ident(k)}" for k in key)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _delta AS
        SELECT {coalesced},
               CASE WHEN o._h IS NULL THEN 'insert'
                    WHEN n._h IS NULL THEN 'delete'
                    ELSE 'update' END AS change_type
        FROM _old_h o FULL OUTER JOIN _new_h n ON {on}
        WHERE o._h IS DISTINCT FROM n._h
        """
    )

    # Phase 2: full rows only for the changed keys.
    def join_on(alias: str, cols: dict[str, str]) -> str:
        return " AND ".join(f"{value(alias, k, cols)} = d.{ident(k)}" for k in key)

    checks = [
        f"CASE WHEN {value('o', c, old_cols)} IS DISTINCT FROM {value('n', c, new_cols)} "
        f"THEN [{literal(c)}] ELSE []::VARCHAR[] END"
        for c in compared
    ]
    changed = f"flatten([{', '.join(checks)}])" if checks else "[]::VARCHAR[]"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {CHANGES_TABLE} AS
        SELECT {idents(key, "d.")}, d.change_type,
               CASE WHEN d.change_type = 'update' THEN {changed}
                    ELSE []::VARCHAR[] END AS changed_columns,
               CASE WHEN d.change_type <> 'insert' THEN {_struct("o", old_cols, key)} END
                   AS before,
               CASE WHEN d.change_type <> 'delete' THEN {_struct("n", new_cols, key)} END
                   AS after
        FROM _delta d
        LEFT JOIN {old} o ON d.change_type <> 'insert' AND {join_on("o", old_cols)}
        LEFT JOIN {new} n ON d.change_type <> 'delete' AND {join_on("n", new_cols)}
        """
    )

    counts = dict(
        con.execute(f"SELECT change_type, count(*) FROM {CHANGES_TABLE} GROUP BY 1").fetchall()
    )
    per_column = con.execute(
        f"""
        SELECT col, count(*) AS n
        FROM (SELECT unnest(changed_columns) AS col FROM {CHANGES_TABLE})
        GROUP BY col ORDER BY n DESC, col
        """
    ).fetchall()
    old_rows = _scalar(con, "SELECT count(*) FROM _old_h")
    new_rows = _scalar(con, "SELECT count(*) FROM _new_h")
    con.execute("DROP TABLE _old_h; DROP TABLE _new_h; DROP TABLE _delta")
    return DiffResult(
        key=key,
        old_rows=old_rows,
        new_rows=new_rows,
        inserted=int(counts.get("insert", 0)),
        updated=int(counts.get("update", 0)),
        deleted=int(counts.get("delete", 0)),
        changed_columns={str(col): int(n) for col, n in per_column},
        schema=schema,
    )


def write_changes(con: duckdb.DuckDBPyConnection, key: Sequence[str], output: Source) -> None:
    """Write the ``changes`` table to a Parquet file, sorted by change type and key."""
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM {CHANGES_TABLE} ORDER BY change_type, {idents(key)}) "
        f"TO {literal(str(path))} (FORMAT parquet, COMPRESSION zstd)"
    )


def _struct(alias: str, cols: dict[str, str], key: tuple[str, ...]) -> str:
    fields = [c for c in cols if c not in key]
    if not fields:
        return "NULL"
    return "struct_pack(" + ", ".join(f"{ident(c)} := {alias}.{ident(c)}" for c in fields) + ")"


def _check_columns(
    key: tuple[str, ...],
    ignore: Sequence[str],
    old_cols: dict[str, str],
    new_cols: dict[str, str],
) -> None:
    for side, cols in (("old", old_cols), ("new", new_cols)):
        missing = [k for k in key if k not in cols]
        if missing:
            raise SnapshotDiffError(f"key column(s) {missing} not found in the {side} snapshot")
    unknown = [c for c in ignore if c not in old_cols and c not in new_cols]
    if unknown:
        raise SnapshotDiffError(f"ignored column(s) {unknown} not found in either snapshot")
    overlap = sorted(set(key) & set(ignore))
    if overlap:
        raise SnapshotDiffError(f"column(s) {overlap} cannot be both key and ignored")


def validate_key(
    con: duckdb.DuckDBPyConnection, relation: str, key: Sequence[str], side: str
) -> int:
    """Check that ``key`` exists, is never NULL and is unique; return the row count."""
    key = tuple(key)
    missing = [k for k in key if k not in columns(con, relation)]
    if missing:
        raise SnapshotDiffError(f"key column(s) {missing} not found in the {side} snapshot")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _keys AS SELECT {idents(key)} FROM {relation}")
    _check_keys(con, "_keys", key, side)
    rows = _scalar(con, "SELECT count(*) FROM _keys")
    con.execute("DROP TABLE _keys")
    return rows


def _check_keys(
    con: duckdb.DuckDBPyConnection, table: str, key: tuple[str, ...], side: str
) -> None:
    null_filter = " OR ".join(f"{ident(k)} IS NULL" for k in key)
    nulls = _scalar(con, f"SELECT count(*) FROM {table} WHERE {null_filter}")
    if nulls:
        raise SnapshotDiffError(f"{nulls:,} row(s) in the {side} snapshot have a NULL key")
    duplicates = con.execute(
        f"""
        SELECT {idents(key)}, count(*) AS n FROM {table}
        GROUP BY ALL HAVING count(*) > 1 ORDER BY n DESC LIMIT 3
        """
    ).fetchall()
    if duplicates:
        examples = "; ".join(
            f"{dict(zip(key, row[:-1], strict=True))} x{row[-1]}" for row in duplicates
        )
        raise SnapshotDiffError(f"the key is not unique in the {side} snapshot, e.g. {examples}")


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row else 0
