from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from snapshot_diff import History, SnapshotDiffError
from tests.conftest import WriteParquet

SCHEMA = {"id": "INTEGER", "status": "VARCHAR"}
OCT, NOV, DEC = date(2026, 10, 1), date(2026, 11, 1), date(2026, 12, 1)


def query(sql: str) -> list[dict[str, Any]]:
    con = duckdb.connect()
    cursor = con.execute(sql)
    names = [d[0] for d in cursor.description]
    rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    con.close()
    return rows


def table(history: History, when: date) -> dict[int, str]:
    return {row["id"]: row["status"] for row in query(history.as_of_sql(when))}


@pytest.fixture
def history(tmp_path: Path, parquet: WriteParquet) -> History:
    history = History(tmp_path / "history")
    oct_ = parquet("oct.parquet", SCHEMA, [{"id": 1, "status": "a"}, {"id": 2, "status": "a"}])
    nov = parquet("nov.parquet", SCHEMA, [{"id": 1, "status": "b"}, {"id": 3, "status": "a"}])
    history.apply(oct_, ["id"], OCT)
    history.apply(nov, ["id"], NOV)
    return history


def test_table_on_each_date(history: History) -> None:
    assert table(history, date(2026, 9, 30)) == {}
    assert table(history, OCT) == {1: "a", 2: "a"}
    assert table(history, date(2026, 10, 31)) == {1: "a", 2: "a"}
    assert table(history, NOV) == {1: "b", 3: "a"}
    assert table(history, DEC) == {1: "b", 3: "a"}


def test_scd2_columns(history: History) -> None:
    rows = query(f"SELECT * FROM ({history.scd2_sql()}) ORDER BY id, _valid_from")

    assert [
        (r["id"], r["status"], r["_valid_from"], r["_valid_to"], r["_is_current"]) for r in rows
    ] == [
        (1, "a", OCT, NOV, False),
        (1, "b", NOV, None, True),
        (2, "a", OCT, NOV, False),  # closed by the delete in November
        (3, "a", NOV, None, True),
    ]


def test_only_changed_rows_are_stored(history: History) -> None:
    versions = history.versions()

    assert [v.as_of for v in versions] == [OCT, NOV]
    assert versions[0].summary["inserted"] == 2
    nov = versions[1].summary
    assert (nov["inserted"], nov["updated"], nov["deleted"]) == (1, 1, 1)
    stored = query(f"SELECT count(*) AS n FROM '{versions[1].path / 'part-0.parquet'}'")
    assert stored == [{"n": 3}]


def test_same_date_twice_does_nothing(history: History, parquet: WriteParquet) -> None:
    other = parquet("other.parquet", SCHEMA, [{"id": 9, "status": "z"}])

    result = history.apply(other, ["id"], NOV)

    assert not result.applied
    assert table(history, NOV) == {1: "b", 3: "a"}


def test_unchanged_snapshot_adds_an_empty_version(history: History, parquet: WriteParquet) -> None:
    same = parquet("dec.parquet", SCHEMA, [{"id": 1, "status": "b"}, {"id": 3, "status": "a"}])

    result = history.apply(same, ["id"], DEC)

    assert result.diff is not None and result.diff.unchanged == 2
    assert result.diff.inserted + result.diff.updated + result.diff.deleted == 0
    assert table(history, DEC) == {1: "b", 3: "a"}


def test_deleted_key_can_come_back(history: History, parquet: WriteParquet) -> None:
    back = parquet("dec.parquet", SCHEMA, [{"id": 2, "status": "c"}])

    history.apply(back, ["id"], DEC)

    assert table(history, NOV) == {1: "b", 3: "a"}
    assert table(history, DEC) == {2: "c"}


def test_new_column_is_filled_once(history: History, parquet: WriteParquet) -> None:
    wider = {"id": "INTEGER", "status": "VARCHAR", "city": "VARCHAR"}
    rows = [{"id": 1, "status": "b", "city": "X"}, {"id": 3, "status": "a", "city": None}]
    history.apply(parquet("dec.parquet", wider, rows), ["id"], DEC)

    # Same data a month later: no row changes, so the new column did not cause noise.
    result = history.apply(parquet("jan.parquet", wider, rows), ["id"], date(2027, 1, 1))

    assert result.diff is not None and result.diff.unchanged == 2
    current = {row["id"]: row["city"] for row in query(history.current_sql())}
    assert current == {1: "X", 3: None}


def test_rules_are_enforced(history: History, parquet: WriteParquet) -> None:
    snapshot = parquet("x.parquet", SCHEMA, [{"id": 1, "status": "a"}])

    with pytest.raises(SnapshotDiffError, match="date order"):
        history.apply(snapshot, ["id"], date(2026, 10, 15))
    with pytest.raises(SnapshotDiffError, match="uses the key"):
        history.apply(snapshot, ["status"], DEC)


def test_unfinished_version_is_ignored(history: History) -> None:
    (history.path / "version=2026-12-01").mkdir()  # no _SUCCESS.json: a run that stopped

    assert [v.as_of for v in history.versions()] == [OCT, NOV]


def test_export(history: History, tmp_path: Path) -> None:
    assert history.export(tmp_path / "oct.parquet", OCT) == 2
    assert history.export(tmp_path / "now.parquet") == 2
    assert query(f"SELECT id FROM '{tmp_path / 'oct.parquet'}'") == [{"id": 1}, {"id": 2}]
