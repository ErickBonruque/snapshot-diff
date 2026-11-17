from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from snapshot_diff import SnapshotDiffError, diff
from tests.conftest import WriteParquet, read_parquet, write_parquet

SCHEMA = {"id": "INTEGER", "name": "VARCHAR", "status": "VARCHAR"}


def test_counts_each_kind_of_change(parquet: WriteParquet, tmp_path: Path) -> None:
    old = parquet(
        "old.parquet",
        SCHEMA,
        [
            {"id": 1, "name": "a", "status": "active"},
            {"id": 2, "name": "b", "status": "active"},
            {"id": 3, "name": "c", "status": "active"},
            {"id": 4, "name": "d", "status": "closed"},
        ],
    )
    new = parquet(
        "new.parquet",
        SCHEMA,
        [
            {"id": 1, "name": "a", "status": "active"},
            {"id": 2, "name": "b", "status": "closed"},
            {"id": 4, "name": "d", "status": "closed"},
            {"id": 5, "name": "e", "status": "active"},
        ],
    )
    out = tmp_path / "changes.parquet"

    result = diff(old, new, ["id"], output=out)

    assert (result.inserted, result.updated, result.deleted, result.unchanged) == (1, 1, 1, 2)
    assert (result.old_rows, result.new_rows) == (4, 4)
    assert result.changed_columns == {"status": 1}
    assert not result.schema
    rows = read_parquet(out, "id")
    assert [(r["id"], r["change_type"]) for r in rows] == [
        (2, "update"),
        (3, "delete"),
        (5, "insert"),
    ]
    update, delete, insert = rows
    assert update["changed_columns"] == ["status"]
    assert update["before"] == {"name": "b", "status": "active"}
    assert update["after"] == {"name": "b", "status": "closed"}
    assert delete["after"] is None
    assert insert["before"] is None


def test_null_equals_null_but_not_a_value(parquet: WriteParquet) -> None:
    old = parquet(
        "old.parquet",
        SCHEMA,
        [{"id": 1, "name": None}, {"id": 2, "name": None}, {"id": 3, "name": "x"}],
    )
    new = parquet(
        "new.parquet",
        SCHEMA,
        [{"id": 1, "name": None}, {"id": 2, "name": "x"}, {"id": 3, "name": None}],
    )

    result = diff(old, new, ["id"])

    assert (result.updated, result.unchanged) == (2, 1)
    assert result.changed_columns == {"name": 2}


def test_composite_key(parquet: WriteParquet) -> None:
    schema = {"year": "INTEGER", "code": "VARCHAR", "value": "DOUBLE"}
    old = parquet(
        "old.parquet",
        schema,
        [{"year": 2023, "code": "A", "value": 1.0}, {"year": 2024, "code": "A", "value": 2.0}],
    )
    new = parquet(
        "new.parquet",
        schema,
        [{"year": 2023, "code": "A", "value": 1.0}, {"year": 2024, "code": "A", "value": 2.5}],
    )

    result = diff(old, new, ["year", "code"])

    assert (result.updated, result.unchanged) == (1, 1)


def test_ignored_columns_are_not_compared(parquet: WriteParquet) -> None:
    old = parquet("old.parquet", SCHEMA, [{"id": 1, "name": "a", "status": "x"}])
    new = parquet("new.parquet", SCHEMA, [{"id": 1, "name": "a", "status": "y"}])

    assert diff(old, new, ["id"], ignore=["status"]).unchanged == 1


def test_schema_change_is_reported(parquet: WriteParquet) -> None:
    old = parquet(
        "old.parquet",
        {"id": "INTEGER", "code": "INTEGER", "gone": "VARCHAR"},
        [{"id": 1, "code": 7, "gone": "x"}],
    )
    new = parquet(
        "new.parquet",
        {"id": "INTEGER", "code": "VARCHAR", "extra": "VARCHAR"},
        [{"id": 1, "code": "7", "extra": "y"}],
    )

    result = diff(old, new, ["id"])

    assert result.schema.added == ("extra",)
    assert result.schema.removed == ("gone",)
    assert result.schema.retyped == (("code", "INTEGER", "VARCHAR"),)
    # 7 and '7' are equal when compared as text; added and removed columns are not compared.
    assert result.unchanged == 1


def test_folder_input_does_not_add_partition_columns(tmp_path: Path) -> None:
    for release in ("2026-10", "2026-11"):
        for part in (0, 1):
            write_parquet(
                tmp_path / release / f"release={release}" / f"part-{part}.parquet",
                SCHEMA,
                [{"id": part, "name": "a", "status": "x"}],
            )

    result = diff(tmp_path / "2026-10", tmp_path / "2026-11", ["id"])

    assert result.unchanged == 2
    assert not result.schema


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([{"id": 1}, {"id": 1}], "not unique"),
        ([{"id": None}], "NULL key"),
    ],
)
def test_invalid_keys_are_rejected(
    parquet: WriteParquet, rows: list[dict[str, int | None]], message: str
) -> None:
    good = parquet("good.parquet", SCHEMA, [{"id": 1}])
    bad = parquet("bad.parquet", SCHEMA, rows)

    with pytest.raises(SnapshotDiffError, match=message):
        diff(good, bad, ["id"])


def test_bad_arguments_are_rejected(parquet: WriteParquet) -> None:
    path = parquet("t.parquet", SCHEMA, [{"id": 1}])

    with pytest.raises(SnapshotDiffError, match="not found"):
        diff(path, path, ["missing"])
    with pytest.raises(SnapshotDiffError, match="both key and ignored"):
        diff(path, path, ["id"], ignore=["id"])
    with pytest.raises(SnapshotDiffError, match="at least one key"):
        diff(path, path, [])


values = st.one_of(st.none(), st.integers(0, 3))
tables = st.dictionaries(st.integers(0, 30), st.tuples(values, values), max_size=25)


@settings(max_examples=60, deadline=None)
@given(old=tables, new=tables)
def test_applying_the_changes_to_old_gives_new(
    tmp_path_factory: pytest.TempPathFactory,
    old: dict[int, tuple[int | None, int | None]],
    new: dict[int, tuple[int | None, int | None]],
) -> None:
    folder = tmp_path_factory.mktemp("prop")
    schema = {"id": "INTEGER", "a": "INTEGER", "b": "INTEGER"}

    def rows(table: dict[int, tuple[int | None, int | None]]) -> list[dict[str, int | None]]:
        return [{"id": k, "a": a, "b": b} for k, (a, b) in table.items()]

    old_path = write_parquet(folder / "old.parquet", schema, rows(old))
    new_path = write_parquet(folder / "new.parquet", schema, rows(new))
    out = folder / "changes.parquet"

    result = diff(old_path, new_path, ["id"], output=out)

    rebuilt = dict(old)
    for change in read_parquet(out, "id"):
        if change["change_type"] == "delete":
            del rebuilt[change["id"]]
        else:
            rebuilt[change["id"]] = (change["after"]["a"], change["after"]["b"])
    assert rebuilt == new
    assert result.inserted == len(new.keys() - old.keys())
    assert result.deleted == len(old.keys() - new.keys())
    assert result.updated == sum(1 for k in old.keys() & new.keys() if old[k] != new[k])
