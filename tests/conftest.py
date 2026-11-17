from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest

Rows = Sequence[dict[str, Any]]
WriteParquet = Callable[[str, dict[str, str], Rows], Path]


def write_parquet(path: Path, schema: dict[str, str], rows: Rows) -> Path:
    """Write ``rows`` to a Parquet file with an explicit DuckDB schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    columns = ", ".join(f'"{name}" {sql_type}' for name, sql_type in schema.items())
    con.execute(f"CREATE TABLE t ({columns})")
    if rows:
        marks = ", ".join("?" for _ in schema)
        con.executemany(
            f"INSERT INTO t VALUES ({marks})", [[row.get(c) for c in schema] for row in rows]
        )
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()
    return path


def read_parquet(path: Path, order_by: str) -> list[dict[str, Any]]:
    con = duckdb.connect()
    cursor = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY {order_by}")
    names = [d[0] for d in cursor.description]
    rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    con.close()
    return rows


@pytest.fixture
def parquet(tmp_path: Path) -> WriteParquet:
    def make(name: str, schema: dict[str, str], rows: Rows) -> Path:
        return write_parquet(tmp_path / name, schema, rows)

    return make
