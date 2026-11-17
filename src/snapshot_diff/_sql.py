"""Small helpers to build DuckDB SQL safely."""

from __future__ import annotations

import tempfile
from collections.abc import Iterable
from pathlib import Path

import duckdb

Source = str | Path


def ident(name: str) -> str:
    """Quote an identifier (column or table name)."""
    return '"' + name.replace('"', '""') + '"'


def literal(value: str) -> str:
    """Quote a string literal."""
    return "'" + value.replace("'", "''") + "'"


def idents(names: Iterable[str], prefix: str = "") -> str:
    """Comma-separated quoted identifiers, optionally qualified (``o."a", o."b"``)."""
    return ", ".join(prefix + ident(name) for name in names)


def parquet_source(source: Source) -> str:
    """SQL table function that reads one file, a glob or a folder of Parquet files.

    Hive partition folders are not turned into columns: a path such as ``release=2026-10/`` would
    otherwise add a column that differs between two snapshots of the same table.
    """
    path = Path(source)
    pattern = str(path / "**" / "*.parquet") if path.is_dir() else str(source)
    return f"read_parquet({literal(pattern)}, union_by_name = true, hive_partitioning = false)"


def connect(
    memory_limit: str | None = None, threads: int | None = None
) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection that can spill large joins to a temporary folder."""
    con = duckdb.connect()
    spill = Path(tempfile.gettempdir()) / "snapshot-diff"
    con.execute(f"SET temp_directory = {literal(str(spill))}")
    con.execute("SET preserve_insertion_order = false")
    if memory_limit:
        con.execute(f"SET memory_limit = {literal(memory_limit)}")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    return con
