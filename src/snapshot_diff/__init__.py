"""Compare snapshots of a table by key and keep a version history (SCD2) in Parquet."""

from snapshot_diff.diff import DiffResult, SchemaChange, SnapshotDiffError, diff
from snapshot_diff.history import ApplyResult, History, Version

__all__ = [
    "ApplyResult",
    "DiffResult",
    "History",
    "SchemaChange",
    "SnapshotDiffError",
    "Version",
    "diff",
]
__version__ = "0.1.0"
