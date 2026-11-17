"""Compare snapshots of a table by key and keep a version history (SCD2) in Parquet."""

from snapshot_diff.diff import DiffResult, SchemaChange, SnapshotDiffError, diff

__all__ = ["DiffResult", "SchemaChange", "SnapshotDiffError", "diff"]
__version__ = "0.1.0"
