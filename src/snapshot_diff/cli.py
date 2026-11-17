"""Command line: ``snapshot-diff diff ...`` and ``snapshot-diff history ...``."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from datetime import date

from snapshot_diff.diff import DiffResult, SnapshotDiffError, diff
from snapshot_diff.history import History


def _columns(values: Sequence[str] | None) -> list[str]:
    """``--key a --key b`` and ``--key a,b`` both give ``["a", "b"]``."""
    return [name.strip() for value in values or () for name in value.split(",") if name.strip()]


def _engine_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 4GB")
    parser.add_argument("--threads", type=int, help="DuckDB threads (default: all cores)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="snapshot-diff",
        description="Compare snapshots of a table by key and keep a version history in Parquet.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    cmd = commands.add_parser("diff", help="compare two snapshots")
    cmd.add_argument("old", help="Parquet file, glob or folder")
    cmd.add_argument("new", help="Parquet file, glob or folder")
    cmd.add_argument("--key", action="append", required=True, help="key column(s)")
    cmd.add_argument("--ignore", action="append", help="column(s) to leave out of the comparison")
    cmd.add_argument("--out", help="write the changed rows to this Parquet file")
    cmd.add_argument("--json", action="store_true", help="print the summary as JSON")
    _engine_options(cmd)

    history = commands.add_parser("history", help="version history (SCD2)")
    actions = history.add_subparsers(dest="action", required=True)

    apply = actions.add_parser("apply", help="add a snapshot to a history")
    apply.add_argument("history", help="history folder (created if missing)")
    apply.add_argument("snapshot", help="Parquet file, glob or folder")
    apply.add_argument("--key", action="append", required=True, help="key column(s)")
    apply.add_argument("--as-of", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    apply.add_argument("--json", action="store_true", help="print the summary as JSON")
    _engine_options(apply)

    export = actions.add_parser("export", help="write the table on a date to Parquet")
    export.add_argument("history", help="history folder")
    export.add_argument("--as-of", type=date.fromisoformat, help="YYYY-MM-DD (default: current)")
    export.add_argument("--out", required=True, help="output Parquet file")
    _engine_options(export)

    versions = actions.add_parser("versions", help="list the applied versions")
    versions.add_argument("history", help="history folder")
    return parser


def format_summary(result: DiffResult, seconds: float) -> str:
    rows = [
        ("old rows", result.old_rows),
        ("new rows", result.new_rows),
        ("inserted", result.inserted),
        ("updated", result.updated),
        ("deleted", result.deleted),
        ("unchanged", result.unchanged),
    ]
    lines = [f"{label:<12}{count:>15,}" for label, count in rows]
    if result.changed_columns:
        lines.append("changed columns (updated rows):")
        width = max(len(name) for name in result.changed_columns)
        lines += [f"  {name:<{width}}  {n:>13,}" for name, n in result.changed_columns.items()]
    schema = result.schema
    if schema.added:
        lines.append(f"added columns: {', '.join(schema.added)}")
    if schema.removed:
        lines.append(f"removed columns: {', '.join(schema.removed)}")
    for name, old_type, new_type in schema.retyped:
        lines.append(f"type changed: {name} {old_type} -> {new_type} (compared as text)")
    lines.append(f"time: {seconds:.1f} s")
    return "\n".join(lines)


def run(args: argparse.Namespace) -> None:
    start = time.perf_counter()
    if args.command == "diff":
        result = diff(
            args.old,
            args.new,
            _columns(args.key),
            output=args.out,
            ignore=_columns(args.ignore),
            memory_limit=args.memory_limit,
            threads=args.threads,
        )
        _print(result, args.json, time.perf_counter() - start)
        return

    history = History(args.history)
    if args.action == "apply":
        applied = history.apply(
            args.snapshot,
            _columns(args.key),
            args.as_of,
            memory_limit=args.memory_limit,
            threads=args.threads,
        )
        if applied.diff is None:
            print(f"{args.as_of} is already in the history; nothing to do")
        else:
            _print(applied.diff, args.json, time.perf_counter() - start)
    elif args.action == "export":
        count = history.export(
            args.out, args.as_of, memory_limit=args.memory_limit, threads=args.threads
        )
        print(f"wrote {count:,} rows to {args.out}")
    elif args.action == "versions":
        for version in history.versions():
            summary = version.summary
            print(
                f"{version.as_of}  rows {summary['new_rows']:>12,}  "
                f"+{summary['inserted']:,} ~{summary['updated']:,} -{summary['deleted']:,}"
            )


def _print(result: DiffResult, as_json: bool, seconds: float) -> None:
    if as_json:
        print(json.dumps({**result.to_dict(), "seconds": round(seconds, 2)}, indent=2))
    else:
        print(format_summary(result, seconds))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(args)
    except SnapshotDiffError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
