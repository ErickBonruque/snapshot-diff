"""Measure snapshot-diff on two monthly releases of the CNPJ establishments table.

The input is the Parquet written by brazil-open-data-ingest (personal columns already removed):

    brazil-ingest cnpj ingest 2026-10 --dest data/cnpj --tables establishments
    brazil-ingest cnpj ingest 2026-11 --dest data/cnpj --tables establishments

Then:

    uv run --group demo python scripts/cnpj_demo.py OLD_DIR NEW_DIR

Only aggregates are written to results/ (no company rows). The changed rows and the history
go to data/, which git ignores.
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import time
from datetime import date
from pathlib import Path

import duckdb

from snapshot_diff import History, diff

KEY = ["cnpj_base", "cnpj_order", "cnpj_check"]
STATUS = {1: "null", 2: "active", 3: "suspended", 4: "unfit", 8: "closed"}
ROOT = Path(__file__).resolve().parent.parent


def peak_memory_mb() -> float:
    # Linux reports ru_maxrss in kilobytes.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def folder_size_mb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0] if __doc__ else None)
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--old-date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--new-date", type=date.fromisoformat, default=date(2026, 11, 1))
    parser.add_argument("--data", type=Path, default=ROOT / "data" / "cnpj-demo")
    parser.add_argument("--results", type=Path, default=ROOT / "results")
    parser.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 3GB")
    args = parser.parse_args()
    args.results.mkdir(parents=True, exist_ok=True)
    changes = args.data / "changes.parquet"

    start = time.perf_counter()
    result = diff(args.old, args.new, KEY, output=changes, memory_limit=args.memory_limit)
    diff_seconds = time.perf_counter() - start
    diff_memory = peak_memory_mb()

    history_dir = args.data / "history"
    if history_dir.exists():
        shutil.rmtree(history_dir)
    history = History(history_dir)
    timings = {}
    for snapshot, as_of in ((args.old, args.old_date), (args.new, args.new_date)):
        start = time.perf_counter()
        history.apply(snapshot, KEY, as_of, memory_limit=args.memory_limit)
        timings[as_of.isoformat()] = round(time.perf_counter() - start, 1)
    sizes = {v.as_of.isoformat(): round(folder_size_mb(v.path), 1) for v in history.versions()}

    con = duckdb.connect()
    source = f"read_parquet('{changes}')"
    transitions = con.execute(
        f"""
        SELECT before.registration_status AS old, after.registration_status AS new, count(*) AS n
        FROM {source}
        WHERE change_type = 'update' AND list_contains(changed_columns, 'registration_status')
        GROUP BY ALL ORDER BY n DESC
        """
    ).fetchall()
    inserted_status = con.execute(
        f"""
        SELECT after.registration_status AS status, count(*) AS n FROM {source}
        WHERE change_type = 'insert' GROUP BY ALL ORDER BY n DESC
        """
    ).fetchall()
    columns_per_update = con.execute(
        f"""
        SELECT len(changed_columns) AS columns, count(*) AS n FROM {source}
        WHERE change_type = 'update' GROUP BY ALL ORDER BY columns
        """
    ).fetchall()
    con.close()

    summary = {
        "old_release": str(args.old_date),
        "new_release": str(args.new_date),
        **result.to_dict(),
        "diff_seconds": round(diff_seconds, 1),
        "diff_peak_memory_mb": round(diff_memory),
        "changes_file_mb": round(changes.stat().st_size / 1e6, 1),
        "history_apply_seconds": timings,
        "history_version_mb": sizes,
        "peak_memory_mb": round(peak_memory_mb()),
        "memory_limit": args.memory_limit,
    }
    (args.results / "cnpj_diff_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_csv(
        args.results / "cnpj_status_transitions.csv",
        ["old_status", "new_status", "establishments"],
        [(STATUS.get(old, old), STATUS.get(new, new), n) for old, new, n in transitions],
    )
    write_csv(
        args.results / "cnpj_inserted_by_status.csv",
        ["status", "establishments"],
        [(STATUS.get(status, status), n) for status, n in inserted_status],
    )
    write_csv(
        args.results / "cnpj_changed_columns.csv",
        ["column", "updated_rows"],
        list(result.changed_columns.items()),
    )
    write_csv(
        args.results / "cnpj_columns_per_update.csv",
        ["changed_columns", "updated_rows"],
        columns_per_update,
    )
    plot(result.changed_columns, args.results.parent / "docs" / "img" / "cnpj_changed_columns.png")
    print(json.dumps(summary, indent=2))


def write_csv(path: Path, header: list[str], rows: list[tuple[object, ...]]) -> None:
    lines = [",".join(header)] + [",".join(str(value) for value in row) for row in rows]
    path.write_text("\n".join(lines) + "\n")


def plot(changed_columns: dict[str, int], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    path.parent.mkdir(parents=True, exist_ok=True)
    items = sorted(changed_columns.items(), key=lambda item: item[1])
    fig, ax = plt.subplots(figsize=(8, 0.42 * len(items) + 1.2), dpi=150)
    ax.barh([name for name, _ in items], [n for _, n in items], color="#7c4dce")
    for index, (_, n) in enumerate(items):
        ax.text(n, index, f" {n:,}", va="center", fontsize=9)
    ax.set_xlabel("updated establishments")
    ax.set_title("CNPJ establishments: changed columns between two monthly releases", loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_xlim(0, max(n for _, n in items) * 1.18)
    ax.xaxis.set_major_formatter(lambda value, _: f"{value:,.0f}")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


if __name__ == "__main__":
    main()
