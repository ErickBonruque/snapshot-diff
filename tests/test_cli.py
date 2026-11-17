from __future__ import annotations

import json
from pathlib import Path

import pytest

from snapshot_diff.cli import main
from tests.conftest import WriteParquet

SCHEMA = {"id": "INTEGER", "status": "VARCHAR"}


@pytest.fixture
def snapshots(parquet: WriteParquet) -> tuple[Path, Path]:
    old = parquet("old.parquet", SCHEMA, [{"id": 1, "status": "a"}, {"id": 2, "status": "a"}])
    new = parquet("new.parquet", SCHEMA, [{"id": 1, "status": "b"}, {"id": 3, "status": "a"}])
    return old, new


def test_diff_prints_a_summary(
    snapshots: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    old, new = snapshots
    out = tmp_path / "changes.parquet"

    assert main(["diff", str(old), str(new), "--key", "id", "--out", str(out)]) == 0

    text = capsys.readouterr().out
    assert "inserted" in text and "status" in text
    assert out.exists()


def test_diff_json(snapshots: tuple[Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
    old, new = snapshots

    main(["diff", str(old), str(new), "--key", "id", "--json"])

    summary = json.loads(capsys.readouterr().out)
    assert (summary["inserted"], summary["updated"], summary["deleted"]) == (1, 1, 1)


def test_error_exit_code(snapshots: tuple[Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
    old, new = snapshots

    assert main(["diff", str(old), str(new), "--key", "nope"]) == 2
    assert "error:" in capsys.readouterr().err


def test_history_commands(
    snapshots: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    old, new = snapshots
    history = str(tmp_path / "history")

    main(["history", "apply", history, str(old), "--key", "id", "--as-of", "2026-10-01"])
    main(["history", "apply", history, str(new), "--key", "id", "--as-of", "2026-11-01"])
    main(["history", "apply", history, str(new), "--key", "id", "--as-of", "2026-11-01"])
    main(["history", "versions", history])
    out = tmp_path / "oct.parquet"
    main(["history", "export", history, "--as-of", "2026-10-01", "--out", str(out)])

    text = capsys.readouterr().out
    assert "already in the history" in text
    assert "2026-10-01" in text and "2026-11-01" in text
    assert "wrote 2 rows" in text
