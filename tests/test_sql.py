from __future__ import annotations

import stat
from pathlib import Path

from snapshot_diff._sql import ident, literal, session


def test_spill_folder_is_private_and_removed() -> None:
    with session() as con:
        row = con.execute("SELECT current_setting('temp_directory')").fetchone()
        assert row is not None
        spill = Path(row[0])
        assert spill.is_dir()
        assert stat.S_IMODE(spill.stat().st_mode) == 0o700
    assert not spill.exists()


def test_quoting() -> None:
    assert ident('a"b') == '"a""b"'
    assert literal("it's") == "'it''s'"
