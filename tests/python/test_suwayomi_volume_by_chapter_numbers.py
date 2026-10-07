"""Volume imports must work for sources whose chapter files don't carry "Vol.<n>".

_vol_chapter_cbzs only matched "Vol.<n>" in file names, which only MangaDex writes. Volume imports from Weeb
Central, Atsumaru or MangaFire always failed with "CBZ files not found in library path". The job knows its chapters,
so the volume is assembled from them by number. File names below are real ones from a Suwayomi downloads folder.
"""
from __future__ import annotations

from pathlib import Path

import conftest  # noqa: F401, E402


def _dir(tmp_path: Path, names: list[str]) -> Path:
    for n in names:
        (tmp_path / n).write_bytes(b"cbz")
    return tmp_path


def test_split_parts_fall_back_to_whole_chapters(tmp_path: Path) -> None:
    """Failure Frame v10: the MangaDex-derived list says 45.1/45.2..., Weeb Central publishes 45, 46..."""
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Chapter 44.5.cbz", "Official_Chapter 45.cbz", "Official_Chapter 46.cbz",
                        "Official_Chapter 47.cbz"])
    nums = [45.0, 45.1, 45.2, 46.0, 46.1, 46.2, 47.0]
    got = suwayomi_._chapter_cbzs_for_numbers(str(d), nums)
    assert [Path(p).name for p in got] == ["Official_Chapter 45.cbz", "Official_Chapter 46.cbz",
                                            "Official_Chapter 47.cbz"]


def test_two_scanlators_used_once(tmp_path: Path) -> None:
    """I'm Standing on a Million Lives v19: chapters 90-94 from both Delta ("Chapter N") and Alpha ("# N")."""
    from routers import suwayomi_

    names = [f"Delta_Chapter {n}.cbz" for n in range(90, 95)] + [f"Alpha_# {n}.cbz" for n in range(90, 95)]
    d = _dir(tmp_path, names)
    got = suwayomi_._chapter_cbzs_for_numbers(str(d), [90.0, 91.0, 92.0, 93.0, 94.0])
    assert len(got) == 5 and len({Path(p).stem.split(" ")[-1] for p in got}) == 5


def test_missing_chapter_means_no_partial_volume(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Chapter 45.cbz", "Official_Chapter 47.cbz"])
    assert suwayomi_._chapter_cbzs_for_numbers(str(d), [45.0, 46.0, 47.0]) == []


def test_order_follows_chapter_numbers(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Mission 101.cbz", "Official_Mission 99.cbz", "Official_Mission 100.cbz"])
    got = suwayomi_._chapter_cbzs_for_numbers(str(d), [99.0, 100.0, 101.0])
    assert [Path(p).name for p in got] == ["Official_Mission 99.cbz", "Official_Mission 100.cbz",
                                            "Official_Mission 101.cbz"]
