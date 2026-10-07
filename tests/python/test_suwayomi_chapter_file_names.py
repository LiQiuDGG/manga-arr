"""Chapter files must be found whatever naming the Suwayomi source uses.

_chapter_cbz only matched "Ch.<n>" (MangaDex). Weeb Central and Atsumaru name chapters "Chapter <n>", MangaFire
"Ch. <n>", Atsumaru also "# <n>", all behind a scanlator prefix, so every chapter from those sources failed to import
with "chapter CBZ not found in library path". Names below are real ones from a Suwayomi downloads folder.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import conftest  # noqa: F401, E402


def _dir(tmp_path: Path, names: list[str]) -> Path:
    for n in names:
        (tmp_path / n).write_bytes(b"cbz")
    return tmp_path


@pytest.mark.parametrize(
    "name",
    [
        "ComicDom_Vol.3 Ch.17.cbz",
        "unofficial_Ch. 17.cbz",
        "Official_Chapter 17.cbz",
        "Unknown_Chapter 17.cbz",
        "Delta_Chapter 17.cbz",
        "Alpha_# 17.cbz",
        "Official_Chapter 017.cbz",
    ],
)
def test_each_source_naming_is_found(tmp_path: Path, name: str) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, [name])
    assert suwayomi_._chapter_cbz(str(d), 17.0) == str(d / name)


def test_whole_number_does_not_match_neighbours(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Chapter 170.cbz", "Official_Chapter 117.cbz", "Official_Chapter 17.5.cbz",
                        "Vol.17 Ch.3.cbz"])
    assert suwayomi_._chapter_cbz(str(d), 17.0) is None


def test_decimal_chapter(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Chapter 17.cbz", "Official_Chapter 17.5.cbz", "Official_Chapter 17.55.cbz"])
    assert suwayomi_._chapter_cbz(str(d), 17.5) == str(d / "Official_Chapter 17.5.cbz")


def test_plain_chapter_preferred_over_extra(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["unofficial_Ch. 1 - Extra story 2.cbz", "unofficial_Ch. 1.cbz"])
    assert suwayomi_._chapter_cbz(str(d), 1.0) == str(d / "unofficial_Ch. 1.cbz")


def test_non_cbz_ignored(tmp_path: Path) -> None:
    from routers import suwayomi_

    d = _dir(tmp_path, ["Official_Chapter 17.zip"])
    assert suwayomi_._chapter_cbz(str(d), 17.0) is None
