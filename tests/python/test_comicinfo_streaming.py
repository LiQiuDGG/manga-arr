"""ComicInfo injection streams the archive instead of loading it into memory.

A digital volume is commonly 300+ MB of stored images. Reading every entry
into memory before rewriting made each import's peak memory the size of the
volume, which OOM-killed a 640Mi/1.5Gi container on a 23-volume pack.
"""
from __future__ import annotations

import os
import stat
import tracemalloc
import zipfile
from pathlib import Path

import pytest

import conftest  # noqa: F401, E402

_XML = "<ComicInfo><Series>Series</Series></ComicInfo>"


def _make_cbz(path: Path, entries: list[tuple[str, bytes]]) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as archive:
        for name, data in entries:
            archive.writestr(name, data)


def _entries(path: Path) -> list[tuple[str, bytes]]:
    with zipfile.ZipFile(path) as archive:
        return [(name, archive.read(name)) for name in archive.namelist()]


def test_inject_preserves_pages_and_replaces_comicinfo(tmp_path: Path) -> None:
    import comicinfo

    cbz = tmp_path / "Series v01.cbz"
    _make_cbz(
        cbz,
        [
            ("ComicInfo.xml", b"<ComicInfo><Series>Old</Series></ComicInfo>"),
            ("chapter 1/", b""),
            ("chapter 1/001.png", b"page-1"),
            ("chapter 1/002.png", b"page-2"),
            ("sub/comicinfo.XML", b"nested old"),
            ("003.png", b"page-3"),
        ],
    )

    assert comicinfo.inject_comicinfo(str(cbz), _XML) is True

    assert _entries(cbz) == [
        ("ComicInfo.xml", _XML.encode("utf-8")),
        ("chapter 1/", b""),
        ("chapter 1/001.png", b"page-1"),
        ("chapter 1/002.png", b"page-2"),
        ("003.png", b"page-3"),
    ]
    with zipfile.ZipFile(cbz) as archive:
        assert archive.testzip() is None
        assert {info.compress_type for info in archive.infolist()} == {
            zipfile.ZIP_STORED
        }
        assert archive.getinfo("chapter 1/").is_dir()
    assert [p.name for p in tmp_path.iterdir()] == [cbz.name]


def test_inject_keeps_file_mode(tmp_path: Path) -> None:
    import comicinfo

    cbz = tmp_path / "Series v01.cbz"
    _make_cbz(cbz, [("001.png", b"page")])
    os.chmod(cbz, 0o644)

    assert comicinfo.inject_comicinfo(str(cbz), _XML) is True
    assert stat.S_IMODE(os.stat(cbz).st_mode) == 0o644


def test_inject_peak_memory_is_bounded_by_copy_buffer(tmp_path: Path) -> None:
    import comicinfo

    page = os.urandom(24 * 1024 * 1024)
    cbz = tmp_path / "Series v01.cbz"
    _make_cbz(cbz, [("001.png", page), ("002.png", page)])
    del page

    tracemalloc.start()
    try:
        assert comicinfo.inject_comicinfo(str(cbz), _XML) is True
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # A whole-archive read would peak above 48 MB; streaming stays near the
    # 1 MB copy buffer.
    assert peak < 8 * 1024 * 1024, f"peak {peak / 2**20:.1f} MiB"
    with zipfile.ZipFile(cbz) as archive:
        assert archive.namelist() == ["ComicInfo.xml", "001.png", "002.png"]
        assert archive.getinfo("001.png").file_size == 24 * 1024 * 1024


def test_failed_rewrite_leaves_original_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import comicinfo

    cbz = tmp_path / "Series v01.cbz"
    _make_cbz(cbz, [("001.png", b"page-1"), ("002.png", b"page-2")])
    original = cbz.read_bytes()
    calls = 0
    real_copy = comicinfo.shutil.copyfileobj

    def failing_copy(src, dst, length=0):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError(28, "No space left on device")
        return real_copy(src, dst, length)

    monkeypatch.setattr(comicinfo.shutil, "copyfileobj", failing_copy)

    assert comicinfo.inject_comicinfo(str(cbz), _XML) is False
    assert cbz.read_bytes() == original
    assert [p.name for p in tmp_path.iterdir()] == [cbz.name]


def test_non_zip_is_left_alone(tmp_path: Path) -> None:
    import comicinfo

    cbz = tmp_path / "Series v01.cbz"
    cbz.write_bytes(b"not a zip archive")

    assert comicinfo.inject_comicinfo(str(cbz), _XML) is False
    assert cbz.read_bytes() == b"not a zip archive"
    assert [p.name for p in tmp_path.iterdir()] == [cbz.name]
