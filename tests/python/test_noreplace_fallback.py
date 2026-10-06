"""No-replace publication on filesystems that reject RENAME_NOREPLACE (NFS).

The Linux NFS client answers ``renameat2(..., RENAME_NOREPLACE)`` with
EINVAL/EOPNOTSUPP. These tests fake that answer in every module that carries
a ``_rename_noreplace`` helper and check the fallback keeps the contract:
the destination is never replaced, and a refused move leaves both names as
they were.
"""
from __future__ import annotations

import ctypes
import errno
import os
import sys
import types
from pathlib import Path

import pytest

import conftest  # noqa: F401, E402

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="renameat2 helpers are Linux-only"
)

_MODULES = (
    "import_publication",
    "import_pack_cleanup",
    "volume_file_deletion",
    "rescan",
)


class _Renameat2Rejected:
    """Stand-in for libc.renameat2 that rejects the flag like NFS does."""

    argtypes = None
    restype = None

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *args):
        self.calls += 1
        return -1


def _nfs_ctypes(error_number: int = errno.EOPNOTSUPP):
    renameat2 = _Renameat2Rejected()
    libc = types.SimpleNamespace(renameat2=renameat2)
    fake = types.SimpleNamespace(
        CDLL=lambda *args, **kwargs: libc,
        get_errno=lambda: error_number,
        c_int=ctypes.c_int,
        c_char_p=ctypes.c_char_p,
        c_uint=ctypes.c_uint,
    )
    return fake, renameat2


def _module(name: str, monkeypatch: pytest.MonkeyPatch, error_number: int):
    module = __import__(name)
    fake, renameat2 = _nfs_ctypes(error_number)
    monkeypatch.setattr(module, "ctypes", fake)
    return module, renameat2


def _occupied(result_or_exc) -> bool:
    return result_or_exc is False or isinstance(result_or_exc, FileExistsError)


def _call(module, source: Path, destination: Path):
    try:
        return module._rename_noreplace(str(source), str(destination))
    except FileExistsError as exc:
        return exc


@pytest.mark.parametrize("error_number", sorted({errno.EINVAL, errno.EOPNOTSUPP}))
@pytest.mark.parametrize("name", _MODULES)
def test_rejected_flag_moves_file(
    name: str,
    error_number: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, renameat2 = _module(name, monkeypatch, error_number)
    source = tmp_path / "Series v01.cbz.stage"
    destination = tmp_path / "Series v01.cbz"
    source.write_bytes(b"volume")

    result = _call(module, source, destination)

    assert renameat2.calls == 1
    assert result in (None, True)
    assert not source.exists()
    assert destination.read_bytes() == b"volume"
    assert destination.stat().st_nlink == 1


@pytest.mark.parametrize("name", _MODULES)
def test_rejected_flag_never_replaces_file(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, _ = _module(name, monkeypatch, errno.EOPNOTSUPP)
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"new")
    destination.write_bytes(b"existing")

    assert _occupied(_call(module, source, destination))
    assert source.read_bytes() == b"new"
    assert destination.read_bytes() == b"existing"


@pytest.mark.parametrize("name", _MODULES)
def test_rejected_flag_moves_directory(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, _ = _module(name, monkeypatch, errno.EOPNOTSUPP)
    source = tmp_path / "pack.private"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "v01.cbz").write_bytes(b"volume")
    destination = tmp_path / "pack"

    result = _call(module, source, destination)

    assert result in (None, True)
    assert not source.exists()
    assert (destination / "nested" / "v01.cbz").read_bytes() == b"volume"


@pytest.mark.parametrize("occupant", ["empty-dir", "full-dir", "file"])
@pytest.mark.parametrize("name", _MODULES)
def test_rejected_flag_never_replaces_directory_target(
    name: str,
    occupant: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, _ = _module(name, monkeypatch, errno.EOPNOTSUPP)
    source = tmp_path / "pack.private"
    source.mkdir()
    (source / "v01.cbz").write_bytes(b"volume")
    destination = tmp_path / "pack"
    if occupant == "file":
        destination.write_bytes(b"existing")
    else:
        destination.mkdir()
        if occupant == "full-dir":
            (destination / "keep").write_bytes(b"existing")

    assert _occupied(_call(module, source, destination))
    assert (source / "v01.cbz").read_bytes() == b"volume"
    if occupant == "file":
        assert destination.read_bytes() == b"existing"
    elif occupant == "full-dir":
        assert (destination / "keep").read_bytes() == b"existing"
    else:
        assert list(destination.iterdir()) == []


@pytest.mark.parametrize("name", _MODULES)
def test_rejected_flag_missing_source(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, _ = _module(name, monkeypatch, errno.EOPNOTSUPP)

    with pytest.raises(FileNotFoundError):
        module._rename_noreplace(
            str(tmp_path / "missing"), str(tmp_path / "destination")
        )
    assert not (tmp_path / "destination").exists()


def test_rejected_flag_other_errors_still_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, _ = _module("import_publication", monkeypatch, errno.EACCES)
    source = tmp_path / "source"
    source.write_bytes(b"source")

    with pytest.raises(PermissionError):
        module._rename_noreplace(str(source), str(tmp_path / "destination"))
    assert source.read_bytes() == b"source"
    assert not (tmp_path / "destination").exists()


def test_rescan_probe_accepts_fallback_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rescan, _ = _module("rescan", monkeypatch, errno.EOPNOTSUPP)

    assert rescan._probe_rename_noreplace(str(tmp_path)) is True
    assert list(tmp_path.iterdir()) == []


def test_unlink_failure_rolls_back_own_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import noreplace_fallback

    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"volume")
    real_unlink = os.unlink

    def failing_unlink(path, *args, **kwargs):
        if os.fspath(path) == str(source):
            raise PermissionError(errno.EACCES, "denied", path)
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(noreplace_fallback.os, "unlink", failing_unlink)

    with pytest.raises(PermissionError):
        noreplace_fallback.rename_noreplace_fallback(str(source), str(destination))
    assert source.read_bytes() == b"volume"
    assert source.stat().st_nlink == 1
    assert not destination.exists()


def test_link_reported_exists_after_landing_counts_as_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NFS retransmission: the server linked, the retry answered EEXIST."""
    import noreplace_fallback

    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"volume")
    real_link = os.link

    def landed_then_exists(src, dst, *args, **kwargs):
        real_link(src, dst, *args, **kwargs)
        raise FileExistsError(errno.EEXIST, "exists", dst)

    monkeypatch.setattr(noreplace_fallback.os, "link", landed_then_exists)

    noreplace_fallback.rename_noreplace_fallback(str(source), str(destination))
    assert not source.exists()
    assert destination.read_bytes() == b"volume"


def test_existing_hardlink_of_source_is_not_mistaken_for_success(
    tmp_path: Path,
) -> None:
    import noreplace_fallback

    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"volume")
    os.link(source, destination)

    with pytest.raises(FileExistsError):
        noreplace_fallback.rename_noreplace_fallback(str(source), str(destination))
    assert source.exists()
    assert destination.exists()
    assert source.stat().st_nlink == 2


def test_directory_reservation_removed_when_rename_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import noreplace_fallback

    source = tmp_path / "pack.private"
    source.mkdir()
    destination = tmp_path / "pack"

    def failing_rename(src, dst, *args, **kwargs):
        raise OSError(errno.EIO, "io error", dst)

    monkeypatch.setattr(noreplace_fallback.os, "rename", failing_rename)

    with pytest.raises(OSError):
        noreplace_fallback.rename_noreplace_fallback(str(source), str(destination))
    assert source.is_dir()
    assert not destination.exists()


def test_directory_reservation_filled_by_racer_is_not_clobbered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import noreplace_fallback

    source = tmp_path / "pack.private"
    source.mkdir()
    (source / "v01.cbz").write_bytes(b"volume")
    destination = tmp_path / "pack"
    real_rename = os.rename

    def racing_rename(src, dst, *args, **kwargs):
        (Path(dst) / "racer").write_bytes(b"racer")
        return real_rename(src, dst, *args, **kwargs)

    monkeypatch.setattr(noreplace_fallback.os, "rename", racing_rename)

    with pytest.raises(OSError) as raised:
        noreplace_fallback.rename_noreplace_fallback(str(source), str(destination))
    assert raised.value.errno in (errno.ENOTEMPTY, errno.EEXIST)
    assert (source / "v01.cbz").read_bytes() == b"volume"
    assert (destination / "racer").read_bytes() == b"racer"
