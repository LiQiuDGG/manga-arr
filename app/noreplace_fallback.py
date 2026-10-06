"""No-replace rename for filesystems that reject ``RENAME_NOREPLACE``.

``renameat2(2)`` with ``RENAME_NOREPLACE`` is implemented by ext4, btrfs,
xfs, tmpfs, cifs and others, but not by the Linux NFS client, which rejects
the flag at every protocol version. On an NFS-mounted library every
publication would otherwise fail closed.

This is not a check-then-rename emulation. Each branch claims the
destination name with an operation that is itself atomic and fails with
``EEXIST`` when the name is occupied:

- non-directories publish with ``link(2)``, then drop the source name;
- directories reserve the name with ``mkdir(2)``, then ``rename(2)`` onto
  that empty reservation (``rename`` never replaces a non-empty directory).

Residual windows, all fail-closed for callers that refuse to clobber:

- a crash between ``link`` and ``unlink`` leaves both names on one inode;
- a crash between ``mkdir`` and ``rename`` leaves an empty reservation;
- a writer that removes the reservation and recreates an empty directory
  before the ``rename`` has that empty directory replaced.
"""

from __future__ import annotations

import errno
import os
import stat

# renameat2 errnos that mean "this host or filesystem does not implement the
# flag", as opposed to an ordinary rename failure.
UNSUPPORTED_ERRNOS = frozenset({errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP})


def rename_noreplace_fallback(source: str, destination: str) -> None:
    """Move ``source`` to ``destination`` without ever replacing it.

    Raises ``FileExistsError`` when the destination is occupied, and
    ``FileNotFoundError`` when the source is missing, like ``renameat2``.
    """
    info = os.lstat(source)
    if stat.S_ISDIR(info.st_mode):
        _rename_directory_noreplace(source, destination)
    else:
        _rename_entry_noreplace(source, destination, info)


def _same_inode(path: str, info: os.stat_result) -> bool:
    try:
        current = os.lstat(path)
    except FileNotFoundError:
        return False
    return (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino)


def _rename_entry_noreplace(
    source: str,
    destination: str,
    info: os.stat_result,
) -> None:
    try:
        os.link(source, destination, follow_symlinks=False)
    except FileExistsError:
        # link(2): over NFS a retransmitted LINK can report EEXIST although
        # the first attempt succeeded. It did only if the destination is the
        # source inode and the source gained exactly one link.
        now = os.lstat(source)
        if not (
            _same_inode(destination, info)
            and now.st_nlink == info.st_nlink + 1
        ):
            raise
    try:
        os.unlink(source)
    except FileNotFoundError:
        # The source name is already gone and the destination holds the
        # inode: the move is complete.
        pass
    except OSError:
        # Undo our own link only; never remove a name we did not create.
        if _same_inode(destination, info):
            os.unlink(destination)
        raise


def _rename_directory_noreplace(source: str, destination: str) -> None:
    os.mkdir(destination, 0o700)
    try:
        os.rename(source, destination)
    except BaseException:
        try:
            os.rmdir(destination)
        except OSError:
            # Not empty or already gone: it is no longer our reservation.
            pass
        raise
