"""Durable coordination for generated import-pack staging."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import importlib
import logging
import math
import os
import secrets
import shutil
import sqlite3
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, cast

from download_identity import (
    DownloadIdentity,
    DownloadProtocol,
    coerce_download_client_id,
    download_identities_match,
    download_identity_key,
    download_identity_path_token,
    normalize_download_id,
    normalize_download_protocol,
    resolve_download_protocol,
)
from files import safe_join_under
from noreplace_fallback import UNSUPPORTED_ERRNOS, rename_noreplace_fallback
from shared import get_db

log = logging.getLogger(__name__)

PACK_RESERVATION_SECONDS = 15 * 60
_TERMINAL_QUEUE_STATUSES = frozenset(("imported", "failed", "skipped"))
_ACTIVE_PUBLICATION_STATES = (
    "staging",
    "prepared",
    "publishing",
    "published",
    "db_committed",
    "cleaning",
)
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
_PACK_OWNER_MARKER_NAME = ".mangarr-pack-owner"
_SYNCHRONOUS_NAMES = {
    0: "OFF",
    1: "NORMAL",
    2: "FULL",
    3: "EXTRA",
}

ReservationPurpose = Literal["queueing", "cleanup"]
FilesystemCheckpoint = Callable[[], None]


@dataclass(frozen=True, slots=True)
class PackCleanupRecovery:
    """Summary of one bounded stale-reservation/tombstone recovery pass."""

    reservations_recovered: int = 0
    tombstones_removed: int = 0
    tombstones_retained: int = 0


@dataclass(frozen=True, slots=True)
class _PackReservation:
    download_identity_key: str
    download_client_id: int | None
    protocol: DownloadProtocol | None
    normalized_download_id: str
    download_id: str
    purpose: ReservationPurpose
    owner_token: str
    artifact_owner_token: str
    queue_id: int | None
    publication_id: int | None
    pack_path: str
    tombstone_path: str | None


def _pack_identity(
    download_id: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
) -> DownloadIdentity:
    """Return a validated identity without assigning legacy ownership."""
    return DownloadIdentity(
        coerce_download_client_id(download_client_id),
        normalize_download_protocol(protocol),
        download_id,
    )


def normalize_pack_download_id(
    download_id: str,
    protocol: DownloadProtocol | None = None,
) -> str:
    """Compatibility wrapper around the shared protocol-aware normalizer."""
    return normalize_download_id(download_id, protocol)


def _lease_modifier(lease_seconds: float | None) -> str:
    duration = (
        PACK_RESERVATION_SECONDS if lease_seconds is None else lease_seconds
    )
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("lease_seconds must be positive")
    return f"+{duration:.6f} seconds"


def _pack_root() -> str:
    pipeline = importlib.import_module("import_pipeline")
    return str(getattr(pipeline, "PACK_STAGING_ROOT"))


def _canonical_pack_path(identity: DownloadIdentity) -> str:
    path_token = download_identity_path_token(identity)
    if not path_token:
        raise ValueError("download_id must be non-empty")
    owner = (
        f"client-{identity.download_client_id}"
        if identity.download_client_id is not None
        else "client-legacy"
    )
    protocol = identity.protocol or "unknown"
    return safe_join_under(
        _pack_root(),
        f"queue-{owner}-{protocol}-{path_token}",
    )


def pack_queue_creation_paths(
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
) -> tuple[str, str]:
    """Return canonical and owner-private paths for one queue reservation."""
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    canonical_path = _canonical_pack_path(identity)
    private_path = safe_join_under(
        _pack_root(),
        f"{os.path.basename(canonical_path)}.owner-{owner_token}",
    )
    return canonical_path, private_path


def _cleanup_tombstone_path(pack_path: str, owner_token: str) -> str:
    return safe_join_under(
        os.path.dirname(pack_path),
        f"{os.path.basename(pack_path)}.cleanup-{owner_token}",
    )


def _active_publication_exists(db: sqlite3.Connection, queue_id: int) -> bool:
    placeholders = ",".join("?" for _ in _ACTIVE_PUBLICATION_STATES)
    row = db.execute(
        f"""
        SELECT 1
        FROM import_publications
        WHERE queue_id=? AND state IN ({placeholders})
        LIMIT 1
        """,
        (queue_id, *_ACTIVE_PUBLICATION_STATES),
    ).fetchone()
    return row is not None


def _terminal_cleanup_eligible(
    db: sqlite3.Connection,
    *,
    queue_id: int,
    identity: DownloadIdentity,
) -> bool:
    row = db.execute(
        "SELECT status, lease_owner FROM import_queue WHERE id=?",
        (queue_id,),
    ).fetchone()
    if row is not None and (
        str(row["status"]) not in _TERMINAL_QUEUE_STATUSES
        or row["lease_owner"] is not None
    ):
        return False
    if _active_publication_exists(db, queue_id):
        return False

    placeholders = ",".join("?" for _ in _ACTIVE_PUBLICATION_STATES)
    siblings = db.execute(
        f"""
        SELECT sibling.download_id, sibling.download_client_id,
               sibling.download_protocol, sibling.torrent_url,
               sibling.series_id
        FROM import_queue AS sibling
        WHERE sibling.id != ?
          AND sibling.download_id IS NOT NULL
          AND (
              sibling.status IN ('pending','partial','importing')
              OR sibling.lease_owner IS NOT NULL
              OR EXISTS (
                  SELECT 1
                  FROM import_publications AS publication
                  WHERE publication.queue_id=sibling.id
                    AND publication.state IN ({placeholders})
              )
          )
        """,
        (queue_id, *_ACTIVE_PUBLICATION_STATES),
    ).fetchall()
    for sibling in siblings:
        sibling_owner = coerce_download_client_id(
            sibling["download_client_id"]
        )
        sibling_protocol = normalize_download_protocol(
            sibling["download_protocol"]
        ) or resolve_download_protocol(
            db,
            download_client_id=sibling_owner,
            series_id=int(sibling["series_id"]),
            download_id=str(sibling["download_id"] or ""),
            source_url=str(sibling["torrent_url"] or ""),
            allow_client_configuration=False,
        )
        if download_identities_match(
            identity,
            DownloadIdentity(
                sibling_owner,
                sibling_protocol,
                str(sibling["download_id"] or ""),
            ),
        ):
            return False
    return True


def _reservation_conflicts(
    db: sqlite3.Connection,
    identity: DownloadIdentity,
    *,
    purpose: ReservationPurpose | None = None,
    live_only: bool = False,
) -> bool:
    """Return whether a journal row overlaps this conservative identity."""
    conditions: list[str] = []
    params: list[object] = []
    if purpose is not None:
        conditions.append("purpose=?")
        params.append(purpose)
    if live_only:
        conditions.append("expires_at > CURRENT_TIMESTAMP")
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = db.execute(
        "SELECT download_client_id, protocol, download_id"
        " FROM import_pack_cleanup_reservations"
        + where,
        params,
    ).fetchall()
    return any(
        download_identities_match(
            identity,
            DownloadIdentity(
                coerce_download_client_id(row["download_client_id"]),
                normalize_download_protocol(row["protocol"]),
                str(row["download_id"] or ""),
            ),
        )
        for row in rows
    )


def reserve_pack_queue_creation(
    db: sqlite3.Connection,
    download_id: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    lease_seconds: float | None = None,
) -> str | None:
    """Reserve an ownership-qualified ID before creating private artifacts."""
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    identity_key = download_identity_key(identity)
    normalized = normalize_download_id(download_id, identity.protocol)
    if not identity_key:
        return None
    if db.in_transaction:
        raise RuntimeError("pack queue reservation requires a clean DB connection")

    owner_token = secrets.token_urlsafe(24)
    canonical_path, private_path = pack_queue_creation_paths(
        download_id,
        owner_token,
        download_client_id=identity.download_client_id,
        protocol=identity.protocol,
    )
    try:
        db.execute("BEGIN IMMEDIATE")
        if _reservation_conflicts(db, identity):
            db.commit()
            return None
        cur = db.execute(
            """
            INSERT INTO import_pack_cleanup_reservations(
                download_identity_key, download_client_id, protocol,
                normalized_download_id, download_id, purpose, owner_token,
                queue_id, publication_id, pack_path, tombstone_path, expires_at
            ) VALUES(
                ?, ?, ?, ?, ?, 'queueing', ?, NULL, NULL, ?, ?,
                datetime('now', ?)
            )
            ON CONFLICT(download_identity_key) DO NOTHING
            """,
            (
                identity_key,
                identity.download_client_id,
                identity.protocol,
                normalized,
                download_id,
                owner_token,
                canonical_path,
                private_path,
                _lease_modifier(lease_seconds),
            ),
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return owner_token if cur.rowcount == 1 else None


def refresh_pack_queue_creation(
    db: sqlite3.Connection,
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    lease_seconds: float | None = None,
    commit: bool,
) -> bool:
    """Renew a live build/attach reservation, optionally committing it."""
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    identity_key = download_identity_key(identity)
    if not identity_key or not owner_token:
        return False
    if not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
    cur = db.execute(
        """
        UPDATE import_pack_cleanup_reservations
        SET expires_at=datetime('now', ?), updated_at=CURRENT_TIMESTAMP
        WHERE download_identity_key=? AND owner_token=?
          AND expires_at > CURRENT_TIMESTAMP
          AND (
              purpose='queueing'
              OR (purpose='cleanup' AND queue_id IS NULL)
          )
        """,
        (
            _lease_modifier(lease_seconds),
            identity_key,
            owner_token,
        ),
    )
    if commit:
        db.commit()
    return cur.rowcount == 1


def begin_pack_queue_attachment(
    db: sqlite3.Connection,
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    lease_seconds: float | None = None,
) -> bool:
    """Owner-CAS a live build reservation into the filesystem attach phase."""
    identity_key = download_identity_key(
        _pack_identity(
            download_id,
            download_client_id=download_client_id,
            protocol=protocol,
        )
    )
    if not identity_key or not owner_token:
        return False
    if db.in_transaction:
        raise RuntimeError("pack attachment CAS requires a clean DB connection")
    synchronous_row = cast(
        tuple[int] | None,
        db.execute("PRAGMA synchronous").fetchone(),
    )
    if synchronous_row is None:
        raise RuntimeError("could not read SQLite synchronous mode")
    synchronous_level = int(synchronous_row[0])
    synchronous_name = _SYNCHRONOUS_NAMES.get(synchronous_level)
    if synchronous_name is None:
        raise RuntimeError(
            f"unsupported SQLite synchronous mode: {synchronous_level}"
        )
    try:
        # The filesystem rename is allowed only after this owner transition is
        # power-loss durable. The application's normal WAL setting is NORMAL,
        # so strengthen this one pre-rename transaction explicitly.
        _ = db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        cur = db.execute(
            """
            UPDATE import_pack_cleanup_reservations
            SET purpose='cleanup', queue_id=NULL,
                expires_at=datetime('now', ?), updated_at=CURRENT_TIMESTAMP
            WHERE download_identity_key=? AND owner_token=?
              AND purpose='queueing' AND expires_at > CURRENT_TIMESTAMP
            """,
            (
                _lease_modifier(lease_seconds),
                identity_key,
                owner_token,
            ),
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        _ = db.execute(f"PRAGMA synchronous={synchronous_name}")
    return cur.rowcount == 1


def release_pack_queue_creation(
    db: sqlite3.Connection,
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    commit: bool,
    attaching: bool = False,
) -> bool:
    """Release only the caller's build or completed attach reservation."""
    identity_key = download_identity_key(
        _pack_identity(
            download_id,
            download_client_id=download_client_id,
            protocol=protocol,
        )
    )
    if not identity_key or not owner_token:
        return False
    if not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
    cur = db.execute(
        """
        DELETE FROM import_pack_cleanup_reservations
        WHERE download_identity_key=? AND owner_token=?
          AND (
              purpose='queueing'
              OR (? AND purpose='cleanup' AND queue_id IS NULL)
          )
        """,
        (identity_key, owner_token, int(attaching)),
    )
    if commit:
        db.commit()
    return cur.rowcount == 1


def cleanup_reservation_blocks(
    db: sqlite3.Connection,
    download_id: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
) -> bool:
    """Return whether a live cleanup/attach reservation blocks new work."""
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    if not download_identity_key(identity):
        return False
    return _reservation_conflicts(
        db,
        identity,
        purpose="cleanup",
        live_only=True,
    )


def _rename_noreplace(source: str, destination: str) -> None:
    """Atomically rename a directory without replacing an existing path."""
    if os.name != "posix":
        raise OSError(
            errno.ENOSYS,
            "atomic no-replace rename requires Linux renameat2",
        )
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:
        raise OSError(
            errno.ENOSYS,
            "C library does not expose Linux renameat2",
        ) from exc
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        if error_number in UNSUPPORTED_ERRNOS:
            rename_noreplace_fallback(source, destination)
            return
        raise OSError(
            error_number,
            os.strerror(error_number),
            destination,
        )


def _fsync_directory(path: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_pack_root(pack_path: str) -> None:
    root = os.path.dirname(pack_path)
    try:
        _fsync_directory(root)
    except FileNotFoundError:
        return


def _fsync_tree(
    root: str,
    *,
    checkpoint: FilesystemCheckpoint | None = None,
) -> None:
    """Durably flush a generated tree before publishing its directory entry."""
    directories: list[str] = []
    for current_root, dirs, files in os.walk(root, topdown=True, followlinks=False):
        if checkpoint is not None:
            checkpoint()
        dirs.sort()
        files.sort()
        directories.append(current_root)
        for name in dirs:
            child = os.path.join(current_root, name)
            if stat.S_ISLNK(os.lstat(child).st_mode):
                raise OSError(f"generated pack contains symlink directory: {child}")
        for name in files:
            if checkpoint is not None:
                checkpoint()
            path = os.path.join(current_root, name)
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode):
                raise OSError(f"generated pack contains non-file artifact: {path}")
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(path, flags)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    for directory in reversed(directories):
        if checkpoint is not None:
            checkpoint()
        _fsync_directory(directory)


def _pack_owner_marker_payload(
    identity: DownloadIdentity,
    owner_token: str,
) -> bytes:
    if not owner_token:
        raise ValueError("owner_token must be non-empty")
    identity_key = download_identity_key(identity)
    if not identity_key:
        raise ValueError("download identity must be non-empty")
    digest = hashlib.sha256(
        f"{identity_key}\0{owner_token}".encode("utf-8")
    ).hexdigest()
    return f"mangarr-pack-owner-v1:{digest}\n".encode()


def _pack_tree_has_owner_marker(
    tree_path: str,
    identity: DownloadIdentity,
    owner_token: str,
) -> bool:
    marker_path = safe_join_under(tree_path, _PACK_OWNER_MARKER_NAME)
    expected = _pack_owner_marker_payload(identity, owner_token)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(marker_path, flags)
    except OSError:
        return False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size != len(expected):
            return False
        payload = bytearray()
        while len(payload) <= len(expected):
            chunk = os.read(descriptor, len(expected) + 1 - len(payload))
            if not chunk:
                break
            payload.extend(chunk)
        return bytes(payload) == expected
    finally:
        os.close(descriptor)


def _write_pack_owner_marker(
    private_path: str,
    identity: DownloadIdentity,
    owner_token: str,
) -> None:
    marker_path = safe_join_under(private_path, _PACK_OWNER_MARKER_NAME)
    payload = _pack_owner_marker_payload(identity, owner_token)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(marker_path, flags, 0o600)
    except FileExistsError:
        if _pack_tree_has_owner_marker(private_path, identity, owner_token):
            return
        raise OSError(
            errno.EEXIST,
            "generated pack owner marker is already occupied",
            marker_path,
        ) from None
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written == 0:
                raise OSError(errno.EIO, "short owner marker write", marker_path)
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durably_attach_pack_queue_directory(
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    checkpoint: FilesystemCheckpoint | None = None,
) -> str:
    """Flush and no-replace attach an owner-private tree to its canonical path."""
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    canonical_path, private_path = pack_queue_creation_paths(
        download_id,
        owner_token,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    info = os.lstat(private_path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OSError("owner-private pack path is not a real directory")
    _write_pack_owner_marker(private_path, identity, owner_token)
    _fsync_tree(private_path, checkpoint=checkpoint)
    if checkpoint is not None:
        checkpoint()
    _rename_noreplace(private_path, canonical_path)
    _fsync_pack_root(canonical_path)
    return canonical_path


def remove_pack_queue_private_artifacts(
    download_id: str,
    owner_token: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
) -> None:
    """Remove only one owner's unpublished artifacts and durably persist it."""
    canonical_path, private_path = pack_queue_creation_paths(
        download_id,
        owner_token,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    try:
        info = os.lstat(private_path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        log.error("Refusing unsafe owner-private pack path: %s", private_path)
        return
    shutil.rmtree(private_path)
    _fsync_pack_root(canonical_path)


def _acquire_cleanup_reservation(
    *,
    queue_id: int,
    download_id: str,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    publication_id: int | None,
    lease_seconds: float | None,
) -> tuple[str, str, str] | None:
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    identity_key = download_identity_key(identity)
    normalized = normalize_download_id(download_id, identity.protocol)
    if not identity_key:
        return None
    owner_token = secrets.token_urlsafe(24)
    pack_path = _canonical_pack_path(identity)
    tombstone_path = _cleanup_tombstone_path(pack_path, owner_token)
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        if not _terminal_cleanup_eligible(
            db,
            queue_id=queue_id,
            identity=identity,
        ) or _reservation_conflicts(
            db,
            identity,
        ):
            return None
        cur = db.execute(
            """
            INSERT INTO import_pack_cleanup_reservations(
                download_identity_key, download_client_id, protocol,
                normalized_download_id, download_id, purpose, owner_token,
                queue_id, publication_id, pack_path, tombstone_path, expires_at
            ) VALUES(
                ?, ?, ?, ?, ?, 'cleanup', ?, ?, ?, ?, ?, datetime('now', ?)
            )
            ON CONFLICT(download_identity_key) DO NOTHING
            """,
            (
                identity_key,
                identity.download_client_id,
                identity.protocol,
                normalized,
                download_id,
                owner_token,
                queue_id,
                publication_id,
                pack_path,
                tombstone_path,
                _lease_modifier(lease_seconds),
            ),
        )
        if cur.rowcount != 1:
            return None
    return owner_token, pack_path, tombstone_path


def _track_detached_tombstone(
    db: sqlite3.Connection,
    *,
    download_identity_key: str,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    normalized_download_id: str,
    download_id: str,
    queue_id: int,
    publication_id: int | None,
    pack_path: str,
    tombstone_path: str,
) -> None:
    db.execute(
        """
        INSERT INTO import_pack_cleanup_tombstones(
            tombstone_path, download_identity_key, download_client_id, protocol,
            normalized_download_id, download_id, queue_id, publication_id,
            pack_path
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tombstone_path) DO UPDATE SET
            publication_id=COALESCE(
                import_pack_cleanup_tombstones.publication_id,
                excluded.publication_id
            ),
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            tombstone_path,
            download_identity_key,
            download_client_id,
            protocol,
            normalized_download_id,
            download_id,
            queue_id,
            publication_id,
            pack_path,
        ),
    )


def _valid_tombstone_path(pack_path: str, tombstone_path: str) -> bool:
    pack_abs = os.path.abspath(pack_path)
    tombstone_abs = os.path.abspath(tombstone_path)
    return (
        os.path.dirname(tombstone_abs) == os.path.dirname(pack_abs)
        and os.path.basename(tombstone_abs).startswith(
            f"{os.path.basename(pack_abs)}.cleanup-"
        )
    )


def _mark_publication_cleanup_complete_in_db(
    db: sqlite3.Connection,
    publication_id: int | None,
) -> bool:
    if publication_id is None:
        return True
    cur = db.execute(
        """
        UPDATE import_publications
        SET pack_cleanup_state='complete',
            pack_cleanup_completed_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP
        WHERE id=? AND state IN ('finalized','deleted')
          AND pack_cleanup_state='pending'
        """,
        (publication_id,),
    )
    if cur.rowcount == 1:
        return True
    row = db.execute(
        "SELECT pack_cleanup_state FROM import_publications WHERE id=?",
        (publication_id,),
    ).fetchone()
    return row is not None and row["pack_cleanup_state"] == "complete"


def _mark_publication_cleanup_complete(publication_id: int | None) -> bool:
    with get_db() as db:
        return _mark_publication_cleanup_complete_in_db(db, publication_id)


def _artifact_owner_from_private_path(
    pack_path: str,
    private_path: str | None,
    fallback: str,
) -> str:
    if private_path is None:
        return fallback
    pack_abs = os.path.abspath(pack_path)
    private_abs = os.path.abspath(private_path)
    prefix = f"{os.path.basename(pack_abs)}.owner-"
    if (
        os.path.dirname(private_abs) == os.path.dirname(pack_abs)
        and os.path.basename(private_abs).startswith(prefix)
    ):
        artifact_owner = os.path.basename(private_abs)[len(prefix) :]
        if artifact_owner:
            return artifact_owner
    return fallback


def _reservation_from_row(row: Mapping[str, object]) -> _PackReservation:
    queue_id = row["queue_id"]
    publication_id = row["publication_id"]
    tombstone_path = row["tombstone_path"]
    owner_token = str(row["owner_token"])
    pack_path = str(row["pack_path"])
    private_path = (
        str(tombstone_path) if tombstone_path is not None else None
    )
    return _PackReservation(
        download_identity_key=str(row["download_identity_key"]),
        download_client_id=coerce_download_client_id(
            row["download_client_id"]
        ),
        protocol=normalize_download_protocol(row["protocol"]),
        normalized_download_id=str(row["normalized_download_id"]),
        download_id=str(row["download_id"]),
        purpose=(
            "queueing" if str(row["purpose"]) == "queueing" else "cleanup"
        ),
        owner_token=owner_token,
        artifact_owner_token=_artifact_owner_from_private_path(
            pack_path,
            private_path,
            owner_token,
        ),
        queue_id=_optional_int(queue_id),
        publication_id=_optional_int(publication_id),
        pack_path=pack_path,
        tombstone_path=private_path,
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, (int, str, bytes)):
        raise TypeError(f"expected SQLite integer value, got {type(value).__name__}")
    return int(value)


def _claim_expired_reservation(
    observed: _PackReservation,
    *,
    lease_seconds: float | None = None,
) -> _PackReservation | None:
    recovery_owner = secrets.token_urlsafe(24)
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        cur = db.execute(
            """
            UPDATE import_pack_cleanup_reservations
            SET owner_token=?, expires_at=datetime('now', ?),
                updated_at=CURRENT_TIMESTAMP
            WHERE download_identity_key=? AND owner_token=?
              AND expires_at <= CURRENT_TIMESTAMP
            """,
            (
                recovery_owner,
                _lease_modifier(lease_seconds),
                observed.download_identity_key,
                observed.owner_token,
            ),
        )
        if cur.rowcount != 1:
            return None
    return _PackReservation(
        download_identity_key=observed.download_identity_key,
        download_client_id=observed.download_client_id,
        protocol=observed.protocol,
        normalized_download_id=observed.normalized_download_id,
        download_id=observed.download_id,
        purpose=observed.purpose,
        owner_token=recovery_owner,
        artifact_owner_token=observed.artifact_owner_token,
        queue_id=observed.queue_id,
        publication_id=observed.publication_id,
        pack_path=observed.pack_path,
        tombstone_path=observed.tombstone_path,
    )


def _real_directory_or_missing(path: str) -> bool:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OSError(f"pack path is not a real directory: {path}")
    return True


def _detach_directory(
    source: str,
    destination: str,
) -> Literal["detached", "missing", "blocked"]:
    if not _real_directory_or_missing(source):
        return "missing"
    try:
        _rename_noreplace(source, destination)
    except FileNotFoundError:
        return "missing"
    except FileExistsError:
        return "blocked"
    return "detached"


def _record_owned_tombstone(
    reservation: _PackReservation,
    tombstone_path: str,
) -> bool:
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        owned = db.execute(
            """
            SELECT 1
            FROM import_pack_cleanup_reservations
            WHERE download_identity_key=? AND owner_token=?
              AND expires_at > CURRENT_TIMESTAMP
            """,
            (
                reservation.download_identity_key,
                reservation.owner_token,
            ),
        ).fetchone()
        if owned is None:
            return False
        if reservation.queue_id is not None and not _terminal_cleanup_eligible(
            db,
            queue_id=reservation.queue_id,
            identity=DownloadIdentity(
                reservation.download_client_id,
                reservation.protocol,
                reservation.download_id,
            ),
        ):
            return False
        _track_detached_tombstone(
            db,
            download_identity_key=reservation.download_identity_key,
            download_client_id=reservation.download_client_id,
            protocol=reservation.protocol,
            normalized_download_id=reservation.normalized_download_id,
            download_id=reservation.download_id,
            queue_id=reservation.queue_id or 0,
            publication_id=reservation.publication_id,
            pack_path=reservation.pack_path,
            tombstone_path=tombstone_path,
        )
        db.execute(
            """
            DELETE FROM import_pack_cleanup_reservations
            WHERE download_identity_key=? AND owner_token=?
            """,
            (
                reservation.download_identity_key,
                reservation.owner_token,
            ),
        )
    return True


def _complete_owned_missing_reservation(
    reservation: _PackReservation,
) -> bool:
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        owned = db.execute(
            """
            SELECT 1
            FROM import_pack_cleanup_reservations
            WHERE download_identity_key=? AND owner_token=?
              AND expires_at > CURRENT_TIMESTAMP
            """,
            (
                reservation.download_identity_key,
                reservation.owner_token,
            ),
        ).fetchone()
        if owned is None:
            return False
        if reservation.queue_id is not None and not _terminal_cleanup_eligible(
            db,
            queue_id=reservation.queue_id,
            identity=DownloadIdentity(
                reservation.download_client_id,
                reservation.protocol,
                reservation.download_id,
            ),
        ):
            return False
        db.execute(
            """
            DELETE FROM import_pack_cleanup_reservations
            WHERE download_identity_key=? AND owner_token=?
            """,
            (
                reservation.download_identity_key,
                reservation.owner_token,
            ),
        )
        return _mark_publication_cleanup_complete_in_db(
            db,
            reservation.publication_id,
        )


def _release_owned_reservation(reservation: _PackReservation) -> bool:
    """Release a fenced stale reservation without declaring cleanup complete."""
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        cur = db.execute(
            """
            DELETE FROM import_pack_cleanup_reservations
            WHERE download_identity_key=? AND owner_token=?
              AND expires_at > CURRENT_TIMESTAMP
            """,
            (
                reservation.download_identity_key,
                reservation.owner_token,
            ),
        )
        return cur.rowcount == 1


def _detach_terminal_cleanup(
    reservation: _PackReservation,
) -> Literal["tracked", "missing", "retry"]:
    tombstone_path = reservation.tombstone_path
    if tombstone_path is None or not _valid_tombstone_path(
        reservation.pack_path,
        tombstone_path,
    ):
        return "retry"
    try:
        if _real_directory_or_missing(tombstone_path):
            _fsync_pack_root(reservation.pack_path)
            return (
                "tracked"
                if _record_owned_tombstone(reservation, tombstone_path)
                else "retry"
            )
        detached = _detach_directory(reservation.pack_path, tombstone_path)
    except OSError as exc:
        log.warning(
            "Could not detach import-pack staging %s: %s",
            reservation.pack_path,
            exc,
        )
        return "retry"
    if detached == "blocked":
        return "retry"
    if detached == "missing":
        return (
            "missing"
            if _complete_owned_missing_reservation(reservation)
            else "retry"
        )
    _fsync_pack_root(reservation.pack_path)
    return (
        "tracked"
        if _record_owned_tombstone(reservation, tombstone_path)
        else "retry"
    )


def _recover_abandoned_queue_creation(
    reservation: _PackReservation,
) -> bool:
    private_path = reservation.tombstone_path
    source: str | None = None
    try:
        if private_path and _real_directory_or_missing(private_path):
            source = private_path
        elif _real_directory_or_missing(reservation.pack_path):
            identity = DownloadIdentity(
                reservation.download_client_id,
                reservation.protocol,
                reservation.download_id,
            )
            expected_path = _canonical_pack_path(identity)
            if (
                os.path.abspath(reservation.pack_path) != expected_path
                or not _pack_tree_has_owner_marker(
                    reservation.pack_path,
                    identity,
                    reservation.artifact_owner_token,
                )
            ):
                # A no-replace attach may have encountered pre-existing data,
                # or this can be a successor's canonical tree. Without the
                # private owner proof, retain both the tree and reservation.
                log.warning(
                    "Refusing unowned canonical import-pack recovery for %s",
                    reservation.pack_path,
                )
                return False
            # The private tree reached its durable canonical name. This can be
            # observed even with purpose='queueing' after an older NORMAL CAS
            # rolls back during power loss.
            source = reservation.pack_path
    except OSError as exc:
        log.warning("Could not inspect abandoned pack reservation: %s", exc)
        return False

    if source is None:
        return _complete_owned_missing_reservation(reservation)

    tombstone_path = _cleanup_tombstone_path(
        reservation.pack_path,
        reservation.owner_token,
    )
    try:
        detached = _detach_directory(source, tombstone_path)
        if detached == "missing" and source != reservation.pack_path:
            # A stale owner may have raced its private-to-canonical attach
            # after recovery fenced its DB token. Detach that stale canonical
            # before allowing a successor reservation.
            detached = _detach_directory(
                reservation.pack_path,
                tombstone_path,
            )
    except OSError as exc:
        log.warning("Could not detach abandoned pack artifacts: %s", exc)
        return False
    if detached == "blocked":
        try:
            tombstone_exists = _real_directory_or_missing(tombstone_path)
        except OSError:
            return False
        if not tombstone_exists:
            return False
    elif detached == "missing":
        return _complete_owned_missing_reservation(reservation)

    _fsync_pack_root(reservation.pack_path)
    return _record_owned_tombstone(reservation, tombstone_path)


def _remove_tracked_tombstone(row: Mapping[str, object]) -> bool:
    tombstone_path = str(row["tombstone_path"])
    pack_path = str(row["pack_path"])
    publication_value = row.get("publication_id")
    publication_id = _optional_int(publication_value)
    if not _valid_tombstone_path(pack_path, tombstone_path):
        log.error("Refusing unsafe import-pack tombstone path: %s", tombstone_path)
        return False
    try:
        info = os.lstat(tombstone_path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        log.warning(
            "Could not inspect import-pack tombstone %s: %s",
            tombstone_path,
            exc,
        )
        return False
    else:
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            log.error(
                "Refusing non-directory import-pack tombstone: %s",
                tombstone_path,
            )
            return False
        try:
            shutil.rmtree(tombstone_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning(
                "Could not remove import-pack tombstone %s: %s",
                tombstone_path,
                exc,
            )
            return False

    # Persist the directory-entry removal before deleting its durable journal.
    _fsync_pack_root(pack_path)
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "DELETE FROM import_pack_cleanup_tombstones WHERE tombstone_path=?",
            (tombstone_path,),
        )
        _mark_publication_cleanup_complete_in_db(db, publication_id)
    return True


def recover_pack_cleanup_state(
    *,
    max_rows: int = 100,
    publication_id: int | None = None,
) -> PackCleanupRecovery:
    """Replay expired reservations and tracked tombstones without long writers."""
    if max_rows <= 0:
        return PackCleanupRecovery()

    with get_db() as db:
        if publication_id is None:
            reservation_params: tuple[object, ...] = (max_rows,)
            reservation_filter = ""
        else:
            reservation_params = (publication_id, max_rows)
            reservation_filter = " AND publication_id=?"
        observed_rows = db.execute(
            """
            SELECT download_identity_key, download_client_id, protocol,
                   normalized_download_id, download_id, purpose, owner_token,
                   queue_id, publication_id, pack_path, tombstone_path
            FROM import_pack_cleanup_reservations
            WHERE expires_at <= CURRENT_TIMESTAMP
            """
            + reservation_filter
            + " ORDER BY updated_at, normalized_download_id LIMIT ?",
            reservation_params,
        ).fetchall()

    recovered = 0
    for observed_row in observed_rows:
        reservation = _claim_expired_reservation(
            _reservation_from_row(observed_row)
        )
        if reservation is None:
            continue
        if reservation.queue_id is None:
            handled = _recover_abandoned_queue_creation(reservation)
        else:
            with get_db() as db:
                eligible = _terminal_cleanup_eligible(
                    db,
                    queue_id=reservation.queue_id,
                    identity=DownloadIdentity(
                        reservation.download_client_id,
                        reservation.protocol,
                        reservation.download_id,
                    ),
                )
            handled = (
                _detach_terminal_cleanup(reservation) != "retry"
                if eligible
                else _release_owned_reservation(reservation)
            )
        recovered += int(handled)

    with get_db() as db:
        if publication_id is None:
            tombstone_params: tuple[object, ...] = (max_rows,)
            tombstone_filter = ""
        else:
            tombstone_params = (publication_id, max_rows)
            tombstone_filter = " WHERE publication_id=?"
        tombstones = [
            dict(row)
            for row in db.execute(
                """
                SELECT tombstone_path, pack_path, publication_id
                FROM import_pack_cleanup_tombstones
                """
                + tombstone_filter
                + " ORDER BY created_at, tombstone_path LIMIT ?",
                tombstone_params,
            ).fetchall()
        ]

    removed = 0
    retained = 0
    for tombstone in tombstones:
        if _remove_tracked_tombstone(tombstone):
            removed += 1
        else:
            retained += 1
    return PackCleanupRecovery(recovered, removed, retained)


def cleanup_terminal_pack_staging(
    queue_id: int,
    download_id: str,
    *,
    download_client_id: int | None,
    protocol: DownloadProtocol | None,
    publication_id: int | None = None,
    lease_seconds: float | None = None,
) -> bool:
    """Durably detach and remove one terminal queue's canonical pack tree."""
    recovery = recover_pack_cleanup_state(publication_id=publication_id)
    if recovery.tombstones_retained:
        return False

    acquired = _acquire_cleanup_reservation(
        queue_id=queue_id,
        download_id=download_id,
        download_client_id=download_client_id,
        protocol=protocol,
        publication_id=publication_id,
        lease_seconds=lease_seconds,
    )
    if acquired is None:
        return False
    owner_token, pack_path, tombstone_path = acquired
    identity = _pack_identity(
        download_id,
        download_client_id=download_client_id,
        protocol=protocol,
    )
    reservation = _PackReservation(
        download_identity_key=download_identity_key(identity),
        download_client_id=identity.download_client_id,
        protocol=identity.protocol,
        normalized_download_id=normalize_download_id(
            download_id,
            identity.protocol,
        ),
        download_id=download_id,
        purpose="cleanup",
        owner_token=owner_token,
        artifact_owner_token=owner_token,
        queue_id=queue_id,
        publication_id=publication_id,
        pack_path=pack_path,
        tombstone_path=tombstone_path,
    )

    detached = _detach_terminal_cleanup(reservation)
    if detached == "retry":
        return False
    if detached == "missing":
        return True

    row: dict[str, object] = {
        "tombstone_path": tombstone_path,
        "pack_path": pack_path,
        "publication_id": publication_id,
    }
    return _remove_tracked_tombstone(row)
