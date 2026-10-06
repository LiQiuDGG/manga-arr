"""Replayable, DB-first deletion of imported volume files.

The active journal is the fence between import admission and destructive
filesystem work. Database helpers in this module never perform filesystem
I/O, and filesystem helpers never retain a database connection.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import hashlib
import os
import sqlite3
import stat
from dataclasses import dataclass
from typing import Literal, cast

from events import add_history, log_event
from noreplace_fallback import UNSUPPORTED_ERRNOS, rename_noreplace_fallback
from parsing import extract_volume_num
from shared import build_volume_label, get_db
from volumes import _cascade_chapters

_AT_FDCWD = -100
_RENAME_NOREPLACE = 1
_HASH_CHUNK_SIZE = 1024 * 1024
ReservationStatus = Literal[
    "reserved",
    "existing",
    "not_found",
    "import_in_progress",
    "changed",
    "unsafe",
]
ReplayOutcome = Literal["completed", "blocked", "terminal"]
DeleteStatus = Literal[
    "complete",
    "pending",
    "not_found",
    "import_in_progress",
    "changed",
    "unsafe",
]


@dataclass(frozen=True, slots=True)
class FileFingerprint:
    """Stable identity of the regular file selected before reservation."""

    dev: int
    inode: int
    size: int
    mtime_ns: int
    sha256: str


@dataclass(frozen=True, slots=True)
class VolumeSnapshot:
    """The volume values compared again while holding the reservation writer."""

    volume_id: int
    series_id: int
    series_title: str
    volume_num: float | None
    status: str | None
    import_path: str
    download_id: str | None
    download_client_id: int | None
    grabbed_at: object | None
    source_url: str | None
    torrent_name: str | None
    indexer: str | None
    protocol: str | None
    client: str | None
    release_group: str | None
    size_bytes: int | None
    quality: str | None
    imported_at: object | None


@dataclass(frozen=True, slots=True)
class DeletionInspection:
    """Filesystem identity and DB snapshot collected before any writer."""

    snapshot: VolumeSnapshot
    target_path: str
    parent_path: str
    claim_path: str
    target_present: bool
    fingerprint: FileFingerprint | None


@dataclass(frozen=True, slots=True)
class DeletionReservation:
    """Outcome of attempting to establish the DB-first deletion fence."""

    status: ReservationStatus
    journal_id: int | None = None
    diagnostic: str = ""


@dataclass(frozen=True, slots=True)
class VolumeFileDeletionResult:
    """Route-facing outcome of reservation followed by one replay attempt."""

    status: DeleteStatus
    journal_id: int | None = None
    diagnostic: str = ""


@dataclass(slots=True)
class DeletionReplaySummary:
    """Bounded replay counters used by startup and the runtime task."""

    examined: int = 0
    completed: int = 0
    blocked: int = 0
    last_id: int = 0


@dataclass(frozen=True, slots=True)
class _DeletionJournal:
    journal_id: int
    volume_id: int
    series_id: int
    state: str
    target_path: str
    parent_path: str
    claim_path: str
    target_present: bool
    fingerprint: FileFingerprint | None
    series_title: str
    volume_num: float | None
    source_title: str
    original_import_path: str
    diagnostic: str


_SNAPSHOT_COLUMNS = (
    "status",
    "import_path",
    "download_id",
    "download_client_id",
    "grabbed_at",
    "source_url",
    "torrent_name",
    "indexer",
    "protocol",
    "client",
    "release_group",
    "size_bytes",
    "quality",
    "imported_at",
    "volume_num",
)

_replay_lock: asyncio.Lock | None = None


class UnsafeDeletionTarget(RuntimeError):
    """Raised when a stored path cannot be safely treated as a regular file."""


def _sha256_fd(descriptor: int) -> str:
    """Hash an already-open descriptor without reopening its path."""
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, _HASH_CHUNK_SIZE):
        digest.update(chunk)
    return digest.hexdigest()


def _regular_fingerprint(path: str) -> FileFingerprint:
    """Hash and stat one non-symlink regular file, detecting concurrent change."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise UnsafeDeletionTarget(f"delete target is not a regular file: {path}")
        digest = _sha256_fd(descriptor)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)

    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if before_identity != after_identity:
        raise UnsafeDeletionTarget(f"delete target changed while hashing: {path}")

    current = os.lstat(path)
    current_identity = (
        current.st_dev,
        current.st_ino,
        current.st_size,
        current.st_mtime_ns,
    )
    if stat.S_ISLNK(current.st_mode) or current_identity != after_identity:
        raise UnsafeDeletionTarget(f"delete target changed while hashing: {path}")
    return FileFingerprint(
        dev=int(after.st_dev),
        inode=int(after.st_ino),
        size=int(after.st_size),
        mtime_ns=int(after.st_mtime_ns),
        sha256=digest,
    )


def _claim_path(volume_id: int, target_path: str) -> str:
    """Return a deterministic same-directory tombstone for one target."""
    target_abs = os.path.abspath(target_path)
    path_digest = hashlib.sha256(os.fsencode(target_abs)).hexdigest()[:20]
    return os.path.join(
        os.path.dirname(target_abs),
        f".mangarr-volume-delete-{volume_id}-{path_digest}.tombstone",
    )


def _resolve_delete_target(
    import_path: str,
    volume_num: float | None,
) -> tuple[str, str]:
    """Resolve the one file the volume action is allowed to remove."""
    if not import_path:
        return "", ""

    import_abs = os.path.abspath(import_path)
    try:
        target_stat = os.lstat(import_abs)
    except FileNotFoundError:
        return import_abs, os.path.dirname(import_abs)

    if stat.S_ISLNK(target_stat.st_mode):
        raise UnsafeDeletionTarget(f"delete target is a symlink: {import_abs}")
    if stat.S_ISREG(target_stat.st_mode):
        return import_abs, os.path.dirname(import_abs)
    if not stat.S_ISDIR(target_stat.st_mode):
        raise UnsafeDeletionTarget(
            f"delete target is neither a file nor directory: {import_abs}"
        )

    if volume_num is not None:
        for filename in sorted(os.listdir(import_abs)):
            file_volume = extract_volume_num(filename)
            if (
                file_volume is not None
                and abs(file_volume - volume_num) < 0.01
            ):
                return os.path.join(import_abs, filename), import_abs
    return "", import_abs


def _snapshot_from_values(
    values: dict[str, object],
    series_title: str,
) -> VolumeSnapshot:
    stored_volume_num = cast(float | None, values["volume_num"])
    return VolumeSnapshot(
        volume_id=int(cast(int, values["id"])),
        series_id=int(cast(int, values["series_id"])),
        series_title=series_title,
        volume_num=(
            float(stored_volume_num) if stored_volume_num is not None else None
        ),
        status=cast(str | None, values["status"]),
        import_path=str(values["import_path"] or ""),
        download_id=cast(str | None, values["download_id"]),
        download_client_id=cast(int | None, values["download_client_id"]),
        grabbed_at=values["grabbed_at"],
        source_url=cast(str | None, values["source_url"]),
        torrent_name=cast(str | None, values["torrent_name"]),
        indexer=cast(str | None, values["indexer"]),
        protocol=cast(str | None, values["protocol"]),
        client=cast(str | None, values["client"]),
        release_group=cast(str | None, values["release_group"]),
        size_bytes=cast(int | None, values["size_bytes"]),
        quality=cast(str | None, values["quality"]),
        imported_at=values["imported_at"],
    )


def _snapshot_values(snapshot: VolumeSnapshot) -> tuple[object | None, ...]:
    return (
        snapshot.status,
        snapshot.import_path,
        snapshot.download_id,
        snapshot.download_client_id,
        snapshot.grabbed_at,
        snapshot.source_url,
        snapshot.torrent_name,
        snapshot.indexer,
        snapshot.protocol,
        snapshot.client,
        snapshot.release_group,
        snapshot.size_bytes,
        snapshot.quality,
        snapshot.imported_at,
        snapshot.volume_num,
    )


def _row_matches_snapshot(row: sqlite3.Row, snapshot: VolumeSnapshot) -> bool:
    current = (
        row["status"],
        str(row["import_path"] or ""),
        *(row[column] for column in _SNAPSHOT_COLUMNS[2:]),
    )
    return current == _snapshot_values(snapshot)


def _active_journal_id(series_id: int, volume_id: int) -> int | None:
    """Read an existing fence without starting a writer transaction."""
    with get_db() as db:
        row = db.execute(
            """
            SELECT id
            FROM volume_file_deletions
            WHERE series_id=? AND volume_id=? AND state='active'
            ORDER BY id
            LIMIT 1
            """,
            (series_id, volume_id),
        ).fetchone()
    return int(row["id"]) if row is not None else None


def inspect_volume_file_deletion(
    series_id: int,
    volume_id: int,
) -> DeletionInspection | None:
    """Read the volume, then resolve and hash its target without a DB writer."""
    with get_db() as db:
        row = db.execute(
            "SELECT * FROM volumes WHERE id=? AND series_id=?",
            (volume_id, series_id),
        ).fetchone()
        series = db.execute(
            "SELECT title FROM series WHERE id=?",
            (series_id,),
        ).fetchone()
        volume = dict(row) if row is not None else None
        series_title = str(series["title"] or "") if series is not None else ""
    if volume is None:
        return None

    snapshot = _snapshot_from_values(volume, series_title)
    target_path, parent_path = _resolve_delete_target(
        snapshot.import_path,
        snapshot.volume_num,
    )
    fingerprint: FileFingerprint | None = None
    target_present = False
    if target_path:
        try:
            fingerprint = _regular_fingerprint(target_path)
        except FileNotFoundError:
            fingerprint = None
        else:
            target_present = True
    return DeletionInspection(
        snapshot=snapshot,
        target_path=target_path,
        parent_path=parent_path,
        claim_path=(
            _claim_path(snapshot.volume_id, target_path) if target_path else ""
        ),
        target_present=target_present,
        fingerprint=fingerprint,
    )


def _matching_prejournal_import_exists(
    db: sqlite3.Connection,
    snapshot: VolumeSnapshot,
) -> bool:
    """Return whether an importing/owned queue may publish this volume."""
    row = db.execute(
        """
        SELECT 1
        FROM import_queue AS queue
        WHERE queue.series_id=?
          AND (queue.status='importing' OR queue.lease_owner IS NOT NULL)
          AND (
              ? IS NULL
              OR queue.volume_num IS NULL
              OR abs(queue.volume_num - ?) < 0.01
              OR (
                  ? IS NOT NULL
                  AND queue.download_id IS NOT NULL
                  AND lower(queue.download_id)=lower(?)
              )
              OR EXISTS (
                  SELECT 1
                  FROM import_queue_files AS file
                  WHERE file.queue_id=queue.id
                    AND (
                        (
                            file.proposed_volume IS NOT NULL
                            AND abs(file.proposed_volume - ?) < 0.01
                        )
                        OR file.proposed_pack_type='complete'
                        OR (
                            file.proposed_volume_range_start IS NOT NULL
                            AND file.proposed_volume_range_end IS NOT NULL
                            AND file.proposed_volume_range_start <= ?
                            AND ? <= file.proposed_volume_range_end
                        )
                    )
              )
          )
        LIMIT 1
        """,
        (
            snapshot.series_id,
            snapshot.volume_num,
            snapshot.volume_num,
            snapshot.download_id,
            snapshot.download_id,
            snapshot.volume_num,
            snapshot.volume_num,
            snapshot.volume_num,
        ),
    ).fetchone()
    return row is not None


def _active_publication_targets_inspection(
    db: sqlite3.Connection,
    inspection: DeletionInspection,
) -> bool:
    """Return whether active journal-owned publication work can touch the target."""
    snapshot = inspection.snapshot
    path_candidates = {
        os.path.abspath(path)
        for path in (snapshot.import_path, inspection.target_path)
        if path
    }
    rows = db.execute(
        """
        SELECT publication.queue_volume_num,
               publication.queue_download_id,
               file.proposed_vol,
               file.vol_range_start,
               file.vol_range_end,
               file.pack_type,
               file.dst_path,
               file.final_path
        FROM import_publications AS publication
        LEFT JOIN import_publication_files AS file
          ON file.publication_id=publication.id
         AND file.plan_status='ready'
        WHERE publication.series_id=?
          AND publication.state IN (
              'staging','prepared','publishing',
              'published','db_committed','cleaning'
          )
        """,
        (snapshot.series_id,),
    ).fetchall()
    for row in rows:
        queue_volume_num = cast(float | None, row["queue_volume_num"])
        queue_download_id = cast(str | None, row["queue_download_id"])
        if snapshot.volume_num is None or queue_volume_num is None:
            return True
        if abs(float(queue_volume_num) - snapshot.volume_num) < 0.01:
            return True
        if (
            snapshot.download_id
            and queue_download_id
            and snapshot.download_id.casefold() == queue_download_id.casefold()
        ):
            return True
        proposed_vol = cast(float | None, row["proposed_vol"])
        if (
            proposed_vol is not None
            and abs(float(proposed_vol) - snapshot.volume_num) < 0.01
        ):
            return True
        if row["pack_type"] == "complete":
            return True
        range_start = cast(float | None, row["vol_range_start"])
        range_end = cast(float | None, row["vol_range_end"])
        if (
            range_start is not None
            and range_end is not None
            and float(range_start) <= snapshot.volume_num <= float(range_end)
        ):
            return True
        for publication_path in (row["dst_path"], row["final_path"]):
            if (
                publication_path
                and os.path.abspath(str(publication_path)) in path_candidates
            ):
                return True
    return False


def reserve_volume_file_deletion(
    series_id: int,
    volume_id: int,
) -> DeletionReservation:
    """Fingerprint first, then atomically fence imports and reset DB state."""
    existing = _active_journal_id(series_id, volume_id)
    if existing is not None:
        return DeletionReservation("existing", existing)

    try:
        inspection = inspect_volume_file_deletion(series_id, volume_id)
    except (OSError, UnsafeDeletionTarget) as exc:
        return DeletionReservation("unsafe", diagnostic=str(exc))
    if inspection is None:
        return DeletionReservation("not_found")

    snapshot = inspection.snapshot
    with get_db() as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        existing_row = db.execute(
            """
            SELECT id
            FROM volume_file_deletions
            WHERE series_id=? AND volume_id=? AND state='active'
            ORDER BY id
            LIMIT 1
            """,
            (series_id, volume_id),
        ).fetchone()
        if existing_row is not None:
            return DeletionReservation("existing", int(existing_row["id"]))

        current = db.execute(
            "SELECT * FROM volumes WHERE id=? AND series_id=?",
            (volume_id, series_id),
        ).fetchone()
        if current is None:
            return DeletionReservation("not_found")
        if not _row_matches_snapshot(current, snapshot):
            return DeletionReservation(
                "changed",
                diagnostic="volume changed while deletion was being prepared",
            )
        if _matching_prejournal_import_exists(db, snapshot):
            return DeletionReservation("import_in_progress")
        if _active_publication_targets_inspection(db, inspection):
            return DeletionReservation("import_in_progress")

        fingerprint = inspection.fingerprint
        cursor = db.execute(
            """
            INSERT INTO volume_file_deletions(
                volume_id, series_id, state, target_path, parent_path,
                claim_path, target_present, target_dev, target_inode,
                target_size, target_mtime_ns, target_sha256, series_title,
                volume_num, source_title, original_import_path
            ) VALUES(
                ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                volume_id,
                series_id,
                inspection.target_path,
                inspection.parent_path,
                inspection.claim_path,
                int(inspection.target_present),
                fingerprint.dev if fingerprint else None,
                fingerprint.inode if fingerprint else None,
                fingerprint.size if fingerprint else None,
                fingerprint.mtime_ns if fingerprint else None,
                fingerprint.sha256 if fingerprint else None,
                snapshot.series_title,
                snapshot.volume_num,
                snapshot.torrent_name or "",
                snapshot.import_path,
            ),
        )
        journal_id = int(cast(int, cursor.lastrowid))

        update = db.execute(
            """
            UPDATE volumes
            SET status='wanted', grabbed_at=NULL, imported_at=NULL,
                import_path=NULL, source_url=NULL, download_id=NULL,
                download_client_id=NULL, torrent_name=NULL, indexer=NULL,
                protocol=NULL, client=NULL, release_group=NULL,
                size_bytes=NULL, quality=NULL
            WHERE id=? AND series_id=?
              AND status IS ?
              AND COALESCE(import_path, '')=?
              AND download_id IS ?
              AND download_client_id IS ?
              AND grabbed_at IS ?
              AND source_url IS ?
              AND torrent_name IS ?
              AND indexer IS ?
              AND protocol IS ?
              AND client IS ?
              AND release_group IS ?
              AND size_bytes IS ?
              AND quality IS ?
              AND imported_at IS ?
              AND volume_num IS ?
            """,
            (volume_id, series_id, *_snapshot_values(snapshot)),
        )
        if update.rowcount != 1:
            raise RuntimeError("volume deletion lost its snapshot CAS")
        _cascade_chapters(
            db,
            series_id,
            [volume_id],
            "wanted",
            grabbed_at=None,
            torrent_name=None,
            torrent_url=None,
            indexer=None,
            protocol=None,
            client=None,
            download_id=None,
            download_client_id=None,
            release_group=None,
            import_path=None,
        )
    return DeletionReservation("reserved", journal_id)


def _fingerprint_from_journal(row: dict[str, object]) -> FileFingerprint | None:
    if not bool(row["target_present"]):
        return None
    values = (
        row["target_dev"],
        row["target_inode"],
        row["target_size"],
        row["target_mtime_ns"],
        row["target_sha256"],
    )
    if any(value is None for value in values):
        raise RuntimeError("active deletion journal has an incomplete fingerprint")
    return FileFingerprint(
        dev=int(cast(int, values[0])),
        inode=int(cast(int, values[1])),
        size=int(cast(int, values[2])),
        mtime_ns=int(cast(int, values[3])),
        sha256=str(values[4]),
    )


def _load_journal(journal_id: int) -> _DeletionJournal | None:
    with get_db() as db:
        row = db.execute(
            "SELECT * FROM volume_file_deletions WHERE id=?",
            (journal_id,),
        ).fetchone()
        values = dict(row) if row is not None else None
    if values is None:
        return None
    return _DeletionJournal(
        journal_id=int(values["id"]),
        volume_id=int(values["volume_id"]),
        series_id=int(values["series_id"]),
        state=str(values["state"]),
        target_path=str(values["target_path"] or ""),
        parent_path=str(values["parent_path"] or ""),
        claim_path=str(values["claim_path"] or ""),
        target_present=bool(values["target_present"]),
        fingerprint=_fingerprint_from_journal(values),
        series_title=str(values["series_title"] or ""),
        volume_num=cast(float | None, values["volume_num"]),
        source_title=str(values["source_title"] or ""),
        original_import_path=str(values["original_import_path"] or ""),
        diagnostic=str(values["diagnostic"] or ""),
    )


def _same_fingerprint(actual: FileFingerprint, expected: FileFingerprint) -> bool:
    return actual == expected


def _rename_noreplace(source: str, destination: str) -> None:
    """Atomically rename without replacing via Linux ``renameat2(2)``."""
    if os.name != "posix":
        raise OSError(
            errno.ENOTSUP,
            "atomic no-replace rename requires a POSIX Linux host",
            destination,
        )
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:
        raise OSError(
            errno.ENOTSUP,
            "C library does not expose Linux renameat2",
            destination,
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
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in UNSUPPORTED_ERRNOS:
        rename_noreplace_fallback(source, destination)
        return
    raise OSError(error_number, os.strerror(error_number), destination)


def _fsync_directory(path: str) -> None:
    descriptor = os.open(
        os.path.abspath(path),
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory_when_possible(path: str) -> None:
    if not path:
        return
    try:
        _fsync_directory(path)
    except FileNotFoundError:
        return


def _unlink_claim(path: str) -> None:
    os.unlink(path)


def _path_exists(path: str) -> bool:
    if not path:
        return False
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


def _restore_claim_without_clobber(journal: _DeletionJournal) -> str:
    """Restore a claimed target only when its published name is still absent."""
    if not journal.claim_path or not journal.target_path:
        return "claim restoration is unavailable"
    if _path_exists(journal.target_path):
        return "target path is occupied; claim was not restored"
    try:
        _rename_noreplace(journal.claim_path, journal.target_path)
        _fsync_directory(journal.parent_path)
    except OSError as exc:
        return f"claim restoration failed: {exc}"
    return "claim restored without replacing another path"


def _record_blocked(journal: _DeletionJournal, diagnostic: str) -> None:
    """Persist a replay diagnostic while retaining the active import fence."""
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        update = db.execute(
            """
            UPDATE volume_file_deletions
            SET diagnostic=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state='active' AND diagnostic IS NOT ?
            """,
            (diagnostic, journal.journal_id, diagnostic),
        )
        if update.rowcount == 1:
            log_event(
                "error",
                f"File delete failed: {diagnostic}",
                journal.series_id,
                db=db,
            )


def _complete_journal(journal: _DeletionJournal, *, deleted: bool) -> bool:
    """CAS active -> completed and emit the historical audit records once."""
    volume_label = build_volume_label(journal.volume_num, None, None)
    message = (
        f"Deleted file for {volume_label}"
        if deleted
        else f"Reset {volume_label} to wanted (file not found)"
    )
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        update = db.execute(
            """
            UPDATE volume_file_deletions
            SET state='completed', diagnostic='', completed_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state='active'
            """,
            (journal.journal_id,),
        )
        if update.rowcount != 1:
            return False
        add_history(
            db,
            "file_deleted",
            journal.series_id,
            journal.series_title,
            volume_label,
            source_title=journal.source_title,
            data={
                "deleted": deleted,
                "path": journal.original_import_path,
            },
        )
        log_event("delete", message, journal.series_id, db=db)
    return True


def _validate_journal_paths(journal: _DeletionJournal) -> None:
    if not journal.target_path:
        if journal.claim_path:
            raise RuntimeError("pathless deletion journal unexpectedly has a claim")
        return
    target_abs = os.path.abspath(journal.target_path)
    parent_abs = os.path.abspath(journal.parent_path)
    if os.path.dirname(target_abs) != parent_abs:
        raise RuntimeError("deletion target is not directly below its recorded parent")
    expected_claim = _claim_path(journal.volume_id, target_abs)
    if os.path.abspath(journal.claim_path) != expected_claim:
        raise RuntimeError("deletion claim path is not deterministic")


def replay_volume_file_deletion(journal_id: int) -> ReplayOutcome:
    """Settle one active deletion journal without holding a SQLite writer."""
    journal = _load_journal(journal_id)
    if journal is None or journal.state != "active":
        return "terminal"

    try:
        _validate_journal_paths(journal)
        target_exists = _path_exists(journal.target_path)
        claim_exists = _path_exists(journal.claim_path)

        if not journal.target_present:
            if target_exists or claim_exists:
                raise UnsafeDeletionTarget(
                    "delete target appeared after it was recorded missing"
                )
            _fsync_directory_when_possible(journal.parent_path)
            return (
                "completed"
                if _complete_journal(journal, deleted=False)
                else "terminal"
            )

        expected = journal.fingerprint
        if expected is None:
            raise RuntimeError("present deletion target has no fingerprint")
        if target_exists and claim_exists:
            raise UnsafeDeletionTarget(
                "both delete target and tombstone exist; refusing to clobber either"
            )

        if target_exists:
            actual_target = _regular_fingerprint(journal.target_path)
            if not _same_fingerprint(actual_target, expected):
                raise UnsafeDeletionTarget(
                    "delete target identity does not match its recorded fingerprint"
                )
            _rename_noreplace(journal.target_path, journal.claim_path)
            _fsync_directory(journal.parent_path)
            claim_exists = True
        elif not claim_exists:
            _fsync_directory_when_possible(journal.parent_path)
            return (
                "completed"
                if _complete_journal(journal, deleted=True)
                else "terminal"
            )

        actual_claim = _regular_fingerprint(journal.claim_path)
        if not _same_fingerprint(actual_claim, expected):
            restoration = _restore_claim_without_clobber(journal)
            raise UnsafeDeletionTarget(
                "delete tombstone identity does not match its recorded "
                f"fingerprint; {restoration}"
            )
        if _path_exists(journal.target_path):
            raise UnsafeDeletionTarget(
                "delete target was recreated after tombstone claim; refusing "
                "to remove the claim or clobber the replacement"
            )
        _unlink_claim(journal.claim_path)
        _fsync_directory(journal.parent_path)
        return (
            "completed"
            if _complete_journal(journal, deleted=True)
            else "terminal"
        )
    except (OSError, RuntimeError) as exc:
        _record_blocked(journal, str(exc))
        current = _load_journal(journal.journal_id)
        return (
            "terminal"
            if current is None or current.state != "active"
            else "blocked"
        )


def delete_volume_file(
    series_id: int,
    volume_id: int,
) -> VolumeFileDeletionResult:
    """Reserve a volume deletion and make one synchronous replay attempt."""
    reservation = reserve_volume_file_deletion(series_id, volume_id)
    if reservation.status in {"reserved", "existing"}:
        if reservation.journal_id is None:
            raise RuntimeError("successful deletion reservation has no journal id")
        outcome = replay_volume_file_deletion(reservation.journal_id)
        if outcome in {"completed", "terminal"}:
            return VolumeFileDeletionResult(
                "complete",
                reservation.journal_id,
            )
        journal = _load_journal(reservation.journal_id)
        if journal is None or journal.state != "active":
            return VolumeFileDeletionResult(
                "complete",
                reservation.journal_id,
            )
        return VolumeFileDeletionResult(
            "pending",
            reservation.journal_id,
            journal.diagnostic,
        )
    return VolumeFileDeletionResult(
        cast(DeleteStatus, reservation.status),
        diagnostic=reservation.diagnostic,
    )


def active_deletion_journal_ids(
    *,
    max_rows: int = 100,
    after_id: int = 0,
) -> list[int]:
    """Return one bounded keyset page of active journal IDs."""
    if max_rows <= 0:
        return []
    with get_db() as db:
        rows = db.execute(
            """
            SELECT id
            FROM volume_file_deletions
            WHERE state='active' AND id>?
            ORDER BY id
            LIMIT ?
            """,
            (after_id, max_rows),
        ).fetchall()
    return [int(row["id"]) for row in rows]


async def replay_volume_file_deletions(
    *,
    max_rows: int = 100,
    after_id: int = 0,
) -> DeletionReplaySummary:
    """Replay one bounded keyset page, settling cancellation at one journal."""
    global _replay_lock
    if _replay_lock is None:
        _replay_lock = asyncio.Lock()

    summary = DeletionReplaySummary()
    async with _replay_lock:
        journal_ids = active_deletion_journal_ids(
            max_rows=max_rows,
            after_id=after_id,
        )
        for journal_id in journal_ids:
            await asyncio.sleep(0)
            summary.examined += 1
            summary.last_id = journal_id
            operation = asyncio.create_task(
                asyncio.to_thread(replay_volume_file_deletion, journal_id)
            )
            try:
                outcome = await asyncio.shield(operation)
            except asyncio.CancelledError:
                await asyncio.shield(operation)
                raise
            if outcome in {"completed", "terminal"}:
                summary.completed += 1
            else:
                summary.blocked += 1
    return summary


async def drain_active_volume_file_deletions(
    *,
    page_size: int = 100,
) -> DeletionReplaySummary:
    """Drain startup-required deletion journals in bounded keyset pages."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    total = DeletionReplaySummary()
    cursor = 0
    while True:
        page = await replay_volume_file_deletions(
            max_rows=page_size,
            after_id=cursor,
        )
        total.examined += page.examined
        total.completed += page.completed
        total.blocked += page.blocked
        if not page.examined:
            break
        if page.last_id <= cursor:
            raise RuntimeError("deletion replay keyset cursor did not advance")
        cursor = page.last_id
        total.last_id = cursor
        if page.examined < page_size:
            break
    return total
