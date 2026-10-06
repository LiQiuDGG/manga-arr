"""Durable publication journal for staged imports.

The journal is the recovery authority from the prepared barrier until cleanup
finishes. Database helpers in this module never perform filesystem I/O, and
filesystem helpers never retain a database connection.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import hashlib
import json
import logging
import os
import secrets
import shutil
import sqlite3
import stat
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Literal, cast
from urllib.parse import urlsplit, urlunsplit

from download_identity import (
    DownloadProtocol,
    normalize_download_protocol,
)
from import_plan import _FilePlan, _ImportPlan
from import_staging import _StageOutcome
from noreplace_fallback import UNSUPPORTED_ERRNOS, rename_noreplace_fallback
from shared import get_cfg, get_db

log = logging.getLogger(__name__)

PublicationState = Literal[
    "staging",
    "prepared",
    "publishing",
    "published",
    "db_committed",
    "cleaning",
    "finalized",
    "deleted",
]
CleanupState = Literal[
    "pending",
    "deleted",
    "missing",
    "replaced",
    "not_applicable",
    "blocked",
]
SuccessEffectType = Literal["cover", "komga_scan", "remove_completed"]
NotificationCompletionReason = Literal[
    "delivered",
    "connection_deleted",
    "connection_disabled",
]

_ACTIVE_STATES = (
    "staging",
    "prepared",
    "publishing",
    "published",
    "db_committed",
    "cleaning",
)
_HASH_CHUNK_SIZE = 1024 * 1024
_OPERATION_LEASE_SECONDS = 120
_NOTIFICATION_LEASE_SECONDS = 60
_NOTIFICATION_MAX_BACKOFF_SECONDS = 60 * 60
_NOTIFICATION_INITIAL_BACKOFF_SECONDS = 5
_SUCCESS_EFFECT_LEASE_SECONDS = 60
_SUCCESS_EFFECT_MAX_BACKOFF_SECONDS = 60 * 60
_SUCCESS_EFFECT_INITIAL_BACKOFF_SECONDS = 5
_AT_FDCWD = -100
_RENAME_NOREPLACE = 1
_replay_lock: asyncio.Lock | None = None


class PublicationBlocked(RuntimeError):
    """Raised when journal artifacts cannot be proven safe to roll forward."""


class PublicationOwnershipLost(RuntimeError):
    """Raised when another live journal owner must be allowed to finish."""


@dataclass(frozen=True, slots=True)
class FileFingerprint:
    """Stable filesystem identity recorded by the publication journal."""

    dev: int
    inode: int
    size: int
    mtime_ns: int
    sha256: str | None = None


@dataclass(frozen=True, slots=True)
class PreparedArtifact:
    """A staged artifact after transforms, fsync, and hashing."""

    file_id: int
    stage_path: str
    final_path: str
    fingerprint: FileFingerprint
    prepared_final_fingerprint: FileFingerprint | None


@dataclass(frozen=True, slots=True)
class PublicationFile:
    """Plain-data representation of one journal child row."""

    row_id: int
    plan: _FilePlan
    outcome: _StageOutcome
    source_fingerprint: FileFingerprint | None
    staged_fingerprint: FileFingerprint | None
    prepared_final_fingerprint: FileFingerprint | None
    final_expected_absent: bool | None
    final_claim_path: str | None
    source_claim_path: str | None
    publish_state: str
    cleanup_state: str
    diagnostic: str


@dataclass(frozen=True, slots=True)
class ImportPublication:
    """Plain-data representation of a complete publication journal."""

    publication_id: int
    queue_id: int
    state: PublicationState
    owner_token: str
    staging_dir: str
    plan: _ImportPlan
    files: tuple[PublicationFile, ...]
    diagnostic: str
    result_ok: bool | None
    result_imported_count: int | None
    result_queue_status: str | None
    notification_state: str
    notification_title: str | None
    notification_label: str | None
    notification_cover_url: str | None
    notification_idempotency_key: str | None
    queue_download_id: str | None
    queue_download_client_id: int | None
    queue_download_protocol: DownloadProtocol | None
    pack_cleanup_state: str


@dataclass(frozen=True, slots=True)
class CleanupOutcome:
    file_id: int
    state: CleanupState
    diagnostic: str = ""


@dataclass(frozen=True, slots=True)
class CleanupResult:
    outcomes: tuple[CleanupOutcome, ...]
    staging_removed: bool
    diagnostic: str = ""


@dataclass(frozen=True, slots=True)
class SuccessEffectIntent:
    """One configured, replay-safe post-import operation."""

    effect_type: SuccessEffectType
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class _DownloadClientIdentity:
    """Non-secret identity of the client that owns a completed download."""

    client_id: int
    name: str
    client_type: Literal["qbittorrent", "sabnzbd"]
    protocol: Literal["torrent", "nzb"]


@dataclass(slots=True)
class ReplaySummary:
    examined: int = 0
    completed: int = 0
    blocked: int = 0
    deferred: int = 0
    aborted_staging: int = 0
    last_id: int = 0


def active_publication_exists(db: sqlite3.Connection, queue_id: int) -> bool:
    """Return whether ``queue_id`` has journal-owned recovery work."""
    row = db.execute(
        """
        SELECT 1
        FROM import_publications
        WHERE queue_id=?
          AND state IN ('staging','prepared','publishing','published',
                        'db_committed','cleaning')
        LIMIT 1
        """,
        (queue_id,),
    ).fetchone()
    return row is not None


def deterministic_staging_dir(
    dst_dir: str,
    queue_id: int,
    owner_token: str | None = None,
) -> str:
    """Return the hidden same-filesystem staging path for one queue owner.

    ``owner_token=None`` preserves the legacy queue-only path so active
    journals created by an earlier build remain recoverable. New staging must
    always pass its lease owner and therefore cannot overlap a successor.
    """
    if queue_id <= 0:
        raise ValueError("queue_id must be positive")
    dst_abs = os.path.abspath(dst_dir)
    if not os.path.isabs(dst_abs):
        raise ValueError("destination directory must be absolute")
    suffix = ""
    if owner_token is not None:
        if not owner_token:
            raise ValueError("owner_token must be non-empty")
        owner_key = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()[:32]
        suffix = f"-{owner_key}"
    return os.path.join(dst_abs, f".mangarr-publication-{queue_id}{suffix}")


def ensure_durable_directory(path: str) -> None:
    """Create ``path`` and persist every newly inserted directory entry."""
    path_abs = os.path.abspath(path)
    missing: list[str] = []
    current = path_abs
    while not os.path.lexists(current):
        missing.append(current)
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    os.makedirs(path_abs, exist_ok=True)
    if not missing:
        return

    # For /existing/new-a/new-b, sync new-b itself, then new-a (which persists
    # new-b's entry), and finally /existing (which persists new-a's entry).
    barriers = [*missing, os.path.dirname(missing[-1])]
    for directory in dict.fromkeys(barriers):
        _fsync_directory(directory)


def _path_is_below(path: str, root: str) -> bool:
    try:
        return path != root and os.path.commonpath((path, root)) == root
    except ValueError:
        return False


def _publication_claim_path(
    path: str,
    publication_id: int,
    file_id: int,
    purpose: Literal["final", "source"],
) -> str:
    """Return a deterministic, same-directory path for an atomic file claim."""
    if publication_id <= 0 or file_id <= 0:
        raise ValueError("publication and file ids must be positive")
    return os.path.join(
        os.path.dirname(os.path.abspath(path)),
        f".mangarr-publication-{publication_id}-{file_id}-{purpose}-claim",
    )


def _rename_noreplace(source: str, destination: str) -> None:
    """Atomically rename without replacing via Linux ``renameat2(2)``.

    Publication cannot safely emulate ``RENAME_NOREPLACE`` with a
    check-then-rename sequence. Fail closed on non-Linux systems and C
    libraries without ``renameat2``. Filesystems that reject the flag (NFS)
    use ``noreplace_fallback``, which claims the name atomically instead.
    """
    if sys.platform != "linux":
        raise OSError(
            errno.ENOTSUP,
            "atomic no-replace rename requires Linux renameat2",
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


def _validate_destination_path(path: str, dst_dir: str) -> str:
    dst_abs = os.path.abspath(dst_dir)
    path_abs = os.path.abspath(path)
    if not _path_is_below(path_abs, dst_abs):
        raise PublicationBlocked(f"destination escapes library directory: {path}")

    dst_stat = os.lstat(dst_abs)
    if stat.S_ISLNK(dst_stat.st_mode) or not stat.S_ISDIR(dst_stat.st_mode):
        raise PublicationBlocked(
            f"destination directory is not a real directory: {dst_abs}"
        )

    parent = os.path.dirname(path_abs)
    if os.path.realpath(parent) != os.path.realpath(dst_abs):
        raise PublicationBlocked(
            f"destination parent escapes library directory: {path}"
        )
    if os.path.lexists(path_abs):
        target_stat = os.lstat(path_abs)
        if stat.S_ISLNK(target_stat.st_mode) or not stat.S_ISREG(target_stat.st_mode):
            raise PublicationBlocked(
                f"destination is a symlink or non-regular file: {path_abs}"
            )
    return path_abs


def _regular_fingerprint(
    path: str,
    *,
    include_hash: bool,
    heartbeat: Callable[[], bool] | None = None,
) -> FileFingerprint:
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise PublicationBlocked(f"artifact is a symlink or non-regular file: {path}")

    digest: str | None = None
    if include_hash:
        hasher = hashlib.sha256()
        last_heartbeat = time.monotonic()
        with open(path, "rb", buffering=0) as handle:
            while chunk := handle.read(_HASH_CHUNK_SIZE):
                hasher.update(chunk)
                now = time.monotonic()
                if heartbeat is not None and now - last_heartbeat >= 30:
                    if not heartbeat():
                        raise PublicationOwnershipLost
                    last_heartbeat = now
        digest = hasher.hexdigest()

    after = os.lstat(path)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if identity_before != identity_after:
        raise PublicationBlocked(f"artifact changed while fingerprinting: {path}")
    return FileFingerprint(
        dev=after.st_dev,
        inode=after.st_ino,
        size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        sha256=digest,
    )


def initialize_publication_filesystem(
    plan: _ImportPlan,
    owner_token: str,
) -> tuple[str, dict[int, FileFingerprint]]:
    """Validate paths, capture source identities, and create private staging."""
    if not owner_token:
        raise ValueError("owner_token must be non-empty")
    dst_dir = os.path.abspath(plan.dst_dir)
    ensure_durable_directory(dst_dir)
    dst_stat = os.lstat(dst_dir)
    if stat.S_ISLNK(dst_stat.st_mode) or not stat.S_ISDIR(dst_stat.st_mode):
        raise PublicationBlocked(f"destination is not a real directory: {dst_dir}")

    source_fingerprints: dict[int, FileFingerprint] = {}
    for file_plan in plan.files:
        if file_plan.plan_status != "ready":
            continue
        _validate_destination_path(file_plan.dst_path, dst_dir)
        source_fingerprints[file_plan.file_id] = _regular_fingerprint(
            file_plan.src_path,
            include_hash=True,
        )

    staging_dir = deterministic_staging_dir(
        dst_dir,
        int(plan.queue["id"]),
        owner_token,
    )
    if os.path.lexists(staging_dir):
        staging_stat = os.lstat(staging_dir)
        if stat.S_ISLNK(staging_stat.st_mode) or not stat.S_ISDIR(staging_stat.st_mode):
            raise PublicationBlocked(
                f"staging path is a symlink or non-directory: {staging_dir}"
            )
        shutil.rmtree(staging_dir)
    os.mkdir(staging_dir, mode=0o700)
    _fsync_directory(dst_dir)
    return staging_dir, source_fingerprints


def _json_mapping(value: object, *, nullable: bool = False) -> dict[str, Any] | None:
    if value is None and nullable:
        return None
    decoded = json.loads(cast(str, value))
    if not isinstance(decoded, dict):
        raise RuntimeError("publication snapshot is not a JSON object")
    return cast(dict[str, Any], decoded)


def create_publication(
    db: sqlite3.Connection,
    plan: _ImportPlan,
    owner_token: str,
    staging_dir: str,
    source_fingerprints: dict[int, FileFingerprint],
) -> int:
    """Persist staging intent and every plan row before publication is possible."""
    if not owner_token:
        raise ValueError("owner_token must be non-empty")
    queue = plan.queue
    queue_id = int(queue["id"])
    if queue.get("status") != "importing":
        raise RuntimeError("publication plan is not an importing queue snapshot")
    if queue.get("lease_owner") != owner_token:
        raise RuntimeError("publication owner does not match queue lease snapshot")

    db.execute(
        "DELETE FROM import_publications"
        " WHERE queue_id=? AND state IN ('finalized','deleted')",
        (queue_id,),
    )
    cur = db.execute(
        """
        INSERT INTO import_publications(
            queue_id, state, owner_token, series_id, dst_dir, import_mode,
            staging_dir, queue_snapshot_json, series_snapshot_json,
            series_tags_json, queue_status, queue_download_id,
            queue_download_client_id, queue_torrent_name, queue_torrent_url,
            queue_volume_num, queue_src_dir, queue_failed_at,
            queue_lease_owner, queue_lease_expires_at, queue_created_at
        )
        SELECT
            ?, 'staging', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        FROM import_queue AS current_queue
        WHERE current_queue.id=?
          AND current_queue.status='importing'
          AND current_queue.lease_owner=?
          AND current_queue.lease_expires_at > CURRENT_TIMESTAMP
        """,
        (
            queue_id,
            owner_token,
            plan.series_id,
            os.path.abspath(plan.dst_dir),
            plan.import_mode,
            staging_dir,
            json.dumps(queue, sort_keys=True, separators=(",", ":"), default=str),
            (
                json.dumps(
                    plan.series,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                if plan.series is not None
                else None
            ),
            json.dumps(plan.series_tags, separators=(",", ":")),
            str(queue.get("status") or ""),
            queue.get("download_id"),
            queue.get("download_client_id"),
            queue.get("torrent_name"),
            queue.get("torrent_url"),
            queue.get("volume_num"),
            queue.get("src_dir"),
            queue.get("failed_at"),
            queue.get("lease_owner"),
            queue.get("lease_expires_at"),
            queue.get("created_at"),
            queue_id,
            owner_token,
        ),
    )
    if cur.rowcount != 1:
        raise PublicationOwnershipLost(
            "queue lease changed or expired before publication creation"
        )
    publication_id = int(cast(int, cur.lastrowid))

    for ordinal, file_plan in enumerate(plan.files):
        source = source_fingerprints.get(file_plan.file_id)
        applicable = file_plan.plan_status == "ready"
        stage_path = (
            os.path.join(staging_dir, os.path.basename(file_plan.dst_path))
            if applicable
            else None
        )
        source_claim_path = (
            _publication_claim_path(
                file_plan.src_path,
                publication_id,
                file_plan.file_id,
                "source",
            )
            if applicable and plan.import_mode == "move"
            else None
        )
        db.execute(
            """
            INSERT INTO import_publication_files(
                publication_id, ordinal, file_id, src_path, filename, dst_path,
                import_kind, file_type, proposed_vol, proposed_chap,
                chap_range_end, vol_range_start, vol_range_end, pack_type,
                is_special, special_title, has_volume_range,
                is_legacy_chapter_stub, is_legacy_chapter_recheck,
                plan_status, plan_failure_reason, stage_path, final_path,
                source_dev, source_inode, source_size, source_mtime_ns,
                source_sha256, source_claim_path,
                stage_state, publish_state, cleanup_state
            ) VALUES(
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                publication_id,
                ordinal,
                file_plan.file_id,
                file_plan.src_path,
                file_plan.filename,
                file_plan.dst_path,
                file_plan.import_kind,
                file_plan.file_type,
                file_plan.proposed_vol,
                file_plan.proposed_chap,
                file_plan.chap_range_end,
                file_plan.vol_range_start,
                file_plan.vol_range_end,
                file_plan.pack_type,
                file_plan.is_special,
                file_plan.special_title,
                int(file_plan.has_volume_range),
                int(file_plan.is_legacy_chapter_stub),
                int(file_plan.is_legacy_chapter_recheck),
                file_plan.plan_status,
                file_plan.plan_failure_reason,
                stage_path,
                file_plan.dst_path if applicable else None,
                source.dev if source else None,
                source.inode if source else None,
                source.size if source else None,
                source.mtime_ns if source else None,
                source.sha256 if source else None,
                source_claim_path,
                "pending" if applicable else "skipped",
                "pending" if applicable else "not_applicable",
                (
                    "pending"
                    if applicable and plan.import_mode == "move"
                    else "not_applicable"
                ),
            ),
        )
    return publication_id


def prepare_staged_artifacts(
    plan: _ImportPlan,
    staging_dir: str,
    outcomes: list[_StageOutcome],
) -> tuple[PreparedArtifact, ...]:
    """Fsync and hash successful staged artifacts without a database context."""
    outcomes_by_id = {outcome.file_id: outcome for outcome in outcomes}
    artifacts: list[PreparedArtifact] = []
    staging_abs = os.path.abspath(staging_dir)
    for file_plan in plan.files:
        if file_plan.plan_status != "ready":
            continue
        outcome = outcomes_by_id.get(file_plan.file_id)
        if outcome is None or not outcome.ok:
            raise PublicationBlocked(
                f"file {file_plan.file_id} has no successful staging outcome"
            )
        stage_path = os.path.abspath(outcome.stage_path)
        if not _path_is_below(stage_path, staging_abs):
            raise PublicationBlocked(
                f"staged path escapes staging directory: {stage_path}"
            )
        final_path = _validate_destination_path(outcome.final_dst, plan.dst_dir)

        descriptor = os.open(stage_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fingerprint = _regular_fingerprint(stage_path, include_hash=True)
        prepared_final_fingerprint = (
            _regular_fingerprint(final_path, include_hash=True)
            if os.path.lexists(final_path)
            else None
        )
        artifacts.append(
            PreparedArtifact(
                file_id=file_plan.file_id,
                stage_path=stage_path,
                final_path=final_path,
                fingerprint=fingerprint,
                prepared_final_fingerprint=prepared_final_fingerprint,
            )
        )
    _fsync_directory(staging_abs)
    return tuple(artifacts)


def commit_prepared_barrier(
    publication_id: int,
    outcomes: list[_StageOutcome],
    artifacts: tuple[PreparedArtifact, ...],
) -> None:
    """Durably record stage results and transition staging -> prepared."""
    outcomes_by_id = {outcome.file_id: outcome for outcome in outcomes}
    artifacts_by_id = {artifact.file_id: artifact for artifact in artifacts}
    with get_db() as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        rows = [
            dict(row)
            for row in db.execute(
                "SELECT file_id, plan_status FROM import_publication_files"
                " WHERE publication_id=? ORDER BY ordinal",
                (publication_id,),
            ).fetchall()
        ]
        for row in rows:
            file_id = int(row["file_id"])
            if row["plan_status"] != "ready":
                continue
            outcome = outcomes_by_id.get(file_id)
            artifact = artifacts_by_id.get(file_id)
            if outcome is None or not outcome.ok or artifact is None:
                raise RuntimeError(f"publication file {file_id} is not prepared")
            fingerprint = artifact.fingerprint
            prepared_final = artifact.prepared_final_fingerprint
            final_claim_path = _publication_claim_path(
                artifact.final_path,
                publication_id,
                file_id,
                "final",
            )
            db.execute(
                """
                UPDATE import_publication_files
                SET stage_ok=1, stage_error='', stage_path=?, final_path=?,
                    staged_dev=?, staged_inode=?, staged_size=?,
                    staged_mtime_ns=?, staged_sha256=?, stage_state='staged',
                    final_expected_absent=?, prepared_final_dev=?,
                    prepared_final_inode=?, prepared_final_size=?,
                    prepared_final_mtime_ns=?, prepared_final_sha256=?,
                    final_claim_path=?, staged_at=CURRENT_TIMESTAMP
                WHERE publication_id=? AND file_id=?
                """,
                (
                    artifact.stage_path,
                    artifact.final_path,
                    fingerprint.dev,
                    fingerprint.inode,
                    fingerprint.size,
                    fingerprint.mtime_ns,
                    fingerprint.sha256,
                    int(prepared_final is None),
                    prepared_final.dev if prepared_final else None,
                    prepared_final.inode if prepared_final else None,
                    prepared_final.size if prepared_final else None,
                    prepared_final.mtime_ns if prepared_final else None,
                    prepared_final.sha256 if prepared_final else None,
                    final_claim_path,
                    publication_id,
                    file_id,
                ),
            )
        cur = db.execute(
            """
            UPDATE import_publications
            SET state='prepared', prepared_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP, diagnostic=''
            WHERE id=? AND state='staging' AND operation_owner IS NULL
            """,
            (publication_id,),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                "publication lost staging ownership before prepared barrier"
            )


def load_publication(
    db: sqlite3.Connection,
    *,
    publication_id: int | None = None,
    queue_id: int | None = None,
) -> ImportPublication | None:
    """Deserialize a journal without allowing ``sqlite3.Row`` to escape."""
    if (publication_id is None) == (queue_id is None):
        raise ValueError("provide exactly one of publication_id or queue_id")
    if publication_id is not None:
        row = db.execute(
            "SELECT * FROM import_publications WHERE id=?",
            (publication_id,),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT * FROM import_publications WHERE queue_id=?",
            (queue_id,),
        ).fetchone()
    if row is None:
        return None
    header = dict(row)
    child_rows = [
        dict(child)
        for child in db.execute(
            "SELECT * FROM import_publication_files"
            " WHERE publication_id=? ORDER BY ordinal",
            (header["id"],),
        ).fetchall()
    ]
    notification_row = db.execute(
        "SELECT * FROM import_publication_notifications WHERE publication_id=?",
        (header["id"],),
    ).fetchone()
    notification = dict(notification_row) if notification_row is not None else None

    queue = cast(dict[str, Any], _json_mapping(header["queue_snapshot_json"]))
    series = _json_mapping(header["series_snapshot_json"], nullable=True)
    tags_value = json.loads(cast(str, header["series_tags_json"]))
    if not isinstance(tags_value, list) or not all(
        isinstance(tag, str) for tag in tags_value
    ):
        raise RuntimeError("publication series tags snapshot is invalid")
    tags = cast(list[str], tags_value)

    files: list[PublicationFile] = []
    plans: list[_FilePlan] = []
    for child in child_rows:
        file_plan = _FilePlan(
            file_id=int(child["file_id"]),
            src_path=str(child["src_path"]),
            filename=str(child["filename"]),
            dst_path=str(child["dst_path"]),
            import_kind=str(child["import_kind"]),
            file_type=str(child["file_type"]),
            proposed_vol=child["proposed_vol"],
            proposed_chap=child["proposed_chap"],
            chap_range_end=child["chap_range_end"],
            vol_range_start=child["vol_range_start"],
            vol_range_end=child["vol_range_end"],
            pack_type=child["pack_type"],
            is_special=int(child["is_special"]),
            special_title=child["special_title"],
            has_volume_range=bool(child["has_volume_range"]),
            is_legacy_chapter_stub=bool(child["is_legacy_chapter_stub"]),
            is_legacy_chapter_recheck=bool(child["is_legacy_chapter_recheck"]),
            plan_status=str(child["plan_status"]),
            plan_failure_reason=str(child["plan_failure_reason"] or ""),
        )
        outcome = _StageOutcome(
            file_id=file_plan.file_id,
            ok=bool(child["stage_ok"]),
            final_dst=str(child["final_path"] or ""),
            error=str(child["stage_error"] or ""),
            stage_path=str(child["stage_path"] or ""),
        )
        source = _fingerprint_from_row(child, "source", include_hash=True)
        staged = _fingerprint_from_row(child, "staged", include_hash=True)
        prepared_final = _fingerprint_from_row(
            child,
            "prepared_final",
            include_hash=True,
        )
        plans.append(file_plan)
        files.append(
            PublicationFile(
                row_id=int(child["id"]),
                plan=file_plan,
                outcome=outcome,
                source_fingerprint=source,
                staged_fingerprint=staged,
                prepared_final_fingerprint=prepared_final,
                final_expected_absent=(
                    bool(child["final_expected_absent"])
                    if child["final_expected_absent"] is not None
                    else None
                ),
                final_claim_path=child["final_claim_path"],
                source_claim_path=child["source_claim_path"],
                publish_state=str(child["publish_state"]),
                cleanup_state=str(child["cleanup_state"]),
                diagnostic=str(child["diagnostic"] or ""),
            )
        )

    plan = _ImportPlan(
        queue=queue,
        series=series,
        series_tags=tags,
        dst_dir=str(header["dst_dir"]),
        import_mode=str(header["import_mode"]),
        now_ts=None,
        files=plans,
        series_id=int(header["series_id"]),
    )
    return ImportPublication(
        publication_id=int(header["id"]),
        queue_id=int(header["queue_id"]),
        state=cast(PublicationState, header["state"]),
        owner_token=str(header["owner_token"]),
        staging_dir=str(header["staging_dir"]),
        plan=plan,
        files=tuple(files),
        diagnostic=str(header["diagnostic"] or ""),
        result_ok=(
            bool(header["result_ok"]) if header["result_ok"] is not None else None
        ),
        result_imported_count=header["result_imported_count"],
        result_queue_status=header["result_queue_status"],
        notification_state=str(
            notification["state"] if notification else header["notification_state"]
        ),
        notification_title=(
            notification["title"] if notification else header["notification_title"]
        ),
        notification_label=(
            notification["label"] if notification else header["notification_label"]
        ),
        notification_cover_url=(
            notification["cover_url"]
            if notification
            else header["notification_cover_url"]
        ),
        notification_idempotency_key=(
            str(notification["idempotency_key"]) if notification else None
        ),
        queue_download_id=(
            str(header["queue_download_id"])
            if header["queue_download_id"] is not None
            else None
        ),
        queue_download_client_id=(
            int(header["queue_download_client_id"])
            if header["queue_download_client_id"] is not None
            else None
        ),
        queue_download_protocol=normalize_download_protocol(
            queue.get("download_protocol")
        ),
        pack_cleanup_state=str(header["pack_cleanup_state"]),
    )


def _fingerprint_from_row(
    row: dict[str, Any],
    prefix: str,
    *,
    include_hash: bool,
) -> FileFingerprint | None:
    dev = row[f"{prefix}_dev"]
    if dev is None:
        return None
    return FileFingerprint(
        dev=int(dev),
        inode=int(row[f"{prefix}_inode"]),
        size=int(row[f"{prefix}_size"]),
        mtime_ns=int(row[f"{prefix}_mtime_ns"]),
        sha256=(
            str(row[f"{prefix}_sha256"])
            if include_hash and row.get(f"{prefix}_sha256") is not None
            else None
        ),
    )


def _same_full_fingerprint(actual: FileFingerprint, expected: FileFingerprint) -> bool:
    return actual == expected


def _same_published_content(actual: FileFingerprint, expected: FileFingerprint) -> bool:
    return (
        actual.size == expected.size
        and actual.sha256 is not None
        and actual.sha256 == expected.sha256
    )


def _set_publication_diagnostic(
    publication_id: int,
    file_id: int,
    diagnostic: str,
    owner_token: str,
) -> None:
    with get_db() as db:
        owned = db.execute(
            "UPDATE import_publication_files"
            " SET publish_state='blocked', diagnostic=?"
            " WHERE publication_id=? AND file_id=? AND EXISTS ("
            "   SELECT 1 FROM import_publications"
            "   WHERE id=? AND state='publishing' AND operation_owner=?"
            " )",
            (diagnostic, publication_id, file_id, publication_id, owner_token),
        )
        if owned.rowcount != 1:
            return
        db.execute(
            "UPDATE import_publications"
            " SET diagnostic=?, updated_at=CURRENT_TIMESTAMP,"
            " operation_owner=NULL, operation_expires_at=NULL"
            " WHERE id=? AND state='publishing' AND operation_owner=?",
            (diagnostic, publication_id, owner_token),
        )


def _claim_publication_operation(
    publication_id: int,
    owner_token: str,
) -> bool:
    """Acquire or renew the DB-clock CAS publishing lease."""
    lease_modifier = f"+{_OPERATION_LEASE_SECONDS} seconds"
    with get_db() as db:
        cur = db.execute(
            """
            UPDATE import_publications
            SET state='publishing', operation_owner=?,
                operation_expires_at=datetime('now', ?),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state IN ('prepared','publishing')
              AND (
                  operation_owner IS NULL
                  OR operation_owner=?
                  OR operation_expires_at IS NULL
                  OR operation_expires_at <= CURRENT_TIMESTAMP
              )
            """,
            (
                owner_token,
                lease_modifier,
                publication_id,
                owner_token,
            ),
        )
        return cur.rowcount == 1


def _refresh_publication_operation(
    publication_id: int,
    owner_token: str,
    state: str,
) -> bool:
    lease_modifier = f"+{_OPERATION_LEASE_SECONDS} seconds"
    with get_db() as db:
        cur = db.execute(
            """
            UPDATE import_publications
            SET operation_expires_at=datetime('now', ?),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state=? AND operation_owner=?
              AND operation_expires_at > CURRENT_TIMESTAMP
            """,
            (lease_modifier, publication_id, state, owner_token),
        )
        return cur.rowcount == 1


def _restore_claim_without_clobber(claim_path: str, original_path: str) -> bool:
    """Restore a rejected claim and durably record both rename directories."""
    try:
        _rename_noreplace(claim_path, original_path)
    except FileExistsError:
        return False
    _fsync_renamed_directories(claim_path, original_path)
    return True


def _delete_verified_claim(
    claim_path: str,
    expected: FileFingerprint,
    *,
    heartbeat: Callable[[], bool] | None = None,
) -> None:
    """Delete a same-directory claim only after its complete fingerprint matches."""
    actual = _regular_fingerprint(
        claim_path,
        include_hash=True,
        heartbeat=heartbeat,
    )
    if not _same_full_fingerprint(actual, expected):
        raise PublicationBlocked(f"claimed artifact changed: {claim_path}")
    os.unlink(claim_path)
    _fsync_directory(os.path.dirname(claim_path))


def _publish_prepared_file(
    publication: ImportPublication,
    file_record: PublicationFile,
    owner_token: str,
) -> None:
    """Publish one staged file against its durable destination precondition."""
    expected = file_record.staged_fingerprint
    expected_absent = file_record.final_expected_absent
    prepared_final = file_record.prepared_final_fingerprint
    claim_path = file_record.final_claim_path
    if expected is None or not expected.sha256:
        raise PublicationBlocked("missing durable staged fingerprint")
    if expected_absent is None:
        raise PublicationBlocked("missing durable final-path precondition")
    if expected_absent:
        if prepared_final is not None:
            raise PublicationBlocked("invalid absent final-path precondition")
    elif prepared_final is None or not prepared_final.sha256 or not claim_path:
        raise PublicationBlocked("missing durable overwrite precondition")

    final_path = _validate_destination_path(
        file_record.outcome.final_dst,
        publication.plan.dst_dir,
    )
    stage_path = os.path.abspath(file_record.outcome.stage_path)
    if not _path_is_below(stage_path, os.path.abspath(publication.staging_dir)):
        raise PublicationBlocked(f"staged path escapes staging directory: {stage_path}")
    heartbeat = lambda: _refresh_publication_operation(
        publication.publication_id,
        owner_token,
        "publishing",
    )

    if not os.path.lexists(stage_path):
        if not os.path.lexists(final_path):
            raise PublicationBlocked(
                f"both staged and final artifacts are missing: {stage_path}"
            )
        final_actual = _regular_fingerprint(
            final_path,
            include_hash=True,
            heartbeat=heartbeat,
        )
        if not _same_published_content(final_actual, expected):
            raise PublicationBlocked(
                f"final artifact does not match staged fingerprint: {final_path}"
            )
        # Replay may be observing a rename whose process died between either
        # directory barrier. Persist both sides before an overwrite claim can
        # be removed (or before an absent-destination publish is accepted).
        _fsync_renamed_directories(stage_path, final_path)
        if claim_path and os.path.lexists(claim_path):
            if prepared_final is None:
                raise PublicationBlocked("missing prepared overwrite fingerprint")
            _delete_verified_claim(
                claim_path,
                prepared_final,
                heartbeat=heartbeat,
            )
        return

    staged_actual = _regular_fingerprint(
        stage_path,
        include_hash=True,
        heartbeat=heartbeat,
    )
    if not _same_full_fingerprint(staged_actual, expected):
        raise PublicationBlocked(f"staged artifact fingerprint changed: {stage_path}")

    if expected_absent:
        try:
            _rename_noreplace(stage_path, final_path)
        except FileExistsError as exc:
            raise PublicationBlocked(
                f"prepared-absent destination appeared before publish: {final_path}"
            ) from exc
        _fsync_renamed_directories(stage_path, final_path)
        return

    if prepared_final is None or claim_path is None:
        raise PublicationBlocked("missing prepared overwrite claim metadata")
    if os.path.lexists(claim_path):
        # A previous process may have died immediately after final -> claim.
        # Establish that directory entry durably before relying on it.
        _fsync_directory(os.path.dirname(final_path))
        claimed_actual = _regular_fingerprint(
            claim_path,
            include_hash=True,
            heartbeat=heartbeat,
        )
        if not _same_full_fingerprint(claimed_actual, prepared_final):
            raise PublicationBlocked(
                f"overwrite claim does not match prepared destination: {claim_path}"
            )
        if os.path.lexists(final_path):
            raise PublicationBlocked(
                f"destination appeared while prepared overwrite was claimed: {final_path}"
            )
    else:
        if not os.path.lexists(final_path):
            raise PublicationBlocked(
                f"prepared overwrite destination disappeared: {final_path}"
            )
        try:
            _rename_noreplace(final_path, claim_path)
        except FileExistsError as exc:
            raise PublicationBlocked(
                f"overwrite claim path is already occupied: {claim_path}"
            ) from exc
        # The old destination must have a durable recovery name before any
        # later operation can publish or discard it.
        _fsync_directory(os.path.dirname(final_path))
        claimed_actual = _regular_fingerprint(
            claim_path,
            include_hash=True,
            heartbeat=heartbeat,
        )
        if not _same_full_fingerprint(claimed_actual, prepared_final):
            restored = _restore_claim_without_clobber(claim_path, final_path)
            suffix = (
                "" if restored else "; claim retained because destination reappeared"
            )
            raise PublicationBlocked(
                f"destination changed after prepared barrier: {final_path}{suffix}"
            )

    try:
        _rename_noreplace(stage_path, final_path)
    except FileExistsError as exc:
        restored = _restore_claim_without_clobber(claim_path, final_path)
        suffix = "" if restored else "; prepared destination claim retained"
        raise PublicationBlocked(
            f"destination appeared during prepared overwrite: {final_path}{suffix}"
        ) from exc
    # Persist both the new destination entry and removal of the staged entry
    # before the old destination claim is unlinked.
    _fsync_renamed_directories(stage_path, final_path)
    _delete_verified_claim(
        claim_path,
        prepared_final,
        heartbeat=heartbeat,
    )


def publish_publication(publication_id: int, owner_token: str) -> bool:
    """Idempotently publish all prepared files, with no DB held during I/O."""
    if not _claim_publication_operation(
        publication_id,
        owner_token,
    ):
        with get_db() as db:
            row = db.execute(
                "SELECT state FROM import_publications WHERE id=?",
                (publication_id,),
            ).fetchone()
        return row is not None and str(row["state"]) in (
            "published",
            "db_committed",
            "cleaning",
            "finalized",
            "deleted",
        )

    with get_db() as db:
        publication = load_publication(db, publication_id=publication_id)
    if publication is None:
        return False

    for file_record in publication.files:
        if file_record.plan.plan_status != "ready":
            continue
        try:
            if not _refresh_publication_operation(
                publication_id,
                owner_token,
                "publishing",
            ):
                raise PublicationOwnershipLost
            _publish_prepared_file(publication, file_record, owner_token)

            with get_db() as db:
                cur = db.execute(
                    """
                    UPDATE import_publication_files
                    SET publish_state='published', diagnostic='',
                        published_at=COALESCE(published_at, CURRENT_TIMESTAMP)
                    WHERE publication_id=? AND file_id=? AND EXISTS (
                        SELECT 1 FROM import_publications
                        WHERE id=? AND state='publishing' AND operation_owner=?
                          AND operation_expires_at > CURRENT_TIMESTAMP
                    )
                    """,
                    (
                        publication_id,
                        file_record.plan.file_id,
                        publication_id,
                        owner_token,
                    ),
                )
                if cur.rowcount != 1:
                    raise PublicationOwnershipLost
        except PublicationOwnershipLost:
            return False
        except (OSError, PublicationBlocked) as exc:
            diagnostic = str(exc)
            _set_publication_diagnostic(
                publication_id,
                file_record.plan.file_id,
                diagnostic,
                owner_token,
            )
            log.error("Import publication %s blocked: %s", publication_id, diagnostic)
            return False

    with get_db() as db:
        pending = db.execute(
            "SELECT 1 FROM import_publication_files"
            " WHERE publication_id=? AND plan_status='ready'"
            " AND publish_state!='published' LIMIT 1",
            (publication_id,),
        ).fetchone()
        if pending is not None:
            return False
        cur = db.execute(
            """
            UPDATE import_publications
            SET state='published', published_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP, diagnostic='',
                operation_owner=NULL, operation_expires_at=NULL
            WHERE id=? AND state='publishing' AND operation_owner=?
              AND operation_expires_at > CURRENT_TIMESTAMP
            """,
            (publication_id, owner_token),
        )
        return cur.rowcount == 1


def claim_publication_phase3(
    db: sqlite3.Connection,
    publication_id: int,
    owner_token: str,
) -> bool:
    """CAS a published journal to this Phase 3 owner inside its transaction."""
    lease_modifier = f"+{_OPERATION_LEASE_SECONDS} seconds"
    cur = db.execute(
        """
        UPDATE import_publications
        SET operation_owner=?, operation_expires_at=datetime('now', ?),
            updated_at=CURRENT_TIMESTAMP
        WHERE id=? AND state='published'
          AND (
              operation_owner IS NULL
              OR operation_owner=?
              OR operation_expires_at IS NULL
              OR operation_expires_at <= CURRENT_TIMESTAMP
          )
        """,
        (owner_token, lease_modifier, publication_id, owner_token),
    )
    return cur.rowcount == 1


def _canonical_http_url(value: str) -> str:
    """Return a stable HTTP service URL without credentials or decorations."""
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("service URL must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("service URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("service URL must not contain a query or fragment")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("service URL has an invalid port") from exc

    hostname = parsed.hostname.lower()
    rendered_host = f"[{hostname}]" if ":" in hostname else hostname
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        rendered_host = f"{rendered_host}:{port}"
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, rendered_host, path, "", ""))


def _komga_target_fingerprint(
    canonical_url: str,
    library_id: str,
    username: str,
) -> str:
    """Fingerprint the non-password owner and target without storing username."""
    target = json.dumps(
        {
            "library_id": library_id,
            "url": canonical_url,
            "username": username,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(target.encode("utf-8")).hexdigest()


def _configured_komga_effect() -> SuccessEffectIntent:
    """Snapshot only the non-secret Komga target and its owner fingerprint."""
    raw_url = str(get_cfg("komga_url", "") or "")
    library_id = str(get_cfg("komga_library_id", "") or "").strip()
    username = str(get_cfg("komga_user", "") or "")
    try:
        canonical_url = _canonical_http_url(raw_url) if raw_url else ""
    except ValueError:
        # Preserve the previous completed no-op for an unusable integration
        # without copying a potentially credential-bearing URL to the journal.
        canonical_url = ""
    return SuccessEffectIntent(
        "komga_scan",
        {
            "url": canonical_url,
            "library_id": library_id,
            "target_fingerprint": _komga_target_fingerprint(
                canonical_url,
                library_id,
                username,
            ),
        },
    )


def _normalized_download_protocol(value: object) -> Literal["torrent", "nzb"] | None:
    return normalize_download_protocol(value)


def _resolve_removal_client_identity(
    db: sqlite3.Connection,
    publication_id: int,
    download_id: str,
) -> _DownloadClientIdentity | None:
    """Resolve the grab-time client ID without consulting current routing."""
    publication = db.execute(
        """
        SELECT publication.queue_download_client_id,
               publication.queue_snapshot_json,
               queue.download_protocol
        FROM import_publications AS publication
        LEFT JOIN import_queue AS queue ON queue.id=publication.queue_id
        WHERE publication.id=?
        """,
        (publication_id,),
    ).fetchone()
    if publication is None or publication["queue_download_client_id"] is None:
        return None
    raw_client_id = publication["queue_download_client_id"]
    if (
        not isinstance(raw_client_id, int)
        or isinstance(raw_client_id, bool)
        or raw_client_id <= 0
    ):
        return None
    client_id = int(raw_client_id)

    queue_snapshot = cast(
        dict[str, Any],
        _json_mapping(publication["queue_snapshot_json"]),
    )
    protocol = _normalized_download_protocol(
        queue_snapshot.get("download_protocol")
    ) or _normalized_download_protocol(publication["download_protocol"])
    if protocol is None:
        evidence = [
            dict(row)
            for row in db.execute(
                """
                SELECT download_id, protocol
                FROM seen
                WHERE download_client_id=?
                  AND lower(trim(download_id))=lower(trim(?))
                UNION ALL
                SELECT download_id, protocol
                FROM volumes
                WHERE download_client_id=?
                  AND lower(trim(download_id))=lower(trim(?))
                UNION ALL
                SELECT download_id, protocol
                FROM chapters
                WHERE download_client_id=?
                  AND lower(trim(download_id))=lower(trim(?))
                """,
                (
                    client_id,
                    download_id,
                    client_id,
                    download_id,
                    client_id,
                    download_id,
                ),
            ).fetchall()
        ]
        protocols = {
            normalized_protocol
            for row in evidence
            if (
                normalized_protocol := _normalized_download_protocol(
                    row.get("protocol")
                )
            )
            is not None
            and (
                (
                    normalized_protocol == "torrent"
                    and str(row.get("download_id") or "").strip().casefold()
                    == download_id.strip().casefold()
                )
                or (
                    normalized_protocol == "nzb"
                    and str(row.get("download_id") or "") == download_id
                )
            )
        }
        if len(protocols) != 1:
            return None
        protocol = cast(Literal["torrent", "nzb"], next(iter(protocols)))

    expected_type: Literal["qbittorrent", "sabnzbd"] = (
        "qbittorrent" if protocol == "torrent" else "sabnzbd"
    )
    selected_row = db.execute(
        "SELECT id, name, type FROM download_clients WHERE id=?",
        (client_id,),
    ).fetchone()
    if selected_row is None:
        return None
    selected = dict(selected_row)
    if str(selected["type"]) != expected_type:
        return None
    return _DownloadClientIdentity(
        client_id=client_id,
        name=str(selected["name"]),
        client_type=expected_type,
        protocol=protocol,
    )


def _remove_completed_is_eligible(
    db: sqlite3.Connection,
    publication_id: int,
    download_id: str,
    queue_status: str,
    protocol: Literal["torrent", "nzb"],
    download_client_id: int,
) -> bool:
    """Require this queue and every download sibling to be safely terminal."""
    if queue_status != "imported":
        return False
    publication = db.execute(
        "SELECT queue_id FROM import_publications WHERE id=?",
        (publication_id,),
    ).fetchone()
    if publication is None:
        return False
    queue_id = int(publication["queue_id"])
    current = db.execute(
        "SELECT status, lease_owner, download_client_id, download_protocol"
        " FROM import_queue WHERE id=?",
        (queue_id,),
    ).fetchone()
    if (
        current is None
        or current["status"] != "imported"
        or current["lease_owner"] is not None
        or current["download_client_id"] != download_client_id
        or (
            _normalized_download_protocol(current["download_protocol"])
            not in (None, protocol)
        )
    ):
        return False
    if db.execute(
        "SELECT 1 FROM import_queue_files"
        " WHERE queue_id=? AND status NOT IN ('imported','skipped') LIMIT 1",
        (queue_id,),
    ).fetchone():
        return False

    unsafe_sibling = db.execute(
        """
        SELECT 1
        FROM import_queue AS sibling
        WHERE sibling.id != ?
          AND sibling.download_id IS NOT NULL
          AND (
              sibling.download_protocol=?
              OR sibling.download_protocol IS NULL
          )
          AND CASE
              WHEN COALESCE(sibling.download_protocol, ?)='nzb'
              THEN sibling.download_id=?
              ELSE lower(trim(sibling.download_id))=lower(trim(?))
          END
          AND (
              sibling.download_client_id=?
              OR sibling.download_client_id IS NULL
          )
          AND (
              sibling.status NOT IN ('imported','skipped')
              OR sibling.lease_owner IS NOT NULL
              OR EXISTS (
                  SELECT 1 FROM import_queue_files AS sibling_file
                  WHERE sibling_file.queue_id=sibling.id
                    AND sibling_file.status NOT IN ('imported','skipped')
              )
        )
        LIMIT 1
        """,
        (
            queue_id,
            protocol,
            protocol,
            download_id,
            download_id,
            download_client_id,
        ),
    ).fetchone()
    return unsafe_sibling is None


def _configured_success_effects(
    db: sqlite3.Connection,
    publication_id: int,
    queue_status: str,
) -> tuple[SuccessEffectIntent, ...]:
    """Snapshot configured success work while Phase 3 still owns its writer."""
    publication = db.execute(
        "SELECT series_id, dst_dir, queue_download_id"
        " FROM import_publications WHERE id=?",
        (publication_id,),
    ).fetchone()
    if publication is None:
        raise RuntimeError("publication disappeared while creating success effects")

    series_id = int(publication["series_id"])
    cover = db.execute(
        "SELECT cover_url FROM series WHERE id=?",
        (series_id,),
    ).fetchone()
    first_cbz = db.execute(
        """
        SELECT final_path
        FROM import_publication_files
        WHERE publication_id=? AND stage_ok=1
          AND lower(final_path) LIKE '%.cbz'
        ORDER BY ordinal
        LIMIT 1
        """,
        (publication_id,),
    ).fetchone()
    effects = [
        SuccessEffectIntent(
            "cover",
            {
                "series_id": series_id,
                "dst_dir": str(publication["dst_dir"]),
                "first_cbz": (
                    str(first_cbz["final_path"])
                    if first_cbz is not None and first_cbz["final_path"]
                    else ""
                ),
                "cover_url": (
                    str(cover["cover_url"])
                    if cover is not None and cover["cover_url"]
                    else ""
                ),
            },
        )
    ]

    if str(get_cfg("komga_scan_enabled", "false") or "").lower() == "true":
        effects.append(_configured_komga_effect())

    download_id_value = publication["queue_download_id"]
    download_id = str(download_id_value or "").strip()
    if (
        download_id
        and str(get_cfg("remove_completed", "false") or "").lower() == "true"
    ):
        client_identity = _resolve_removal_client_identity(
            db,
            publication_id,
            download_id,
        )
        if client_identity is not None and _remove_completed_is_eligible(
            db,
            publication_id,
            download_id,
            queue_status,
            client_identity.protocol,
            client_identity.client_id,
        ):
            effects.append(
                SuccessEffectIntent(
                    "remove_completed",
                    {
                        "download_id": download_id,
                        "protocol": client_identity.protocol,
                        "client_id": client_identity.client_id,
                        "client_type": client_identity.client_type,
                        "client_name": client_identity.name,
                    },
                )
            )
    return tuple(effects)


def mark_publication_db_committed(
    db: sqlite3.Connection,
    publication_id: int,
    owner_token: str,
    *,
    result_ok: bool,
    imported_count: int,
    queue_status: str,
    notification: tuple[str, str, str] | None,
) -> None:
    """Commit Phase 3 outcome and journal state in the caller's transaction."""
    connection_snapshots: tuple[tuple[int, str, str], ...] = ()
    if notification:
        connection_snapshots = tuple(
            (
                int(row["id"]),
                str(row["name"]),
                str(row["type"]),
            )
            for row in db.execute(
                """
                SELECT id, name, type
                FROM notification_connections
                WHERE enabled=1 AND on_download=1
                ORDER BY id
                """
            ).fetchall()
        )
    notification_state = (
        ("pending" if connection_snapshots else "dispatched")
        if notification
        else "none"
    )
    title, label, cover_url = notification or (None, None, None)
    cur = db.execute(
        """
        UPDATE import_publications
        SET state='db_committed', result_ok=?, result_imported_count=?,
            result_queue_status=?, notification_state=?,
            notification_title=?, notification_label=?,
            notification_cover_url=?, db_committed_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP, operation_owner=NULL,
            operation_expires_at=NULL
        WHERE id=? AND state='published' AND operation_owner=?
          AND operation_expires_at > CURRENT_TIMESTAMP
        """,
        (
            int(result_ok),
            imported_count,
            queue_status,
            notification_state,
            title,
            label,
            cover_url,
            publication_id,
            owner_token,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError("publication lost Phase 3 ownership")
    if notification:
        db.execute(
            """
            INSERT INTO import_publication_notifications(
                publication_id, state, idempotency_key, title, label, cover_url,
                dispatched_at
            ) VALUES(
                ?, ?, ?, ?, ?, ?,
                CASE WHEN ?='dispatched' THEN CURRENT_TIMESTAMP END
            )
            """,
            (
                publication_id,
                notification_state,
                f"mangarr-import-publication:{publication_id}",
                title,
                label,
                cover_url or "",
                notification_state,
            ),
        )
        db.executemany(
            """
            INSERT INTO import_publication_notification_deliveries(
                publication_id, connection_id, connection_name,
                connection_type, state
            ) VALUES(?, ?, ?, ?, 'pending')
            """,
            (
                (publication_id, connection_id, name, connection_type)
                for connection_id, name, connection_type in connection_snapshots
            ),
        )
    if result_ok:
        for effect in _configured_success_effects(
            db,
            publication_id,
            queue_status,
        ):
            db.execute(
                """
                INSERT INTO import_publication_success_effects(
                    publication_id, effect_type, state, idempotency_key,
                    payload_json
                ) VALUES(?, ?, 'pending', ?, ?)
                """,
                (
                    publication_id,
                    effect.effect_type,
                    (
                        f"mangarr-import-publication:{publication_id}:"
                        f"success:{effect.effect_type}"
                    ),
                    json.dumps(
                        effect.payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )


def claim_publication_cleanup(
    db: sqlite3.Connection,
    publication_id: int,
    owner_token: str,
) -> bool:
    """Claim post-commit cleanup; cleaning takeover is safe and idempotent."""
    lease_modifier = f"+{_OPERATION_LEASE_SECONDS} seconds"
    cur = db.execute(
        """
        UPDATE import_publications
        SET state='cleaning', operation_owner=?,
            operation_expires_at=datetime('now', ?),
            cleaning_at=COALESCE(cleaning_at, CURRENT_TIMESTAMP),
            updated_at=CURRENT_TIMESTAMP
        WHERE id=? AND state IN ('db_committed','cleaning')
          AND (
              operation_owner IS NULL
              OR operation_owner=?
              OR operation_expires_at IS NULL
              OR operation_expires_at <= CURRENT_TIMESTAMP
          )
        """,
        (owner_token, lease_modifier, publication_id, owner_token),
    )
    return cur.rowcount == 1


def _cleanup_move_source(
    publication_id: int,
    file_record: PublicationFile,
    owner_token: str,
) -> CleanupOutcome:
    """Atomically claim, verify, and delete one exact move source."""
    source = file_record.source_fingerprint
    claim_path = file_record.source_claim_path
    file_id = file_record.plan.file_id
    if source is None or not source.sha256 or not claim_path:
        return CleanupOutcome(
            file_id,
            "blocked",
            "missing durable source fingerprint or claim path",
        )

    source_path = os.path.abspath(file_record.plan.src_path)
    parent = os.path.dirname(source_path)
    heartbeat = lambda: _refresh_publication_operation(
        publication_id,
        owner_token,
        "cleaning",
    )
    try:
        if os.path.lexists(claim_path):
            # Recovery can begin after source -> claim reached the filesystem
            # but before its directory entry was durable.
            _fsync_directory(parent)
            claimed = _regular_fingerprint(
                claim_path,
                include_hash=True,
                heartbeat=heartbeat,
            )
            if not _same_full_fingerprint(claimed, source):
                if not os.path.lexists(source_path) and _restore_claim_without_clobber(
                    claim_path,
                    source_path,
                ):
                    return CleanupOutcome(
                        file_id,
                        "replaced",
                        "unexpected source claim restored and retained",
                    )
                return CleanupOutcome(
                    file_id,
                    "blocked",
                    "unexpected source claim retained without clobbering source",
                )
            _delete_verified_claim(
                claim_path,
                source,
                heartbeat=heartbeat,
            )
            return CleanupOutcome(file_id, "deleted")

        if not os.path.lexists(source_path):
            return CleanupOutcome(file_id, "missing")

        try:
            _rename_noreplace(source_path, claim_path)
        except FileExistsError:
            return _cleanup_move_source(
                publication_id,
                file_record,
                owner_token,
            )
        _fsync_directory(parent)
        claimed = _regular_fingerprint(
            claim_path,
            include_hash=True,
            heartbeat=heartbeat,
        )
        if not _same_full_fingerprint(claimed, source):
            restored = _restore_claim_without_clobber(claim_path, source_path)
            if restored:
                return CleanupOutcome(
                    file_id,
                    "replaced",
                    "source changed after staging; restored and retained",
                )
            return CleanupOutcome(
                file_id,
                "blocked",
                "changed source claim retained because source path reappeared",
            )
        _delete_verified_claim(
            claim_path,
            source,
            heartbeat=heartbeat,
        )
        return CleanupOutcome(file_id, "deleted")
    except (OSError, PublicationBlocked) as exc:
        return CleanupOutcome(file_id, "blocked", str(exc))


def cleanup_publication_filesystem(
    publication: ImportPublication,
    owner_token: str,
) -> CleanupResult:
    """Delete atomically claimed move sources and staging artifacts after Phase 3."""
    outcomes: list[CleanupOutcome] = []
    if publication.plan.import_mode == "move":
        for file_record in publication.files:
            if not _refresh_publication_operation(
                publication.publication_id,
                owner_token,
                "cleaning",
            ):
                raise PublicationOwnershipLost
            if file_record.plan.plan_status != "ready":
                outcomes.append(
                    CleanupOutcome(file_record.plan.file_id, "not_applicable")
                )
                continue
            outcomes.append(
                _cleanup_move_source(
                    publication.publication_id,
                    file_record,
                    owner_token,
                )
            )
    else:
        outcomes.extend(
            CleanupOutcome(file_record.plan.file_id, "not_applicable")
            for file_record in publication.files
        )

    if not _refresh_publication_operation(
        publication.publication_id,
        owner_token,
        "cleaning",
    ):
        raise PublicationOwnershipLost
    staging_removed = False
    diagnostic = ""
    expected_staging = deterministic_staging_dir(
        publication.plan.dst_dir,
        publication.queue_id,
        publication.owner_token,
    )
    legacy_staging = deterministic_staging_dir(
        publication.plan.dst_dir,
        publication.queue_id,
    )
    try:
        if os.path.abspath(publication.staging_dir) not in (
            expected_staging,
            legacy_staging,
        ):
            raise PublicationBlocked("journal staging path is not deterministic")
        staging_path = os.path.abspath(publication.staging_dir)
        if os.path.lexists(staging_path):
            staging_stat = os.lstat(staging_path)
            if stat.S_ISLNK(staging_stat.st_mode) or not stat.S_ISDIR(
                staging_stat.st_mode
            ):
                raise PublicationBlocked(
                    "journal staging path is a symlink or non-directory"
                )
            shutil.rmtree(staging_path)
            _fsync_directory(publication.plan.dst_dir)
        staging_removed = True
    except (OSError, PublicationBlocked) as exc:
        diagnostic = str(exc)
    return CleanupResult(tuple(outcomes), staging_removed, diagnostic)


def finalize_publication(
    db: sqlite3.Connection,
    publication_id: int,
    owner_token: str,
    cleanup: CleanupResult,
) -> bool:
    """Record cleanup and finalize/delete queue state in one short transaction."""
    ownership = db.execute(
        "SELECT 1 FROM import_publications"
        " WHERE id=? AND state='cleaning' AND operation_owner=?"
        " AND operation_expires_at > CURRENT_TIMESTAMP",
        (publication_id, owner_token),
    ).fetchone()
    if ownership is None:
        return False
    for outcome in cleanup.outcomes:
        db.execute(
            """
            UPDATE import_publication_files
            SET cleanup_state=?, diagnostic=CASE
                    WHEN ?='' THEN diagnostic ELSE ?
                END,
                cleaned_at=CURRENT_TIMESTAMP
            WHERE publication_id=? AND file_id=?
            """,
            (
                outcome.state,
                outcome.diagnostic,
                outcome.diagnostic,
                publication_id,
                outcome.file_id,
            ),
        )
    blocked_cleanup = any(outcome.state == "blocked" for outcome in cleanup.outcomes)
    if not cleanup.staging_removed or blocked_cleanup:
        db.execute(
            "UPDATE import_publications"
            " SET diagnostic=?, updated_at=CURRENT_TIMESTAMP,"
            " operation_owner=NULL, operation_expires_at=NULL"
            " WHERE id=? AND state='cleaning' AND operation_owner=?",
            (
                cleanup.diagnostic
                or next(
                    (
                        outcome.diagnostic
                        for outcome in cleanup.outcomes
                        if outcome.state == "blocked"
                    ),
                    "cleanup blocked",
                ),
                publication_id,
                owner_token,
            ),
        )
        return False

    row = db.execute(
        "SELECT queue_id, result_queue_status FROM import_publications"
        " WHERE id=? AND state='cleaning' AND operation_owner=?"
        " AND operation_expires_at > CURRENT_TIMESTAMP",
        (publication_id, owner_token),
    ).fetchone()
    if row is None:
        return False
    queue_id = int(row["queue_id"])
    queue_status = str(row["result_queue_status"] or "")
    terminal_state = "finalized"
    if queue_status == "imported":
        db.execute("DELETE FROM import_queue_files WHERE queue_id=?", (queue_id,))
        db.execute("DELETE FROM import_queue WHERE id=?", (queue_id,))
        terminal_state = "deleted"

    cur = db.execute(
        """
        UPDATE import_publications
        SET state=?, finalized_at=CURRENT_TIMESTAMP,
            deleted_at=CASE WHEN ?='deleted' THEN CURRENT_TIMESTAMP ELSE deleted_at END,
            pack_cleanup_state=CASE
                WHEN result_queue_status IN ('imported','failed','skipped')
                     AND queue_download_id IS NOT NULL
                     AND trim(queue_download_id)!=''
                THEN 'pending'
                ELSE 'retained'
            END,
            pack_cleanup_completed_at=NULL,
            updated_at=CURRENT_TIMESTAMP, operation_owner=NULL,
            operation_expires_at=NULL, diagnostic=CASE
                WHEN diagnostic='' THEN ? ELSE diagnostic
            END
        WHERE id=? AND state='cleaning' AND operation_owner=?
        """,
        (
            terminal_state,
            terminal_state,
            cleanup.diagnostic,
            publication_id,
            owner_token,
        ),
    )
    return cur.rowcount == 1


def _claim_staging_recovery(
    publication_id: int,
    recovery_owner: str,
) -> bool:
    """Fence a reversible journal only when its queue has no live worker."""
    if not recovery_owner:
        raise ValueError("recovery_owner must be non-empty")
    lease_modifier = f"+{_OPERATION_LEASE_SECONDS} seconds"
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        cur = db.execute(
            """
            UPDATE import_publications
            SET operation_owner=?, operation_expires_at=datetime('now', ?),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state='staging'
              AND (
                  operation_owner IS NULL
                  OR operation_owner=?
                  OR operation_expires_at IS NULL
                  OR operation_expires_at <= CURRENT_TIMESTAMP
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM import_queue AS queue
                  WHERE queue.id=import_publications.queue_id
                    AND queue.lease_owner IS NOT NULL
                    AND queue.lease_expires_at IS NOT NULL
                    AND queue.lease_expires_at > CURRENT_TIMESTAMP
              )
            """,
            (
                recovery_owner,
                lease_modifier,
                publication_id,
                recovery_owner,
            ),
        )
        return cur.rowcount == 1


def _release_staging_recovery(
    publication_id: int,
    recovery_owner: str,
) -> None:
    """Release a recovery fence after non-destructive filesystem refusal."""
    with get_db() as db:
        db.execute(
            """
            UPDATE import_publications
            SET operation_owner=NULL, operation_expires_at=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND state='staging' AND operation_owner=?
            """,
            (publication_id, recovery_owner),
        )


def abort_staging_publication(
    publication_id: int,
    *,
    release_queue: bool,
    recovery_owner: str | None = None,
) -> bool:
    """Delete a fenced reversible journal after its directory is gone.

    A worker-owned abort is allowed only while the queue owner still matches
    the journal owner. Replay must first claim ``recovery_owner`` while no
    queue lease is live.
    """
    with get_db() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT publication.queue_id, publication.owner_token,
                   publication.operation_owner, queue.id AS queue_exists,
                   queue.lease_owner,
                   CASE
                       WHEN queue.lease_owner IS NOT NULL
                        AND queue.lease_expires_at IS NOT NULL
                        AND queue.lease_expires_at > CURRENT_TIMESTAMP
                       THEN 1 ELSE 0
                   END AS queue_lease_is_live
            FROM import_publications AS publication
            LEFT JOIN import_queue AS queue ON queue.id=publication.queue_id
            WHERE publication.id=? AND publication.state='staging'
            """,
            (publication_id,),
        ).fetchone()
        if row is None:
            return False
        if recovery_owner is None:
            if (
                row["operation_owner"] is not None
                or row["queue_exists"] is None
                or row["lease_owner"] != row["owner_token"]
            ):
                return False
        elif (
            row["operation_owner"] != recovery_owner
            or bool(row["queue_lease_is_live"])
        ):
            return False

        queue_id = int(row["queue_id"])
        cur = db.execute(
            """
            DELETE FROM import_publications
            WHERE id=? AND state='staging' AND operation_owner IS ?
            """,
            (publication_id, recovery_owner),
        )
        if cur.rowcount != 1:
            return False
        if release_queue:
            db.execute(
                """
                UPDATE import_queue
                SET status=CASE WHEN EXISTS (
                        SELECT 1 FROM import_queue_files
                        WHERE queue_id=import_queue.id AND status='needs_review'
                    ) THEN 'partial' ELSE 'pending' END,
                    lease_owner=NULL, lease_expires_at=NULL
                WHERE id=? AND status='importing'
                  AND (
                      lease_owner IS NULL
                      OR lease_expires_at IS NULL
                      OR lease_expires_at <= CURRENT_TIMESTAMP
                  )
                """,
                (queue_id,),
            )
        return True


def remove_staging_directory(publication: ImportPublication) -> bool:
    """Remove a reversible staging directory with strict path validation."""
    expected = deterministic_staging_dir(
        publication.plan.dst_dir,
        publication.queue_id,
        publication.owner_token,
    )
    legacy = deterministic_staging_dir(
        publication.plan.dst_dir,
        publication.queue_id,
    )
    actual = os.path.abspath(publication.staging_dir)
    if actual not in (expected, legacy):
        return False
    if not os.path.lexists(actual):
        return True
    info = os.lstat(actual)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        return False
    shutil.rmtree(actual)
    _fsync_directory(publication.plan.dst_dir)
    return True


def pending_publication_ids(
    db: sqlite3.Connection,
    max_rows: int | None,
    *,
    after_id: int = 0,
    include_terminal: bool = True,
) -> list[int]:
    """Snapshot replayable IDs after a keyset cursor."""
    if max_rows is not None and max_rows <= 0:
        return []
    row_limit = max_rows if max_rows is not None else 9_223_372_036_854_775_807
    rows = db.execute(
        """
        SELECT replay_id
        FROM (
            SELECT p.id AS replay_id
            FROM import_publications AS p
            WHERE p.state IN (
                'staging','prepared','publishing','published',
                'db_committed','cleaning'
            )
               OR (
                   ?
                   AND
                   p.state IN ('finalized','deleted')
                   AND p.pack_cleanup_state='pending'
               )
            UNION
            SELECT d.publication_id AS replay_id
            FROM import_publication_notification_deliveries AS d
            WHERE ?
              AND (
                (
                    d.state='pending'
                    AND (
                        d.next_attempt_at IS NULL
                        OR d.next_attempt_at <= CURRENT_TIMESTAMP
                    )
                )
                OR (
                    d.state='dispatching'
                    AND (
                        d.operation_expires_at IS NULL
                        OR d.operation_expires_at <= CURRENT_TIMESTAMP
                    )
                )
              )
            UNION
            SELECT e.publication_id AS replay_id
            FROM import_publication_success_effects AS e
            WHERE ?
              AND (
                (
                    e.state='pending'
                    AND (
                        e.next_attempt_at IS NULL
                        OR e.next_attempt_at <= CURRENT_TIMESTAMP
                    )
                )
                OR (
                    e.state='dispatching'
                    AND (
                        e.operation_expires_at IS NULL
                        OR e.operation_expires_at <= CURRENT_TIMESTAMP
                    )
                )
              )
        )
        WHERE replay_id > ?
        ORDER BY replay_id
        LIMIT ?
        """,
        (
            include_terminal,
            include_terminal,
            include_terminal,
            after_id,
            row_limit,
        ),
    ).fetchall()
    return [int(row[0]) for row in rows]


def _has_live_operation_owner(publication_id: int) -> bool:
    with get_db() as db:
        return (
            db.execute(
                "SELECT 1 FROM import_publications"
                " WHERE id=? AND operation_owner IS NOT NULL"
                " AND operation_expires_at > CURRENT_TIMESTAMP",
                (publication_id,),
            ).fetchone()
            is not None
        )


def _notification_backoff_seconds(attempt_count: int) -> int:
    exponent = max(0, min(attempt_count - 1, 10))
    return min(
        _NOTIFICATION_MAX_BACKOFF_SECONDS,
        _NOTIFICATION_INITIAL_BACKOFF_SECONDS * (2**exponent),
    )


async def _dispatch_journal_notification(
    publication_id: int,
    owner_token: str | None = None,
) -> bool:
    """Dispatch due connection rows without delaying publication cleanup.

    Each connection owns its lease and retry state. A crash after provider
    acceptance and before that connection's completion CAS remains
    intentionally at-least-once.
    """
    owner = owner_token or secrets.token_urlsafe(32)
    with get_db() as db:
        if _refresh_notification_parent(db, publication_id):
            return True
        connection_ids = [
            int(row["connection_id"])
            for row in db.execute(
                """
                SELECT connection_id
                FROM import_publication_notification_deliveries
                WHERE publication_id=?
                  AND (
                      (
                          state='pending'
                          AND (
                              next_attempt_at IS NULL
                              OR next_attempt_at <= CURRENT_TIMESTAMP
                          )
                      )
                      OR (
                          state='dispatching'
                          AND (
                              operation_expires_at IS NULL
                              OR operation_expires_at <= CURRENT_TIMESTAMP
                          )
                      )
                  )
                ORDER BY connection_id
                """,
                (publication_id,),
            ).fetchall()
        ]
    if not connection_ids:
        return False

    delivery_results = await asyncio.gather(
        *(
            _dispatch_notification_delivery(
                publication_id,
                connection_id,
                owner,
            )
            for connection_id in connection_ids
        ),
        return_exceptions=True,
    )
    for connection_id, delivery_result in zip(
        connection_ids,
        delivery_results,
        strict=True,
    ):
        if isinstance(delivery_result, asyncio.CancelledError):
            raise delivery_result
        if isinstance(delivery_result, BaseException):
            log.warning(
                "Import publication %s connection %s delivery failed (%s)",
                publication_id,
                connection_id,
                type(delivery_result).__name__,
            )
    with get_db() as db:
        return _refresh_notification_parent(db, publication_id)


def _refresh_notification_parent(
    db: sqlite3.Connection,
    publication_id: int,
) -> bool:
    """Project child delivery state onto the publication-level summary row."""
    parent = db.execute(
        "SELECT 1 FROM import_publication_notifications WHERE publication_id=?",
        (publication_id,),
    ).fetchone()
    if parent is None:
        return False
    summary = db.execute(
        """
        SELECT COUNT(*) AS delivery_count,
               COALESCE(SUM(attempt_count), 0) AS attempt_count,
               COALESCE(SUM(CASE WHEN state='completed' THEN 1 ELSE 0 END), 0)
                   AS completed_count,
               COALESCE(SUM(
                   CASE WHEN state='pending' AND last_error!='' THEN 1 ELSE 0 END
               ), 0) AS failed_count,
               MIN(CASE WHEN state='pending' THEN next_attempt_at END)
                   AS next_attempt_at
        FROM import_publication_notification_deliveries
        WHERE publication_id=?
        """,
        (publication_id,),
    ).fetchone()
    delivery_count = int(summary["delivery_count"])
    completed_count = int(summary["completed_count"])
    attempt_count = int(summary["attempt_count"])
    if completed_count == delivery_count:
        db.execute(
            """
            UPDATE import_publication_notifications
            SET state='dispatched', operation_owner=NULL,
                operation_expires_at=NULL, attempt_count=?,
                next_attempt_at=NULL, last_error='',
                dispatched_at=COALESCE(dispatched_at, CURRENT_TIMESTAMP),
                updated_at=CURRENT_TIMESTAMP
            WHERE publication_id=?
            """,
            (attempt_count, publication_id),
        )
        db.execute(
            "UPDATE import_publications SET notification_state='dispatched',"
            " updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (publication_id,),
        )
        return True
    db.execute(
        """
        UPDATE import_publication_notifications
        SET state='pending', operation_owner=NULL, operation_expires_at=NULL,
            attempt_count=?, next_attempt_at=?,
            last_error=CASE WHEN ? > 0
                            THEN 'NotificationDeliveryError' ELSE '' END,
            updated_at=CURRENT_TIMESTAMP
        WHERE publication_id=?
        """,
        (
            attempt_count,
            summary["next_attempt_at"],
            int(summary["failed_count"]),
            publication_id,
        ),
    )
    return False


async def _dispatch_notification_delivery(
    publication_id: int,
    connection_id: int,
    owner_token: str,
) -> bool:
    """Lease and attempt one snapshotted connection delivery."""
    lease_modifier = f"+{_NOTIFICATION_LEASE_SECONDS} seconds"
    with get_db() as db:
        claimed = db.execute(
            """
            UPDATE import_publication_notification_deliveries
            SET state='dispatching', operation_owner=?,
                operation_expires_at=datetime('now', ?),
                attempt_count=attempt_count + 1,
                updated_at=CURRENT_TIMESTAMP
            WHERE publication_id=? AND connection_id=?
              AND (
                  (
                      state='pending'
                      AND (
                          next_attempt_at IS NULL
                          OR next_attempt_at <= CURRENT_TIMESTAMP
                      )
                  )
                  OR (
                      state='dispatching'
                      AND (
                          operation_expires_at IS NULL
                          OR operation_expires_at <= CURRENT_TIMESTAMP
                      )
                  )
              )
            """,
            (owner_token, lease_modifier, publication_id, connection_id),
        )
        if claimed.rowcount != 1:
            return False
        row = db.execute(
            """
            SELECT n.title, n.label, n.cover_url, d.attempt_count,
                   d.connection_name, d.connection_type
            FROM import_publication_notification_deliveries AS d
            JOIN import_publication_notifications AS n
              ON n.publication_id=d.publication_id
            WHERE d.publication_id=? AND d.connection_id=?
              AND d.state='dispatching' AND d.operation_owner=?
            """,
            (publication_id, connection_id, owner_token),
        ).fetchone()
        if row is None:
            return False
        payload = dict(row)

    from notifications import make_complete_embed
    from routers.notification_connections import deliver_notification_connection

    provider = f"{payload['connection_type']} — {payload['connection_name']}"
    try:
        delivery = await deliver_notification_connection(
            connection_id,
            "on_download",
            "",
            embed=make_complete_embed(
                str(payload["title"]),
                str(payload["label"]),
                str(payload["cover_url"] or ""),
            ),
        )
    except Exception as exc:
        error_name = type(exc).__name__
        completion_reason: NotificationCompletionReason | None = None
    else:
        error_name = delivery.error
        completion_reason = (
            cast(NotificationCompletionReason, delivery.outcome)
            if delivery.outcome != "failed"
            else None
        )

    if completion_reason is None:
        backoff = _notification_backoff_seconds(int(payload["attempt_count"]))
        with get_db() as db:
            failed = db.execute(
                """
                UPDATE import_publication_notification_deliveries
                SET state='pending', operation_owner=NULL,
                    operation_expires_at=NULL,
                    next_attempt_at=datetime('now', ?),
                    last_error=?, updated_at=CURRENT_TIMESTAMP
                WHERE publication_id=? AND connection_id=?
                  AND state='dispatching' AND operation_owner=?
                """,
                (
                    f"+{backoff} seconds",
                    error_name or "provider_rejected",
                    publication_id,
                    connection_id,
                    owner_token,
                ),
            )
            if failed.rowcount == 1:
                _refresh_notification_parent(db, publication_id)
        log.warning(
            "Import publication %s notification to %s failed (%s); retry scheduled",
            publication_id,
            provider,
            error_name or "provider_rejected",
        )
        return False

    with get_db() as db:
        completed = db.execute(
            """
            UPDATE import_publication_notification_deliveries
            SET state='completed', completion_reason=?, operation_owner=NULL,
                operation_expires_at=NULL, next_attempt_at=NULL,
                last_error='', completed_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE publication_id=? AND connection_id=?
              AND state='dispatching' AND operation_owner=?
            """,
            (
                completion_reason,
                publication_id,
                connection_id,
                owner_token,
            ),
        )
        if completed.rowcount == 1:
            _refresh_notification_parent(db, publication_id)
    return completed.rowcount == 1


def _success_effect_backoff_seconds(attempt_count: int) -> int:
    exponent = max(0, min(attempt_count - 1, 10))
    return min(
        _SUCCESS_EFFECT_MAX_BACKOFF_SECONDS,
        _SUCCESS_EFFECT_INITIAL_BACKOFF_SECONDS * (2**exponent),
    )


def _terminal_skip_success_effect(
    effect_type: SuccessEffectType,
    reason: str,
) -> bool:
    """Record a deliberate, non-retryable safety skip as completed work."""
    from events import log_event

    message = f"Skipped journaled {effect_type} success effect: {reason}"
    log.warning(message)
    log_event("import_success_effect_skipped", message)
    return True


async def _dispatch_journaled_komga_scan(payload: dict[str, object]) -> bool:
    """Trigger the snapshotted Komga target and expose failures for retry."""
    url_value = payload.get("url", "")
    library_id_value = payload.get("library_id", "")
    fingerprint_value = payload.get("target_fingerprint", "")
    url = url_value if isinstance(url_value, str) else ""
    library_id = library_id_value if isinstance(library_id_value, str) else ""
    if not url or not library_id:
        # Preserve the legacy no-op for an enabled but incomplete integration.
        return True

    import httpx
    from events import log_event

    if not isinstance(fingerprint_value, str) or not fingerprint_value:
        return _terminal_skip_success_effect(
            "komga_scan",
            "the committed Komga target has no credential-owner binding",
        )
    try:
        if _canonical_http_url(url) != url:
            raise ValueError("journaled URL is not canonical")
        current_url = _canonical_http_url(str(get_cfg("komga_url", "") or ""))
    except ValueError:
        return _terminal_skip_success_effect(
            "komga_scan",
            "the configured Komga target is invalid or changed",
        )
    current_library_id = str(get_cfg("komga_library_id", "") or "").strip()
    user = str(get_cfg("komga_user", "") or "")
    current_fingerprint = _komga_target_fingerprint(
        current_url,
        current_library_id,
        user,
    )
    if (
        current_url != url
        or current_library_id != library_id
        or current_fingerprint != fingerprint_value
    ):
        return _terminal_skip_success_effect(
            "komga_scan",
            "the configured URL, library, or credential owner changed",
        )

    # Load the password only after the complete non-secret identity matches.
    password = str(get_cfg("komga_pass", "") or "")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            endpoint = f"{current_url}/api/v1/libraries/{current_library_id}/scan"
            if user:
                response = await client.post(
                    endpoint,
                    auth=httpx.BasicAuth(user, password),
                )
            else:
                response = await client.post(endpoint)
    except Exception as exc:
        error_name = type(exc).__name__
        log_event("error", f"Komga scan attempt failed ({error_name})")
        return False
    if response.is_success:
        log_event(
            "komga_scan",
            f"Triggered Komga library scan \N{RIGHTWARDS ARROW}"
            f" HTTP {response.status_code}",
        )
        return True
    log_event(
        "error",
        f"Komga scan attempt failed (HTTP {response.status_code})",
    )
    return False


async def dispatch_import_success_side_effect(
    effect_type: SuccessEffectType,
    payload: dict[str, object],
) -> bool:
    """Execute one journaled success effect with repeat-safe semantics."""
    if effect_type == "cover":
        series_id_value = payload.get("series_id")
        if (
            not isinstance(series_id_value, int)
            or isinstance(series_id_value, bool)
            or series_id_value <= 0
        ):
            raise ValueError("cover effect requires a positive series_id")
        series_id = series_id_value
        local_cover = f"/config/covers/{series_id}.jpg"
        from cover_images import (
            cached_cover_is_valid,
            download_cover,
            extract_cbz_cover,
        )

        if cached_cover_is_valid(local_cover):
            return True
        first_cbz_value = payload.get("first_cbz", "")
        first_cbz = first_cbz_value if isinstance(first_cbz_value, str) else ""
        if first_cbz:
            dst_dir_value = payload.get("dst_dir", "")
            if not isinstance(dst_dir_value, str) or not dst_dir_value:
                raise ValueError("cover effect requires a destination directory")
            dst_dir = os.path.abspath(dst_dir_value)
            first_cbz = os.path.abspath(first_cbz)
            if not _path_is_below(first_cbz, dst_dir) or os.path.realpath(
                os.path.dirname(first_cbz)
            ) != os.path.realpath(dst_dir):
                raise ValueError("cover effect CBZ escapes its destination directory")
            first_cbz_stat = os.lstat(first_cbz)
            if stat.S_ISLNK(first_cbz_stat.st_mode) or not stat.S_ISREG(
                first_cbz_stat.st_mode
            ):
                raise ValueError("cover effect CBZ is not a regular file")
            extracted = await asyncio.to_thread(
                extract_cbz_cover,
                series_id,
                first_cbz,
            )
            if extracted or cached_cover_is_valid(local_cover):
                return True

        cover_url_value = payload.get("cover_url", "")
        cover_url = cover_url_value if isinstance(cover_url_value, str) else ""
        if not cover_url:
            return True
        result = await download_cover(series_id, cover_url)
        if bool(result.get("ok")):
            return True
        # These outcomes are deterministic for the snapshotted URL/payload.
        # Retrying them would create a permanent hot outbox row.
        return result.get("status") in {
            "missing_url",
            "rejected",
            "invalid_image",
        }

    if effect_type == "komga_scan":
        return await _dispatch_journaled_komga_scan(payload)

    if effect_type == "remove_completed":
        download_id_value = payload.get("download_id")
        if not isinstance(download_id_value, str) or not download_id_value.strip():
            raise ValueError("remove_completed effect requires a download_id")
        protocol_value = payload.get("protocol")
        client_id_value = payload.get("client_id")
        client_type_value = payload.get("client_type")
        client_name_value = payload.get("client_name")
        if (
            not isinstance(protocol_value, str)
            or protocol_value not in {"torrent", "nzb"}
            or not isinstance(client_id_value, int)
            or isinstance(client_id_value, bool)
            or client_id_value <= 0
            or not isinstance(client_type_value, str)
            or client_type_value not in {"qbittorrent", "sabnzbd"}
            or not isinstance(client_name_value, str)
            or not client_name_value
        ):
            return _terminal_skip_success_effect(
                "remove_completed",
                "the committed download-client identity is incomplete",
            )
        expected_type = "qbittorrent" if protocol_value == "torrent" else "sabnzbd"
        if client_type_value != expected_type:
            return _terminal_skip_success_effect(
                "remove_completed",
                "the committed protocol and client type disagree",
            )

        from clients import (
            load_bound_download_client,
            qbit_remove,
            sab_remove,
        )

        loaded = load_bound_download_client(
            client_id_value,
            expected_type=client_type_value,
            expected_name=client_name_value,
        )
        if loaded.client is None:
            return _terminal_skip_success_effect(
                "remove_completed",
                f"download client {client_id_value} is {loaded.reason}",
            )
        if protocol_value == "torrent":
            return bool(
                await qbit_remove(
                    download_id_value,
                    client=loaded.client,
                )
            )
        return bool(
            await sab_remove(
                download_id_value,
                client=loaded.client,
            )
        )

    raise ValueError(f"unsupported import success effect: {effect_type}")


async def _dispatch_success_effect(
    publication_id: int,
    effect_type: SuccessEffectType,
    owner_token: str | None = None,
) -> bool:
    """Lease and attempt one replay-safe success effect outside SQLite."""
    owner = owner_token or secrets.token_urlsafe(32)
    lease_modifier = f"+{_SUCCESS_EFFECT_LEASE_SECONDS} seconds"
    with get_db() as db:
        claimed = db.execute(
            """
            UPDATE import_publication_success_effects
            SET state='dispatching', operation_owner=?,
                operation_expires_at=datetime('now', ?),
                attempt_count=attempt_count + 1,
                updated_at=CURRENT_TIMESTAMP
            WHERE publication_id=? AND effect_type=?
              AND (
                  (
                      state='pending'
                      AND (
                          next_attempt_at IS NULL
                          OR next_attempt_at <= CURRENT_TIMESTAMP
                      )
                  )
                  OR (
                      state='dispatching'
                      AND (
                          operation_expires_at IS NULL
                          OR operation_expires_at <= CURRENT_TIMESTAMP
                      )
                  )
              )
            """,
            (owner, lease_modifier, publication_id, effect_type),
        )
        if claimed.rowcount != 1:
            return False
        row = db.execute(
            """
            SELECT payload_json, attempt_count
            FROM import_publication_success_effects
            WHERE publication_id=? AND effect_type=? AND state='dispatching'
              AND operation_owner=?
            """,
            (publication_id, effect_type, owner),
        ).fetchone()
        if row is None:
            return False
        payload_json = str(row["payload_json"])
        attempt_count = int(row["attempt_count"])

    error_name = ""
    try:
        payload = _json_mapping(payload_json)
        if payload is None:
            raise RuntimeError("success effect payload is null")

        succeeded = await dispatch_import_success_side_effect(
            effect_type,
            payload,
        )
        if not succeeded:
            error_name = "unsuccessful_result"
    except Exception as exc:
        succeeded = False
        error_name = type(exc).__name__

    if not succeeded:
        backoff = _success_effect_backoff_seconds(attempt_count)
        with get_db() as db:
            db.execute(
                """
                UPDATE import_publication_success_effects
                SET state='pending', operation_owner=NULL,
                    operation_expires_at=NULL,
                    next_attempt_at=datetime('now', ?),
                    last_error=?, updated_at=CURRENT_TIMESTAMP
                WHERE publication_id=? AND effect_type=?
                  AND state='dispatching' AND operation_owner=?
                """,
                (
                    f"+{backoff} seconds",
                    error_name,
                    publication_id,
                    effect_type,
                    owner,
                ),
            )
        log.warning(
            "Import publication %s success effect %s failed (%s); retry scheduled",
            publication_id,
            effect_type,
            error_name,
        )
        return False

    with get_db() as db:
        completed = db.execute(
            """
            UPDATE import_publication_success_effects
            SET state='completed', operation_owner=NULL,
                operation_expires_at=NULL, next_attempt_at=NULL,
                last_error='', completed_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            WHERE publication_id=? AND effect_type=? AND state='dispatching'
              AND operation_owner=?
            """,
            (publication_id, effect_type, owner),
        )
    return completed.rowcount == 1


async def _dispatch_success_effects(publication_id: int) -> bool:
    """Attempt every incomplete effect independently for one publication."""
    with get_db() as db:
        effect_types: list[SuccessEffectType] = [
            cast(SuccessEffectType, row["effect_type"])
            for row in db.execute(
                "SELECT effect_type FROM import_publication_success_effects"
                " WHERE publication_id=? AND state!='completed'"
                " ORDER BY effect_type",
                (publication_id,),
            ).fetchall()
        ]
    if not effect_types:
        return True

    results = await asyncio.gather(
        *(
            _dispatch_success_effect(publication_id, effect_type)
            for effect_type in effect_types
        ),
        return_exceptions=True,
    )
    all_completed = True
    for effect_type, result in zip(effect_types, results, strict=True):
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, BaseException):
            log.warning(
                "Import publication %s success effect %s failed (%s)",
                publication_id,
                effect_type,
                type(result).__name__,
            )
            all_completed = False
        else:
            all_completed &= result
    return all_completed


async def complete_publication(
    publication_id: int,
    owner_token: str | None = None,
    *,
    process_terminal: bool = True,
) -> bool:
    """Roll one prepared-or-later journal forward to a terminal state."""
    owner = owner_token or secrets.token_urlsafe(32)
    with get_db() as db:
        publication = load_publication(db, publication_id=publication_id)
    if publication is None:
        return False

    if publication.state in ("prepared", "publishing"):
        published = await asyncio.to_thread(
            publish_publication,
            publication_id,
            owner,
        )
        if not published:
            return False
        with get_db() as db:
            publication = load_publication(db, publication_id=publication_id)
        if publication is None:
            return False

    if publication.state == "published":
        from import_commit import _commit_import
        from import_lease import IMPORT_LEASE_SECONDS

        with get_db() as db:
            commit_result = _commit_import(
                db,
                publication.plan,
                [file_record.outcome for file_record in publication.files],
                fs_committed=True,
                commit_failure_reason="",
                lease_owner=owner,
                lease_seconds=IMPORT_LEASE_SECONDS,
                publication_id=publication_id,
            )
        if commit_result[2] == "journal_claim_lost":
            return False
        with get_db() as db:
            publication = load_publication(db, publication_id=publication_id)
        if publication is None:
            return False

    if publication.state in ("db_committed", "cleaning"):
        with get_db() as db:
            if not claim_publication_cleanup(db, publication_id, owner):
                return False
            publication = load_publication(db, publication_id=publication_id)
        if publication is None:
            return False
        try:
            cleanup = await asyncio.to_thread(
                cleanup_publication_filesystem,
                publication,
                owner,
            )
        except PublicationOwnershipLost:
            return False
        with get_db() as db:
            if not finalize_publication(db, publication_id, owner, cleanup):
                return False
            publication = load_publication(db, publication_id=publication_id)
        if publication is None:
            return False

    if publication.state in ("finalized", "deleted"):
        if not process_terminal:
            return True

        operations: list[tuple[str, asyncio.Task[bool]]] = []

        if publication.pack_cleanup_state == "pending":
            if publication.queue_download_id:
                from import_pack_cleanup import cleanup_terminal_pack_staging

                operations.append(
                    (
                        "pack cleanup",
                        asyncio.create_task(
                            asyncio.to_thread(
                                cleanup_terminal_pack_staging,
                                publication.queue_id,
                                publication.queue_download_id,
                                download_client_id=(
                                    publication.queue_download_client_id
                                ),
                                protocol=publication.queue_download_protocol,
                                publication_id=publication.publication_id,
                            )
                        ),
                    )
                )
        operations.append(
            (
                "notification",
                asyncio.create_task(_dispatch_journal_notification(publication_id)),
            )
        )
        operations.append(
            (
                "success effects",
                asyncio.create_task(_dispatch_success_effects(publication_id)),
            )
        )

        if operations:
            operation_results = await asyncio.gather(
                *(task for _, task in operations),
                return_exceptions=True,
            )
            for (operation, _), operation_result in zip(
                operations,
                operation_results,
                strict=True,
            ):
                if isinstance(operation_result, asyncio.CancelledError):
                    raise operation_result
                if isinstance(operation_result, BaseException):
                    log.warning(
                        "Import publication %s terminal %s failed (%s)",
                        publication_id,
                        operation,
                        type(operation_result).__name__,
                    )
                elif operation_result is False and operation == "pack cleanup":
                    log.info(
                        "Import publication %s terminal %s remains pending",
                        publication_id,
                        operation,
                    )
        # Pack cleanup, notifications, and success effects are independent,
        # durable auxiliary work. Their retry state must not turn an already
        # committed import into a domain failure.
        return True
    return False


async def _replay_publication_id(
    publication_id: int,
    *,
    process_terminal: bool,
) -> Literal["completed", "blocked", "deferred", "aborted_staging"]:
    """Replay one journal ID so cancellation settling stays operation-scoped."""
    with get_db() as db:
        publication = load_publication(db, publication_id=publication_id)
        notification_exists = (
            db.execute(
                "SELECT 1 FROM import_publication_notifications WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            is not None
        )
        success_effects_exist = (
            db.execute(
                "SELECT 1 FROM import_publication_success_effects"
                " WHERE publication_id=? LIMIT 1",
                (publication_id,),
            ).fetchone()
            is not None
        )
    if publication is None:
        if process_terminal and (notification_exists or success_effects_exist):
            operations: list[asyncio.Task[bool]] = []
            if notification_exists:
                operations.append(
                    asyncio.create_task(_dispatch_journal_notification(publication_id))
                )
            if success_effects_exist:
                operations.append(
                    asyncio.create_task(_dispatch_success_effects(publication_id))
                )
            try:
                results = await asyncio.gather(
                    *operations,
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, asyncio.CancelledError):
                        raise result
                    if isinstance(result, BaseException):
                        log.warning(
                            "Import publication %s terminal outbox replay failed (%s)",
                            publication_id,
                            type(result).__name__,
                        )
                if any(result is True for result in results):
                    return "completed"
            except Exception as exc:
                log.warning(
                    "Import publication %s terminal outbox replay failed (%s)",
                    publication_id,
                    type(exc).__name__,
                )
        return "deferred"
    if publication.state == "staging":
        recovery_owner = secrets.token_urlsafe(32)
        if not _claim_staging_recovery(publication_id, recovery_owner):
            return "deferred"
        try:
            removed = await asyncio.to_thread(
                remove_staging_directory,
                publication,
            )
        except Exception:
            _release_staging_recovery(publication_id, recovery_owner)
            log.exception(
                "Import publication %s staging recovery failed",
                publication_id,
            )
            return "blocked"
        if not removed:
            _release_staging_recovery(publication_id, recovery_owner)
            return "blocked"
        if abort_staging_publication(
            publication_id,
            release_queue=True,
            recovery_owner=recovery_owner,
        ):
            return "aborted_staging"
        return "blocked"
    try:
        if await complete_publication(
            publication_id,
            process_terminal=process_terminal,
        ):
            return "completed"
        if _has_live_operation_owner(publication_id):
            return "deferred"
        return "blocked"
    except Exception:
        log.exception("Import publication %s replay failed", publication_id)
        return "blocked"


async def replay_import_publications(
    *,
    max_rows: int | None = 100,
    after_id: int = 0,
    include_terminal: bool = True,
) -> ReplaySummary:
    """Replay one bounded keyset page, settling at most the current ID."""
    global _replay_lock
    if _replay_lock is None:
        _replay_lock = asyncio.Lock()

    summary = ReplaySummary()
    async with _replay_lock:
        with get_db() as db:
            publication_ids = pending_publication_ids(
                db,
                max_rows,
                after_id=after_id,
                include_terminal=include_terminal,
            )
        for publication_id in publication_ids:
            # Give runtime cancellation a delivery point before the next
            # journal operation is created.
            await asyncio.sleep(0)
            summary.examined += 1
            summary.last_id = publication_id
            operation = asyncio.create_task(
                _replay_publication_id(
                    publication_id,
                    process_terminal=include_terminal,
                )
            )
            try:
                outcome = await asyncio.shield(operation)
            except asyncio.CancelledError:
                # Filesystem and Phase 3 work cannot be abandoned halfway
                # through, but only the one in-flight ID is allowed to settle.
                await asyncio.shield(operation)
                raise
            if outcome == "completed":
                summary.completed += 1
            elif outcome == "aborted_staging":
                summary.aborted_staging += 1
            elif outcome == "deferred":
                summary.deferred += 1
            else:
                summary.blocked += 1
    return summary


async def drain_active_import_publications(
    *,
    page_size: int = 100,
) -> ReplaySummary:
    """Drain startup-required active journals in bounded keyset pages."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    total = ReplaySummary()
    cursor = 0
    while True:
        page = await replay_import_publications(
            max_rows=page_size,
            after_id=cursor,
            include_terminal=False,
        )
        total.examined += page.examined
        total.completed += page.completed
        total.blocked += page.blocked
        total.deferred += page.deferred
        total.aborted_staging += page.aborted_staging
        if not page.examined:
            break
        if page.last_id <= cursor:
            raise RuntimeError(
                "startup publication replay keyset cursor did not advance"
            )
        cursor = page.last_id
        total.last_id = cursor
        if page.examined < page_size:
            break
    return total


def _fsync_renamed_directories(source: str, destination: str) -> None:
    """Persist a rename's destination entry, then its source-entry removal."""
    destination_dir = os.path.dirname(os.path.abspath(destination))
    source_dir = os.path.dirname(os.path.abspath(source))
    _fsync_directory(destination_dir)
    if source_dir != destination_dir:
        _fsync_directory(source_dir)


def _fsync_directory(path: str) -> None:
    descriptor = os.open(
        os.path.abspath(path),
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
