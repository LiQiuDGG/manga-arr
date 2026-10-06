"""Series library directory resolution and transaction-safe reconciliation."""

from __future__ import annotations

import ctypes
import errno
import math
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

from comicinfo import build_comicinfo_xml, inject_comicinfo
from events import add_history
from files import (
    MANGA_EXTENSIONS,
    _apply_format_tokens,
    convert_cbr_to_cbz,
    detect_file_type_magic,
    quality_from_filename,
    sanitize_filename,
)
from helpers import _resolve_series_dest_root
from noreplace_fallback import UNSUPPORTED_ERRNOS, rename_noreplace_fallback
from parsing import extract_volume_num, vol_num_to_display
from shared import get_cfg, get_db


_AT_FDCWD = -100
_RENAME_NOREPLACE = 1


class RescanResult(TypedDict):
    found: int
    recovered: int
    missing: int
    lost: int
    created: int


@dataclass(frozen=True)
class SeriesRescanSnapshot:
    series: dict[str, Any]
    series_dir: str | None
    numbered: tuple[dict[str, Any], ...]
    packs: tuple[dict[str, Any], ...]
    chapters: tuple[dict[str, Any], ...]
    chapters_by_volume: dict[int, tuple[dict[str, Any], ...]]


@dataclass(frozen=True)
class FileFingerprint:
    size_bytes: int
    mtime_ns: int
    device: int
    inode: int


@dataclass(frozen=True)
class InventoryFile:
    path: str
    size_bytes: int
    quality: str | None
    imported_at: str
    fingerprint: FileFingerprint


@dataclass(frozen=True)
class SeriesFilesystemInventory:
    files_by_volume: dict[float, InventoryFile]
    any_library_files: bool
    pack_paths_present: dict[int, bool]

    @property
    def on_disk(self) -> frozenset[float]:
        return frozenset(self.files_by_volume)


@dataclass(frozen=True)
class _Reconciliation:
    result: RescanResult
    enrichment_targets: tuple["_EnrichmentTarget", ...] = ()


@dataclass(frozen=True)
class _EnrichmentTarget:
    volume: dict[str, Any]
    volume_num: float
    source_path: str
    source_fingerprint: FileFingerprint


@dataclass(frozen=True)
class _EnrichmentContext:
    series: dict[str, Any]
    tags: tuple[str, ...]


@dataclass(frozen=True)
class _PathClaim:
    original_path: str
    claimed_path: str
    claim_dir: str
    fingerprint: FileFingerprint


_VOLUME_GUARD = (
    "id",
    "series_id",
    "volume_num",
    "status",
    "import_path",
    "download_id",
    "download_client_id",
    "torrent_name",
    "indexer",
    "protocol",
    "client",
    "grabbed_at",
    "imported_at",
    "source_url",
    "release_group",
    "size_bytes",
    "quality",
)
_PACK_GUARD = (
    "id",
    "series_id",
    "volume_num",
    "status",
    "pack_type",
    "vol_range_start",
    "vol_range_end",
    "download_id",
    "download_client_id",
    "import_path",
    "quality",
)
_CHAPTER_GUARD = (
    "id",
    "series_id",
    "volume_id",
    "status",
    "monitored",
    "grabbed_at",
    "torrent_name",
    "torrent_url",
    "indexer",
    "protocol",
    "client",
    "download_id",
    "download_client_id",
    "release_group",
    "import_path",
    "quality",
    "imported_at",
    "size_bytes",
)
_VOLUME_GUARD_SQL = (
    "id IS ? AND series_id IS ? AND volume_num IS ? AND status IS ?"
    " AND import_path IS ? AND download_id IS ? AND download_client_id IS ?"
    " AND torrent_name IS ? AND indexer IS ? AND protocol IS ? AND client IS ?"
    " AND grabbed_at IS ? AND imported_at IS ? AND source_url IS ?"
    " AND release_group IS ? AND size_bytes IS ? AND quality IS ?"
)
_PACK_GUARD_SQL = (
    "id IS ? AND series_id IS ? AND volume_num IS ? AND status IS ?"
    " AND pack_type IS ? AND vol_range_start IS ? AND vol_range_end IS ?"
    " AND download_id IS ? AND download_client_id IS ?"
    " AND import_path IS ? AND quality IS ?"
)
_CHAPTER_GUARD_SQL = (
    "id IS ? AND series_id IS ? AND volume_id IS ? AND status IS ?"
    " AND monitored IS ? AND grabbed_at IS ? AND torrent_name IS ?"
    " AND torrent_url IS ? AND indexer IS ? AND protocol IS ? AND client IS ?"
    " AND download_id IS ? AND download_client_id IS ? AND release_group IS ?"
    " AND import_path IS ? AND quality IS ? AND imported_at IS ?"
    " AND size_bytes IS ?"
)
_RECOVER_VOLUME_SQL = (
    "UPDATE volumes SET status='downloaded',import_path=?,"
    " size_bytes=COALESCE(NULLIF(size_bytes,0),?),quality=COALESCE(quality,?),"
    " imported_at=COALESCE(imported_at,?) WHERE id IS ? AND series_id IS ?"
    " AND volume_num IS ? AND status IS ? AND import_path IS ? AND download_id IS ?"
    " AND download_client_id IS ? AND torrent_name IS ? AND indexer IS ?"
    " AND protocol IS ? AND client IS ? AND grabbed_at IS ? AND imported_at IS ?"
    " AND source_url IS ? AND release_group IS ? AND size_bytes IS ?"
    " AND quality IS ?"
)
_RESET_MISSING_VOLUME_SQL = (
    "UPDATE volumes SET status='wanted',import_path=NULL,download_id=NULL,"
    " download_client_id=NULL,torrent_name=NULL,indexer=NULL,protocol=NULL,"
    " client=NULL,grabbed_at=NULL,imported_at=NULL,source_url=NULL,"
    " release_group=NULL"
    " WHERE id IS ? AND series_id IS ? AND volume_num IS ? AND status IS ?"
    " AND import_path IS ? AND download_id IS ? AND download_client_id IS ?"
    " AND torrent_name IS ? AND indexer IS ? AND protocol IS ?"
    " AND client IS ? AND grabbed_at IS ? AND imported_at IS ?"
    " AND source_url IS ? AND release_group IS ? AND size_bytes IS ?"
    " AND quality IS ?"
)
_MARK_VOLUME_DOWNLOADED_SQL = (
    "UPDATE volumes SET status='downloaded' WHERE id IS ? AND series_id IS ?"
    " AND volume_num IS ? AND status IS ? AND import_path IS ? AND download_id IS ?"
    " AND download_client_id IS ? AND torrent_name IS ? AND indexer IS ?"
    " AND protocol IS ? AND client IS ? AND grabbed_at IS ? AND imported_at IS ?"
    " AND source_url IS ? AND release_group IS ? AND size_bytes IS ?"
    " AND quality IS ?"
)
_MARK_CHAPTER_DOWNLOADED_SQL = (
    "UPDATE chapters SET status=? WHERE id IS ? AND series_id IS ?"
    " AND volume_id IS ? AND status IS ? AND monitored IS ? AND grabbed_at IS ?"
    " AND torrent_name IS ? AND torrent_url IS ? AND indexer IS ? AND protocol IS ?"
    " AND client IS ? AND download_id IS ? AND download_client_id IS ?"
    " AND release_group IS ? AND import_path IS ? AND quality IS ?"
    " AND imported_at IS ? AND size_bytes IS ?"
)
_RESET_MISSING_CHAPTER_SQL = (
    "UPDATE chapters SET status=?,grabbed_at=NULL,torrent_name=NULL,"
    " torrent_url=NULL,indexer=NULL,protocol=NULL,client=NULL,download_id=NULL,"
    " download_client_id=NULL,release_group=NULL"
    " WHERE id IS ? AND series_id IS ? AND volume_id IS ?"
    " AND status IS ? AND monitored IS ? AND grabbed_at IS ?"
    " AND torrent_name IS ? AND torrent_url IS ? AND indexer IS ?"
    " AND protocol IS ? AND client IS ? AND download_id IS ?"
    " AND download_client_id IS ? AND release_group IS ? AND import_path IS ?"
    " AND quality IS ? AND imported_at IS ? AND size_bytes IS ?"
)
_QUALITY_BACKFILL_SQL = """
    UPDATE volumes
    SET quality = CASE
        WHEN LOWER(SUBSTR(import_path, -4)) = '.cbz' THEN 'cbz'
        WHEN LOWER(SUBSTR(import_path, -4)) = '.zip' THEN 'zip'
        WHEN LOWER(SUBSTR(import_path, -4)) = '.cbr' THEN 'cbr'
        WHEN LOWER(SUBSTR(import_path, -4)) = '.rar' THEN 'rar'
        WHEN LOWER(SUBSTR(import_path, -5)) = '.epub' THEN 'epub'
        WHEN LOWER(SUBSTR(import_path, -5)) = '.mobi' THEN 'mobi'
        WHEN LOWER(SUBSTR(import_path, -4)) = '.pdf' THEN 'pdf'
    END
    WHERE quality IS NULL AND
"""
_DELETE_PACK_SQL = (
    "DELETE FROM volumes WHERE id IS ? AND series_id IS ? AND volume_num IS ?"
    " AND status IS ? AND pack_type IS ? AND vol_range_start IS ?"
    " AND vol_range_end IS ? AND download_id IS ? AND download_client_id IS ?"
    " AND import_path IS ? AND quality IS ?"
)
_MARK_PACK_DOWNLOADED_SQL = (
    "UPDATE volumes SET status='downloaded' WHERE id IS ? AND series_id IS ?"
    " AND volume_num IS ? AND status IS ? AND pack_type IS ?"
    " AND vol_range_start IS ? AND vol_range_end IS ? AND download_id IS ?"
    " AND download_client_id IS ? AND import_path IS ? AND quality IS ?"
)
_ENRICHMENT_VOLUME_SELECT = (
    "SELECT id,series_id,volume_num,status,download_id,download_client_id,"
    " torrent_name,indexer,protocol,client,grabbed_at,imported_at,source_url,"
    " release_group,import_path,size_bytes,quality FROM volumes WHERE id=?"
)
_UPDATE_CONVERTED_VOLUME_SQL = (
    "UPDATE volumes SET import_path=?,size_bytes=?,quality='cbz' WHERE "
    + _VOLUME_GUARD_SQL
)


def _empty_result() -> RescanResult:
    return {"found": 0, "recovered": 0, "missing": 0, "lost": 0, "created": 0}


def _series_library_dir(db: sqlite3.Connection, series_id: int) -> str | None:
    """Return the configured library directory for a series."""
    series_row = db.execute(
        "SELECT title, root_folder_id, pub_year, folder_name FROM series WHERE id=?",
        (series_id,),
    ).fetchone()
    if not series_row:
        return None
    series = dict(series_row)
    root_row = (
        db.execute(
            "SELECT path FROM root_folders WHERE id=?",
            (series["root_folder_id"],),
        ).fetchone()
        if series["root_folder_id"]
        else None
    )
    root = dict(root_row) if root_row else None
    dest_root = _resolve_series_dest_root(db, series["root_folder_id"], root)
    folder_name = (series["folder_name"] or "").strip()
    if (
        folder_name
        and folder_name not in (".", "..")
        and os.path.basename(folder_name) == folder_name
        and "/" not in folder_name
        and "\\" not in folder_name
    ):
        safe_dir = folder_name
    else:
        title = series["title"] or "Unknown"
        folder_format = get_cfg("folder_format", "").strip()
        if folder_format:
            safe_dir = _apply_format_tokens(
                folder_format,
                title,
                pub_year=series["pub_year"],
            )
            safe_dir = sanitize_filename(safe_dir)
        else:
            safe_dir = sanitize_filename(title)
    return os.path.join(dest_root, safe_dir)


def snapshot_series_rescan(
    db: sqlite3.Connection, series_id: int
) -> SeriesRescanSnapshot | None:
    """Copy every row needed by a rescan into connection-independent data."""
    started_transaction = not db.in_transaction
    if started_transaction:
        db.execute("BEGIN")
    try:
        series_row = db.execute(
            "SELECT * FROM series WHERE id=? AND deleted_at IS NULL",
            (series_id,),
        ).fetchone()
        if not series_row:
            snapshot = None
        else:
            series = dict(series_row)
            numbered = tuple(
                dict(row)
                for row in db.execute(
                    "SELECT id,series_id,volume_num,status,download_id,"
                    " download_client_id,torrent_name,indexer,protocol,client,"
                    " grabbed_at,imported_at,source_url,release_group,import_path,"
                    " size_bytes,quality"
                    " FROM volumes WHERE series_id=? AND volume_num IS NOT NULL"
                    " ORDER BY id",
                    (series_id,),
                ).fetchall()
            )
            packs = tuple(
                dict(row)
                for row in db.execute(
                    "SELECT id,series_id,volume_num,pack_type,vol_range_start,"
                    " vol_range_end,status,download_id,download_client_id,"
                    " import_path,quality"
                    " FROM volumes WHERE series_id=? AND volume_num IS NULL ORDER BY id",
                    (series_id,),
                ).fetchall()
            )
            chapters = tuple(
                dict(row)
                for row in db.execute(
                    "SELECT id,series_id,volume_id,status,monitored,grabbed_at,"
                    " torrent_name,torrent_url,indexer,protocol,client,download_id,"
                    " download_client_id,release_group,import_path,quality,"
                    " imported_at,size_bytes"
                    " FROM chapters WHERE series_id=? ORDER BY id",
                    (series_id,),
                ).fetchall()
            )
            snapshot = SeriesRescanSnapshot(
                series=series,
                series_dir=_series_library_dir(db, series_id),
                numbered=numbered,
                packs=packs,
                chapters=chapters,
                chapters_by_volume=_index_chapters(chapters),
            )
    except BaseException:
        if started_transaction:
            db.rollback()
        raise
    if started_transaction:
        db.commit()
    return snapshot


def _index_chapters(
    chapters: tuple[dict[str, Any], ...],
) -> dict[int, tuple[dict[str, Any], ...]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for chapter in chapters:
        volume_id = chapter["volume_id"]
        if volume_id is None:
            continue
        grouped.setdefault(int(volume_id), []).append(chapter)
    return {
        volume_id: tuple(volume_chapters)
        for volume_id, volume_chapters in grouped.items()
    }


def _fingerprint(stat_result: os.stat_result) -> FileFingerprint:
    return FileFingerprint(
        size_bytes=stat_result.st_size,
        mtime_ns=stat_result.st_mtime_ns,
        device=stat_result.st_dev,
        inode=stat_result.st_ino,
    )


def build_filesystem_inventory(
    snapshot: SeriesRescanSnapshot,
) -> SeriesFilesystemInventory:
    """Walk a series directory once and finish all filesystem work for reconciliation."""
    files_by_volume: dict[float, InventoryFile] = {}
    any_library_files = False
    imported_at = datetime.utcnow().isoformat()
    preferred_paths = {
        float(volume["volume_num"]): os.path.normcase(
            os.path.abspath(str(volume["import_path"]))
        )
        for volume in snapshot.numbered
        if volume["import_path"]
    }
    series_dir = snapshot.series_dir
    if series_dir and os.path.isdir(series_dir):
        for root, dirs, files in os.walk(series_dir):
            dirs.sort(key=str.casefold)
            for filename in sorted(files, key=str.casefold):
                if os.path.splitext(filename)[1].lower() not in MANGA_EXTENSIONS:
                    continue
                path = os.path.join(root, filename)
                try:
                    stat_result = os.stat(path)
                except OSError:
                    continue
                volume_num = extract_volume_num(filename)
                any_library_files = True
                if volume_num is not None and (
                    volume_num not in files_by_volume
                    or preferred_paths.get(volume_num)
                    == os.path.normcase(os.path.abspath(path))
                ):
                    fingerprint = _fingerprint(stat_result)
                    files_by_volume[volume_num] = InventoryFile(
                        path=path,
                        size_bytes=fingerprint.size_bytes,
                        quality=quality_from_filename(path),
                        imported_at=imported_at,
                        fingerprint=fingerprint,
                    )

    pack_paths_present: dict[int, bool] = {}
    for pack in snapshot.packs:
        import_path = pack["import_path"]
        pack_paths_present[int(pack["id"])] = bool(
            import_path and os.path.exists(import_path)
        )
    return SeriesFilesystemInventory(
        files_by_volume=files_by_volume,
        any_library_files=any_library_files,
        pack_paths_present=pack_paths_present,
    )


def _guard_values(row: dict[str, Any], columns: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row[column] for column in columns)


def _recover_volume(
    db: sqlite3.Connection,
    row: dict[str, Any],
    inventory_file: InventoryFile,
) -> bool:
    cursor = db.execute(
        _RECOVER_VOLUME_SQL,
        (
            inventory_file.path,
            inventory_file.size_bytes,
            inventory_file.quality,
            inventory_file.imported_at,
            *_guard_values(row, _VOLUME_GUARD),
        ),
    )
    return cursor.rowcount == 1


def _reset_missing_volume(
    db: sqlite3.Connection,
    row: dict[str, Any],
) -> bool:
    cursor = db.execute(
        _RESET_MISSING_VOLUME_SQL,
        _guard_values(row, _VOLUME_GUARD),
    )
    return cursor.rowcount == 1


def _cascade_chapter_snapshot(
    db: sqlite3.Connection,
    chapters_by_volume: dict[int, tuple[dict[str, Any], ...]],
    *,
    status: str,
    volume_ids: set[int],
    clear_grab: bool = False,
) -> None:
    for volume_id in volume_ids:
        for chapter in chapters_by_volume.get(volume_id, ()):
            if not chapter["monitored"]:
                continue
            db.execute(
                _RESET_MISSING_CHAPTER_SQL
                if clear_grab
                else _MARK_CHAPTER_DOWNLOADED_SQL,
                (status, *_guard_values(chapter, _CHAPTER_GUARD)),
            )


def _mark_volume_from_pack(
    db: sqlite3.Connection,
    row: dict[str, Any],
) -> bool:
    cursor = db.execute(
        _MARK_VOLUME_DOWNLOADED_SQL,
        _guard_values(row, _VOLUME_GUARD),
    )
    return cursor.rowcount == 1


def _capture_enrichment_target(
    db: sqlite3.Connection,
    volume_id: int,
    volume_num: float,
    inventory_file: InventoryFile,
) -> _EnrichmentTarget | None:
    row = db.execute(_ENRICHMENT_VOLUME_SELECT, (volume_id,)).fetchone()
    if not row:
        return None
    return _EnrichmentTarget(
        volume=dict(row),
        volume_num=volume_num,
        source_path=inventory_file.path,
        source_fingerprint=inventory_file.fingerprint,
    )


def _series_writer_state(
    db: sqlite3.Connection,
    snapshot: SeriesRescanSnapshot,
) -> dict[str, Any] | None:
    series_id = int(snapshot.series["id"])
    current_row = db.execute(
        "SELECT root_folder_id,folder_name,monitor_mode FROM series"
        " WHERE id=? AND deleted_at IS NULL",
        (series_id,),
    ).fetchone()
    if not current_row:
        return None
    current = dict(current_row)
    if (
        current["root_folder_id"] != snapshot.series["root_folder_id"]
        or current["folder_name"] != snapshot.series["folder_name"]
    ):
        return None
    if (
        current["root_folder_id"] is not None
        and not db.execute(
            "SELECT 1 FROM root_folders WHERE id=?",
            (current["root_folder_id"],),
        ).fetchone()
    ):
        return None
    current_dir = _series_library_dir(db, series_id)
    if current_dir is None or snapshot.series_dir is None:
        return current if current_dir == snapshot.series_dir else None
    if os.path.normcase(os.path.abspath(current_dir)) != os.path.normcase(
        os.path.abspath(snapshot.series_dir)
    ):
        return None
    return current


def _backfill_snapshot_quality(
    db: sqlite3.Connection,
    snapshot: SeriesRescanSnapshot,
) -> None:
    guard: tuple[str, ...]
    for volume in (*snapshot.numbered, *snapshot.packs):
        if (
            volume["status"] != "downloaded"
            or volume["quality"] is not None
            or not volume["import_path"]
        ):
            continue
        if volume["volume_num"] is not None:
            guard = _VOLUME_GUARD
            sql = _QUALITY_BACKFILL_SQL + _VOLUME_GUARD_SQL
        else:
            guard = _PACK_GUARD
            sql = _QUALITY_BACKFILL_SQL + _PACK_GUARD_SQL
        db.execute(
            sql,
            _guard_values(volume, guard),
        )


def _record_local_volume_count(
    db: sqlite3.Connection,
    series_id: int,
) -> None:
    max_row = db.execute(
        "SELECT MAX(volume_num) AS m FROM volumes"
        " WHERE series_id=? AND volume_num IS NOT NULL",
        (series_id,),
    ).fetchone()
    if not max_row or max_row["m"] is None:
        return
    new_max = math.ceil(float(max_row["m"]))
    count_row = db.execute(
        "SELECT total_volumes,vol_count_source FROM series WHERE id=?",
        (series_id,),
    ).fetchone()
    if not count_row:
        return
    current = dict(count_row)
    if current["total_volumes"] is not None and new_max <= current["total_volumes"]:
        return

    from metadata_provenance import (
        record_metadata_candidates,
        record_metadata_selections,
    )

    record_metadata_candidates(
        series_id,
        "local",
        {"total_volumes": new_max},
        confidence=1.0,
        db=db,
    )
    lock_row = db.execute(
        "SELECT locked FROM series_metadata_fields"
        " WHERE series_id=? AND field_name='total_volumes'",
        (series_id,),
    ).fetchone()
    count_locked = (
        bool(lock_row["locked"])
        if lock_row is not None
        else current["vol_count_source"] == "manual"
    )
    if count_locked:
        return
    cursor = db.execute(
        "UPDATE series SET total_volumes=?,vol_count_source='local'"
        " WHERE id=? AND total_volumes IS ? AND vol_count_source IS ?",
        (
            new_max,
            series_id,
            current["total_volumes"],
            current["vol_count_source"],
        ),
    )
    if cursor.rowcount == 1:
        record_metadata_selections(
            series_id,
            {"total_volumes": new_max},
            {"total_volumes": "local"},
            locks={"total_volumes": False},
            db=db,
        )


def reconcile_series_inventory(
    db: sqlite3.Connection,
    snapshot: SeriesRescanSnapshot,
    inventory: SeriesFilesystemInventory,
) -> _Reconciliation:
    """Apply one snapshot/inventory pair without performing filesystem I/O."""
    series_id = int(snapshot.series["id"])
    result = _empty_result()
    enrichment_targets: list[_EnrichmentTarget] = []
    result["found"] = len(inventory.on_disk)
    if not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
    writer_state = _series_writer_state(db, snapshot)
    if writer_state is None:
        return _Reconciliation(result)

    for volume in snapshot.numbered:
        volume_num = float(volume["volume_num"])
        inventory_file = inventory.files_by_volume.get(volume_num)
        if volume["status"] == "downloaded" and inventory_file is None:
            if volume["client"] == "suwayomi":
                continue
            if not _reset_missing_volume(db, volume):
                continue
            add_history(
                db,
                "file_deleted",
                series_id,
                str(snapshot.series["title"] or ""),
                f"Vol {vol_num_to_display(volume_num)}",
                source_title=volume["torrent_name"] or "",
                data={"download_client_id": volume["download_client_id"]},
            )
            _cascade_chapter_snapshot(
                db,
                snapshot.chapters_by_volume,
                status="wanted",
                volume_ids={int(volume["id"])},
                clear_grab=True,
            )
            result["missing"] += 1
        elif (
            volume["status"] in ("wanted", "grabbed")
            and inventory_file is not None
            and _recover_volume(db, volume, inventory_file)
        ):
            result["recovered"] += 1
            target = _capture_enrichment_target(
                db,
                int(volume["id"]),
                volume_num,
                inventory_file,
            )
            if target is not None:
                enrichment_targets.append(target)

    for pack in snapshot.packs:
        pack_id = int(pack["id"])
        if pack["status"] == "downloaded":
            if pack["import_path"] and not inventory.pack_paths_present.get(
                pack_id, False
            ):
                cursor = db.execute(
                    _DELETE_PACK_SQL,
                    _guard_values(pack, _PACK_GUARD),
                )
                if cursor.rowcount == 1:
                    result["missing"] += 1
            continue
        if pack["status"] != "grabbed":
            continue

        pack_type = pack["pack_type"] or ""
        confirmed = pack_type == "complete" and inventory.any_library_files
        if (
            pack_type == "volume"
            and pack["vol_range_start"] is not None
            and pack["vol_range_end"] is not None
        ):
            confirmed = any(
                pack["vol_range_start"] <= volume_num <= pack["vol_range_end"]
                for volume_num in inventory.on_disk
            )
        if not confirmed:
            continue
        cursor = db.execute(
            _MARK_PACK_DOWNLOADED_SQL,
            _guard_values(pack, _PACK_GUARD),
        )
        if cursor.rowcount != 1:
            continue

        if pack_type == "complete":
            covered = snapshot.numbered
        else:
            covered = tuple(
                volume
                for volume in snapshot.numbered
                if pack["vol_range_start"]
                <= volume["volume_num"]
                <= pack["vol_range_end"]
            )
        guarded_parent_ids: set[int] = set()
        for volume in covered:
            if _mark_volume_from_pack(db, volume):
                guarded_parent_ids.add(int(volume["id"]))
        _cascade_chapter_snapshot(
            db,
            snapshot.chapters_by_volume,
            status="downloaded",
            volume_ids=guarded_parent_ids,
        )
        result["recovered"] += 1

    stubbed = {float(row["volume_num"]) for row in snapshot.numbered}
    monitor_mode = writer_state["monitor_mode"] or "all"
    monitored = 1 if monitor_mode in ("all", "missing") else 0
    for volume_num in sorted(inventory.on_disk - stubbed):
        inventory_file = inventory.files_by_volume[volume_num]
        cursor = db.execute(
            "INSERT INTO volumes(series_id,volume_num,status,import_path,"
            " size_bytes,quality,imported_at,monitored)"
            " SELECT ?,?,'downloaded',?,?,?,?,?"
            " WHERE NOT EXISTS (SELECT 1 FROM volumes"
            " WHERE series_id=? AND volume_num=?)",
            (
                series_id,
                volume_num,
                inventory_file.path,
                inventory_file.size_bytes,
                inventory_file.quality,
                inventory_file.imported_at,
                monitored,
                series_id,
                volume_num,
            ),
        )
        if cursor.rowcount != 1:
            continue
        result["created"] += 1
        if cursor.lastrowid is not None:
            target = _capture_enrichment_target(
                db,
                int(cursor.lastrowid),
                volume_num,
                inventory_file,
            )
            if target is not None:
                enrichment_targets.append(target)

    if result["created"]:
        _record_local_volume_count(db, series_id)
    _backfill_snapshot_quality(db, snapshot)
    return _Reconciliation(result, tuple(enrichment_targets))


def _current_enrichment_context(
    target: _EnrichmentTarget,
) -> _EnrichmentContext | None:
    with get_db() as db:
        volume_row = db.execute(
            _ENRICHMENT_VOLUME_SELECT,
            (target.volume["id"],),
        ).fetchone()
        if not volume_row or any(
            volume_row[column] != target.volume[column] for column in _VOLUME_GUARD
        ):
            return None
        series_row = db.execute(
            "SELECT id,title,description,status,pub_year,total_volumes,"
            " total_chapters,language,anilist_id FROM series"
            " WHERE id=? AND deleted_at IS NULL",
            (target.volume["series_id"],),
        ).fetchone()
        if not series_row:
            return None
        tags = tuple(
            str(row["tag"])
            for row in db.execute(
                "SELECT tag FROM series_tags WHERE series_id=? ORDER BY tag",
                (target.volume["series_id"],),
            ).fetchall()
        )
        return _EnrichmentContext(dict(series_row), tags)


def _enrichment_context_is_current(
    target: _EnrichmentTarget,
    context: _EnrichmentContext,
) -> bool:
    return _current_enrichment_context(target) == context


def _source_fingerprint_is_current(target: _EnrichmentTarget) -> bool:
    try:
        return _fingerprint(os.stat(target.source_path)) == target.source_fingerprint
    except OSError:
        return False


def _cas_converted_volume(
    target: _EnrichmentTarget,
    converted_path: str,
    converted_size: int,
) -> bool:
    with get_db() as db:
        cursor = db.execute(
            _UPDATE_CONVERTED_VOLUME_SQL,
            (
                converted_path,
                converted_size,
                *_guard_values(target.volume, _VOLUME_GUARD),
            ),
        )
        return cursor.rowcount == 1


def _rename_noreplace(source: str, destination: str) -> bool:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise OSError(errno.ENOSYS, "renameat2 is unavailable")
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
        return True
    error_number = ctypes.get_errno()
    if error_number in UNSUPPORTED_ERRNOS:
        try:
            rename_noreplace_fallback(source, destination)
        except OSError as exc:
            if exc.errno in (errno.EEXIST, errno.ENOTEMPTY):
                return False
            raise
        return True
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        return False
    raise OSError(error_number, os.strerror(error_number), destination)


def _probe_rename_noreplace(directory: str) -> bool:
    try:
        with tempfile.TemporaryDirectory(
            prefix=".mangarr-noreplace-probe-",
            dir=directory,
        ) as probe_dir:
            source = os.path.join(probe_dir, "source")
            occupied = os.path.join(probe_dir, "occupied")
            moved = os.path.join(probe_dir, "moved")
            with open(source, "xb") as probe_file:
                probe_file.write(b"source")
            with open(occupied, "xb") as probe_file:
                probe_file.write(b"occupied")
            if _rename_noreplace(source, occupied):
                return False
            if not os.path.isfile(source) or not os.path.isfile(occupied):
                return False
            if not _rename_noreplace(source, moved):
                return False
            return os.path.isfile(moved) and not os.path.exists(source)
    except OSError:
        return False


def _restore_claim(claim: _PathClaim) -> bool:
    try:
        restored = _rename_noreplace(claim.claimed_path, claim.original_path)
    except OSError:
        return False
    if not restored:
        return False
    try:
        os.rmdir(claim.claim_dir)
    except OSError:
        pass
    return True


def _discard_claim(claim: _PathClaim) -> bool:
    try:
        os.unlink(claim.claimed_path)
    except OSError:
        return False
    else:
        try:
            os.rmdir(claim.claim_dir)
        except OSError:
            pass
    return True


def _claim_exact_path(
    path: str,
    expected_fingerprint: FileFingerprint,
) -> _PathClaim | None:
    claim_dir = tempfile.mkdtemp(
        prefix=".mangarr-claim-",
        dir=os.path.dirname(path) or ".",
    )
    claimed_path = os.path.join(claim_dir, os.path.basename(path))
    renamed = False
    try:
        os.rename(path, claimed_path)
        renamed = True
        claim = _PathClaim(
            original_path=path,
            claimed_path=claimed_path,
            claim_dir=claim_dir,
            fingerprint=_fingerprint(os.stat(claimed_path)),
        )
    except OSError:
        if renamed:
            _restore_claim(
                _PathClaim(
                    original_path=path,
                    claimed_path=claimed_path,
                    claim_dir=claim_dir,
                    fingerprint=expected_fingerprint,
                )
            )
        else:
            try:
                os.rmdir(claim_dir)
            except OSError:
                pass
        return None
    if claim.fingerprint != expected_fingerprint:
        _restore_claim(claim)
        return None
    return claim


def _publish_no_replace(
    staged_path: str,
    destination: str,
) -> FileFingerprint | None:
    try:
        fingerprint = _fingerprint(os.stat(staged_path))
        published = _rename_noreplace(staged_path, destination)
    except OSError:
        return None
    if not published:
        return None
    return fingerprint


def _remove_exact_artifact(
    path: str,
    fingerprint: FileFingerprint,
) -> None:
    try:
        claim = _claim_exact_path(path, fingerprint)
        if claim is not None:
            _discard_claim(claim)
    except Exception:
        return


def _stage_and_enrich_target(
    target: _EnrichmentTarget,
    context: _EnrichmentContext,
) -> None:
    if not _source_fingerprint_is_current(target):
        return
    source_dir = os.path.dirname(target.source_path) or "."
    if not _probe_rename_noreplace(source_dir):
        return
    with tempfile.TemporaryDirectory(
        prefix=".mangarr-rescan-",
        dir=source_dir,
    ) as stage_dir:
        staged_source = os.path.join(stage_dir, os.path.basename(target.source_path))
        shutil.copy2(target.source_path, staged_source)
        file_type = detect_file_type_magic(staged_source)
        staged_publish = staged_source
        converted_path: str | None = None
        if file_type == "cbr":
            staged_converted = convert_cbr_to_cbz(staged_source)
            if not staged_converted:
                return
            staged_publish = staged_converted
            converted_path = os.path.splitext(target.source_path)[0] + ".cbz"

        xml_content = build_comicinfo_xml(
            context.series,
            volume_num=target.volume_num,
            tags=list(context.tags),
        )
        if not inject_comicinfo(staged_publish, xml_content):
            return
        if not _enrichment_context_is_current(
            target, context
        ) or not _source_fingerprint_is_current(target):
            return

        if converted_path is not None and os.path.abspath(
            converted_path
        ) == os.path.abspath(target.source_path):
            return
        source_claim = _claim_exact_path(
            target.source_path,
            target.source_fingerprint,
        )
        if source_claim is None:
            return
        published_fingerprint: FileFingerprint | None = None
        succeeded = False
        try:
            if not _enrichment_context_is_current(target, context):
                return
            destination = converted_path or target.source_path
            published_fingerprint = _publish_no_replace(
                staged_publish,
                destination,
            )
            if published_fingerprint is None:
                return
            if converted_path is None:
                if not _enrichment_context_is_current(target, context):
                    return
                succeeded = True
                return
            if not _cas_converted_volume(
                target,
                converted_path,
                published_fingerprint.size_bytes,
            ):
                return
            succeeded = True
        finally:
            if succeeded:
                _discard_claim(source_claim)
            else:
                try:
                    if published_fingerprint is not None:
                        _remove_exact_artifact(
                            converted_path or target.source_path,
                            published_fingerprint,
                        )
                finally:
                    _restore_claim(source_claim)


def enrich_reconciled_files(reconciliation: _Reconciliation) -> None:
    """Best-effort post-commit archive conversion and ComicInfo enrichment."""
    for target in reconciliation.enrichment_targets:
        try:
            context = _current_enrichment_context(target)
            if context is not None:
                _stage_and_enrich_target(target, context)
        except Exception:
            continue


def rescan_series_folder(series_id: int) -> RescanResult:
    """Snapshot, inventory, and reconcile one series using short DB contexts."""
    with get_db() as db:
        snapshot = snapshot_series_rescan(db, series_id)
    if snapshot is None:
        return _empty_result()

    inventory = build_filesystem_inventory(snapshot)
    with get_db() as db:
        reconciliation = reconcile_series_inventory(db, snapshot, inventory)
    enrich_reconciled_files(reconciliation)
    return reconciliation.result
