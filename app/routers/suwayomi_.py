"""Suwayomi download client integration — GraphQL API (v2+).

Multi-source support: works with any Suwayomi extension (MangaDex, Manga Plus,
ComicWalker, etc.), not just MangaDex. Source linkages stored in suwayomi_sources table.

Flow:
  1. find_or_add_manga()     → locate/add manga in Suwayomi library (any source)
  2. fetch_chapters()        → pull chapter list from source
  3. suwayomi_grab()         → resolve volume→chapters, enqueue + start downloader
  4. check_suwayomi_jobs()   → poll isDownloaded on tracked chapters, mark complete
"""

import asyncio as _aio
import json
import logging
import os
import re
import unicodedata

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from shared import get_cfg, get_db, timed_block

log = logging.getLogger(__name__)
router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _swy_base(c: dict) -> str:
    from routers.download_clients import client_base_url

    return client_base_url(c).rstrip("/")


def _auth(c: dict):
    u = c.get("username") or ""
    p = c.get("password") or ""
    return (u, p) if u else None


async def _gql(c: dict, query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL operation against Suwayomi and return data dict."""
    base = _swy_base(c)
    auth = _auth(c)
    async with httpx.AsyncClient(
        timeout=30, auth=auth, headers={"User-Agent": "Mangarr/1.0"}
    ) as cli:
        r = await cli.post(
            f"{base}/api/graphql",
            json={"query": query, "variables": variables or {}},
        )
        r.raise_for_status()
        payload = r.json()
        if "errors" in payload:
            msgs = "; ".join(e.get("message", str(e)) for e in payload["errors"])
            raise RuntimeError(f"GraphQL: {msgs}")
        return payload.get("data") or {}

async def _start_downloader(c: dict, chapters: list[dict]) -> None:
    """Start Suwayomi's downloader for chapters just enqueued.

    Suwayomi drops chapters it already has from the queue at once, and startDownloader then blocks until it
    times out (30 s), so skip the call when every chapter is already downloaded. Any other failure is logged
    rather than raised: the chapters are enqueued, and check_suwayomi_jobs() imports them once downloaded.
    """
    if chapters and all(ch.get("isDownloaded") for ch in chapters):
        return
    try:
        await _gql(c, "mutation { startDownloader(input: {}) { clientMutationId } }")
    except Exception as e:
        log.warning("Suwayomi startDownloader failed (chapters stay queued): %s", e)



def get_suwayomi_client(db) -> dict | None:
    row = db.execute(
        "SELECT * FROM download_clients WHERE type='suwayomi' AND enabled=1"
        " ORDER BY priority, id LIMIT 1"
    ).fetchone()
    if not row:
        return None
    from routers.download_clients import _row_decrypted

    return _row_decrypted(row)


# ── Source classification ─────────────────────────────────────────────────────

SOURCE_CLASSIFICATION: dict[str, str] = {
    "manga plus": "official",
    "mangaplus": "official",
    "manga up": "official",
    "mangaup": "official",
    "comicwalker": "official",
    "comic walker": "official",
    "webtoons": "official",
    "comikey": "official",
    "bilibili": "official",
    "azuki": "official",
    "j-manga": "official",
    "kodansha": "official",
    "shonen jump": "official",
    "viz": "official",
    "mangadex": "aggregator",
}


def classify_source(name: str) -> str:
    """Classify a Suwayomi source as official/aggregator/fan."""
    name_lower = name.lower()
    for pattern, stype in SOURCE_CLASSIFICATION.items():
        if pattern in name_lower:
            return stype
    return "fan"


# ── Connection test ───────────────────────────────────────────────────────────


async def test_connection(c: dict) -> tuple[bool, str]:
    try:
        data = await _gql(c, "{ sources { nodes { id name lang } } }")
        nodes = data.get("sources", {}).get("nodes", [])
        official = [
            s for s in nodes if classify_source(s.get("name", "")) == "official"
        ]
        return True, f"Connected · {len(nodes)} sources ({len(official)} official)"
    except Exception as e:
        return False, str(e)


# ── Source resolution ─────────────────────────────────────────────────────────

_SOURCE_CACHE: dict[str, str] = {}  # "base:source_name:lang" → source_id


async def get_source_id(c: dict, source_name: str, lang: str = "en") -> str | None:
    """Return the Suwayomi source ID for a named source in the requested language."""
    cache_key = f"{_swy_base(c)}:{source_name.lower()}:{lang}"
    if cache_key in _SOURCE_CACHE:
        return _SOURCE_CACHE[cache_key]

    data = await _gql(c, "{ sources { nodes { id name lang } } }")
    nodes = data.get("sources", {}).get("nodes", [])
    target = source_name.lower()

    # Exact language match first
    for s in nodes:
        if target in s.get("name", "").lower() and s.get("lang", "") == lang:
            _SOURCE_CACHE[cache_key] = s["id"]
            return s["id"]
    # Fall back to English
    for s in nodes:
        if target in s.get("name", "").lower() and s.get("lang", "") == "en":
            _SOURCE_CACHE[f"{_swy_base(c)}:{target}:en"] = s["id"]
            return s["id"]
    return None


# Backward-compat alias
async def get_mangadex_source_id(c: dict, lang: str = "en") -> str | None:
    return await get_source_id(c, "mangadex", lang)


# ── Series-to-source linkage ─────────────────────────────────────────────────


def _get_series_source(series_id: int, series_row: dict) -> dict | None:
    """Get the best Suwayomi source for a series.
    Checks suwayomi_sources table first, falls back to mangadex_id for backward compat.
    Returns dict with source_name, source_id, suwayomi_manga_id or None.
    """
    with get_db() as db:
        src = db.execute(
            "SELECT * FROM suwayomi_sources WHERE series_id=? ORDER BY priority ASC LIMIT 1",
            (series_id,),
        ).fetchone()
    if src:
        return dict(src)
    # Backward compat: series has mangadex_id but no suwayomi_sources row yet
    if series_row.get("mangadex_id"):
        return {
            "source_name": "MangaDex",
            "source_id": "mangadex",
            "suwayomi_manga_id": series_row.get("suwayomi_id"),
        }
    return None


def _store_source_linkage(
    series_id: int,
    source_name: str,
    source_id_str: str,
    manga_id: int,
    url: str | None = None,
):
    """Store/update the suwayomi_sources linkage."""
    source_type = classify_source(source_name)
    with get_db() as db:
        db.execute(
            """
            INSERT INTO suwayomi_sources
                (series_id, source_id, source_name, suwayomi_manga_id, source_manga_url, source_type)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(series_id, source_id) DO UPDATE SET
                suwayomi_manga_id=excluded.suwayomi_manga_id,
                source_manga_url=COALESCE(excluded.source_manga_url, source_manga_url)
        """,
            (series_id, source_id_str, source_name, manga_id, url, source_type),
        )
        db.execute("UPDATE series SET suwayomi_id=? WHERE id=?", (manga_id, series_id))


def _titles_match(a: str, b: str) -> bool:
    """Fuzzy title comparison — normalize and compare."""

    def _norm(s):
        s = unicodedata.normalize("NFKC", s.lower().strip())
        s = re.sub(r"[^\w\s]", "", s)
        s = re.sub(r"\s+", " ", s)
        return s

    return _norm(a) == _norm(b)


def _best_title_match(results: list[dict], title: str) -> int | None:
    """Find the best matching manga from search results by title similarity."""
    if not results:
        return None
    for m in results:
        if _titles_match(title, m.get("title", "")):
            return int(m["id"])
    if len(results) == 1:
        return int(results[0]["id"])
    return None


async def _search_source(c: dict, swy_source_id: str, query: str) -> list[dict]:
    """Search a Suwayomi source and return manga results."""
    data = await _gql(
        c,
        """
        mutation($src: LongString!, $q: String!, $p: Int!) {
            fetchSourceManga(input: {source: $src, type: SEARCH, query: $q, page: $p}) {
                mangas { id url title thumbnailUrl }
            }
        }
    """,
        {"src": swy_source_id, "q": query, "p": 1},
    )
    return (data.get("fetchSourceManga") or {}).get("mangas") or []


# ── Manga library management ──────────────────────────────────────────────────


async def find_or_add_manga(
    c: dict,
    series_id: int,
    title: str,
    lang: str = "en",
    mangadex_uuid: str | None = None,
    source_name: str = "mangadex",
) -> int:
    """
    Return Suwayomi manga ID for a series on the specified source.
    Strategy:
      1. Check suwayomi_sources table for existing linkage
      2. Check Suwayomi library (MangaDex UUID fast path or title match)
      3. Search source by title
      4. Add to library and store linkage
    """
    # 1. Check existing linkage
    with get_db() as db:
        existing = db.execute(
            "SELECT suwayomi_manga_id FROM suwayomi_sources"
            " WHERE series_id=? AND LOWER(source_name)=LOWER(?) AND suwayomi_manga_id IS NOT NULL",
            (series_id, source_name),
        ).fetchone()
    if existing and existing["suwayomi_manga_id"]:
        return existing["suwayomi_manga_id"]

    # 2. Check Suwayomi library
    data = await _gql(
        c, "{ mangas(condition: {inLibrary: true}) { nodes { id url title } } }"
    )
    swy_source_id: str | None = None
    manga_id: int | None = None
    for m in data.get("mangas", {}).get("nodes", []):
        # MangaDex fast path: match by UUID in URL
        if mangadex_uuid and mangadex_uuid in (m.get("url") or ""):
            manga_id = int(m["id"])
            swy_source_id = await get_source_id(c, source_name, lang) or source_name
            _store_source_linkage(
                series_id, source_name, swy_source_id, manga_id, m.get("url")
            )
            return manga_id
        # Generic path: title match in library
        if not mangadex_uuid and _titles_match(title, m.get("title", "")):
            manga_id = int(m["id"])
            swy_source_id = await get_source_id(c, source_name, lang) or source_name
            _store_source_linkage(
                series_id, source_name, swy_source_id, manga_id, m.get("url")
            )
            return manga_id

    # 3. Search source
    swy_source_id = await get_source_id(c, source_name, lang)
    if not swy_source_id:
        raise RuntimeError(f"No {source_name} source for lang={lang!r}")

    search_results = await _search_source(c, swy_source_id, title)

    manga_id = None
    if mangadex_uuid:
        # MangaDex: match by UUID in URL
        for m in search_results:
            if mangadex_uuid in (m.get("url") or ""):
                manga_id = int(m["id"])
                break
    if manga_id is None:
        # Generic: best title match
        manga_id = _best_title_match(search_results, title)

    if manga_id is None:
        raise RuntimeError(
            f"Manga {title!r} not found in Suwayomi source {source_name}"
        )

    # 4. Add to library
    await _gql(
        c,
        """
        mutation($id: Int!) {
            updateManga(input: {id: $id, patch: {inLibrary: true}}) { clientMutationId }
        }
    """,
        {"id": manga_id},
    )

    url = next((m.get("url") for m in search_results if int(m["id"]) == manga_id), None)
    _store_source_linkage(series_id, source_name, swy_source_id, manga_id, url)
    return manga_id


# ── Chapter management ────────────────────────────────────────────────────────


async def fetch_chapters(c: dict, manga_id: int) -> list[dict]:
    """Fetch/refresh chapters from source. Returns full chapter list."""
    data = await _gql(
        c,
        """
        mutation($mid: Int!) {
            fetchChapters(input: {mangaId: $mid}) {
                chapters {
                    id chapterNumber name sourceOrder isDownloaded
                }
            }
        }
    """,
        {"mid": manga_id},
    )
    return (data.get("fetchChapters") or {}).get("chapters") or []


def _vol_from_name(name: str | None) -> float | None:
    """Extract volume number from chapter name like 'Vol.1 Ch.2 - Title'."""
    m = re.search(r"Vol\.(\d+(?:\.\d+)?)", name or "", re.IGNORECASE)
    return float(m.group(1)) if m else None


def _chapters_for_volume(
    chapters: list[dict], volume_num: float, series_id: int | None = None
) -> list[dict]:
    """
    Filter chapters belonging to a given volume.
    1. Primary:  parse 'Vol.X' from chapter name (source-agnostic).
    2. Fallback: use chapter_vol_map JSON from series table (source-agnostic).
    3. Fallback: look up mangadex_chapters table (MangaDex-specific).
    """
    # 1. Parse from chapter name
    matched = [
        ch
        for ch in chapters
        if (v := _vol_from_name(ch.get("name"))) is not None
        and abs(v - volume_num) < 0.1
    ]
    if matched:
        return matched

    if series_id is None:
        return []

    # 2. Use chapter_vol_map JSON (works for any source)
    with get_db() as db:
        s_row = db.execute(
            "SELECT chapter_vol_map FROM series WHERE id=?", (series_id,)
        ).fetchone()
    if s_row and s_row["chapter_vol_map"]:
        try:
            cvm = json.loads(s_row["chapter_vol_map"])
            ch_nums = {
                float(k) for k, v in cvm.items() if abs(float(v) - volume_num) < 0.1
            }
            if ch_nums:
                return [
                    ch
                    for ch in chapters
                    if ch.get("chapterNumber") is not None
                    and float(ch["chapterNumber"]) in ch_nums
                ]
        except Exception:
            pass

    # 3. MangaDex chapters table fallback
    with get_db() as db:
        rows = db.execute(
            "SELECT chapter_num FROM mangadex_chapters WHERE series_id=? AND volume_num=?",
            (series_id, volume_num),
        ).fetchall()

    if not rows:
        return []

    ch_nums = {float(r["chapter_num"]) for r in rows if r["chapter_num"] is not None}
    return [
        ch
        for ch in chapters
        if ch.get("chapterNumber") is not None and float(ch["chapterNumber"]) in ch_nums
    ]


# ── Filesystem helpers ───────────────────────────────────────────────────────


def _swy_library_base(c: dict) -> str | None:
    """Return the host-visible path to Suwayomi's download root directory.
    Configured via the download_path field on the Suwayomi client.
    e.g.  Suwayomi container sees  /manga
          Mangarr container sees    /data/media/manga  → user sets this
    """
    return (c.get("download_path") or "").strip() or None


def _normalise_dir_name(name: str) -> str:
    """Collapse all non-alphanumeric chars to spaces for fuzzy matching."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", name.lower())).strip()


def _find_suwayomi_manga_dir(c: dict, *titles: str) -> str | None:
    """Find the host-visible download directory for a manga title.
    Structure: {library_base}/mangas/{source_name}/{manga_title}/
    Accepts multiple candidate titles (e.g. Suwayomi title + Mangarr title)
    and tries exact match first, then normalised fuzzy match.
    """
    base = _swy_library_base(c)
    if not base:
        return None
    mangas_root = os.path.join(base, "mangas")
    if not os.path.isdir(mangas_root):
        return None

    # Collect all candidate directories
    source_dirs = [
        os.path.join(mangas_root, sd)
        for sd in os.listdir(mangas_root)
        if os.path.isdir(os.path.join(mangas_root, sd))
    ]

    # Pass 1: exact match on any title
    for t in titles:
        if not t:
            continue
        for sd in source_dirs:
            manga_dir = os.path.join(sd, t)
            if os.path.isdir(manga_dir):
                return manga_dir

    # Pass 2: normalised match (handles : → _, etc.)
    norm_titles = [_normalise_dir_name(t) for t in titles if t]
    for sd in source_dirs:
        for entry in os.listdir(sd):
            entry_path = os.path.join(sd, entry)
            if not os.path.isdir(entry_path):
                continue
            norm_entry = _normalise_dir_name(entry)
            for nt in norm_titles:
                if nt == norm_entry or nt in norm_entry or norm_entry in nt:
                    return entry_path

    return None


def _ch_sort_key(path: str) -> float:
    m = re.search(r"Ch\.(\d+(?:\.\d+)?)", os.path.basename(path), re.IGNORECASE)
    return float(m.group(1)) if m else 9999


def _vol_chapter_cbzs(manga_dir: str, volume_num: float) -> list[str]:
    """Return sorted list of chapter CBZ paths belonging to a volume."""
    vol_int = int(volume_num)
    frac = volume_num - vol_int
    # Use exact match for non-integer volumes (e.g. Vol.1.5), word boundary for integers
    vol_pat = rf"Vol\.{volume_num}" if frac else rf"Vol\.{vol_int}\b"
    paths = [
        os.path.join(manga_dir, fname)
        for fname in os.listdir(manga_dir)
        if fname.lower().endswith(".cbz") and re.search(vol_pat, fname, re.IGNORECASE)
    ]
    return sorted(paths, key=_ch_sort_key)


def _chapter_cbz(manga_dir: str, chapter_num: float) -> str | None:
    """Find the CBZ file for a specific chapter number in manga_dir."""
    ch_int = int(chapter_num)
    frac = chapter_num - ch_int
    # Match exact chapter: Ch.5 for integer, Ch.5.5 for decimal
    pattern = rf"Ch\.{ch_int}\b" if frac == 0 else rf"Ch\.{chapter_num}"
    for fname in os.listdir(manga_dir):
        if not fname.lower().endswith(".cbz"):
            continue
        if re.search(pattern, fname, re.IGNORECASE):
            return os.path.join(manga_dir, fname)
    return None


def _merge_cbzs(chapter_paths: list[str], output_path: str) -> int:
    """Merge ordered chapter CBZ files into a single volume CBZ.
    Returns total byte size of the output file, or 0 on failure.
    """
    import zipfile as _zf

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    page = 1
    try:
        with _zf.ZipFile(output_path, "w", _zf.ZIP_STORED) as out:
            for ch_path in chapter_paths:
                with _zf.ZipFile(ch_path, "r") as ch:
                    images = sorted(
                        [
                            n
                            for n in ch.namelist()
                            if os.path.splitext(n)[1].lower()
                            in (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif")
                            and not os.path.basename(n).startswith(".")
                        ]
                    )
                    for img in images:
                        ext = os.path.splitext(img)[1]
                        with ch.open(img) as f:
                            out.writestr(f"{page:04d}{ext}", f.read())
                        page += 1
        return os.path.getsize(output_path)
    except Exception as e:
        log.error("_merge_cbzs → %s: %s", output_path, e)
        try:
            os.remove(output_path)
        except OSError:
            pass
        return 0


# ── Main grab entry point ─────────────────────────────────────────────────────


def _ddl_enabled() -> bool:
    """Return False if the user has turned DDL off globally."""
    return get_cfg("ddl_grab_mode", "fallback") != "off"


async def suwayomi_grab(series_id: int, volume_num: float) -> bool:
    """
    Queue a volume download via Suwayomi DDL.
    Returns True if queued successfully (or already queued).
    """
    import main as _m

    if not _ddl_enabled():
        return False

    with get_db() as db:
        s = db.execute("SELECT * FROM series WHERE id=?", (series_id,)).fetchone()
        c = get_suwayomi_client(db)
        # Duplicate guard — don't re-queue an already-active job
        existing = db.execute(
            "SELECT id FROM suwayomi_downloads"
            " WHERE series_id=? AND volume_num=? AND status IN ('queued','error')",
            (series_id, volume_num),
        ).fetchone()

    if not c or not s:
        return False
    source_info = _get_series_source(series_id, dict(s))
    if not source_info:
        log.debug("Series %d has no Suwayomi source — skipping DDL", series_id)
        return False
    if existing:
        log.debug(
            "suwayomi_grab: active job already exists for series %d vol %s",
            series_id,
            volume_num,
        )
        return True  # already in queue, treat as success

    sd = dict(s)
    lang = sd.get("ddl_language") or get_cfg("ddl_language", "en")
    src_name = source_info.get("source_name", "MangaDex")

    try:
        manga_id = await find_or_add_manga(
            c,
            series_id,
            sd["title"],
            lang,
            mangadex_uuid=sd.get("mangadex_id"),
            source_name=src_name,
        )
        chapters = await fetch_chapters(c, manga_id)
        vol_chs = _chapters_for_volume(chapters, volume_num, series_id)

        if not vol_chs:
            log.warning(
                "No chapters found in Suwayomi for series %d vol %s",
                series_id,
                volume_num,
            )
            return False

        chapter_ids = [ch["id"] for ch in vol_chs]

        await _gql(
            c,
            """
            mutation($ids: [Int!]!) {
                enqueueChapterDownloads(input: {ids: $ids}) { clientMutationId }
            }
        """,
            {"ids": chapter_ids},
        )
        await _start_downloader(c, vol_chs)

        with get_db() as db:
            source_url = (
                source_info.get("source_manga_url")
                if source_info and source_info.get("source_manga_url")
                else f"suwayomi:{manga_id}"
            )
            torrent_name = f"Suwayomi DDL: {sd['title']} vol {volume_num:g}"
            db.execute(
                "UPDATE volumes SET status='grabbed', grabbed_at=CURRENT_TIMESTAMP,"
                " source_url=?, torrent_name=?,"
                " indexer='Suwayomi', protocol='ddl', client='suwayomi'"
                " WHERE series_id=? AND volume_num=? AND status='wanted'",
                (source_url, torrent_name, series_id, volume_num),
            )
            cur = db.execute(
                "INSERT INTO suwayomi_downloads"
                "(series_id, volume_num, suwayomi_manga_id, chapter_ids, status, total)"
                " VALUES(?,?,?,?,?,?)",
                (
                    series_id,
                    volume_num,
                    manga_id,
                    json.dumps(chapter_ids),
                    "queued",
                    len(chapter_ids),
                ),
            )
            db.execute(
                "UPDATE series SET suwayomi_id=? WHERE id=?", (manga_id, series_id)
            )

        vol_label = _m.build_volume_label(volume_num, None, None)
        _m.log_event(
            "grab",
            f"DDL queued: {sd['title']} vol {volume_num} ({len(chapter_ids)} chapters via Suwayomi)",
            series_id,
        )
        with get_db() as db:
            _m.add_history(
                db,
                "grabbed",
                series_id,
                sd["title"],
                vol_label,
                client="suwayomi",
                protocol="ddl",
            )
        return True

    except Exception as e:
        log.error(
            "suwayomi_grab series=%d vol=%s: %s",
            series_id,
            volume_num,
            e,
            exc_info=True,
        )
        return False


async def suwayomi_chapter_grab(series_id: int, chapter_num: float) -> bool:
    """Queue a single chapter download via Suwayomi DDL for ongoing/uncollected chapters."""
    import main as _m

    if not _ddl_enabled():
        return False

    with get_db() as db:
        s = db.execute("SELECT * FROM series WHERE id=?", (series_id,)).fetchone()
        c = get_suwayomi_client(db)
        existing = db.execute(
            "SELECT id FROM suwayomi_downloads"
            " WHERE series_id=? AND chapter_num=? AND status IN ('queued','error')",
            (series_id, chapter_num),
        ).fetchone()

    if not c or not s:
        return False
    sd = dict(s)
    source_info = _get_series_source(series_id, sd)
    if not source_info:
        log.debug(
            "Series %d has no Suwayomi source — skipping DDL chapter grab", series_id
        )
        return False
    if existing:
        log.debug(
            "suwayomi_chapter_grab: active job already exists for series %d ch %s",
            series_id,
            chapter_num,
        )
        return True

    lang = sd.get("ddl_language") or get_cfg("ddl_language", "en")
    src_name = source_info.get("source_name", "MangaDex")

    try:
        manga_id = await find_or_add_manga(
            c,
            series_id,
            sd["title"],
            lang,
            mangadex_uuid=sd.get("mangadex_id"),
            source_name=src_name,
        )
        chapters = await fetch_chapters(c, manga_id)

        matched = [
            ch
            for ch in chapters
            if ch.get("chapterNumber") is not None
            and abs(float(ch["chapterNumber"]) - chapter_num) < 0.01
        ]
        if not matched:
            log.warning(
                "Chapter %.3g not found in Suwayomi for series %d",
                chapter_num,
                series_id,
            )
            return False

        chapter_ids = [matched[0]["id"]]

        await _gql(
            c,
            """
            mutation($ids: [Int!]!) {
                enqueueChapterDownloads(input: {ids: $ids}) { clientMutationId }
            }
        """,
            {"ids": chapter_ids},
        )
        await _start_downloader(c, matched)

        with get_db() as db:
            torrent_name = f"Suwayomi DDL: {sd['title']} ch {chapter_num:g}"
            db.execute(
                "UPDATE chapters SET status='grabbed', grabbed_at=CURRENT_TIMESTAMP,"
                " torrent_name=?, torrent_url=?,"
                " indexer='Suwayomi', protocol='ddl', client='suwayomi'"
                " WHERE series_id=? AND chapter_num=? AND status='wanted'",
                (torrent_name, f"suwayomi:{manga_id}", series_id, chapter_num),
            )
            db.execute(
                "INSERT INTO suwayomi_downloads"
                "(series_id, chapter_num, suwayomi_manga_id, chapter_ids, status, total)"
                " VALUES(?,?,?,?,?,?)",
                (
                    series_id,
                    chapter_num,
                    manga_id,
                    json.dumps(chapter_ids),
                    "queued",
                    1,
                ),
            )
            db.execute(
                "UPDATE series SET suwayomi_id=? WHERE id=?", (manga_id, series_id)
            )

        ch_label = (
            f"Ch {int(chapter_num)}"
            if chapter_num == int(chapter_num)
            else f"Ch {chapter_num}"
        )
        _m.log_event(
            "grab",
            f"DDL queued: {sd['title']} ch {chapter_num} via Suwayomi",
            series_id,
        )
        with get_db() as db:
            _m.add_history(
                db,
                "grabbed",
                series_id,
                sd["title"],
                ch_label,
                client="suwayomi",
                protocol="ddl",
            )
        return True

    except Exception as e:
        log.error(
            "suwayomi_chapter_grab series=%d ch=%s: %s",
            series_id,
            chapter_num,
            e,
            exc_info=True,
        )
        return False


# ── Import completed DDL download into managed library ────────────────────────


def _should_merge(c: dict) -> bool:
    """Return True if chapter CBZs should be merged into a single volume CBZ."""
    return bool(c.get("merge_chapters", 1))


async def _import_suwayomi_volume(
    c: dict,
    series_id: int,
    volume_num: float,
    *,
    swy_title: str = "",
) -> tuple[str | None, int]:
    """Import completed volume download into the managed library.
    If merge_chapters is enabled (default): merges chapter CBZs into one volume CBZ.
    If disabled: copies individual chapter CBZs to a volume subdirectory.
    Returns (import_path, size_bytes).
    """
    import main as _m

    with get_db() as db:
        s_row = db.execute("SELECT * FROM series WHERE id=?", (series_id,)).fetchone()
        series_dir = _m._series_library_dir(db, series_id) if s_row else None

    if not s_row:
        return None, 0

    manga_dir = _find_suwayomi_manga_dir(c, swy_title, s_row["title"])
    if not manga_dir:
        log.warning(
            "Suwayomi library base not configured — skipping import series %d vol %s",
            series_id,
            volume_num,
        )
        return None, 0

    chapter_paths = _vol_chapter_cbzs(manga_dir, volume_num)
    if not chapter_paths:
        log.warning(
            "No chapter CBZs found for series %d vol %s in %s",
            series_id,
            volume_num,
            manga_dir,
        )
        return None, 0

    safe_title = _m.sanitize_filename(s_row["title"])
    vol_str = (
        str(int(volume_num)).zfill(2)
        if volume_num == int(volume_num)
        else str(volume_num)
    )

    if _should_merge(c):
        # ── Merge all chapters into one volume CBZ ────────────────────────────
        out_name = f"{safe_title} v{vol_str}.cbz"
        out_path = os.path.join(series_dir, out_name) if series_dir else None
        if not out_path:
            return None, 0
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            return out_path, os.path.getsize(out_path)
        log.info("Merging %d chapter(s) → %s", len(chapter_paths), out_path)
        size = _merge_cbzs(chapter_paths, out_path)
        return (out_path, size) if size else (None, 0)
    else:
        # ── Copy individual chapter CBZs to a volume subdirectory ─────────────
        vol_dir = os.path.join(series_dir, f"v{vol_str}") if series_dir else None
        if not vol_dir:
            return None, 0
        os.makedirs(vol_dir, exist_ok=True)
        total_size = 0
        import shutil

        for src in chapter_paths:
            dst = os.path.join(vol_dir, os.path.basename(src))
            if not os.path.exists(dst):
                shutil.copy2(src, dst)
            total_size += os.path.getsize(dst)
        # Return the directory path as import_path when not merging
        return (vol_dir, total_size) if total_size else (None, 0)


async def _import_suwayomi_chapter(
    c: dict,
    series_id: int,
    chapter_num: float,
    *,
    swy_title: str = "",
) -> tuple[str | None, int]:
    """Import a single downloaded chapter CBZ into the managed library.
    Individual chapters are always kept as individual files (merge doesn't apply).
    Returns (import_path, size_bytes).
    """
    import main as _m

    with get_db() as db:
        s_row = db.execute("SELECT * FROM series WHERE id=?", (series_id,)).fetchone()
        series_dir = _m._series_library_dir(db, series_id) if s_row else None

    if not s_row:
        return None, 0

    manga_dir = _find_suwayomi_manga_dir(c, swy_title, s_row["title"])
    if not manga_dir:
        return None, 0

    src = _chapter_cbz(manga_dir, chapter_num)
    if not src:
        log.warning(
            "Chapter CBZ not found for series %d ch %s in %s",
            series_id,
            chapter_num,
            manga_dir,
        )
        return None, 0

    if not series_dir:
        return src, os.path.getsize(
            src
        )  # keep in Suwayomi dir if no library configured

    safe_title = _m.sanitize_filename(s_row["title"])
    ch_str = (
        str(int(chapter_num)).zfill(3)
        if chapter_num == int(chapter_num)
        else str(chapter_num)
    )
    out_name = f"{safe_title} Ch{ch_str}.cbz"
    out_path = os.path.join(series_dir, out_name)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path, os.path.getsize(out_path)

    import shutil

    os.makedirs(series_dir, exist_ok=True)
    shutil.copy2(src, out_path)
    return out_path, os.path.getsize(out_path)


# ── Background job checker ────────────────────────────────────────────────────


async def check_suwayomi_jobs():
    """
    Called periodically from main.check_download_status().
    Polls each active suwayomi_downloads job and marks complete when done.
    """
    with timed_block("check_suwayomi_jobs"):
        return await _check_suwayomi_jobs_impl()


async def _check_suwayomi_jobs_impl():
    """Inner body (wrapped for timing — issue #31 follow-up A)."""
    import main as _m

    with get_db() as db:
        jobs = db.execute(
            "SELECT * FROM suwayomi_downloads WHERE status='queued'"
        ).fetchall()
        if not jobs:
            return
        c = get_suwayomi_client(db)

    if not c:
        return

    for job in jobs:
        # Retry transient failures up to 3 times before marking 'error'.
        # A brief Suwayomi GraphQL timeout used to leave user-initiated
        # DDL grabs stranded until an operator hit /retry manually.
        _last_exc: Exception | None = None
        for _attempt in range(3):
            if _attempt > 0:
                await _aio.sleep(2**_attempt)  # 2s, 4s
            try:
                await _process_suwayomi_job(c, job)
                _last_exc = None
                break
            except Exception as e:
                _last_exc = e
                log.warning(
                    "check_suwayomi_jobs job=%d attempt=%d: %s",
                    job["id"],
                    _attempt + 1,
                    e,
                )
        if _last_exc is not None:
            log.error(
                "check_suwayomi_jobs job=%d: all retries exhausted: %s",
                job["id"],
                _last_exc,
            )
            try:
                with get_db() as db:
                    db.execute(
                        "UPDATE suwayomi_downloads SET status='error', error=? WHERE id=?",
                        (f"{type(_last_exc).__name__}: {_last_exc}"[:500], job["id"]),
                    )
            except Exception:
                pass
    return


async def _process_suwayomi_job(c: dict, job) -> None:
    """Per-job body extracted from check_suwayomi_jobs so the retry loop
    can call it. Raises on failure; caller decides whether to retry or
    mark the job errored."""
    import main as _m

    chapter_ids = json.loads(job["chapter_ids"])

    # Fetch manga title + chapters for the manga
    data = await _gql(
        c,
        """
        query($mid: Int!) {
            manga(id: $mid) {
                title
                chapters { nodes { id isDownloaded } }
            }
        }
    """,
        {"mid": job["suwayomi_manga_id"]},
    )
    swy_title = (data.get("manga") or {}).get("title") or ""

    ch_map: dict[int, bool] = {
        int(ch["id"]): bool(ch["isDownloaded"])
        for ch in (data.get("manga") or {}).get("chapters", {}).get("nodes") or []
    }
    done = sum(1 for cid in chapter_ids if ch_map.get(cid, False))

    with get_db() as db:
        db.execute(
            "UPDATE suwayomi_downloads SET progress=? WHERE id=?",
            (done, job["id"]),
        )

    if done < len(chapter_ids):
        return  # still downloading — recheck next cycle

    if job["chapter_num"] is not None:
        # ── Chapter-level job ─────────────────────────────────────
        import_path, file_bytes = await _import_suwayomi_chapter(
            c,
            job["series_id"],
            float(job["chapter_num"]),
            swy_title=swy_title,
        )
        if not import_path:
            err_msg = "Import failed — chapter CBZ not found in library path"
            with get_db() as db:
                db.execute(
                    "UPDATE suwayomi_downloads SET status='error', error=? WHERE id=?",
                    (err_msg, job["id"]),
                )
            log.error(
                "DDL chapter import failed: series=%d ch=%s",
                job["series_id"],
                job["chapter_num"],
            )
            return
        with get_db() as db:
            db.execute(
                "UPDATE suwayomi_downloads SET status='completed' WHERE id=?",
                (job["id"],),
            )
            db.execute(
                "UPDATE chapters SET status='downloaded',"
                " imported_at=CURRENT_TIMESTAMP, client='suwayomi',"
                " import_path=?, quality=COALESCE(quality,?),"
                " size_bytes=COALESCE(NULLIF(size_bytes,0),?),"
                " indexer='Suwayomi', protocol='ddl', torrent_name=COALESCE(torrent_name,?),"
                " torrent_url=NULL, download_id=NULL, release_group=NULL"
                " WHERE series_id=? AND chapter_num=?"
                " AND status IN ('grabbed','wanted','downloaded')",
                (
                    import_path,
                    _m.quality_from_filename(import_path),
                    file_bytes or None,
                    f"Suwayomi DDL: ch {float(job['chapter_num']):g}",
                    job["series_id"],
                    job["chapter_num"],
                ),
            )
        _m.log_event(
            "download_complete",
            f"DDL imported: series {job['series_id']} ch {job['chapter_num']}"
            + (f" → {os.path.basename(import_path)}" if import_path else ""),
            job["series_id"],
        )
        ch_num = float(job["chapter_num"])
        ch_label = f"Ch {int(ch_num)}" if ch_num == int(ch_num) else f"Ch {ch_num}"
        with get_db() as db:
            s_row = db.execute(
                "SELECT title FROM series WHERE id=?", (job["series_id"],)
            ).fetchone()
            s_title = s_row["title"] if s_row else ""
            _m.add_history(
                db,
                "imported",
                job["series_id"],
                s_title,
                ch_label,
                source_title=os.path.basename(import_path) if import_path else "",
                client="suwayomi",
                protocol="ddl",
                size_bytes=file_bytes or 0,
            )
    else:
        # ── Volume-level job ──────────────────────────────────────
        import_path, file_bytes = await _import_suwayomi_volume(
            c,
            job["series_id"],
            job["volume_num"],
            swy_title=swy_title,
        )
        if not import_path:
            err_msg = "Import failed — CBZ files not found in library path"
            with get_db() as db:
                db.execute(
                    "UPDATE suwayomi_downloads SET status='error', error=? WHERE id=?",
                    (err_msg, job["id"]),
                )
            log.error(
                "DDL volume import failed: series=%d vol=%s",
                job["series_id"],
                job["volume_num"],
            )
            return
        with get_db() as db:
            db.execute(
                "UPDATE suwayomi_downloads SET status='completed' WHERE id=?",
                (job["id"],),
            )
            db.execute(
                "UPDATE volumes SET status='downloaded',"
                " imported_at=CURRENT_TIMESTAMP,"
                " client='suwayomi', import_path=?,"
                " quality=COALESCE(quality,?),"
                " size_bytes=COALESCE(NULLIF(size_bytes,0),?),"
                " indexer='Suwayomi', protocol='ddl', torrent_name=COALESCE(torrent_name,?),"
                " download_id=NULL, release_group=NULL"
                " WHERE series_id=? AND volume_num=?"
                " AND status IN ('grabbed','wanted','downloaded')",
                (
                    import_path,
                    _m.quality_from_filename(import_path),
                    file_bytes or None,
                    f"Suwayomi DDL: vol {float(job['volume_num']):g}",
                    job["series_id"],
                    job["volume_num"],
                ),
            )
        _m.log_event(
            "download_complete",
            f"DDL imported: series {job['series_id']} vol {job['volume_num']}"
            + (f" → {os.path.basename(import_path)}" if import_path else ""),
            job["series_id"],
        )
        vol_label = _m.build_volume_label(job["volume_num"], None, None)
        with get_db() as db:
            s_row = db.execute(
                "SELECT title FROM series WHERE id=?", (job["series_id"],)
            ).fetchone()
            s_title = s_row["title"] if s_row else ""
            _m.add_history(
                db,
                "imported",
                job["series_id"],
                s_title,
                vol_label,
                source_title=os.path.basename(import_path) if import_path else "",
                client="suwayomi",
                protocol="ddl",
                size_bytes=file_bytes or 0,
            )


# ── Monitoring loop ───────────────────────────────────────────────────────────


async def _suwayomi_sync_series(c: dict, s: dict) -> tuple[int, int]:
    """
    Sync one series against Suwayomi's live chapter feed.
    - Grabs wanted volumes whose chapters are now available.
    - Grabs wanted uncollected chapters (volume_id IS NULL) now available.
    - Discovers chapters Suwayomi knows about that have no stub yet → creates them.
    Returns (volumes_grabbed, chapters_grabbed).
    """
    import main as _m

    lang = s.get("ddl_language") or get_cfg("ddl_language", "en")

    source_info = _get_series_source(s["id"], dict(s))
    if not source_info:
        return 0, 0
    src_name = source_info.get("source_name", "MangaDex")

    manga_id = await find_or_add_manga(
        c,
        s["id"],
        s["title"],
        lang,
        mangadex_uuid=s.get("mangadex_id"),
        source_name=src_name,
    )
    chapters = await fetch_chapters(c, manga_id)

    if not chapters:
        return 0, 0

    # Build a map of chapter_num → suwayomi chapter for fast lookup
    swy_by_num: dict[float, dict] = {}
    for ch in chapters:
        cn = ch.get("chapterNumber")
        if cn is not None:
            try:
                swy_by_num[float(cn)] = ch
            except (TypeError, ValueError):
                pass

    if not swy_by_num:
        return 0, 0

    vol_grabbed = ch_grabbed = 0
    series_id = s["id"]

    with get_db() as db:
        # ── 1. Discover new chapters ───────────────────────────────────────────
        # Only for actively-releasing series — finished series won't get new chapters
        if (s.get("status") or "").upper() in ("RELEASING", "HIATUS"):
            existing_nums = {
                float(r["chapter_num"])
                for r in db.execute(
                    "SELECT chapter_num FROM chapters WHERE series_id=?", (series_id,)
                ).fetchall()
            }
            vol_id_map: dict[float, int] = {
                float(r["volume_num"]): r["id"]
                for r in db.execute(
                    "SELECT id, volume_num FROM volumes"
                    " WHERE series_id=? AND volume_num IS NOT NULL",
                    (series_id,),
                ).fetchall()
            }
            try:
                ch_vol_map: dict = (
                    json.loads(s["chapter_vol_map"]) if s.get("chapter_vol_map") else {}
                )
            except Exception:
                ch_vol_map = {}

            newly_created = 0
            for ch_num, swy_ch in swy_by_num.items():
                if ch_num in existing_nums:
                    continue
                # Determine volume assignment
                vol_num = (
                    ch_vol_map.get(str(int(ch_num)))
                    if ch_num == int(ch_num)
                    else ch_vol_map.get(str(ch_num))
                )
                if vol_num is None:
                    # Try extracting from the Suwayomi chapter name (e.g. "Vol.3 Ch.21")
                    vol_num = _vol_from_name(swy_ch.get("name"))
                if vol_num is not None:
                    vol_num = float(vol_num)
                vol_id = vol_id_map.get(vol_num) if vol_num is not None else None

                cur = db.execute(
                    "INSERT OR IGNORE INTO chapters"
                    "(series_id, volume_id, chapter_num, status, monitored)"
                    " VALUES(?,?,?,'wanted',1)",
                    (series_id, vol_id, ch_num),
                )
                if cur.rowcount:
                    newly_created += 1
            if newly_created:
                log.info(
                    "suwayomi_monitor series %d: discovered %d new chapter(s)",
                    series_id,
                    newly_created,
                )

        # ── 2. Wanted volumes ──────────────────────────────────────────────────
        wanted_vols = db.execute(
            "SELECT volume_num FROM volumes WHERE series_id=? AND status='wanted'"
            " AND monitored=1 AND volume_num IS NOT NULL",
            (series_id,),
        ).fetchall()

        # ── 3. Wanted uncollected chapters ────────────────────────────────────
        wanted_chs = db.execute(
            "SELECT chapter_num FROM chapters WHERE series_id=? AND status='wanted'"
            " AND monitored=1 AND volume_id IS NULL",
            (series_id,),
        ).fetchall()

    # Grab wanted volumes whose chapters are available in Suwayomi
    for row in wanted_vols:
        vol_num = float(row["volume_num"])
        vol_chs = _chapters_for_volume(chapters, vol_num, series_id)
        if vol_chs:
            ok = await suwayomi_grab(series_id, vol_num)
            if ok:
                vol_grabbed += 1

    # Grab wanted uncollected chapters available in Suwayomi
    for row in wanted_chs:
        ch_num = float(row["chapter_num"])
        if ch_num in swy_by_num:
            ok = await suwayomi_chapter_grab(series_id, ch_num)
            if ok:
                ch_grabbed += 1

    return vol_grabbed, ch_grabbed


async def suwayomi_monitor_loop():
    """
    Periodic loop: scan all monitored series and auto-grab wanted items via Suwayomi DDL.

    Interval is controlled by the 'suwayomi_check_interval' setting (seconds, default 6h).
    Only runs when a Suwayomi client is configured and enabled.

    What it does each pass:
      1. For RELEASING/HIATUS series: detects new chapters from Suwayomi's source feed
         and creates chapter stubs in the DB for any not yet tracked.
      2. For all monitored series: grabs wanted volumes when their chapters are available.
      3. For all monitored series: grabs wanted uncollected chapters when available.
    """
    import main as _m

    await _aio.sleep(180)  # startup delay — let other loops and DB init settle
    while True:
        try:
            if not _ddl_enabled():
                interval = max(3600, int(get_cfg("suwayomi_check_interval", "21600")))
                await _aio.sleep(interval)
                continue

            with get_db() as db:
                c = get_suwayomi_client(db)

            if c:
                with get_db() as db:
                    candidates = db.execute(
                        "SELECT s.id, s.title, s.mangadex_id, s.ddl_language,"
                        " s.chapter_vol_map, s.status"
                        " FROM series s"
                        " WHERE s.monitored=1"
                        " AND (s.mangadex_id IS NOT NULL"
                        "      OR EXISTS (SELECT 1 FROM suwayomi_sources ss"
                        "                 WHERE ss.series_id=s.id))"
                    ).fetchall()

                vol_total = ch_total = 0
                for row in candidates:
                    try:
                        vg, cg = await _suwayomi_sync_series(c, dict(row))
                        vol_total += vg
                        ch_total += cg
                    except Exception as e:
                        log.warning(
                            "suwayomi_monitor series %d (%s): %s",
                            row["id"],
                            row["title"],
                            e,
                        )
                    await _aio.sleep(3)  # rate-limit: ~20 series/min

                if vol_total or ch_total:
                    _m.log_event(
                        "suwayomi_sync",
                        f"Suwayomi monitor: {vol_total} vol(s), {ch_total} chapter(s) queued",
                    )
        except _aio.CancelledError:
            log.error(
                "suwayomi_monitor_loop: Loop cancelled during shutdown", exc_info=True
            )
            raise
        except Exception as e:
            log.error("suwayomi_monitor_loop: %s", e, exc_info=True)

        interval = max(3600, int(get_cfg("suwayomi_check_interval", "21600")))
        try:
            await _aio.sleep(interval)
        except _aio.CancelledError:
            log.error(
                "suwayomi_monitor_loop: Loop cancelled during shutdown", exc_info=True
            )
            raise


# ── API endpoints ─────────────────────────────────────────────────────────────


@router.get("/api/suwayomi/sources")
async def list_sources():
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        data = await _gql(c, "{ sources { nodes { id name lang } } }")
        sources = data.get("sources", {}).get("nodes", [])
        return JSONResponse({"ok": True, "sources": sources})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/jobs/{job_id}/retry")
async def retry_suwayomi_job(job_id: int):
    """Reset a failed DDL job back to queued and re-enqueue downloads in Suwayomi."""
    with get_db() as db:
        job = db.execute(
            "SELECT * FROM suwayomi_downloads WHERE id=? AND status='error'", (job_id,)
        ).fetchone()
        if not job:
            return JSONResponse(
                {"ok": False, "message": "Job not found or not in error state"},
                status_code=404,
            )
        c = get_suwayomi_client(db)
        db.execute(
            "UPDATE suwayomi_downloads SET status='queued', error=NULL, progress=0 WHERE id=?",
            (job_id,),
        )
        # Reset volume/chapter back to grabbed so the completion UPDATE will match
        if job["volume_num"] is not None:
            db.execute(
                "UPDATE volumes SET status='grabbed',"
                " grabbed_at=COALESCE(grabbed_at,CURRENT_TIMESTAMP),"
                " source_url=COALESCE(source_url,?),"
                " torrent_name=COALESCE(torrent_name,?),"
                " indexer=COALESCE(indexer,'Suwayomi'),"
                " protocol=COALESCE(protocol,'ddl'), client='suwayomi'"
                " WHERE series_id=? AND volume_num=?"
                " AND status!='downloaded'",
                (
                    f"suwayomi:{job['suwayomi_manga_id']}",
                    f"Suwayomi DDL: vol {float(job['volume_num']):g}",
                    job["series_id"],
                    job["volume_num"],
                ),
            )
        elif job["chapter_num"] is not None:
            db.execute(
                "UPDATE chapters SET status='grabbed',"
                " grabbed_at=COALESCE(grabbed_at,CURRENT_TIMESTAMP),"
                " torrent_url=COALESCE(torrent_url,?),"
                " torrent_name=COALESCE(torrent_name,?),"
                " indexer=COALESCE(indexer,'Suwayomi'),"
                " protocol=COALESCE(protocol,'ddl'), client='suwayomi'"
                " WHERE series_id=? AND chapter_num=?"
                " AND status!='downloaded'",
                (
                    f"suwayomi:{job['suwayomi_manga_id']}",
                    f"Suwayomi DDL: ch {float(job['chapter_num']):g}",
                    job["series_id"],
                    job["chapter_num"],
                ),
            )

    # Re-enqueue the chapter downloads in Suwayomi so they actually retry
    if c:
        try:
            chapter_ids = json.loads(job["chapter_ids"]) if job["chapter_ids"] else []
            if chapter_ids:
                await _gql(
                    c,
                    """
                    mutation($ids: [Int!]!) {
                        enqueueChapterDownloads(input: {ids: $ids}) { clientMutationId }
                    }
                """,
                    {"ids": chapter_ids},
                )
                await _gql(
                    c, "mutation { startDownloader(input: {}) { clientMutationId } }"
                )
        except Exception as e:
            log.warning(
                "retry_suwayomi_job: re-enqueue failed for job %d: %s", job_id, e
            )

    return JSONResponse({"ok": True})


@router.post("/api/suwayomi/jobs/{job_id}/cancel")
async def cancel_suwayomi_job(job_id: int):
    """Cancel a queued/errored DDL job and reset the volume/chapter back to wanted."""
    with get_db() as db:
        job = db.execute(
            "SELECT * FROM suwayomi_downloads WHERE id=? AND status IN ('queued','error')",
            (job_id,),
        ).fetchone()
        if not job:
            return JSONResponse(
                {"ok": False, "message": "Job not found or already completed"},
                status_code=404,
            )
        db.execute("DELETE FROM suwayomi_downloads WHERE id=?", (job_id,))
        if job["volume_num"] is not None:
            db.execute(
                "UPDATE volumes SET status='wanted', client=NULL, grabbed_at=NULL,"
                " source_url=NULL, torrent_name=NULL, indexer=NULL, protocol=NULL"
                " WHERE series_id=? AND volume_num=? AND status='grabbed' AND client='suwayomi'",
                (job["series_id"], job["volume_num"]),
            )
        elif job["chapter_num"] is not None:
            db.execute(
                "UPDATE chapters SET status='wanted', client=NULL, grabbed_at=NULL,"
                " torrent_url=NULL, torrent_name=NULL, indexer=NULL, protocol=NULL"
                " WHERE series_id=? AND chapter_num=? AND status='grabbed' AND client='suwayomi'",
                (job["series_id"], job["chapter_num"]),
            )
    return JSONResponse({"ok": True})


@router.post("/api/series/{series_id}/suwayomi/match")
async def match_series_endpoint(series_id: int):
    with get_db() as db:
        s = db.execute("SELECT * FROM series WHERE id=?", (series_id,)).fetchone()
        c = get_suwayomi_client(db)
    if not c or not s:
        return JSONResponse({"ok": False, "message": "Not found"}, status_code=404)
    try:
        lang = s["ddl_language"] or get_cfg("ddl_language", "en")
        source_info = _get_series_source(series_id, dict(s))
        src_name = source_info["source_name"] if source_info else "MangaDex"
        manga_id = await find_or_add_manga(
            c,
            series_id,
            s["title"],
            lang,
            mangadex_uuid=s.get("mangadex_id"),
            source_name=src_name,
        )
        return JSONResponse({"ok": True, "suwayomi_id": manga_id})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@router.post("/api/series/{series_id}/suwayomi/grab/{volume_num:float}")
async def grab_volume_endpoint(series_id: int, volume_num: float):
    ok = await suwayomi_grab(series_id, volume_num)
    if ok:
        return JSONResponse({"ok": True})
    return JSONResponse(
        {"ok": False, "message": "DDL grab failed — check logs"}, status_code=500
    )


@router.get("/api/suwayomi/downloads")
async def list_downloads():
    with get_db() as db:
        rows = db.execute(
            "SELECT sd.*, s.title FROM suwayomi_downloads sd"
            " JOIN series s ON s.id=sd.series_id"
            " ORDER BY sd.created_at DESC LIMIT 100"
        ).fetchall()
    return JSONResponse({"downloads": [dict(r) for r in rows]})


# ── Extension management ─────────────────────────────────────────────────────

RECOMMENDED_EXTENSIONS = [
    {"pattern": "mangadex", "reason": "Largest fan translation library"},
    {"pattern": "mangaplus", "reason": "Official Shueisha (Shonen Jump, etc.)"},
    {"pattern": "comicwalker", "reason": "Official Kadokawa publisher"},
]


@router.get("/api/suwayomi/extensions")
async def list_extensions():
    """List all available Suwayomi extensions with install status."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        data = await _gql(
            c,
            """
            { extensions { nodes { pkgName name lang isInstalled hasUpdate isNsfw isObsolete } } }
        """,
        )
        extensions = data.get("extensions", {}).get("nodes", [])
        for ext in extensions:
            ext["sourceType"] = classify_source(ext.get("name", ""))
        return JSONResponse({"ok": True, "extensions": extensions})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/extensions/refresh")
async def refresh_extensions():
    """Refresh the extension catalog from Suwayomi's configured repository."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        data = await _gql(
            c,
            """
            mutation { fetchExtensions(input: {}) { extensions { pkgName name isInstalled } } }
        """,
        )
        extensions = (data.get("fetchExtensions") or {}).get("extensions") or []
        return JSONResponse({"ok": True, "count": len(extensions)})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/extensions/{pkg_name}/install")
async def install_extension(pkg_name: str):
    """Install a Suwayomi extension by package name."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        await _gql(
            c,
            """
            mutation($pkg: String!) {
                updateExtension(input: {id: $pkg, patch: {install: true}}) {
                    extension { pkgName name isInstalled }
                }
            }
        """,
            {"pkg": pkg_name},
        )
        _SOURCE_CACHE.clear()  # invalidate cache after extension change
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/extensions/{pkg_name}/update")
async def update_extension(pkg_name: str):
    """Update a Suwayomi extension."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        await _gql(
            c,
            """
            mutation($pkg: String!) {
                updateExtension(input: {id: $pkg, patch: {update: true}}) {
                    extension { pkgName name isInstalled }
                }
            }
        """,
            {"pkg": pkg_name},
        )
        _SOURCE_CACHE.clear()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/extensions/{pkg_name}/uninstall")
async def uninstall_extension(pkg_name: str):
    """Uninstall a Suwayomi extension."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        await _gql(
            c,
            """
            mutation($pkg: String!) {
                updateExtension(input: {id: $pkg, patch: {uninstall: true}}) {
                    extension { pkgName name isInstalled }
                }
            }
        """,
            {"pkg": pkg_name},
        )
        _SOURCE_CACHE.clear()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/suwayomi/extensions/install-recommended")
async def install_recommended():
    """Install core recommended extensions (MangaDex, Manga Plus, etc.)."""
    with get_db() as db:
        c = get_suwayomi_client(db)
    if not c:
        return JSONResponse({"ok": False, "message": "No Suwayomi client configured"})
    try:
        # Refresh catalog first
        await _gql(
            c, "mutation { fetchExtensions(input: {}) { extensions { pkgName } } }"
        )
        # List all available extensions
        data = await _gql(
            c,
            """
            { extensions(filter: {isInstalled: {eq: false}}) {
                nodes { pkgName name lang }
            } }
        """,
        )
        available = data.get("extensions", {}).get("nodes", [])
        installed = []
        for rec in RECOMMENDED_EXTENSIONS:
            for ext in available:
                if rec["pattern"] in ext.get("name", "").lower() and ext.get(
                    "lang"
                ) in ("en", "all"):
                    try:
                        await _gql(
                            c,
                            """
                            mutation($pkg: String!) {
                                updateExtension(input: {id: $pkg, patch: {install: true}}) {
                                    extension { pkgName name }
                                }
                            }
                        """,
                            {"pkg": ext["pkgName"]},
                        )
                        installed.append(ext["name"])
                    except Exception:
                        pass
                    break
        _SOURCE_CACHE.clear()
        return JSONResponse({"ok": True, "installed": installed})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


# ── Per-series source management ─────────────────────────────────────────────


@router.get("/api/series/{series_id}/suwayomi/search")
async def search_sources_for_series(series_id: int, source: str = ""):
    """Search installed Suwayomi sources for a series title. Returns matches grouped by source."""
    with get_db() as db:
        s = db.execute(
            "SELECT title, ddl_language FROM series WHERE id=?", (series_id,)
        ).fetchone()
        c = get_suwayomi_client(db)
    if not c or not s:
        return JSONResponse({"ok": False, "message": "Not found"}, status_code=404)

    lang = s["ddl_language"] or get_cfg("ddl_language", "en")
    title = s["title"]

    try:
        data = await _gql(c, "{ sources { nodes { id name lang } } }")
        all_sources = data.get("sources", {}).get("nodes", [])

        # Filter to requested source or all non-NSFW sources
        if source:
            sources = [
                src
                for src in all_sources
                if source.lower() in src.get("name", "").lower()
            ]
        else:
            sources = [
                src for src in all_sources if src.get("lang") in (lang, "all", "en")
            ]

        results = []
        for src in sources[:10]:  # limit to 10 sources to avoid rate limits
            try:
                matches = await _search_source(c, src["id"], title)
                for m in matches[:5]:  # top 5 per source
                    results.append(
                        {
                            "source_id": src["id"],
                            "source_name": src["name"],
                            "source_lang": src["lang"],
                            "source_type": classify_source(src["name"]),
                            "manga_id": int(m["id"]),
                            "title": m.get("title", ""),
                            "url": m.get("url", ""),
                            "thumbnail": m.get("thumbnailUrl", ""),
                        }
                    )
            except Exception:
                continue

        # Sort: official first, then aggregator, then fan
        type_order = {"official": 0, "aggregator": 1, "fan": 2}
        results.sort(key=lambda r: type_order.get(r["source_type"], 9))
        return JSONResponse({"ok": True, "results": results})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@router.post("/api/series/{series_id}/suwayomi/link")
async def link_series_to_source(series_id: int, request: Request):
    """Link a series to a specific Suwayomi source + manga."""
    body = await request.json()
    source_id = body.get("source_id", "")
    source_name = body.get("source_name", "")
    manga_id = body.get("suwayomi_manga_id")
    if not source_id or not manga_id:
        return JSONResponse(
            {"ok": False, "message": "source_id and suwayomi_manga_id required"},
            status_code=400,
        )
    _store_source_linkage(series_id, source_name, source_id, int(manga_id))
    return JSONResponse({"ok": True})


@router.delete("/api/series/{series_id}/suwayomi/link/{source_id}")
async def unlink_source(series_id: int, source_id: str):
    """Remove a source linkage from a series."""
    with get_db() as db:
        db.execute(
            "DELETE FROM suwayomi_sources WHERE series_id=? AND source_id=?",
            (series_id, source_id),
        )
    return JSONResponse({"ok": True})


@router.get("/api/series/{series_id}/suwayomi/sources")
async def get_series_sources(series_id: int):
    """Get all Suwayomi sources linked to a series."""
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM suwayomi_sources WHERE series_id=? ORDER BY priority ASC",
            (series_id,),
        ).fetchall()
    return JSONResponse({"ok": True, "sources": [dict(r) for r in rows]})
