"""ComicInfo.xml read/build/inject helpers.

Eighth module extracted from main.py. Handles the three things Mangarr
does with ComicInfo.xml:

  - read_comic_info      — parse an existing CBZ's ComicInfo.xml
  - build_comicinfo_xml  — generate a fresh ComicInfo.xml v2.1 string
                           (Anansi Project spec; Kavita + Komga compatible)
  - inject_comicinfo     — write/replace ComicInfo.xml inside a CBZ,
                           detecting file type via magic bytes
  - _try_inject_comicinfo — best-effort wrapper used by the import
                            pipeline; swallows errors so a bad archive
                            never aborts the import

XML parsing uses defusedxml to block external entity / billion-laughs
attacks (ComicInfo.xml comes from untrusted release archives).

Pure move — no DB access, no state — just zip and XML I/O.
"""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import time
import zipfile

from events import log_event

# ET is imported only for ParseError (an exception class, not a parser entry
# point) and for the serialize-only XML write in build_comicinfo_xml. All
# actual parsing uses _safe_xml_parse from defusedxml below.
import xml.etree.ElementTree as ET  # nosemgrep: python.lang.security.use-defused-xml.use-defused-xml

from defusedxml.ElementTree import parse as _safe_xml_parse
from defusedxml.ElementTree import ParseError as _SafeXMLParseError
from defusedxml.common import DefusedXmlException as _DefusedXmlException

from files import detect_file_type_magic
from parsing import _parse_vol_suffix


def read_comic_info(cbz_path: str) -> dict:
    """Open a .cbz/.zip file and parse ComicInfo.xml if present.

    Returns dict with keys: series (str|None), number (float|None),
    volume (float|None). Returns all-None dict on any error or if
    ComicInfo.xml is absent.
    """
    result: dict = {"series": None, "number": None, "volume": None}
    try:
        with zipfile.ZipFile(cbz_path, "r") as zf:
            ci_name = next(
                (n for n in zf.namelist() if n.lower().endswith("comicinfo.xml")), None
            )
            if not ci_name:
                return result
            with zf.open(ci_name) as f:
                root = _safe_xml_parse(f).getroot()

        assert root is not None

        def _text(tag: str) -> str | None:
            el = root.find(tag)
            return el.text.strip() if el is not None and el.text else None

        result["series"] = _text("Series")
        for field, key in (("Volume", "volume"), ("Number", "number")):
            raw = _text(field)
            if raw:
                val = _parse_vol_suffix(raw)
                if val is not None:
                    result[key] = val
    except (
        zipfile.BadZipFile,
        ET.ParseError,
        _SafeXMLParseError,
        _DefusedXmlException,
        KeyError,
        OSError,
        StopIteration,
    ):
        pass
    return result


def build_comicinfo_xml(
    series: dict,
    volume_num: float | None = None,
    chapter_num: float | None = None,
    tags: list[str] | None = None,
) -> str:
    """Build a ComicInfo.xml v2.1 string (Anansi Project spec) for a volume
    or chapter file. Compatible with both Kavita and Komga.

    series dict keys used: title, description, status, pub_year,
    total_volumes, total_chapters, language, anilist_id.
    """

    def esc(v: str) -> str:
        return (
            (v or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    title = esc(series.get("title") or "")
    description = esc(series.get("description") or "")
    pub_year = series.get("pub_year") or ""
    total_vols = series.get("total_volumes") or -1
    language = series.get("language") or "en"
    status = (series.get("status") or "").upper()
    tag_str = esc(",".join(tags or []))

    # Map AniList status to ComicInfo Count hint
    is_complete = status in ("FINISHED", "CANCELLED")
    count_val = (
        str(total_vols) if (is_complete and total_vols and total_vols > 0) else "-1"
    )

    # Volume or chapter context
    if volume_num is not None:
        vol_tag = f"  <Volume>{int(volume_num)}</Volume>\n"
        num_tag = ""
    elif chapter_num is not None:
        ch_int = int(chapter_num) if chapter_num == int(chapter_num) else chapter_num
        vol_tag = "  <Volume>0</Volume>\n"
        num_tag = f"  <Number>{ch_int}</Number>\n"
    else:
        vol_tag = ""
        num_tag = ""

    manga_val = "YesAndRightToLeft"  # manga reads right-to-left

    lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<ComicInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '           xmlns:xsd="http://www.w3.org/2001/XMLSchema">',
        f"  <Series>{title}</Series>",
    ]
    if vol_tag:
        lines.append(vol_tag.rstrip())
    if num_tag:
        lines.append(num_tag.rstrip())
    if count_val != "-1":
        lines.append(f"  <Count>{count_val}</Count>")
    if description:
        lines.append(f"  <Summary>{description}</Summary>")
    if pub_year:
        lines.append(f"  <Year>{pub_year}</Year>")
    if language:
        lines.append(f"  <LanguageISO>{language}</LanguageISO>")
    if tag_str:
        lines.append(f"  <Tags>{tag_str}</Tags>")
    lines += [
        f"  <Manga>{manga_val}</Manga>",
        "</ComicInfo>",
    ]
    return "\n".join(lines)


def inject_comicinfo(cbz_path: str, xml_content: str) -> bool:
    """Inject or replace ComicInfo.xml at the root of a CBZ (ZIP) file.

    Returns True on success, False if the file is not a valid ZIP or on
    error. Uses magic bytes (not extension) to detect file type so files
    with wrong extensions still work. CBR/RAR, EPUB, and PDF are skipped
    (return False). Any existing ComicInfo.xml is stripped and replaced —
    our DB metadata is authoritative.
    """
    # Use magic bytes first; fall back to extension only if file not readable
    file_type = detect_file_type_magic(cbz_path)
    if file_type is None:
        # Unreadable or non-existent file — fall back to extension check
        ext = os.path.splitext(cbz_path)[1].lower()
        if ext not in (".cbz", ".zip"):
            return False
    elif file_type != "cbz":
        return False  # CBR, EPUB, PDF — not injectable
    try:
        _rewrite_with_comicinfo(cbz_path, xml_content)
        return True
    except (zipfile.BadZipFile, OSError, Exception) as e:
        log_event("error", f"[ComicInfo] Failed to inject into {cbz_path}: {e}")
        return False


_COPY_CHUNK = 1024 * 1024


def _rewrite_with_comicinfo(cbz_path: str, xml_content: str) -> None:
    """Rewrite ``cbz_path`` with ``xml_content`` as its root ComicInfo.xml.

    Entries stream one at a time into a sibling temp file, so memory stays at
    one copy buffer rather than the whole volume, and the temp file replaces
    the original only once complete; a failure leaves the original untouched.
    Entry names, order, ZIP_STORED and writestr-style metadata are unchanged.
    """
    directory = os.path.dirname(os.path.abspath(cbz_path))
    fd, temp_path = tempfile.mkstemp(
        prefix=".comicinfo-", suffix=".cbz.tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "wb") as temp_file:
            with zipfile.ZipFile(cbz_path, "r") as src, zipfile.ZipFile(
                temp_file, "w", zipfile.ZIP_STORED
            ) as dst:
                dst.writestr("ComicInfo.xml", xml_content.encode("utf-8"))
                for info in src.infolist():
                    if info.filename.lower().endswith("comicinfo.xml"):
                        continue
                    if info.is_dir():
                        dst.writestr(info.filename, b"")
                        continue
                    # Same metadata writestr() gives a str name.
                    out_info = zipfile.ZipInfo(
                        info.filename,
                        date_time=time.localtime(time.time())[:6],
                    )
                    out_info.compress_type = zipfile.ZIP_STORED
                    out_info.external_attr = 0o600 << 16
                    with src.open(info) as entry, dst.open(
                        out_info,
                        "w",
                        force_zip64=info.file_size >= zipfile.ZIP64_LIMIT,
                    ) as out:
                        shutil.copyfileobj(entry, out, _COPY_CHUNK)
        os.chmod(temp_path, stat.S_IMODE(os.stat(cbz_path).st_mode))
        os.replace(temp_path, cbz_path)
    except BaseException:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def _try_inject_comicinfo(
    dst_path: str,
    series_row,
    volume_num=None,
    chapter_num=None,
    tags: list[str] | None = None,
) -> None:
    """Best-effort ComicInfo.xml injection — uses magic bytes, non-fatal on error."""
    if not dst_path or not os.path.isfile(dst_path):
        return
    # Fast-path: skip obvious non-injectables by extension before opening the file
    ext = os.path.splitext(dst_path)[1].lower()
    if ext in (".epub", ".pdf", ".mobi", ".azw3"):
        return
    try:
        xml = build_comicinfo_xml(
            dict(series_row),
            volume_num=volume_num,
            chapter_num=chapter_num,
            tags=tags or [],
        )
        inject_comicinfo(dst_path, xml)
    except Exception as e:
        log_event("error", f"[ComicInfo] Inject failed for {dst_path}: {e}")
