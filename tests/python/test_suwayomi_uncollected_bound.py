"""Loose (volume-less) chapters already inside an owned volume must not be downloaded again.

Chapters missing from the chapter->volume map are stored with volume_id NULL even when a downloaded volume holds
them. The Suwayomi loop grabbed every such chapter, e.g. Blue Lock chapters 77-346 although volumes 1-39 (to
chapter 346) were on disk. Only chapters past the highest chapter in a downloaded volume are uncollected.
"""
from __future__ import annotations

import json
import sqlite3

import conftest  # noqa: F401, E402


def _db(chapter_vol_map=None):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE series(id INTEGER PRIMARY KEY, chapter_vol_map TEXT);
        CREATE TABLE volumes(id INTEGER PRIMARY KEY, series_id INT, volume_num REAL, status TEXT);
        CREATE TABLE chapters(id INTEGER PRIMARY KEY, series_id INT, chapter_num REAL, status TEXT,
                              monitored INT, volume_id INT);
        """
    )
    db.execute("INSERT INTO series(id, chapter_vol_map) VALUES(1, ?)",
               (json.dumps(chapter_vol_map) if chapter_vol_map is not None else None,))
    return db


def _nums(rows):
    return sorted(float(r["chapter_num"]) for r in rows)


def test_chapters_inside_owned_volumes_are_skipped() -> None:
    from routers import suwayomi_

    db = _db()
    db.execute("INSERT INTO volumes VALUES(39, 1, 39, 'downloaded')")
    db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, 346, 'downloaded', 1, 39)")
    for ch in (77, 200, 346, 347, 364):  # loose chapters, as on Blue Lock
        db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, ?, 'wanted', 1, NULL)", (ch,))

    assert _nums(suwayomi_._uncollected_chapters_to_grab(db, 1)) == [347.0, 364.0]


def test_no_downloaded_volume_keeps_every_chapter() -> None:
    from routers import suwayomi_

    db = _db()
    db.execute("INSERT INTO volumes VALUES(1, 1, 1, 'wanted')")
    for ch in (1, 2, 3):
        db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, ?, 'wanted', 1, NULL)", (ch,))

    assert _nums(suwayomi_._uncollected_chapters_to_grab(db, 1)) == [1.0, 2.0, 3.0]


def test_map_bounds_owned_volumes_when_chapters_are_unlinked() -> None:
    from routers import suwayomi_

    db = _db({"001": 1, "010": 2, "020": 3})
    db.execute("INSERT INTO volumes VALUES(2, 1, 2, 'downloaded')")
    for ch in (5, 10, 15, 20, 21):
        db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, ?, 'wanted', 1, NULL)", (ch,))

    # volume 2 is the newest owned; the map puts chapter 10 in it, so 5 and 10 are covered
    assert _nums(suwayomi_._uncollected_chapters_to_grab(db, 1)) == [15.0, 20.0, 21.0]


def test_unmonitored_or_unwanted_chapters_stay_out() -> None:
    from routers import suwayomi_

    db = _db()
    db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, 5, 'wanted', 0, NULL)")
    db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, 6, 'downloaded', 1, NULL)")
    db.execute("INSERT INTO chapters(series_id, chapter_num, status, monitored, volume_id) VALUES(1, 7, 'wanted', 1, NULL)")

    assert _nums(suwayomi_._uncollected_chapters_to_grab(db, 1)) == [7.0]
