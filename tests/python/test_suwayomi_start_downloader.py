"""startDownloader must not fail a grab of chapters Suwayomi already has.

Suwayomi drops already-downloaded chapters from its queue at once; startDownloader then blocks until its 30 s
timeout and errors. manga-arr treated that as a failed grab and never recorded the job, so those chapters were
never imported.
"""
from __future__ import annotations

import asyncio

import pytest

import conftest  # noqa: F401, E402


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch):
    from routers import suwayomi_

    seen: list[str] = []

    async def fake_gql(c, query, variables=None):
        seen.append(query)
        return {}

    monkeypatch.setattr(suwayomi_, "_gql", fake_gql)
    return seen


def test_all_downloaded_skips_start(calls) -> None:
    from routers import suwayomi_

    _run(suwayomi_._start_downloader({}, [{"id": 1, "isDownloaded": True}, {"id": 2, "isDownloaded": True}]))

    assert calls == []


def test_new_chapters_start_downloader(calls) -> None:
    from routers import suwayomi_

    _run(suwayomi_._start_downloader({}, [{"id": 1, "isDownloaded": True}, {"id": 2, "isDownloaded": False}]))

    assert len(calls) == 1 and "startDownloader" in calls[0]


def test_unknown_state_still_starts(calls) -> None:
    from routers import suwayomi_

    _run(suwayomi_._start_downloader({}, []))

    assert len(calls) == 1


def test_start_failure_is_logged_not_raised(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    from routers import suwayomi_

    async def timing_out(c, query, variables=None):
        raise TimeoutError("Timed out waiting for 30000 ms")

    monkeypatch.setattr(suwayomi_, "_gql", timing_out)

    _run(suwayomi_._start_downloader({}, [{"id": 1, "isDownloaded": False}]))

    assert "startDownloader failed" in caplog.text
