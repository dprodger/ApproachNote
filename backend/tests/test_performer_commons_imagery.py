"""
Tests for the Commons performer-imagery enrichment:

  - research_worker/handlers/commons.py::enrich_performer_imagery — handler
  - core/performer_commons_imagery.py                             — sweep enqueuer

Both touch real DB tables (performers, research_jobs) so we use deterministic
fixture UUIDs and explicit row-level cleanup, mirroring
test_performer_wikipedia_enrichment.py.

The gathering / visual-analysis / persistence pipeline (core.commons_imagery)
is mocked in the handler tests — we never hit Wikimedia, Flickr, or Claude.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from core import performer_commons_imagery as sweep_mod
from db_utils import get_db_connection
from research_worker.errors import PermanentError, QuotaExhausted
from research_worker.handlers import commons as handler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-5000000{:05x}"

PERFORMER_NEVER = _NS.format(0x00001)   # last_imagery_check NULL -> due
PERFORMER_STALE = _NS.format(0x00002)   # checked 100 days ago    -> due (>90d)
PERFORMER_FRESH = _NS.format(0x00003)   # checked 10 days ago      -> not due

_ALL_FIXTURE_IDS = (PERFORMER_NEVER, PERFORMER_STALE, PERFORMER_FRESH)


def _cleanup(conn):
    placeholders = ", ".join(["%s"] * len(_ALL_FIXTURE_IDS))
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM research_jobs WHERE target_id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
        cur.execute(
            f"DELETE FROM performers WHERE id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
    conn.commit()


@pytest.fixture
def perf_fixture(db):
    """Performers exercising the never / stale / fresh candidate cases."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO performers (id, name, last_imagery_check) "
            "VALUES (%s, %s, NULL)",
            (PERFORMER_NEVER, "Never Checked"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, last_imagery_check) "
            "VALUES (%s, %s, now() - make_interval(days => 100))",
            (PERFORMER_STALE, "Stale Checked"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, last_imagery_check) "
            "VALUES (%s, %s, now() - make_interval(days => 10))",
            (PERFORMER_FRESH, "Fresh Checked"),
        )
    db.commit()
    yield
    _cleanup(db)


def _last_check(conn, performer_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_imagery_check FROM performers WHERE id = %s",
            (performer_id,),
        )
        return cur.fetchone()[0]


class FakeCtx:
    """Minimal JobContext stand-in for handler tests."""

    def __init__(self, target_id, *, quota_exhausted=False):
        self.target_id = str(target_id)
        self.source = "commons"
        self.job_type = "enrich_performer_imagery"
        self.target_type = "performer"
        self.attempt = 0
        self.log = logging.getLogger("test.commons")
        self.quota_calls: list = []
        self._quota_exhausted = quota_exhausted

    def consume_quota(self, cost, window="day"):
        self.quota_calls.append((cost, window))
        if self._quota_exhausted:
            raise QuotaExhausted("commons", datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_find_candidates_includes_never_and_stale_not_fresh(self, perf_fixture):
        candidates = sweep_mod.find_candidate_performer_ids()
        assert PERFORMER_NEVER in candidates
        assert PERFORMER_STALE in candidates
        assert PERFORMER_FRESH not in candidates

    def test_stale_days_window_excludes_within_window(self, perf_fixture):
        # With a 200-day window, the 100-day-stale row is no longer due;
        # the never-checked row always is.
        candidates = sweep_mod.find_candidate_performer_ids(stale_days=200)
        assert PERFORMER_NEVER in candidates
        assert PERFORMER_STALE not in candidates

    def test_enqueue_sweep_creates_commons_jobs(self, perf_fixture, db):
        result = sweep_mod.enqueue_sweep()
        assert result["candidates"] >= 2
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'commons' "
                "AND job_type = 'enrich_performer_imagery' "
                "AND target_id IN (%s, %s)",
                (PERFORMER_NEVER, PERFORMER_STALE),
            )
            assert cur.fetchone()[0] == 2

    def test_enqueue_sweep_is_idempotent(self, perf_fixture, db):
        sweep_mod.enqueue_sweep()
        sweep_mod.enqueue_sweep()
        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE source = 'commons' "
                "AND job_type = 'enrich_performer_imagery' "
                "AND target_id IN (%s, %s) GROUP BY target_id",
                (PERFORMER_NEVER, PERFORMER_STALE),
            )
            counts = {str(r[0]): r[1] for r in cur.fetchall()}
        assert counts.get(PERFORMER_NEVER) == 1
        assert counts.get(PERFORMER_STALE) == 1

    def test_second_sweep_reports_skipped_via_dedup(self, perf_fixture):
        first = sweep_mod.enqueue_sweep()
        second = sweep_mod.enqueue_sweep()
        assert first["enqueued"] >= 2
        assert second["enqueued"] == 0
        assert second["skipped"] == second["candidates"]

    def test_no_candidates_returns_zero(self, mocker):
        mocker.patch(
            "core.performer_commons_imagery.find_candidate_performer_ids",
            return_value=[],
        )
        assert sweep_mod.enqueue_sweep() == {
            "candidates": 0, "enqueued": 0, "skipped": 0,
        }

    def test_enqueue_one_creates_single_job(self, perf_fixture, db):
        job_id = sweep_mod.enqueue_one(PERFORMER_NEVER, limit=5)
        assert job_id is not None
        with db.cursor() as cur:
            cur.execute(
                "SELECT payload FROM research_jobs WHERE id = %s", (job_id,),
            )
            assert cur.fetchone()[0] == {"limit": 5}


# ---------------------------------------------------------------------------
# Handler (pipeline mocked)
# ---------------------------------------------------------------------------

def _fake_record():
    return handler.ci.ImageRecord(
        url="http://test.example/x.jpg", source="wikimedia_commons",
        source_identifier="123", source_page_url="http://test.example/File:x",
        license_type="cc_by",
    )


class TestHandler:
    def test_unknown_performer_raises_permanent(self, perf_fixture):
        bogus = "00000000-0000-4000-8000-500000099999"
        with pytest.raises(PermanentError):
            handler.enrich_performer_imagery({}, FakeCtx(bogus))

    def test_happy_path_persists_and_stamps(self, perf_fixture, db, mocker):
        mocker.patch.object(handler.ci, "gather_candidates",
                            return_value=[_fake_record()])
        mocker.patch.object(handler.ci, "analyze_and_rank",
                            return_value=[_fake_record()])
        persist = mocker.patch.object(
            handler.ci, "persist_images",
            return_value={"saved": 1, "primary_set": True})

        assert _last_check(db, PERFORMER_NEVER) is None
        result = handler.enrich_performer_imagery({}, FakeCtx(PERFORMER_NEVER))

        assert result["images_added"] == 1
        assert result["updated"] is True
        persist.assert_called_once()
        # last_imagery_check is stamped on completion
        db.commit()
        assert _last_check(db, PERFORMER_NEVER) is not None

    def test_no_candidates_still_stamps(self, perf_fixture, db, mocker):
        mocker.patch.object(handler.ci, "gather_candidates", return_value=[])
        mocker.patch.object(handler.ci, "analyze_and_rank", return_value=[])
        result = handler.enrich_performer_imagery({}, FakeCtx(PERFORMER_STALE))
        assert result["images_added"] == 0
        assert result["updated"] is False
        db.commit()
        assert _last_check(db, PERFORMER_STALE) is not None

    def test_rerank_budget_consumes_quota(self, perf_fixture, mocker):
        # analyze_and_rank should receive a rerank_budget callable; simulate it
        # firing for 3 images and assert the handler charged quota for them.
        def fake_analyze(records, **kwargs):
            kwargs["rerank_budget"](3)
            return []
        mocker.patch.object(handler.ci, "gather_candidates",
                            return_value=[_fake_record()])
        mocker.patch.object(handler.ci, "analyze_and_rank", side_effect=fake_analyze)
        mocker.patch.object(handler.ci, "persist_images",
                            return_value={"saved": 0, "primary_set": False})

        ctx = FakeCtx(PERFORMER_NEVER)
        handler.enrich_performer_imagery({}, ctx)
        assert (3, "day") in ctx.quota_calls

    def test_quota_exhausted_propagates_and_does_not_stamp(
        self, perf_fixture, db, mocker,
    ):
        def fake_analyze(records, **kwargs):
            kwargs["rerank_budget"](5)  # will raise via FakeCtx
            return []
        mocker.patch.object(handler.ci, "gather_candidates",
                            return_value=[_fake_record()])
        mocker.patch.object(handler.ci, "analyze_and_rank", side_effect=fake_analyze)

        ctx = FakeCtx(PERFORMER_NEVER, quota_exhausted=True)
        with pytest.raises(QuotaExhausted):
            handler.enrich_performer_imagery({}, ctx)
        # Work didn't complete -> no stamp, so the producer can retry later.
        db.commit()
        assert _last_check(db, PERFORMER_NEVER) is None
