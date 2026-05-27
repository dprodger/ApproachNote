"""
Tests for core.research_jobs — the producer-side helpers that web routes
and scripts call to put work onto the durable queue.

Focus: enqueue idempotency (the partial unique index that collapses
duplicate in-flight jobs), and the read helpers used by the client-facing
research_status endpoints.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from core import research_jobs


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

class TestEnqueue:
    """enqueue() inserts a row, returns id, dedups in-flight duplicates."""

    def test_inserts_new_job_and_returns_id(self, db):
        target_id = str(uuid4())
        job_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
        )
        assert isinstance(job_id, int)

        with db.cursor() as cur:
            cur.execute("SELECT source, status, target_id FROM research_jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
        assert row[0] == 'youtube'
        assert row[1] == 'queued'
        assert str(row[2]) == target_id

    def test_payload_priority_max_attempts_persist(self, db):
        target_id = str(uuid4())
        job_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
            payload={'rematch': True, 'note': 'manual'},
            priority=10,
            max_attempts=3,
        )
        with db.cursor() as cur:
            cur.execute(
                "SELECT priority, max_attempts, payload FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 10
        assert row[1] == 3
        assert row[2] == {'rematch': True, 'note': 'manual'}

    def test_dedup_returns_existing_id_for_in_flight_job(self, db):
        target_id = str(uuid4())
        first_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
        )
        # Same target while first is still queued → dedup hit, same id back.
        second_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
        )
        assert second_id == first_id

        with db.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM research_jobs "
                "WHERE source = 'youtube' AND target_id = %s",
                (target_id,),
            )
            assert cur.fetchone()[0] == 1

    def test_dedup_also_collapses_against_running_jobs(self, db, make_job):
        # Pre-existing running job for the same target.
        target_id = str(uuid4())
        existing = make_job(target_id=target_id, status='running')
        new_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
        )
        assert new_id == existing

    def test_no_dedup_against_done_jobs(self, db, make_job):
        # A done job for the same target should NOT block re-enqueue.
        # This is what enables "Quick Refresh" to re-run after a job finished.
        target_id = str(uuid4())
        done_id = make_job(target_id=target_id, status='done')
        new_id = research_jobs.enqueue(
            source='youtube',
            job_type='match_recording',
            target_type='recording',
            target_id=target_id,
        )
        assert new_id != done_id

    def test_no_dedup_across_different_job_types(self, db):
        # Same target but different job_type → independent in-flight rows.
        target_id = str(uuid4())
        a = research_jobs.enqueue(
            source='youtube', job_type='match_recording',
            target_type='recording', target_id=target_id,
        )
        b = research_jobs.enqueue(
            source='youtube', job_type='fetch_metadata',
            target_type='recording', target_id=target_id,
        )
        assert a != b

    def test_no_dedup_across_different_sources(self, db):
        target_id = str(uuid4())
        a = research_jobs.enqueue(
            source='youtube', job_type='match_recording',
            target_type='recording', target_id=target_id,
        )
        b = research_jobs.enqueue(
            source='spotify', job_type='match_recording',
            target_type='recording', target_id=target_id,
        )
        assert a != b


# ---------------------------------------------------------------------------
# enqueue_many_for_targets — bulk INSERT path for backfill sweeps
# ---------------------------------------------------------------------------

class TestEnqueueMany:
    """Bulk path: one multi-row INSERT per batch, ON CONFLICT DO NOTHING.

    DB end-state must match what an equivalent loop of enqueue() would
    produce; only the round-trip count differs.
    """

    def _count_jobs_for_targets(self, db, target_ids):
        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE target_id = ANY(%s::uuid[]) GROUP BY target_id",
                (target_ids,),
            )
            return {str(row[0]): row[1] for row in cur.fetchall()}

    def test_empty_input_returns_zeros_without_db_call(self, mocker):
        # Defensive: don't issue an INSERT with empty VALUES.
        get_conn = mocker.patch('core.research_jobs.get_db_connection')
        result = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=[],
        )
        assert result == {'requested': 0, 'inserted': 0, 'skipped': 0}
        get_conn.assert_not_called()

    def test_inserts_all_rows_in_single_batch(self, db):
        target_ids = [str(uuid4()) for _ in range(5)]
        result = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=target_ids,
        )
        assert result == {'requested': 5, 'inserted': 5, 'skipped': 0}

        counts = self._count_jobs_for_targets(db, target_ids)
        for tid in target_ids:
            assert counts.get(tid) == 1

    def test_second_call_dedups_via_unique_index(self, db):
        target_ids = [str(uuid4()) for _ in range(3)]
        first = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=target_ids,
        )
        second = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=target_ids,
        )
        assert first['inserted'] == 3
        assert second['inserted'] == 0
        assert second['skipped'] == 3

        # Exactly one job per target, not two — same end-state as the
        # single-row enqueue() path.
        counts = self._count_jobs_for_targets(db, target_ids)
        for tid in target_ids:
            assert counts.get(tid) == 1

    def test_mixed_new_and_existing_targets(self, db):
        # Pre-enqueue 2 targets via the single-row path; then bulk-enqueue
        # those 2 plus 3 new ones. Bulk path should insert 3, skip 2.
        existing = [str(uuid4()) for _ in range(2)]
        new = [str(uuid4()) for _ in range(3)]
        for tid in existing:
            research_jobs.enqueue(
                source='musicbrainz',
                job_type='backfill_release_label',
                target_type='release',
                target_id=tid,
            )

        result = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=existing + new,
        )
        assert result == {'requested': 5, 'inserted': 3, 'skipped': 2}

    def test_batches_when_input_exceeds_batch_size(self, db):
        # 7 rows across batch_size=3 → batches of 3, 3, 1.
        target_ids = [str(uuid4()) for _ in range(7)]
        result = research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=target_ids,
            batch_size=3,
        )
        assert result == {'requested': 7, 'inserted': 7, 'skipped': 0}

        counts = self._count_jobs_for_targets(db, target_ids)
        for tid in target_ids:
            assert counts.get(tid) == 1

    def test_payload_priority_max_attempts_persist(self, db):
        target_ids = [str(uuid4())]
        research_jobs.enqueue_many_for_targets(
            source='musicbrainz',
            job_type='backfill_release_label',
            target_type='release',
            target_ids=target_ids,
            payload={'note': 'bulk-test'},
            priority=42,
            max_attempts=9,
        )
        with db.cursor() as cur:
            cur.execute(
                "SELECT payload, priority, max_attempts FROM research_jobs "
                "WHERE target_id = %s",
                (target_ids[0],),
            )
            row = cur.fetchone()
        assert row[0] == {'note': 'bulk-test'}
        assert row[1] == 42
        assert row[2] == 9


# ---------------------------------------------------------------------------
# get_job + status_for_target
# ---------------------------------------------------------------------------

class TestReadHelpers:
    def test_get_job_returns_row_or_none(self, db, make_job):
        job_id = make_job()
        row = research_jobs.get_job(job_id)
        assert row is not None
        assert row['id'] == job_id

        assert research_jobs.get_job(99_999_999) is None

    def test_status_for_target_returns_one_row_per_source(self, db, make_job):
        target_id = str(uuid4())
        # Two YouTube jobs (one done, one queued — should return the newest)
        # and one Spotify job; expect 2 rows total, one per source.
        make_job(target_id=target_id, source='youtube', status='done')
        make_job(target_id=target_id, source='youtube', status='queued')
        make_job(target_id=target_id, source='spotify', status='running')

        rows = research_jobs.status_for_target('recording', target_id)
        assert len(rows) == 2
        sources = {r['source'] for r in rows}
        assert sources == {'youtube', 'spotify'}

        # Newest YouTube wins (the queued one inserted second).
        yt = next(r for r in rows if r['source'] == 'youtube')
        assert yt['status'] == 'queued'

    def test_status_for_target_empty_when_no_jobs(self):
        rows = research_jobs.status_for_target('recording', str(uuid4()))
        assert rows == []
