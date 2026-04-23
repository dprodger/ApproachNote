"""
Tests for research_worker.claim — the SELECT FOR UPDATE SKIP LOCKED claim
plus the finalize/release transitions (mark_done, release_for_quota,
schedule_retry, mark_dead).

The today-bug this covers: release_for_quota MUST decrement attempts so
that a job deferred for quota doesn't burn a retry attempt. Without it,
a few quota-exhausted cycles would push every job to 'dead' even though
it never actually got to do its work.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from research_worker import claim
from research_worker.errors import utcnow


# ---------------------------------------------------------------------------
# claim_next
# ---------------------------------------------------------------------------

class TestClaimNext:
    def test_claims_oldest_eligible_queued_job(self, db, make_job):
        # Two queued jobs; first inserted should be claimed first.
        first = make_job()
        make_job()

        row = claim.claim_next('youtube', worker_id='test-worker')
        assert row is not None
        assert row['id'] == first
        assert row['status'] == 'running'
        assert row['claimed_by'] == 'test-worker'
        assert row['attempts'] == 1  # incremented from 0 by claim

    def test_returns_none_when_nothing_eligible(self):
        assert claim.claim_next('youtube', 'test-worker') is None

    def test_skips_jobs_with_run_after_in_future(self, db, make_job):
        future = utcnow() + timedelta(hours=1)
        make_job(run_after=future)
        assert claim.claim_next('youtube', 'test-worker') is None

    def test_respects_source_filter(self, db, make_job):
        make_job(source='spotify')
        # Worker for youtube finds nothing.
        assert claim.claim_next('youtube', 'test-worker') is None
        # Worker for spotify finds it.
        assert claim.claim_next('spotify', 'test-worker') is not None

    def test_priority_orders_before_age(self, db, make_job):
        # Older but lower priority (higher number) job; newer but
        # higher priority. Higher priority (lower number) wins.
        old_low_prio = make_job(priority=200)
        new_high_prio = make_job(priority=10)
        row = claim.claim_next('youtube', 'test-worker')
        assert row['id'] == new_high_prio

    def test_skip_locked_does_not_block_concurrent_workers(self, db, make_job):
        # Two queued jobs, two concurrent workers — each should claim a
        # different one, neither blocks. We can't truly run two workers
        # in one test process, but we can verify the lock pattern by
        # claiming inside an open transaction and confirming a second
        # claim succeeds against the *other* job.
        import os

        import psycopg

        make_job()
        make_job()

        # Open a separate connection so we have an isolated transaction
        # that can hold a row lock independently of the worker call.
        dsn = {
            'host': os.environ['DB_HOST'],
            'port': int(os.environ.get('DB_PORT', '5432')),
            'dbname': os.environ['DB_NAME'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
        }
        with psycopg.connect(**dsn) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # Mimic the claim query, holding the row lock.
                cur.execute(
                    """
                    UPDATE research_jobs SET status = 'running',
                                             claimed_at = now(),
                                             claimed_by = %s,
                                             attempts = attempts + 1
                    WHERE id = (
                        SELECT id FROM research_jobs
                        WHERE source = 'youtube' AND status = 'queued'
                          AND run_after <= now()
                        ORDER BY priority, run_after, id
                        FOR UPDATE SKIP LOCKED LIMIT 1
                    ) RETURNING id
                    """,
                    ('worker-1',),
                )
                claimed_a = cur.fetchone()[0]

                # Without committing, second worker claims via the helper.
                row = claim.claim_next('youtube', 'worker-2')
                assert row is not None
                assert row['id'] != claimed_a, (
                    "Second worker should have skipped the locked row"
                )
            conn.rollback()


# ---------------------------------------------------------------------------
# mark_done
# ---------------------------------------------------------------------------

class TestMarkDone:
    def test_sets_done_status_finished_at_and_result(self, db, make_job):
        job_id = make_job(status='running')
        claim.mark_done(job_id, {'matched': True, 'video_id': 'abc'})

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, finished_at, result, last_error "
                "FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'done'
        assert row[1] is not None
        assert row[2] == {'matched': True, 'video_id': 'abc'}
        assert row[3] is None  # last_error cleared


# ---------------------------------------------------------------------------
# release_for_quota — the today-bug fix
# ---------------------------------------------------------------------------

class TestReleaseForQuota:
    def test_returns_to_queued_with_run_after_set_to_resets_at(self, db, make_job):
        job_id = make_job(status='running', attempts=1)
        future = utcnow() + timedelta(hours=12)
        claim.release_for_quota(job_id, future)

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, run_after, claimed_at, claimed_by, last_error "
                "FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'queued'
        # Compare ignoring microsecond drift from psycopg roundtripping.
        assert abs((row[1] - future).total_seconds()) < 1
        assert row[2] is None
        assert row[3] is None
        assert row[4] == 'quota_exhausted'

    def test_decrements_attempts_so_deferral_does_not_burn_retry(self, db, make_job):
        # Critical behavior: a quota deferral is not a "try" — the work
        # never happened. Without the decrement, a job hitting quota a
        # few times in a row would die before ever doing real work.
        job_id = make_job(status='running', attempts=3)
        claim.release_for_quota(job_id, utcnow() + timedelta(hours=1))

        with db.cursor() as cur:
            cur.execute("SELECT attempts FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 2  # 3 -> 2

    def test_decrement_clamps_at_zero(self, db, make_job):
        job_id = make_job(status='running', attempts=0)
        claim.release_for_quota(job_id, utcnow() + timedelta(hours=1))

        with db.cursor() as cur:
            cur.execute("SELECT attempts FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# schedule_retry
# ---------------------------------------------------------------------------

class TestScheduleRetry:
    def test_returns_queued_when_attempts_under_max(self, db, make_job):
        job_id = make_job(status='running', attempts=1, max_attempts=5)
        before = utcnow()
        result = claim.schedule_retry(job_id, attempts=1, max_attempts=5, error='boom')
        assert result == 'queued'

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, run_after, last_error FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'queued'
        # Backoff for attempt 1 is ~60s ±20%; should be > now.
        assert row[1] > before
        assert row[2] == 'boom'

    def test_returns_dead_when_attempts_at_max(self, db, make_job):
        job_id = make_job(status='running', attempts=5, max_attempts=5)
        result = claim.schedule_retry(job_id, attempts=5, max_attempts=5, error='final')
        assert result == 'dead'

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, finished_at, last_error FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'dead'
        assert row[1] is not None
        assert row[2] == 'final'

    def test_truncates_long_error_messages(self, db, make_job):
        job_id = make_job(status='running')
        long_err = 'x' * 5000
        claim.schedule_retry(job_id, attempts=1, max_attempts=5, error=long_err)

        with db.cursor() as cur:
            cur.execute("SELECT length(last_error) FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 2000


# ---------------------------------------------------------------------------
# mark_dead
# ---------------------------------------------------------------------------

class TestMarkDead:
    def test_sets_dead_status_and_records_error(self, db, make_job):
        job_id = make_job(status='running')
        claim.mark_dead(job_id, 'permanent failure')

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, finished_at, last_error FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'dead'
        assert row[1] is not None
        assert row[2] == 'permanent failure'
