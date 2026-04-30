"""
Tests for research_worker.janitor — reap stuck jobs + prune old terminal rows.

The today-bug this covers: the janitor's UPDATE used `%s` placeholders inside
a SQL string literal, which psycopg can't substitute, leading to "could not
determine data type of parameter $1" on every sweep. The fix passes Python
timedelta values directly (psycopg maps timedelta -> interval natively); these
tests exercise both that the SQL runs cleanly and that the time thresholds
are respected.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from research_worker import janitor
from research_worker.errors import utcnow


# ---------------------------------------------------------------------------
# reap_stuck_jobs
# ---------------------------------------------------------------------------

class TestReapStuckJobs:
    def test_returns_zero_when_nothing_stuck(self):
        assert janitor.reap_stuck_jobs() == 0

    def test_does_not_reap_recently_claimed_running_jobs(self, db, make_job):
        job_id = make_job(status='running')
        # claimed_at is set by claim.claim_next, not by make_job, so set it
        # to a recent value directly.
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET claimed_at = now() WHERE id = %s",
                (job_id,),
            )
        db.commit()

        assert janitor.reap_stuck_jobs() == 0
        with db.cursor() as cur:
            cur.execute("SELECT status FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 'running'

    def test_reaps_jobs_claimed_longer_than_threshold(self, db, make_job):
        # Claimed 30 minutes ago — stuck_after defaults to 15 min, so this
        # one should be reaped.
        job_id = make_job(status='running', attempts=1, max_attempts=5)
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET claimed_at = now() - interval '30 minutes', "
                "claimed_by = 'dead-worker' WHERE id = %s",
                (job_id,),
            )
        db.commit()

        reaped = janitor.reap_stuck_jobs()
        assert reaped == 1

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, claimed_at, claimed_by, last_error "
                "FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'queued'
        assert row[1] is None
        assert row[2] is None
        assert row[3] is not None and 'reaped' in row[3]

    def test_marks_dead_when_attempts_already_exhausted(self, db, make_job):
        # Worker OOM/crash loop: handler dies before schedule_retry can
        # finalize the row, so attempts ratchets past max_attempts. The
        # janitor must finalize these as dead rather than re-queueing
        # forever.
        job_id = make_job(status='running', attempts=2, max_attempts=1)
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET claimed_at = now() - interval '30 minutes', "
                "claimed_by = 'dead-worker' WHERE id = %s",
                (job_id,),
            )
        db.commit()

        reaped = janitor.reap_stuck_jobs()
        assert reaped == 1

        with db.cursor() as cur:
            cur.execute(
                "SELECT status, finished_at, last_error "
                "FROM research_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
        assert row[0] == 'dead'
        assert row[1] is not None
        assert row[2] is not None and 'max_attempts exhausted' in row[2]


# ---------------------------------------------------------------------------
# prune_terminal_jobs
# ---------------------------------------------------------------------------

class TestPruneTerminalJobs:
    def test_returns_zero_when_nothing_to_prune(self):
        assert janitor.prune_terminal_jobs() == 0

    def test_does_not_delete_recent_done_jobs(self, db, make_job):
        job_id = make_job(status='done')
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET finished_at = now() WHERE id = %s",
                (job_id,),
            )
        db.commit()

        assert janitor.prune_terminal_jobs() == 0
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 1

    def test_deletes_done_jobs_older_than_horizon(self, db, make_job):
        # PRUNE_DONE_AFTER defaults to 30 days — 60 days ago is well past.
        job_id = make_job(status='done')
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET finished_at = now() - interval '60 days' "
                "WHERE id = %s",
                (job_id,),
            )
        db.commit()

        assert janitor.prune_terminal_jobs() == 1
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM research_jobs WHERE id = %s", (job_id,))
            assert cur.fetchone()[0] == 0

    def test_dead_jobs_have_longer_retention(self, db, make_job):
        # PRUNE_DEAD_AFTER defaults to 90 days. A dead job from 60 days
        # ago should survive; from 100 days ago should be pruned.
        recent_dead = make_job(status='dead')
        old_dead = make_job(status='dead')
        with db.cursor() as cur:
            cur.execute(
                "UPDATE research_jobs SET finished_at = now() - interval '60 days' "
                "WHERE id = %s",
                (recent_dead,),
            )
            cur.execute(
                "UPDATE research_jobs SET finished_at = now() - interval '100 days' "
                "WHERE id = %s",
                (old_dead,),
            )
        db.commit()

        assert janitor.prune_terminal_jobs() == 1
        with db.cursor() as cur:
            cur.execute(
                "SELECT id FROM research_jobs WHERE id IN (%s, %s) ORDER BY id",
                (recent_dead, old_dead),
            )
            remaining = [r[0] for r in cur.fetchall()]
        assert remaining == [recent_dead]
