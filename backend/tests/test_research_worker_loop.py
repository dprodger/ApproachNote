"""
Tests for research_worker.loop — the per-job dispatch that ties claim
→ registered handler → finalize together.

The handler tests (test_{spotify,apple,youtube}_handler.py) verify the
handler's return shape. The claim tests exercise state transitions
directly. Neither exercises the tick glue: how an exception raised from
a handler translates into the right claim.* finalize call, how non-dict
results get coerced, how QuotaExhausted routes through release_for_quota
instead of schedule_retry, etc. That's what this file covers.

Strategy:
  * JobContext tests are pure (no DB) — mock quota.consume and construct
    a hand-built job dict.
  * _process_one tests go through the real DB: make_job inserts a row,
    claim_next flips it to running, a stub handler drives one of the
    outcomes, and the test asserts the resulting research_jobs row.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import pytest

from research_worker import claim
from research_worker.errors import (
    HandlerError,
    PermanentError,
    QuotaExhausted,
    RetryableError,
    utcnow,
)
from research_worker.loop import JobContext, _process_one


log = logging.getLogger("test.research_worker_loop")


# ---------------------------------------------------------------------------
# JobContext (pure — no DB)
# ---------------------------------------------------------------------------

def _fake_job_row(**overrides):
    """Hand-built row matching claim_next's RETURNING shape, minus the
    columns _process_one / JobContext don't read. Tests for the context
    object and for the `attempts` wiring live here; the DB-backed tests
    below use claim_next to get a real row."""
    base = {
        'id': 42,
        'source': 'youtube',
        'job_type': 'match_recording',
        'target_type': 'recording',
        'target_id': '00000000-0000-4000-8000-000000000001',
        'attempts': 1,
        'max_attempts': 5,
        'payload': {'rematch': False},
    }
    base.update(overrides)
    return base


class TestJobContext:
    def test_initializes_fields_from_job_row(self):
        ctx = JobContext(_fake_job_row())
        assert ctx.job_id == 42
        assert ctx.source == 'youtube'
        assert ctx.job_type == 'match_recording'
        assert ctx.target_type == 'recording'
        assert ctx.target_id == '00000000-0000-4000-8000-000000000001'
        assert ctx.attempt == 1

    def test_consume_quota_delegates_to_quota_module(self, mocker):
        # consume_quota is a thin pass-through to quota.consume. Worth
        # pinning the contract (source + window + cost) because handlers
        # rely on it for per-call deduction and mis-wiring was the bug
        # that spawned the quota tests in the first place.
        patched = mocker.patch('research_worker.loop.quota.consume')
        ctx = JobContext(_fake_job_row(source='youtube'))

        ctx.consume_quota(100)

        patched.assert_called_once_with('youtube', 'day', 100)

    def test_consume_quota_accepts_window_override(self, mocker):
        patched = mocker.patch('research_worker.loop.quota.consume')
        ctx = JobContext(_fake_job_row(source='spotify'))

        ctx.consume_quota(5, window='minute')

        patched.assert_called_once_with('spotify', 'minute', 5)

    def test_consume_quota_propagates_quota_exhausted(self, mocker):
        resets_at = datetime.now(timezone.utc) + timedelta(hours=1)
        mocker.patch(
            'research_worker.loop.quota.consume',
            side_effect=QuotaExhausted('youtube', resets_at),
        )
        ctx = JobContext(_fake_job_row())

        with pytest.raises(QuotaExhausted):
            ctx.consume_quota(100)

    def test_elapsed_ms_reflects_elapsed_wallclock(self):
        ctx = JobContext(_fake_job_row())
        time.sleep(0.01)
        # Can't pin an exact value without pinning perf_counter — but we
        # can assert the order of magnitude and monotonicity.
        first = ctx.elapsed_ms()
        time.sleep(0.005)
        second = ctx.elapsed_ms()
        assert first >= 1
        assert second >= first


# ---------------------------------------------------------------------------
# _process_one — outcome translation
# ---------------------------------------------------------------------------

def _get_job_row(db, job_id):
    """Fetch the research_jobs row as a dict keyed by column name.

    The shared `db` fixture yields a plain psycopg connection that returns
    tuples by default, so we build the dict here rather than relying on
    a row factory the fixture doesn't install.
    """
    with db.cursor() as cur:
        cur.execute(
            "SELECT status, attempts, result, last_error, "
            "       run_after, finished_at "
            "FROM research_jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


class TestProcessOneSuccess:
    def test_handler_dict_result_marks_done(self, db, make_job):
        job_id = make_job()
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')
        assert job['id'] == job_id  # sanity

        def handler(payload, ctx):
            return {'matched': True, 'releases_processed': 3}

        _process_one(handler, job, log)

        row = _get_job_row(db, job_id)
        assert row['status'] == 'done'
        assert row['result'] == {'matched': True, 'releases_processed': 3}
        assert row['last_error'] is None
        assert row['finished_at'] is not None

    def test_non_dict_result_coerced_to_empty_dict(self, db, make_job):
        # Handlers are supposed to return dicts; a None or bare string is
        # a bug, but the loop coerces rather than crashing the worker.
        make_job()
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        def handler(payload, ctx):
            return None

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'done'
        assert row['result'] == {}


class TestProcessOneQuotaExhausted:
    def test_releases_job_and_decrements_attempts(self, db, make_job):
        # Quota exhaustion must NOT count as a retry attempt — otherwise a
        # few quota-blocked cycles would burn through max_attempts and
        # kill the job prematurely. release_for_quota bumps attempts back
        # down by 1.
        make_job(attempts=0)  # post-claim attempts = 1
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')
        assert job['attempts'] == 1

        resets_at = utcnow() + timedelta(hours=1)

        def handler(payload, ctx):
            raise QuotaExhausted('youtube', resets_at)

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'queued'
        assert row['attempts'] == 0  # decremented back
        assert row['last_error'] == 'quota_exhausted'
        # run_after is the resets_at we passed (give or take DB rounding)
        assert abs((row['run_after'] - resets_at).total_seconds()) < 1


class TestProcessOnePermanentError:
    def test_marks_dead_with_error_message(self, db, make_job):
        make_job()
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        def handler(payload, ctx):
            raise PermanentError('target missing')

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'dead'
        assert row['last_error'] == 'target missing'
        assert row['finished_at'] is not None


class TestProcessOneRetryable:
    def test_schedules_retry_when_attempts_remaining(self, db, make_job):
        # Fresh job (attempts=0) → claim bumps to 1 → retryable failure
        # should leave status='queued' with a future run_after.
        make_job(attempts=0, max_attempts=5)
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')
        assert job['attempts'] == 1

        def handler(payload, ctx):
            raise RetryableError('network blip')

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'queued'
        assert row['attempts'] == 1  # schedule_retry does not re-increment
        assert row['last_error'] == 'network blip'
        assert row['run_after'] > utcnow()

    def test_marks_dead_when_max_attempts_reached(self, db, make_job):
        # Pre-claim attempts=4 → post-claim attempts=5 → retryable failure
        # triggers the "out of attempts" branch in schedule_retry.
        make_job(attempts=4, max_attempts=5)
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')
        assert job['attempts'] == 5

        def handler(payload, ctx):
            raise RetryableError('still failing')

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'dead'
        assert row['last_error'] == 'still failing'


class TestProcessOneUnknownException:
    def test_treats_unexpected_exception_as_retryable(self, db, make_job):
        # Handler bugs (ValueError, TypeError, etc.) must not kill the
        # worker thread. The loop catches them and reschedules.
        make_job(attempts=0, max_attempts=5)
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        def handler(payload, ctx):
            raise ValueError('unexpected')

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'queued'
        # last_error is prefixed with the exception class for debuggability.
        assert 'ValueError' in row['last_error']
        assert 'unexpected' in row['last_error']

    def test_unknown_handler_error_subclass_treated_as_retryable(self, db, make_job):
        # A HandlerError that isn't one of the three known subclasses
        # (Quota/Permanent/Retryable) takes the generic HandlerError
        # branch and routes to schedule_retry.
        class UnknownHandlerError(HandlerError):
            pass

        make_job(attempts=0, max_attempts=5)
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        def handler(payload, ctx):
            raise UnknownHandlerError('custom')

        _process_one(handler, job, log)

        row = _get_job_row(db, job['id'])
        assert row['status'] == 'queued'
        assert row['last_error'] == 'custom'


class TestProcessOnePayload:
    def test_handler_receives_payload_and_context(self, db, make_job):
        # Pin the handler-call contract: (payload, ctx) with payload
        # being the dict stored on the job row and ctx a JobContext.
        make_job(payload={'rematch': True, 'extra': 'ok'})
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        seen = {}

        def handler(payload, ctx):
            seen['payload'] = payload
            seen['ctx_type'] = type(ctx).__name__
            seen['target_id'] = ctx.target_id
            return {'ok': True}

        _process_one(handler, job, log)

        assert seen['payload'] == {'rematch': True, 'extra': 'ok'}
        assert seen['ctx_type'] == 'JobContext'
        assert seen['target_id'] == str(job['target_id'])

    def test_empty_payload_coerced_to_dict(self, db, make_job):
        # The loop passes `job['payload'] or {}` so handlers never see None.
        make_job(payload={})
        job = claim.claim_next('youtube', 'match_recording', 'test-worker')

        seen = {}

        def handler(payload, ctx):
            seen['payload'] = payload
            return {'ok': True}

        _process_one(handler, job, log)

        assert seen['payload'] == {}
