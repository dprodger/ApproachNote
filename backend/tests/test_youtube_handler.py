"""
Tests for research_worker.handlers.youtube.match_recording.

Most important coverage in this file: the quota-accounting paths that
caused the over-deduction bug discovered in production. Specifically:

  1. Skip outcomes (has_youtube=true with rematch=false, or no
     default_recording_release_id) MUST NOT consume any quota — the
     matcher would have skipped without making API calls.
  2. Match outcomes MUST refund the unspent portion of the worst-case
     reservation so net cost reflects actual API usage, not 301-flat.
  3. Upstream-driven QuotaExceeded MUST slam the bucket via
     mark_exhausted and skip the refund (the API itself confirmed empty).
  4. Other errors (PermanentError / RetryableError paths) MUST still
     refund unused units in the finally block.

The matcher and client are mocked — these tests are about the worker's
plumbing around them, not the matcher's matching quality.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from integrations.youtube.client import (
    QUOTA_COST_SEARCH,
    QUOTA_COST_VIDEOS,
    YouTubeAPIError,
    YouTubeQuotaExceededError,
)
from research_worker.errors import (
    PermanentError,
    QuotaExhausted,
    RetryableError,
)
from research_worker.handlers import youtube as handler
from research_worker import quota as quota_mod


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeCtx:
    """Stand-in for research_worker.loop.JobContext.

    Real consume_quota delegates to quota.consume — we do the same so the
    underlying source_quotas row gets touched and tests can assert on it.
    """

    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'youtube'
        self.log = logging.getLogger('test.fake_ctx')
        self.consume_quota_calls: list[int] = []

    def consume_quota(self, cost: int, window: str = 'day') -> None:
        self.consume_quota_calls.append(cost)
        quota_mod.consume(self.source, window, cost)


@pytest.fixture
def patched_youtube(mocker):
    """Mock YouTubeClient + YouTubeMatcher that the handler instantiates.

    Returns a SimpleNamespace with `client` and `matcher` so tests can:
      - Set `client.stats['quota_units']` to fake actual API usage
      - Configure `matcher.match_recording.return_value` / .side_effect
      - Assert on calls
    """
    client = mocker.MagicMock()
    client.stats = {'quota_units': 0}

    matcher = mocker.MagicMock()
    matcher.match_recording = mocker.MagicMock()

    mocker.patch(
        'research_worker.handlers.youtube.YouTubeClient',
        return_value=client,
    )
    mocker.patch(
        'research_worker.handlers.youtube.YouTubeMatcher',
        return_value=matcher,
    )
    return SimpleNamespace(client=client, matcher=matcher)


@pytest.fixture
def patched_load_recording(mocker):
    """Set the row that yt_db.load_recording will return for this test."""
    def _set(row: dict | None):
        mocker.patch(
            'research_worker.handlers.youtube.yt_db.load_recording',
            return_value=row,
        )
    return _set


# ---------------------------------------------------------------------------
# Pre-quota skip paths — the today-bug fixes
# ---------------------------------------------------------------------------

class TestSkipsCostZeroQuota:
    """Skip outcomes must not touch the quota bucket at all."""

    def test_has_youtube_and_not_rematch_returns_skip_without_consuming(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': True,
            'default_recording_release_id': 'rrid-1',
        })
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        result = handler.match_recording({}, ctx)

        assert result == {
            'matched': False,
            'skipped': 'has_youtube',
            'reason': 'skipped_has_youtube',
        }
        assert ctx.consume_quota_calls == [], "skip path must not consume quota"
        assert quota_row.snapshot()['units_used'] == 0
        patched_youtube.matcher.match_recording.assert_not_called()

    def test_no_default_release_returns_skip_without_consuming(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': None,
        })
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        result = handler.match_recording({}, ctx)

        assert result['skipped'] == 'no_default_release'
        assert ctx.consume_quota_calls == []
        assert quota_row.snapshot()['units_used'] == 0
        patched_youtube.matcher.match_recording.assert_not_called()

    def test_rematch_true_bypasses_has_youtube_skip(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # When the user clicks Force Refresh, has_youtube=true should NOT skip.
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': True,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.matcher.match_recording.return_value = {
            'matched': True,
            'video_id': 'abc',
            'video_url': 'https://...',
            'video_title': 't',
            'channel': 'c',
            'confidence': 1.0,
            'rows_written': 1,
        }
        ctx = FakeCtx('rec-1')

        result = handler.match_recording({'rematch': True}, ctx)

        assert result['matched'] is True
        assert ctx.consume_quota_calls == [handler.WORST_CASE_QUOTA]
        patched_youtube.matcher.match_recording.assert_called_once_with('rec-1')


# ---------------------------------------------------------------------------
# Quota refund — the today-bug fix
# ---------------------------------------------------------------------------

class TestRefundUnusedQuota:
    """After the matcher returns, unspent units must be refunded."""

    def test_full_refund_on_cache_hit(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # Matcher used 0 actual units (everything cached) -> bucket should
        # end at the same value it started at, despite reserving 301.
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = 0
        patched_youtube.matcher.match_recording.return_value = {
            'matched': True,
            'video_id': 'abc',
            'confidence': 0.9,
            'rows_written': 1,
        }
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        handler.match_recording({}, ctx)

        assert quota_row.snapshot()['units_used'] == 0

    def test_partial_refund_when_one_search_used(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # Matcher fired 1 search (100) + 1 videos (1) = 101 actual.
        # Net cost should be 101, refund 200 of the 301 reserved.
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        actual_used = QUOTA_COST_SEARCH + QUOTA_COST_VIDEOS  # 101
        patched_youtube.client.stats['quota_units'] = actual_used
        patched_youtube.matcher.match_recording.return_value = {
            'matched': True, 'video_id': 'abc',
        }
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        handler.match_recording({}, ctx)

        assert quota_row.snapshot()['units_used'] == actual_used

    def test_no_refund_when_actual_equals_worst_case(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # Worst case actually realized — refund is 0, no net change vs
        # what consume already did.
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = handler.WORST_CASE_QUOTA
        patched_youtube.matcher.match_recording.return_value = {
            'matched': False, 'reason': 'no_match',
        }
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        handler.match_recording({}, ctx)

        assert quota_row.snapshot()['units_used'] == handler.WORST_CASE_QUOTA


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestErrorPaths:
    def test_missing_recording_raises_permanent_without_quota(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording(None)
        ctx = FakeCtx('rec-missing')
        quota_row.set(units_used=0)

        with pytest.raises(PermanentError):
            handler.match_recording({}, ctx)

        assert ctx.consume_quota_calls == []
        assert quota_row.snapshot()['units_used'] == 0

    def test_upstream_quota_exceeded_marks_bucket_full_and_skips_refund(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # Even if our local bucket said we had room, an upstream 403
        # quotaExceeded is authoritative — slam units_used to the limit
        # and surface QuotaExhausted to the loop. NO refund (the API
        # confirmed we're empty regardless of our counter).
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        # Matcher pretends it tried, then upstream said no.
        patched_youtube.client.stats['quota_units'] = 50
        patched_youtube.matcher.match_recording.side_effect = (
            YouTubeQuotaExceededError("403 quotaExceeded")
        )
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        with pytest.raises(QuotaExhausted):
            handler.match_recording({}, ctx)

        snap = quota_row.snapshot()
        # mark_exhausted slammed units_used to the limit, AND no refund
        # subtracted from it.
        assert snap['units_used'] == snap['units_limit']

    def test_youtube_api_error_raises_permanent_and_still_refunds(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = 0  # error before any call
        patched_youtube.matcher.match_recording.side_effect = YouTubeAPIError("400 bad")
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        with pytest.raises(PermanentError):
            handler.match_recording({}, ctx)

        # Reserved 301, used 0, must refund 301 -> back to 0.
        assert quota_row.snapshot()['units_used'] == 0

    def test_generic_exception_raises_retryable_and_still_refunds(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = QUOTA_COST_SEARCH  # one search burned
        patched_youtube.matcher.match_recording.side_effect = ConnectionError("dns blip")
        ctx = FakeCtx('rec-1')
        quota_row.set(units_used=0)

        with pytest.raises(RetryableError):
            handler.match_recording({}, ctx)

        # 301 reserved, 100 actually used, refund 201 -> end at 100.
        assert quota_row.snapshot()['units_used'] == QUOTA_COST_SEARCH

    def test_consume_quota_failure_short_circuits_before_matcher(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        # Bucket already empty before this job — should raise QuotaExhausted
        # without ever calling the matcher.
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        ctx = FakeCtx('rec-1')
        snap = quota_row.snapshot()
        quota_row.set(units_used=snap['units_limit'])

        with pytest.raises(QuotaExhausted):
            handler.match_recording({}, ctx)

        patched_youtube.matcher.match_recording.assert_not_called()


# ---------------------------------------------------------------------------
# Result shaping
# ---------------------------------------------------------------------------

class TestResultShape:
    def test_match_result_includes_video_fields(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = 101
        patched_youtube.matcher.match_recording.return_value = {
            'matched': True,
            'video_id': 'abc123',
            'video_url': 'https://www.youtube.com/watch?v=abc123',
            'video_title': 'Some Tune',
            'channel': 'Some Artist - Topic',
            'confidence': 0.95,
            'rows_written': 2,
        }
        result = handler.match_recording({}, FakeCtx('rec-1'))

        assert result == {
            'matched': True,
            'video_id': 'abc123',
            'video_url': 'https://www.youtube.com/watch?v=abc123',
            'video_title': 'Some Tune',
            'channel': 'Some Artist - Topic',
            'confidence': 0.95,
            'rows_written': 2,
        }

    def test_no_match_result_normalises_reason(
        self, quota_row, patched_load_recording, patched_youtube,
    ):
        patched_load_recording({
            'recording_id': 'rec-1',
            'has_youtube': False,
            'default_recording_release_id': 'rrid-1',
        })
        patched_youtube.client.stats['quota_units'] = 301
        patched_youtube.matcher.match_recording.return_value = {
            'recording_id': 'rec-1',
            'matched': False,
            'rejected': [],
        }
        result = handler.match_recording({}, FakeCtx('rec-1'))

        assert result == {
            'matched': False,
            'skipped': None,
            'reason': 'no_match',
        }
