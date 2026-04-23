"""
Tests for research_worker.quota — atomic consume + refund + mark_exhausted.

The today-bug this covers: refund() must let `units_used` decrement back
toward 0 after a job's actual API spend turns out to be far below the
worst-case reservation. Without it, every job costs 301 units regardless
of how few searches it actually fired, blowing the daily YouTube budget
in ~33 jobs.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from research_worker import quota
from research_worker.errors import QuotaExhausted, utcnow


# ---------------------------------------------------------------------------
# consume
# ---------------------------------------------------------------------------

class TestConsume:
    def test_succeeds_within_budget_and_increments_units(self, quota_row):
        quota_row.set(units_used=0)
        resets_at = quota.consume('youtube', 'day', 100)
        snap = quota_row.snapshot()
        assert snap['units_used'] == 100
        # Returned resets_at should match what's in the row.
        assert resets_at == snap['resets_at']

    def test_multiple_consumes_accumulate(self, quota_row):
        quota_row.set(units_used=0)
        quota.consume('youtube', 'day', 100)
        quota.consume('youtube', 'day', 50)
        assert quota_row.snapshot()['units_used'] == 150

    def test_raises_when_would_exceed_limit(self, quota_row):
        # 9750 used, asking for 301 → would push to 10051 > 10000.
        quota_row.set(units_used=9750)
        with pytest.raises(QuotaExhausted) as exc:
            quota.consume('youtube', 'day', 301)
        assert exc.value.source == 'youtube'
        # No partial deduction on failure.
        assert quota_row.snapshot()['units_used'] == 9750

    def test_exact_fit_at_limit_succeeds(self, quota_row):
        # Boundary: units_used + cost == units_limit must succeed.
        quota_row.set(units_used=9699)
        quota.consume('youtube', 'day', 301)
        assert quota_row.snapshot()['units_used'] == 10000

    def test_one_over_limit_raises(self, quota_row):
        quota_row.set(units_used=9700)
        with pytest.raises(QuotaExhausted):
            quota.consume('youtube', 'day', 301)

    def test_resets_window_when_resets_at_in_past(self, quota_row):
        # Simulate the day rollover: units_used near limit, resets_at in
        # the past. consume() should _maybe_reset first, zero units_used,
        # push resets_at forward, then succeed.
        quota_row.set(
            units_used=9900,
            resets_at=utcnow() - timedelta(minutes=5),
        )
        quota.consume('youtube', 'day', 100)
        snap = quota_row.snapshot()
        assert snap['units_used'] == 100  # 0 (after reset) + 100
        assert snap['resets_at'] > utcnow()


# ---------------------------------------------------------------------------
# refund (the today-bug fix)
# ---------------------------------------------------------------------------

class TestRefund:
    def test_returns_unused_units_to_bucket(self, quota_row):
        quota_row.set(units_used=301)  # post-reservation state
        quota.refund('youtube', 'day', 200)  # actual spend was 101
        assert quota_row.snapshot()['units_used'] == 101

    def test_refund_zero_is_noop(self, quota_row):
        quota_row.set(units_used=200)
        quota.refund('youtube', 'day', 0)
        assert quota_row.snapshot()['units_used'] == 200

    def test_refund_negative_is_noop(self, quota_row):
        # Defensive: a caller computing actual > reserved must not double-bill.
        quota_row.set(units_used=200)
        quota.refund('youtube', 'day', -50)
        assert quota_row.snapshot()['units_used'] == 200

    def test_refund_clamps_at_zero(self, quota_row):
        # Pathological: refund larger than current used (e.g. window
        # rolled over between consume + refund). Must not go negative —
        # source_quotas has CHECK (units_used >= 0).
        quota_row.set(units_used=50)
        quota.refund('youtube', 'day', 500)
        assert quota_row.snapshot()['units_used'] == 0

    def test_full_reserve_then_refund_round_trip(self, quota_row):
        # The actual flow: reserve 301, do work that cost 0 (e.g. cache
        # hit), refund 301. Bucket back to 0.
        quota_row.set(units_used=0)
        quota.consume('youtube', 'day', 301)
        assert quota_row.snapshot()['units_used'] == 301
        quota.refund('youtube', 'day', 301)
        assert quota_row.snapshot()['units_used'] == 0


# ---------------------------------------------------------------------------
# mark_exhausted
# ---------------------------------------------------------------------------

class TestMarkExhausted:
    def test_slams_units_used_to_limit(self, quota_row):
        quota_row.set(units_used=2000)
        resets_at = quota.mark_exhausted('youtube', 'day')
        snap = quota_row.snapshot()
        assert snap['units_used'] == snap['units_limit']
        assert resets_at == snap['resets_at']

    def test_subsequent_consume_raises(self, quota_row):
        quota_row.set(units_used=0)
        quota.mark_exhausted('youtube', 'day')
        # Even a 1-unit consume should fail now.
        with pytest.raises(QuotaExhausted):
            quota.consume('youtube', 'day', 1)


# ---------------------------------------------------------------------------
# current_resets_at + snapshot
# ---------------------------------------------------------------------------

class TestReadHelpers:
    def test_current_resets_at_returns_value(self, quota_row):
        ts = quota.current_resets_at('youtube', 'day')
        assert ts == quota_row.snapshot()['resets_at']

    def test_current_resets_at_none_for_unknown(self):
        assert quota.current_resets_at('nonexistent', 'day') is None

    def test_snapshot_returns_all_windows_for_source(self):
        rows = quota.snapshot('youtube')
        assert len(rows) >= 1
        assert all(r['source'] == 'youtube' for r in rows)
        windows = {r['window_name'] for r in rows}
        assert 'day' in windows
