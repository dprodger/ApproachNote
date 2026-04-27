"""
Tests for the Spotify duration-mismatch rematch path (issue #100, phase 2):

  - research_worker/handlers/spotify.py::rematch_duration_mismatches  — handler
  - core/spotify_rematch_mismatches.py                                — sweep

The handler delegates to SpotifyMatcher, which we mock — same pattern as
test_spotify_handler.py for match_song. The sweep is exercised against
the real test DB using the existing autouse research_jobs cleanup.

We don't re-test the matcher itself here (covered by test_spotify_matcher.py
and the existing match_song handler tests); we only assert the handler
constructs SpotifyMatcher with the right threshold and translates its
return shape into the worker's contract correctly.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from core import spotify_rematch_mismatches
from research_worker.errors import PermanentError, RetryableError
from research_worker.handlers import spotify as handler


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeCtx:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'spotify'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_matcher(mocker):
    """Mock the SpotifyMatcher class the handler instantiates."""
    instance = mocker.MagicMock()
    instance.match_releases = mocker.MagicMock()
    cls_mock = mocker.patch(
        'research_worker.handlers.spotify.SpotifyMatcher',
        return_value=instance,
    )
    return SimpleNamespace(cls=cls_mock, instance=instance)


# ---------------------------------------------------------------------------
# Handler — rematch_duration_mismatches
# ---------------------------------------------------------------------------

class TestHandlerSuccess:
    def test_default_threshold_used_when_payload_omits_it(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True,
            'song': {'id': 'abc'},
            'stats': {
                'releases_processed': 3,
                'releases_updated': 2,
                'releases_no_match': 0,
                'tracks_matched': 5,
                'tracks_had_previous': 1,
                'cache_hits': 0,
                'api_calls': 7,
                'rate_limit_hits': 0,
            },
        }

        result = handler.rematch_duration_mismatches({}, FakeCtx('song-1'))

        # 60_000 ms is the default — must reach the matcher constructor.
        kwargs = patched_matcher.cls.call_args.kwargs
        assert kwargs['duration_mismatch_threshold'] == 60_000
        assert result['threshold_ms'] == 60_000
        assert result['releases_updated'] == 2
        assert result['tracks_matched'] == 5

    def test_payload_threshold_overrides_default(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True,
            'song': {'id': 'abc'},
            'stats': {},
        }

        result = handler.rematch_duration_mismatches(
            {'threshold_ms': 30_000}, FakeCtx('song-1'),
        )

        kwargs = patched_matcher.cls.call_args.kwargs
        assert kwargs['duration_mismatch_threshold'] == 30_000
        assert result['threshold_ms'] == 30_000

    def test_target_id_passed_to_match_releases(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True, 'song': {}, 'stats': {},
        }
        handler.rematch_duration_mismatches({}, FakeCtx('song-uuid-xyz'))
        patched_matcher.instance.match_releases.assert_called_once_with(
            'song-uuid-xyz',
        )


class TestHandlerErrorPaths:
    def test_song_not_found_raises_permanent(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False, 'error': 'Song not found',
        }
        with pytest.raises(PermanentError):
            handler.rematch_duration_mismatches({}, FakeCtx('missing'))

    def test_no_releases_is_clean_no_op(self, patched_matcher):
        # Whatever found mismatches when the sweep enqueued may have been
        # cleaned up by the time the worker drains — handle that race.
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'song': {'id': 'abc'},
            'error': 'No releases found for this song',
        }

        result = handler.rematch_duration_mismatches({}, FakeCtx('song-1'))
        assert result['reason'] == 'no_mismatched_releases'
        assert result['releases_processed'] == 0
        assert result['threshold_ms'] == 60_000

    def test_rate_limit_after_retries_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'Rate limit exceeded after 3 retries',
        }
        with pytest.raises(RetryableError):
            handler.rematch_duration_mismatches({}, FakeCtx('song-1'))

    def test_unknown_error_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'Connection reset by peer',
        }
        with pytest.raises(RetryableError):
            handler.rematch_duration_mismatches({}, FakeCtx('song-1'))


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_default_threshold_constant_is_sixty_seconds(self):
        # Guards against silent drift from the admin UI / matcher CLI.
        assert spotify_rematch_mismatches.DEFAULT_THRESHOLD_MS == 60_000

    def test_no_candidates_returns_zero_with_threshold_in_payload(self, mocker):
        mocker.patch(
            'core.spotify_rematch_mismatches.get_songs_with_duration_mismatches',
            return_value=[],
        )
        result = spotify_rematch_mismatches.enqueue_sweep(threshold_ms=45_000)
        assert result == {
            'candidates': 0,
            'enqueued': 0,
            'errors': 0,
            'threshold_ms': 45_000,
        }

    def test_threshold_is_passed_through_to_db_helper(self, mocker):
        spy = mocker.patch(
            'core.spotify_rematch_mismatches.get_songs_with_duration_mismatches',
            return_value=[],
        )
        spotify_rematch_mismatches.enqueue_sweep(threshold_ms=15_000)
        spy.assert_called_once_with(threshold_ms=15_000)

    def test_enqueue_sweep_creates_jobs_with_threshold_payload(
        self, mocker, db,
    ):
        # Stub the candidate query so the test doesn't depend on whatever
        # mismatch state the dev DB happens to be in.
        fake_song_ids = [
            '00000000-0000-4000-8000-2000000000a1',
            '00000000-0000-4000-8000-2000000000a2',
        ]
        mocker.patch(
            'core.spotify_rematch_mismatches.get_songs_with_duration_mismatches',
            return_value=[{'id': sid, 'title': 'Test'} for sid in fake_song_ids],
        )
        # Stub research_jobs.enqueue too so we don't have to satisfy the
        # FK constraint on target_id (no real song row exists for these ids).
        captured_calls = []

        def _fake_enqueue(**kwargs):
            captured_calls.append(kwargs)
            return 999

        mocker.patch(
            'core.spotify_rematch_mismatches.research_jobs.enqueue',
            side_effect=_fake_enqueue,
        )

        result = spotify_rematch_mismatches.enqueue_sweep(threshold_ms=45_000)

        assert result == {
            'candidates': 2,
            'enqueued': 2,
            'errors': 0,
            'threshold_ms': 45_000,
        }
        # Each enqueue carries the threshold so the handler re-checks
        # against the same value the sweep used.
        for call in captured_calls:
            assert call['source'] == 'spotify'
            assert call['job_type'] == 'rematch_duration_mismatches'
            assert call['target_type'] == 'song'
            assert call['payload'] == {'threshold_ms': 45_000}
        assert {c['target_id'] for c in captured_calls} == set(fake_song_ids)

    def test_limit_caps_candidates(self, mocker):
        mocker.patch(
            'core.spotify_rematch_mismatches.get_songs_with_duration_mismatches',
            return_value=[
                {'id': f'00000000-0000-4000-8000-3000000000{i:02x}',
                 'title': 'Test'}
                for i in range(5)
            ],
        )
        ids = spotify_rematch_mismatches.find_candidate_song_ids(limit=2)
        assert len(ids) == 2

    def test_enqueue_failure_counts_as_error_not_success(self, mocker):
        mocker.patch(
            'core.spotify_rematch_mismatches.get_songs_with_duration_mismatches',
            return_value=[
                {'id': '00000000-0000-4000-8000-300000000099', 'title': 'X'},
            ],
        )
        # research_jobs.enqueue can return None on a hard insert failure.
        mocker.patch(
            'core.spotify_rematch_mismatches.research_jobs.enqueue',
            return_value=None,
        )
        result = spotify_rematch_mismatches.enqueue_sweep()
        assert result['candidates'] == 1
        assert result['enqueued'] == 0
        assert result['errors'] == 1
