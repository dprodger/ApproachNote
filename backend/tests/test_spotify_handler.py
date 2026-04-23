"""
Tests for research_worker.handlers.spotify.match_song.

Mirrors test_youtube_handler.py in shape — mocks SpotifyMatcher and
verifies the handler translates the matcher's `{success, error, stats}`
return shape into the worker's `(result | exception)` contract correctly.

The Spotify matcher catches all exceptions internally and returns
success=False with the error string, so we test outcome by simulating
those return values rather than by raising from the matcher.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from research_worker.errors import PermanentError, RetryableError
from research_worker.handlers import spotify as handler


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeCtx:
    """Stand-in for research_worker.loop.JobContext.

    Spotify handler doesn't call consume_quota (Spotify uses 429s, not a
    daily budget). target_id and log are all the handler needs.
    """

    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'spotify'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_matcher(mocker):
    """Mock the SpotifyMatcher class that the handler instantiates.

    Returns a SimpleNamespace where `instance.match_releases` is the mock
    used to drive the test's chosen outcome.
    """
    instance = mocker.MagicMock()
    instance.match_releases = mocker.MagicMock()
    mocker.patch(
        'research_worker.handlers.spotify.SpotifyMatcher',
        return_value=instance,
    )
    return SimpleNamespace(instance=instance)


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------

class TestSuccess:
    def test_success_returns_normalized_stats_dict(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True,
            'song': {'id': 'abc', 'title': 'Take Five'},
            'stats': {
                'releases_processed': 5,
                'releases_with_spotify': 4,
                'releases_updated': 4,
                'releases_no_match': 1,
                'tracks_matched': 12,
                'cache_hits': 3,
                'api_calls': 18,
                'rate_limit_hits': 0,
            },
        }
        result = handler.match_song({}, FakeCtx('song-id-1'))

        assert result == {
            'matched': True,
            'releases_processed': 5,
            'releases_with_spotify': 4,
            'releases_updated': 4,
            'releases_no_match': 1,
            'tracks_matched': 12,
            'cache_hits': 3,
            'api_calls': 18,
            'rate_limit_hits': 0,
        }

    def test_matched_false_when_no_releases_with_spotify(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True,
            'stats': {
                'releases_processed': 5,
                'releases_with_spotify': 0,
                'tracks_matched': 0,
            },
        }
        result = handler.match_song({}, FakeCtx('song-id-1'))
        assert result['matched'] is False
        assert result['releases_processed'] == 5

    def test_rematch_payload_passed_to_matcher(self, mocker, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True, 'stats': {},
        }
        # Capture the constructor call to verify rematch=True propagates.
        spotify_class = mocker.patch(
            'research_worker.handlers.spotify.SpotifyMatcher',
            return_value=patched_matcher.instance,
        )
        handler.match_song({'rematch': True}, FakeCtx('song-id-1'))

        kwargs = spotify_class.call_args.kwargs
        assert kwargs.get('rematch') is True

    def test_target_id_passed_to_match_releases(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True, 'stats': {},
        }
        handler.match_song({}, FakeCtx('song-uuid-xyz'))
        patched_matcher.instance.match_releases.assert_called_once_with('song-uuid-xyz')


# ---------------------------------------------------------------------------
# Permanent error: song not found
# ---------------------------------------------------------------------------

class TestPermanentError:
    def test_song_not_found_raises_permanent(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'Song not found',
        }
        with pytest.raises(PermanentError):
            handler.match_song({}, FakeCtx('missing'))

    def test_song_not_found_case_insensitive(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'song NOT FOUND',
        }
        with pytest.raises(PermanentError):
            handler.match_song({}, FakeCtx('missing'))


# ---------------------------------------------------------------------------
# No-op: no releases to match
# ---------------------------------------------------------------------------

class TestNoReleasesIsCleanNoOp:
    """The matcher reports 'No releases found' via success=False, but it's
    not a failure — there's just nothing to do. Handler must return cleanly
    so the job goes to 'done', not 'dead'."""

    def test_no_releases_returns_done_result(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'No releases found for this song',
        }
        result = handler.match_song({}, FakeCtx('song-1'))
        assert result == {
            'matched': False,
            'reason': 'no_releases',
            'releases_processed': 0,
        }


# ---------------------------------------------------------------------------
# Retryable: anything else
# ---------------------------------------------------------------------------

class TestRetryableError:
    def test_rate_limit_after_retries_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'Spotify rate limit exceeded after 3 retries',
        }
        with pytest.raises(RetryableError) as exc:
            handler.match_song({}, FakeCtx('song-1'))
        assert 'rate limit' in str(exc.value).lower()

    def test_unknown_error_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'error': 'connection reset by peer',
        }
        with pytest.raises(RetryableError):
            handler.match_song({}, FakeCtx('song-1'))

    def test_missing_error_field_is_retryable(self, patched_matcher):
        # Defensive: matcher returned success=False but no 'error' key.
        patched_matcher.instance.match_releases.return_value = {'success': False}
        with pytest.raises(RetryableError):
            handler.match_song({}, FakeCtx('song-1'))
