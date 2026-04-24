"""
Tests for research_worker.handlers.apple.match_song.

Mirrors test_spotify_handler.py — mocks AppleMusicMatcher and verifies
the handler translates the matcher's `{success, message, stats}` return
shape into the worker's `(result | exception)` contract.

Two notable differences from the Spotify handler tests:
  - Apple matcher returns `success=True` even when there are no releases
    (Spotify returned `success=False` for that case), so there's no
    no-op path to test.
  - Apple uses `message` instead of `error` for the failure reason.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from research_worker.errors import PermanentError, RetryableError
from research_worker.handlers import apple as handler


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeCtx:
    """Stand-in for research_worker.loop.JobContext.

    Apple handler doesn't call consume_quota — local catalog usage means
    no daily budget to track.
    """

    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'apple'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_matcher(mocker):
    """Mock the AppleMusicMatcher class that the handler instantiates."""
    instance = mocker.MagicMock()
    instance.match_releases = mocker.MagicMock()
    mocker.patch(
        'research_worker.handlers.apple.AppleMusicMatcher',
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
            'message': 'Processed 5 releases',
            'stats': {
                'releases_processed': 5,
                'releases_matched': 4,
                'releases_with_apple_music': 4,
                'releases_no_match': 1,
                'tracks_matched': 12,
                'tracks_no_match': 3,
                'artwork_added': 2,
                'cache_hits': 3,
                'api_calls': 7,
                'catalog_queries': 5,
            },
        }
        result = handler.match_song({}, FakeCtx('song-id-1'))

        assert result == {
            'matched': True,
            'releases_processed': 5,
            'releases_matched': 4,
            'releases_with_apple_music': 4,
            'releases_no_match': 1,
            'tracks_matched': 12,
            'tracks_no_match': 3,
            'artwork_added': 2,
            'cache_hits': 3,
            'api_calls': 7,
            'catalog_queries': 5,
        }

    def test_matched_false_when_no_releases_matched(self, patched_matcher):
        # Apple reports "no releases" via success=True with a message —
        # confirmed by the matcher source. Treat it as a successful no-op.
        patched_matcher.instance.match_releases.return_value = {
            'success': True,
            'message': 'No releases found for song: Foo',
            'stats': {
                'releases_processed': 0,
                'releases_matched': 0,
            },
        }
        result = handler.match_song({}, FakeCtx('song-id-1'))
        assert result['matched'] is False
        assert result['releases_processed'] == 0

    def test_rematch_payload_passed_to_matcher(self, mocker, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': True, 'stats': {},
        }
        apple_class = mocker.patch(
            'research_worker.handlers.apple.AppleMusicMatcher',
            return_value=patched_matcher.instance,
        )
        handler.match_song({'rematch': True}, FakeCtx('song-id-1'))

        kwargs = apple_class.call_args.kwargs
        assert kwargs.get('rematch') is True
        # local_catalog_only must mirror the previous in-process config
        # so behavior is identical.
        assert kwargs.get('local_catalog_only') is True

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
            'message': 'Song not found: missing-song',
        }
        with pytest.raises(PermanentError):
            handler.match_song({}, FakeCtx('missing-song'))

    def test_song_not_found_case_insensitive(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'message': 'song NOT FOUND',
        }
        with pytest.raises(PermanentError):
            handler.match_song({}, FakeCtx('missing'))


# ---------------------------------------------------------------------------
# Retryable: anything else
# ---------------------------------------------------------------------------

class TestRetryableError:
    def test_unknown_failure_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {
            'success': False,
            'message': 'catalog connection lost',
        }
        with pytest.raises(RetryableError):
            handler.match_song({}, FakeCtx('song-1'))

    def test_missing_message_field_is_retryable(self, patched_matcher):
        patched_matcher.instance.match_releases.return_value = {'success': False}
        with pytest.raises(RetryableError):
            handler.match_song({}, FakeCtx('song-1'))
