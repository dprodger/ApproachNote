"""
Tests for core/spotify_live_suffix_backfill.py — the targeted backfill for
Spotify links rejected by the version-keyword asymmetry guard on live
albums (the "After the Fall" case: MB "I'll See You Again" vs Spotify
"I'll See You Again - Live").

The matcher itself is covered by test_spotify_matcher.py, so it's mocked
here — same pattern as test_spotify_rematch_mismatches.py. What matters in
this module is the planning and the matcher modes it chooses, because the
wrong mode on the wrong group destroys good links:

  - release WITH a Spotify album link -> rematch_tracks, no force_refresh,
    never rematch_all. Fills only missing track links; cannot clear the
    release's existing Spotify data.
  - release WITHOUT one -> rematch + force_refresh (force_refresh is what
    gets past the cached track-match failure), never rematch_all.
"""

from __future__ import annotations

import logging

import pytest

from core import spotify_live_suffix_backfill as backfill


log = logging.getLogger('test.live_suffix_backfill')


def pair(song, release, has_album_link):
    return {'song_id': song, 'release_id': release,
            'has_album_link': has_album_link}


# ---------------------------------------------------------------------------
# plan_from_pairs
# ---------------------------------------------------------------------------

class TestPlanFromPairs:
    def test_splits_on_album_link_presence(self):
        plan = backfill.plan_from_pairs([
            pair('s1', 'r1', True),
            pair('s1', 'r2', False),
            pair('s2', 'r3', True),
        ])
        assert plan['tracks_only'] == {'s1': ['r1'], 's2': ['r3']}
        assert plan['full_search'] == {'s1': ['r2']}

    def test_groups_every_release_of_a_song_in_one_entry(self):
        # One matcher call per (song, mode) is the point of grouping — the
        # matcher's entry point is per-song with a release filter.
        plan = backfill.plan_from_pairs([
            pair('s1', 'r1', False),
            pair('s1', 'r2', False),
            pair('s1', 'r3', False),
        ])
        assert plan['full_search'] == {'s1': ['r1', 'r2', 'r3']}
        assert plan['tracks_only'] == {}

    def test_empty_input(self):
        assert backfill.plan_from_pairs([]) == {
            'tracks_only': {}, 'full_search': {}}


# ---------------------------------------------------------------------------
# run_backfill — matcher construction
# ---------------------------------------------------------------------------

def _ok(stats=None):
    return {'success': True, 'stats': stats or {}}


class RecordingFactory:
    """Captures the kwargs each matcher was built with, and the
    match_releases calls made on it."""

    def __init__(self, result=None, raises=None):
        self.calls = []        # list of (kwargs, song_id, release_ids)
        self._result = result if result is not None else _ok()
        self._raises = raises

    def __call__(self, **kwargs):
        factory = self

        class FakeMatcher:
            def match_releases(self, song_id, release_ids=None):
                factory.calls.append((kwargs, song_id, release_ids))
                if factory._raises:
                    raise factory._raises
                return factory._result

        return FakeMatcher()


class TestMatcherModes:
    def test_known_album_uses_rematch_tracks(self):
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', True)],
                              logger_=log, matcher_factory=f)
        kwargs, song_id, release_ids = f.calls[0]
        assert kwargs['rematch_tracks'] is True
        assert kwargs.get('rematch') in (None, False)
        assert song_id == 's1'
        assert release_ids == ['r1']

    def test_missing_album_uses_rematch(self):
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', False)],
                              logger_=log, matcher_factory=f)
        kwargs, _, _ = f.calls[0]
        assert kwargs['rematch'] is True
        assert kwargs.get('rematch_tracks') in (None, False)

    def test_force_refresh_is_off_by_default(self):
        # The failure-cache purge makes it unnecessary, and it would
        # multiply API calls across songs sharing a release.
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', False)],
                              logger_=log, matcher_factory=f)
        assert f.calls[0][0]['force_refresh'] is False

        f2 = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', False)], force_refresh=True,
                              logger_=log, matcher_factory=f2)
        assert f2.calls[0][0]['force_refresh'] is True

    def test_dry_run_forces_refresh_for_the_searching_mode_only(self):
        # A dry run can't delete the cache files, so the album-search mode
        # needs force_refresh to get past them and preview anything at
        # all. tracks_only never checks that cache, so it stays cheap.
        f = RecordingFactory()
        backfill.run_backfill(
            [pair('s1', 'rFULL', False), pair('s1', 'rTRACKS', True)],
            dry_run=True, logger_=log, matcher_factory=f)
        modes = {kwargs.get('rematch_tracks') and 'tracks_only' or 'full_search':
                 kwargs['force_refresh'] for kwargs, _, _ in f.calls}
        assert modes['full_search'] is True
        assert modes['tracks_only'] is False

    @pytest.mark.parametrize('has_album_link', [True, False])
    def test_never_sets_rematch_all(self, has_album_link):
        # rematch_all would re-evaluate links that are already good and
        # clear any the fresh pass rejected. Signal B selects exactly the
        # releases that have good links on their other recordings.
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', has_album_link)],
                              logger_=log, matcher_factory=f)
        kwargs, _, _ = f.calls[0]
        assert kwargs.get('rematch_all') in (None, False)

    def test_release_filter_is_always_passed(self):
        # Without release_ids the matcher walks every release of the song,
        # which is the ~92x cost this script exists to avoid.
        f = RecordingFactory()
        backfill.run_backfill(
            [pair('s1', 'r1', True), pair('s1', 'r2', False)],
            logger_=log, matcher_factory=f)
        assert all(rel for _, _, rel in f.calls)
        assert len(f.calls) == 2       # one per mode

    def test_album_context_off_by_default_and_passed_when_set(self):
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', True)],
                              logger_=log, matcher_factory=f)
        assert f.calls[0][0]['album_context'] is None

        f2 = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', True)], album_context='rescue',
                              logger_=log, matcher_factory=f2)
        assert f2.calls[0][0]['album_context'] == 'rescue'

    def test_dry_run_reaches_the_matcher(self):
        f = RecordingFactory()
        backfill.run_backfill([pair('s1', 'r1', True)], dry_run=True,
                              logger_=log, matcher_factory=f)
        assert f.calls[0][0]['dry_run'] is True


# ---------------------------------------------------------------------------
# run_backfill — stats and resilience
# ---------------------------------------------------------------------------

class TestStats:
    def test_empty_pairs_short_circuits(self):
        f = RecordingFactory()
        stats = backfill.run_backfill([], logger_=log, matcher_factory=f)
        assert f.calls == []
        assert stats['pairs'] == 0
        assert stats['tracks_matched'] == 0

    def test_counts_pairs_songs_and_modes(self):
        f = RecordingFactory()
        stats = backfill.run_backfill([
            pair('s1', 'r1', True),
            pair('s1', 'r2', False),
            pair('s2', 'r3', False),
        ], logger_=log, matcher_factory=f)
        assert stats['pairs'] == 3
        assert stats['songs'] == 2
        assert stats['tracks_only_pairs'] == 1
        assert stats['full_search_pairs'] == 2

    def test_one_song_in_both_modes_is_two_runs_but_one_song(self):
        # Guards the summary from reading "songs=2/1": a song with
        # candidates in both modes gets two matcher calls.
        f = RecordingFactory()
        stats = backfill.run_backfill(
            [pair('s1', 'r1', True), pair('s1', 'r2', False)],
            logger_=log, matcher_factory=f)
        assert stats['songs'] == 1
        assert stats['song_runs_ok'] == 2

    def test_aggregates_matcher_stats(self):
        f = RecordingFactory(result=_ok({
            'releases_processed': 2, 'releases_with_spotify': 1,
            'tracks_matched': 3, 'api_calls': 4, 'cache_hits': 5,
        }))
        stats = backfill.run_backfill(
            [pair('s1', 'r1', True), pair('s2', 'r2', True)],
            logger_=log, matcher_factory=f)
        assert stats['tracks_matched'] == 6     # 3 per song, two songs
        assert stats['releases_with_spotify'] == 2
        assert stats['api_calls'] == 8
        assert stats['song_runs_ok'] == 2

    def test_a_raising_song_does_not_abort_the_run(self):
        f = RecordingFactory(raises=RuntimeError('spotify exploded'))
        stats = backfill.run_backfill(
            [pair('s1', 'r1', True), pair('s2', 'r2', True)],
            logger_=log, matcher_factory=f)
        assert stats['song_runs_failed'] == 2
        assert stats['song_runs_ok'] == 0
        assert len(f.calls) == 2       # both attempted

    def test_unsuccessful_result_counts_as_failed_but_rolls_up_stats(self):
        f = RecordingFactory(result={
            'success': False, 'error': 'No releases found for this song',
            'stats': {'tracks_matched': 1},
        })
        stats = backfill.run_backfill([pair('s1', 'r1', True)],
                                      logger_=log, matcher_factory=f)
        assert stats['song_runs_failed'] == 1
        assert stats['tracks_matched'] == 1

    def test_missing_stats_key_is_tolerated(self):
        f = RecordingFactory(result={'success': True})
        stats = backfill.run_backfill([pair('s1', 'r1', True)],
                                      logger_=log, matcher_factory=f)
        assert stats['tracks_matched'] == 0
        assert stats['song_runs_ok'] == 1

    def test_progress_callback_reports_per_mode(self):
        seen = []
        f = RecordingFactory()
        backfill.run_backfill(
            [pair('s1', 'r1', True), pair('s2', 'r2', True),
             pair('s3', 'r3', False)],
            logger_=log, matcher_factory=f,
            progress_callback=lambda mode, done, total: seen.append(
                (mode, done, total)))
        assert ('tracks_only', 1, 2) in seen
        assert ('tracks_only', 2, 2) in seen
        assert ('full_search', 1, 1) in seen


# ---------------------------------------------------------------------------
# purge_failure_cache
# ---------------------------------------------------------------------------

class TestPurgeFailureCache:
    def _cache(self, tmp_path, names):
        d = tmp_path / 'track_failures'
        d.mkdir()
        for n in names:
            (d / n).write_text('{}')
        return tmp_path

    def test_purges_entry_for_the_pair_whatever_the_album(self, tmp_path):
        # The album id isn't known before the search, hence the glob.
        cache = self._cache(tmp_path, ['fail_s1_r1_ALBUMXYZ.json'])
        removed = backfill.purge_failure_cache(
            [pair('s1', 'r1', False)], logger_=log, cache_dir=cache)
        assert removed == 1
        assert not (cache / 'track_failures' / 'fail_s1_r1_ALBUMXYZ.json').exists()

    def test_leaves_other_pairs_alone(self, tmp_path):
        cache = self._cache(tmp_path, [
            'fail_s1_r1_A.json',
            'fail_s1_r2_A.json',       # different release
            'fail_s9_r1_A.json',       # different song
        ])
        removed = backfill.purge_failure_cache(
            [pair('s1', 'r1', False)], logger_=log, cache_dir=cache)
        assert removed == 1
        remaining = sorted(p.name for p in (cache / 'track_failures').iterdir())
        assert remaining == ['fail_s1_r2_A.json', 'fail_s9_r1_A.json']

    def test_dry_run_counts_without_deleting(self, tmp_path):
        cache = self._cache(tmp_path, ['fail_s1_r1_A.json'])
        removed = backfill.purge_failure_cache(
            [pair('s1', 'r1', False)], dry_run=True, logger_=log,
            cache_dir=cache)
        assert removed == 1
        assert (cache / 'track_failures' / 'fail_s1_r1_A.json').exists()

    def test_missing_directory_is_not_an_error(self, tmp_path):
        assert backfill.purge_failure_cache(
            [pair('s1', 'r1', False)], logger_=log, cache_dir=tmp_path) == 0

    def test_run_backfill_purges_only_full_search_pairs(self, tmp_path, mocker):
        # tracks_only never reaches the failure-cache check, so its
        # entries (if any) must be left in place.
        cache = self._cache(tmp_path, [
            'fail_s1_rFULL_A.json',
            'fail_s1_rTRACKS_A.json',
        ])
        mocker.patch('core.cache_utils.get_cache_dir', return_value=cache)
        f = RecordingFactory()
        stats = backfill.run_backfill(
            [pair('s1', 'rFULL', False), pair('s1', 'rTRACKS', True)],
            logger_=log, matcher_factory=f)
        assert stats['failure_cache_purged'] == 1
        assert (cache / 'track_failures' / 'fail_s1_rTRACKS_A.json').exists()
        assert not (cache / 'track_failures' / 'fail_s1_rFULL_A.json').exists()


# ---------------------------------------------------------------------------
# Candidate SQL constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_ratio_matches_the_matcher(self):
        from integrations.spotify.matching import _ALBUM_WIDE_VERSION_RATIO
        assert backfill.ALBUM_WIDE_RATIO == _ALBUM_WIDE_VERSION_RATIO

    def test_regex_covers_every_matcher_keyword(self):
        from integrations.spotify.matching import _VERSION_KEYWORDS
        for kw in _VERSION_KEYWORDS:
            assert kw in backfill.VERSION_KEYWORD_REGEX, kw

    def test_regex_uses_postgres_word_boundaries(self):
        # \m / \M keep 'live' from matching inside 'olive', mirroring the
        # word-boundary discipline in has_version_keyword.
        assert backfill.VERSION_KEYWORD_REGEX.startswith(r'\m')
        assert backfill.VERSION_KEYWORD_REGEX.endswith(r'\M')
