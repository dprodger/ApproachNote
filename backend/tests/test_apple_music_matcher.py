"""
Pure-function tests for integrations.apple_music.matching.

Apple Music shares the primitive normalization/similarity helpers with
Spotify (see integrations.spotify.matching, covered in
test_spotify_matcher.py). This file covers the Apple-Music-specific
orchestration: album validation with compilation/name-variant handling
and the track picker.

No DB, no Apple/iTunes API, no AppleMusicMatcher class instantiation —
a minimal matcher stand-in carries the thresholds and logger that
validate_album_match / find_matching_track read.

See GH #138 for scope.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from integrations.apple_music.matching import (
    find_matching_track,
    validate_album_match,
)


log = logging.getLogger("test.apple_music_matcher")


def _matcher(strict: bool = True) -> SimpleNamespace:
    """
    Stand-in for AppleMusicMatcher carrying just the fields the
    matching functions read (logger + three similarity thresholds).
    Saves the real matcher's client/catalog/DB wiring.
    """
    if strict:
        return SimpleNamespace(
            logger=log,
            min_artist_similarity=75,
            min_album_similarity=65,
            min_track_similarity=85,
        )
    return SimpleNamespace(
        logger=log,
        min_artist_similarity=65,
        min_album_similarity=55,
        min_track_similarity=75,
    )


# ---------------------------------------------------------------------------
# validate_album_match
# ---------------------------------------------------------------------------

class TestValidateAlbumMatch:
    def test_accepts_clean_match(self):
        album = {
            'artist': 'Bill Evans Trio',
            'name': 'Waltz for Debby',
            'release_date': '1961-06-25',
        }
        ok, confidence = validate_album_match(
            _matcher(), album, 'Bill Evans Trio', 'Waltz for Debby',
            expected_year=1961,
        )
        assert ok is True
        assert confidence > 0.9

    def test_rejects_compilation_mismatch(self):
        # Hard reject: Apple says "Various Artists" but we expected a
        # specific artist. Without this, any Various-Artists comp that
        # happens to share an album title would false-positive.
        album = {
            'artist': 'Various Artists',
            'name': 'Jazz Classics',
            'release_date': None,
        }
        ok, confidence = validate_album_match(
            _matcher(), album, 'Miles Davis', 'Kind of Blue',
        )
        assert ok is False
        assert confidence == 0.0

    def test_both_compilations_accepted(self):
        album = {
            'artist': 'Various Artists',
            'name': 'Jazz Classics',
            'release_date': None,
        }
        ok, confidence = validate_album_match(
            _matcher(), album, 'Various Artists', 'Jazz Classics',
        )
        assert ok is True
        assert confidence > 0

    def test_name_variant_rescues_artist(self):
        # "Dave" vs "David" would fail raw fuzzy. The variant table
        # canonicalizes both to "david" so the match is accepted.
        album = {
            'artist': 'David Liebman',
            'name': 'Back on the Corner',
            'release_date': None,
        }
        ok, confidence = validate_album_match(
            _matcher(), album, 'Dave Liebman', 'Back on the Corner',
        )
        assert ok is True
        assert confidence > 0

    def test_year_bonus_increases_confidence(self):
        album = {
            'artist': 'Bill Evans Trio',
            'name': 'Waltz for Debby',
            'release_date': '1961-06-25',
        }
        _, with_year = validate_album_match(
            _matcher(), album, 'Bill Evans Trio', 'Waltz for Debby',
            expected_year=1961,
        )
        _, without_year = validate_album_match(
            _matcher(), album, 'Bill Evans Trio', 'Waltz for Debby',
        )
        assert with_year > without_year

    def test_rejects_unrelated_album(self):
        album = {
            'artist': 'Some Other Artist',
            'name': 'Completely Different Album',
            'release_date': None,
        }
        ok, confidence = validate_album_match(
            _matcher(), album, 'Miles Davis', 'Kind of Blue',
        )
        assert ok is False
        assert confidence == 0.0


# ---------------------------------------------------------------------------
# find_matching_track
# ---------------------------------------------------------------------------

class TestFindMatchingTrack:
    def test_picks_exact_title_match(self):
        tracks = [
            {'name': 'Autumn Leaves', 'disc_number': 1, 'track_number': 1},
            {'name': 'Blue in Green', 'disc_number': 1, 'track_number': 2},
        ]
        match = find_matching_track(_matcher(), 'Autumn Leaves', tracks)
        assert match is not None
        assert match['name'] == 'Autumn Leaves'
        assert '_match_confidence' in match

    def test_position_bonus_breaks_tie_between_takes(self):
        # Two titles normalize identically — only disc/track position
        # distinguishes them. The position-matching track wins.
        tracks = [
            {'name': 'Body and Soul - Take 1',
             'disc_number': 1, 'track_number': 1},
            {'name': 'Body and Soul - Take 2',
             'disc_number': 1, 'track_number': 2},
        ]
        match = find_matching_track(
            _matcher(), 'Body and Soul', tracks,
            expected_disc=1, expected_track=2,
        )
        assert match is not None
        assert match['name'] == 'Body and Soul - Take 2'

    def test_returns_none_when_no_candidate_passes_threshold(self):
        tracks = [
            {'name': 'Completely Unrelated', 'disc_number': 1, 'track_number': 1},
        ]
        assert find_matching_track(_matcher(), 'Take Five', tracks) is None

    def test_empty_tracklist_returns_none(self):
        assert find_matching_track(_matcher(), 'Take Five', []) is None

    def test_position_bonus_raises_reported_confidence(self):
        # Same track, same fuzzy title score — but with matching
        # disc/track numbers the returned _match_confidence is higher
        # thanks to the +10 position bonus. Callers downstream persist
        # this confidence on the streaming link, so the surfaced value
        # matters even when both lookups return a match.
        #
        # Fresh dicts per call: find_matching_track mutates the track
        # it returns to stamp _match_confidence.
        def tracks():
            return [{'name': 'A Simple Blues', 'disc_number': 1, 'track_number': 5}]

        without_pos = find_matching_track(_matcher(), 'Blues Simple', tracks())
        with_pos = find_matching_track(
            _matcher(), 'Blues Simple', tracks(),
            expected_disc=1, expected_track=5,
        )
        assert without_pos is not None and with_pos is not None
        assert with_pos['_match_confidence'] > without_pos['_match_confidence']
