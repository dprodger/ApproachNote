"""
Tests for the album-fit display logic in routes.admin.

The route's heavy lifting lives in `_compute_album_fits_for_rows`, which:
  - skips rows without a spotify_album_id,
  - dedupes by release_id so each album is fetched at most once per request,
  - swallows fetch / matcher failures gracefully (the page must still render),
  - and returns a dict of release_id -> fit data.

We mock SpotifyClient.get_album_tracks and the
check_album_context_via_tracklist helper so the tests don't hit Spotify
or MusicBrainz.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest


log = logging.getLogger("test.album_fit")


@pytest.fixture
def patched_helpers(mocker):
    """Patch the upstream calls that _compute_album_fits_for_rows makes
    (SpotifyClient + check_album_context_via_tracklist) so we drive
    behaviour from the test."""
    spotify_client = MagicMock()
    spotify_client.get_album_tracks = MagicMock(
        return_value=[{'name': 'Track 1'}, {'name': 'Track 2'}],
    )
    mocker.patch(
        'routes.admin.SpotifyClient',
        return_value=spotify_client,
    ) if False else None  # placeholder

    # SpotifyClient and check_album_context_via_tracklist are imported
    # *inside* the function, so patch by their fully-qualified module path.
    sp_cls = mocker.patch(
        'integrations.spotify.client.SpotifyClient',
        return_value=spotify_client,
    )
    fit_fn = mocker.patch(
        'integrations.spotify.matching.check_album_context_via_tracklist',
        return_value={
            'mb_track_count': 12,
            'spotify_track_count': 17,
            'matched_count': 12,
            'match_ratio': 1.0,
            'matched_titles': [],
        },
    )
    return {
        'spotify_client_cls': sp_cls,
        'spotify_client': spotify_client,
        'check_fit': fit_fn,
    }


@pytest.fixture
def conn():
    """A minimal stand-in for a DB connection. The mocked
    check_album_context_via_tracklist never touches it, so a sentinel
    object is fine."""
    return object()


def _row(release_id: str, spotify_album_id: str | None = 'sp-album-1'):
    return {'release_id': release_id, 'spotify_album_id': spotify_album_id}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_empty_rows_returns_empty_dict(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    result = _compute_album_fits_for_rows(conn, [], log=log)
    assert result == {}
    # No client constructed when there's nothing to do.
    patched_helpers['spotify_client_cls'].assert_not_called()


def test_row_without_spotify_album_id_is_skipped(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    rows = [_row('rel-1', spotify_album_id=None)]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert result == {}
    patched_helpers['spotify_client'].get_album_tracks.assert_not_called()


def test_row_with_album_id_returns_fit(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    rows = [_row('rel-1', 'sp-album-1')]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert 'rel-1' in result
    assert result['rel-1']['matched_count'] == 12
    assert result['rel-1']['match_ratio'] == 1.0
    patched_helpers['spotify_client'].get_album_tracks.assert_called_once_with('sp-album-1')


def test_repeated_release_id_is_computed_once(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    # Two streaming-link rows on the same release — we should only hit
    # the upstream APIs once.
    rows = [_row('rel-1'), _row('rel-1')]
    _compute_album_fits_for_rows(conn, rows, log=log)
    patched_helpers['spotify_client'].get_album_tracks.assert_called_once()
    patched_helpers['check_fit'].assert_called_once()


def test_spotify_fetch_failure_is_swallowed(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    patched_helpers['spotify_client'].get_album_tracks.side_effect = RuntimeError(
        "spotify down")
    rows = [_row('rel-1')]
    # Must NOT raise — page render trumps album-fit signal.
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert result == {}


def test_spotify_returns_no_tracks_is_skipped(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    patched_helpers['spotify_client'].get_album_tracks.return_value = None
    rows = [_row('rel-1')]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert result == {}


def test_check_fit_failure_is_swallowed(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    patched_helpers['check_fit'].side_effect = RuntimeError("mb api down")
    rows = [_row('rel-1')]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert result == {}


def test_zero_mb_tracks_is_skipped(conn, patched_helpers):
    """If MB returns nothing for a release (no MB ID, network blip, etc.)
    the function returns mb_track_count=0 — we don't show an empty 0/0
    badge, just leave the cell out so the template can render '—'."""
    from routes.admin import _compute_album_fits_for_rows
    patched_helpers['check_fit'].return_value = {
        'mb_track_count': 0,
        'spotify_track_count': 0,
        'matched_count': 0,
        'match_ratio': 0.0,
        'matched_titles': [],
    }
    rows = [_row('rel-1')]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert result == {}


def test_multiple_releases_each_computed(conn, patched_helpers):
    from routes.admin import _compute_album_fits_for_rows
    rows = [
        _row('rel-1', 'sp-album-1'),
        _row('rel-2', 'sp-album-2'),
        _row('rel-1', 'sp-album-1'),  # duplicate, should not double-call
    ]
    result = _compute_album_fits_for_rows(conn, rows, log=log)
    assert set(result.keys()) == {'rel-1', 'rel-2'}
    # Two unique album IDs → two fetches.
    assert patched_helpers['spotify_client'].get_album_tracks.call_count == 2
    assert patched_helpers['check_fit'].call_count == 2
