"""
Tests for the per-release-track-length pipeline.

Three layers under test:

1. integrations.musicbrainz.db.link_recording_to_release populates
   recording_releases.track_length_ms from the MB release tracklist's
   `length` field, and refreshes it on re-import without clobbering a
   previously-good value when MB returns NULL.

2. integrations.spotify.db's mismatch-finder queries prefer
   COALESCE(rr.track_length_ms, rec.duration_ms) so a recording whose
   canonical length differs from its track length on a specific release
   doesn't falsely trigger as a duration mismatch.

3. get_recordings_for_release surfaces the same COALESCE'd value as
   `recording_duration_ms`, so the matcher's track scoring treats the
   release-specific length as the "expected" duration when present.
"""

from __future__ import annotations

import pytest

from db_utils import get_db_connection
from integrations.musicbrainz.db import link_recording_to_release
from integrations.spotify.db import (
    get_recordings_for_release,
    get_releases_with_duration_mismatches,
    get_songs_with_duration_mismatches,
)


# ---------------------------------------------------------------------------
# Fixture UUIDs — fixture-only range, distinct from other test files.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-400000000{:03x}"

SONG_ID = _NS.format(0x001)
RECORDING_ID = _NS.format(0x010)
RELEASE_ID = _NS.format(0x040)
RR_ID = _NS.format(0x050)
LINK_ID = _NS.format(0x070)

# A made-up MB recording UUID — the importer matches by this against the
# `recording.id` field inside MB release media tracks.
MB_RECORDING_ID = "11111111-2222-3333-4444-555555555555"


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (LINK_ID,),
        )
        cur.execute("DELETE FROM recording_releases WHERE id = %s", (RR_ID,))
        cur.execute(
            "DELETE FROM recording_releases "
            "WHERE recording_id = %s AND release_id = %s",
            (RECORDING_ID, RELEASE_ID),
        )
        cur.execute("DELETE FROM recordings WHERE id = %s", (RECORDING_ID,))
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def base_song(db):
    """Bare song + recording + release (no recording_releases junction yet
    — the importer-under-test creates it)."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Track Length Test Song"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) "
            "VALUES (%s, %s, %s)",
            (RELEASE_ID, "Track Length Test Album", "Test Artist"),
        )
        # Canonical recording length: 9:26 (566s). Real MB-style case:
        # the release ships a shorter edit of the same recording.
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms, musicbrainz_id) "
            "VALUES (%s, %s, %s, %s, %s)",
            (RECORDING_ID, SONG_ID, "Track Length Test Recording",
             566_000, MB_RECORDING_ID),
        )
    db.commit()
    yield
    _cleanup(db)


# ---------------------------------------------------------------------------
# Importer — link_recording_to_release populates track_length_ms
# ---------------------------------------------------------------------------

def _mb_release_with_track_length(length_ms):
    """Build an MB-API-shaped release dict with one track whose `length`
    is the given value."""
    return {
        'media': [
            {
                'position': 1,
                'tracks': [
                    {
                        'position': 1,
                        'title': 'Track Length Test Track',
                        'length': length_ms,
                        'recording': {'id': MB_RECORDING_ID},
                    },
                ],
            },
        ],
    }


class TestLinkRecordingToRelease:
    def test_captures_track_length_from_mb(self, base_song):
        # Real-world case: recording is 9:26 canonically, but THIS release
        # ships a 5:50 edit. The importer must keep both — duration_ms
        # stays at 9:26 (canonical) and track_length_ms stores 5:50.
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID,
                _mb_release_with_track_length(350_000),  # 5:50
            )
            conn.commit()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT track_length_ms FROM recording_releases "
                    "WHERE recording_id = %s AND release_id = %s",
                    (RECORDING_ID, RELEASE_ID),
                )
                row = cur.fetchone()
        assert row is not None
        track_length_ms = row['track_length_ms'] if isinstance(row, dict) else row[0]
        assert track_length_ms == 350_000

    def test_missing_length_leaves_null(self, base_song):
        # MB returns no `length` for the track — track_length_ms stays NULL,
        # duration_ms (canonical) is the only thing to fall back on.
        mb_release = {
            'media': [{
                'position': 1,
                'tracks': [{
                    'position': 1,
                    'title': 'No length',
                    'recording': {'id': MB_RECORDING_ID},
                }],
            }],
        }
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID, mb_release,
            )
            conn.commit()
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT track_length_ms FROM recording_releases "
                    "WHERE recording_id = %s AND release_id = %s",
                    (RECORDING_ID, RELEASE_ID),
                )
                row = cur.fetchone()
        track_length_ms = row['track_length_ms'] if isinstance(row, dict) else row[0]
        assert track_length_ms is None

    def test_re_import_refreshes_track_length(self, base_song):
        # First import: 5:50.
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID,
                _mb_release_with_track_length(350_000),
            )
            conn.commit()
        # Re-import with a corrected length: 5:48. The ON CONFLICT ... DO
        # UPDATE branch must refresh, not skip.
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID,
                _mb_release_with_track_length(348_000),
            )
            conn.commit()
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT track_length_ms FROM recording_releases "
                    "WHERE recording_id = %s AND release_id = %s",
                    (RECORDING_ID, RELEASE_ID),
                )
                row = cur.fetchone()
        track_length_ms = row['track_length_ms'] if isinstance(row, dict) else row[0]
        assert track_length_ms == 348_000

    def test_re_import_with_null_does_not_clobber_existing_value(self, base_song):
        # First import: real value.
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID,
                _mb_release_with_track_length(350_000),
            )
            conn.commit()
        # Re-import with no length on the track — must NOT overwrite our
        # known-good value with NULL (partial MB response, network blip,
        # etc.).
        mb_partial = {
            'media': [{
                'position': 1,
                'tracks': [{
                    'position': 1,
                    'title': 'Partial',
                    'recording': {'id': MB_RECORDING_ID},
                }],
            }],
        }
        with get_db_connection() as conn:
            link_recording_to_release(
                conn, RECORDING_ID, RELEASE_ID, MB_RECORDING_ID, mb_partial,
            )
            conn.commit()
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT track_length_ms FROM recording_releases "
                    "WHERE recording_id = %s AND release_id = %s",
                    (RECORDING_ID, RELEASE_ID),
                )
                row = cur.fetchone()
        track_length_ms = row['track_length_ms'] if isinstance(row, dict) else row[0]
        assert track_length_ms == 350_000


# ---------------------------------------------------------------------------
# Mismatch queries — prefer track_length_ms over recording.duration_ms
# ---------------------------------------------------------------------------

@pytest.fixture
def mismatch_setup(db):
    """Recording with a 9:26 canonical length, linked to a release where
    MB says the track is actually 5:50, paired with a Spotify link at
    5:49. Without the COALESCE fix, this would compare 9:26 vs 5:49 and
    flag as a mismatch. With the fix, comparison is 5:50 vs 5:49 — fine."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Compare Test"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) "
            "VALUES (%s, %s, %s)",
            (RELEASE_ID, "Compare Test Album", "Compare Test Artist"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms) "
            "VALUES (%s, %s, %s, %s)",
            (RECORDING_ID, SONG_ID, "Compare Test Recording", 566_000),
        )
        # track_length_ms = 350_000 — the per-release edited cut.
        cur.execute(
            "INSERT INTO recording_releases "
            "(id, recording_id, release_id, track_number, disc_number, "
            " track_length_ms) "
            "VALUES (%s, %s, %s, 1, 1, %s)",
            (RR_ID, RECORDING_ID, RELEASE_ID, 350_000),
        )
        # Spotify duration 5:49 — only 1s off the track length.
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms, match_method) "
            "VALUES (%s, %s, 'spotify', %s, %s, %s, 'fuzzy_search')",
            (LINK_ID, RR_ID, 'sp-track-1',
             'http://example.test/sp/track1', 349_000),
        )
    db.commit()
    yield
    _cleanup(db)


class TestMismatchQueriesPreferTrackLength:
    def test_per_song_query_compares_against_track_length(self, mismatch_setup):
        # 60s threshold: |350_000 - 349_000| = 1_000ms = 1s. Below threshold.
        rels = get_releases_with_duration_mismatches(SONG_ID, threshold_ms=60_000)
        assert not any(str(r['id']) == RELEASE_ID for r in rels), (
            "Recording shipped as a release-specific edit must not be "
            "flagged as a mismatch when the Spotify track aligns with "
            "the per-release track length"
        )

    def test_distinct_songs_query_compares_against_track_length(self, mismatch_setup):
        songs = get_songs_with_duration_mismatches(threshold_ms=60_000)
        assert not any(str(s['id']) == SONG_ID for s in songs)

    def test_canonical_duration_used_when_track_length_is_null(self, mismatch_setup, db):
        # Clear track_length_ms — query should fall back to recording.duration_ms
        # (9:26 canonical), which is now wildly off from Spotify's 5:49 →
        # mismatch surfaces.
        with db.cursor() as cur:
            cur.execute(
                "UPDATE recording_releases SET track_length_ms = NULL "
                "WHERE id = %s",
                (RR_ID,),
            )
        db.commit()
        rels = get_releases_with_duration_mismatches(SONG_ID, threshold_ms=60_000)
        assert any(str(r['id']) == RELEASE_ID for r in rels), (
            "When track_length_ms is NULL the query must fall back to "
            "the canonical recording duration"
        )


# ---------------------------------------------------------------------------
# get_recordings_for_release — matcher's "expected duration" path
# ---------------------------------------------------------------------------

class TestGetRecordingsForReleaseUsesTrackLength:
    def test_returns_track_length_when_present(self, mismatch_setup):
        rows = get_recordings_for_release(SONG_ID, RELEASE_ID)
        assert len(rows) == 1
        # `recording_duration_ms` is what the matcher passes as
        # expected_duration_ms when scoring Spotify candidates. It must
        # be the per-release track length (350_000), not the canonical
        # 566_000 — otherwise track scoring penalises a Spotify candidate
        # that's correctly aligned with the edited cut.
        assert rows[0]['recording_duration_ms'] == 350_000

    def test_returns_canonical_duration_when_track_length_null(
        self, mismatch_setup, db,
    ):
        with db.cursor() as cur:
            cur.execute(
                "UPDATE recording_releases SET track_length_ms = NULL "
                "WHERE id = %s",
                (RR_ID,),
            )
        db.commit()
        rows = get_recordings_for_release(SONG_ID, RELEASE_ID)
        assert rows[0]['recording_duration_ms'] == 566_000
