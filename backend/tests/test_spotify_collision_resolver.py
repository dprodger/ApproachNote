"""
Tests for SpotifyMatcher._resolve_release_track_collisions — the
post-match cleanup pass that resolves cases where multiple
recording_releases on the same release got linked to the same Spotify
track ID.

Real production case (Bossa Nova / "One Note Samba"): MB had two
recordings of the song on a 2-disc release, Spotify reissued just
disc 2 as a single disc, and the per-recording matcher linked both
MB recordings to the one Spotify track. The cleanup pass detects the
collision and keeps only the link with the strongest duration
confidence — manual-override links always win regardless.
"""

from __future__ import annotations

import logging

import pytest

from db_utils import get_db_connection
from integrations.spotify.matcher import SpotifyMatcher


_NS = "00000000-0000-4000-8000-50000000{:04x}"

SONG_ID         = _NS.format(0x0001)
RELEASE_ID      = _NS.format(0x0040)
RECORDING_GOOD  = _NS.format(0x0010)   # 4:32 — matches Spotify's 4:32 perfectly
RECORDING_BAD   = _NS.format(0x0011)   # 3:22 — mismatched
RR_GOOD         = _NS.format(0x0050)
RR_BAD          = _NS.format(0x0051)
LINK_GOOD       = _NS.format(0x0070)
LINK_BAD        = _NS.format(0x0071)


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links "
            "WHERE id IN (%s, %s)",
            (LINK_GOOD, LINK_BAD),
        )
        cur.execute(
            "DELETE FROM recording_releases WHERE id IN (%s, %s)",
            (RR_GOOD, RR_BAD),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s)",
            (RECORDING_GOOD, RECORDING_BAD),
        )
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def colliding_links(db):
    """Two recording_releases on one release, both pointing at the same
    Spotify track. RR_GOOD's duration matches the Spotify track perfectly;
    RR_BAD's duration is wildly off — exactly the production collision
    pattern."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Collision Test Song"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) "
            "VALUES (%s, %s, %s)",
            (RELEASE_ID, "Collision Test Album", "Collision Test Artist"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms) "
            "VALUES (%s, %s, %s, %s)",
            (RECORDING_GOOD, SONG_ID, "Collision Test Recording (good)", 272_000),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms) "
            "VALUES (%s, %s, %s, %s)",
            (RECORDING_BAD, SONG_ID, "Collision Test Recording (bad)", 202_000),
        )
        cur.execute(
            "INSERT INTO recording_releases "
            "(id, recording_id, release_id, disc_number, track_number) "
            "VALUES (%s, %s, %s, 2, 5)",
            (RR_GOOD, RECORDING_GOOD, RELEASE_ID),
        )
        cur.execute(
            "INSERT INTO recording_releases "
            "(id, recording_id, release_id, disc_number, track_number) "
            "VALUES (%s, %s, %s, 1, 5)",
            (RR_BAD, RECORDING_BAD, RELEASE_ID),
        )
        # Both junctions link to the SAME Spotify track ID. The unique
        # constraint on rrsl is (recording_release_id, service) so this
        # is allowed — only one link per junction, but two junctions
        # CAN both point at the same service_id.
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms, match_method) "
            "VALUES (%s, %s, 'spotify', 'sp-track-shared', %s, %s, 'fuzzy_search')",
            (LINK_GOOD, RR_GOOD, 'http://example.test/sp/shared', 272_000),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms, match_method) "
            "VALUES (%s, %s, 'spotify', 'sp-track-shared', %s, %s, 'fuzzy_search')",
            (LINK_BAD, RR_BAD, 'http://example.test/sp/shared', 272_000),
        )
    db.commit()
    yield
    _cleanup(db)


def _link_exists(conn, link_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM recording_release_streaming_links WHERE id = %s",
            (link_id,),
        )
        return cur.fetchone() is not None


def _make_matcher() -> SpotifyMatcher:
    matcher = SpotifyMatcher(logger=logging.getLogger("test.collision_resolver"))
    return matcher


# ---------------------------------------------------------------------------
# Happy path: clear the lower-confidence link
# ---------------------------------------------------------------------------

def test_collision_keeps_high_confidence_link(colliding_links):
    matcher = _make_matcher()
    with get_db_connection() as conn:
        cleared = matcher._resolve_release_track_collisions(conn, RELEASE_ID)
        conn.commit()

    assert cleared == 1
    assert matcher.stats.get('tracks_collisions_cleared') == 1
    with get_db_connection() as conn:
        # Perfect-duration-match link survives.
        assert _link_exists(conn, LINK_GOOD)
        # Wildly-off-duration link gets cleared.
        assert not _link_exists(conn, LINK_BAD)


def test_no_op_when_no_collision(db):
    """Resolver must not touch anything when each Spotify track is
    linked to at most one junction on the release."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "No Collision Song"),
        )
        cur.execute(
            "INSERT INTO releases (id, title) VALUES (%s, %s)",
            (RELEASE_ID, "No Collision Album"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms) "
            "VALUES (%s, %s, %s, 240000)",
            (RECORDING_GOOD, SONG_ID, "Rec"),
        )
        cur.execute(
            "INSERT INTO recording_releases "
            "(id, recording_id, release_id, disc_number, track_number) "
            "VALUES (%s, %s, %s, 1, 1)",
            (RR_GOOD, RECORDING_GOOD, RELEASE_ID),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms) VALUES "
            "(%s, %s, 'spotify', 'sp-only', 'http://x', 240000)",
            (LINK_GOOD, RR_GOOD),
        )
    db.commit()
    try:
        matcher = _make_matcher()
        with get_db_connection() as conn:
            cleared = matcher._resolve_release_track_collisions(conn, RELEASE_ID)
            conn.commit()
        assert cleared == 0
        with get_db_connection() as conn:
            assert _link_exists(conn, LINK_GOOD)
    finally:
        _cleanup(db)


# ---------------------------------------------------------------------------
# Manual override always wins
# ---------------------------------------------------------------------------

def test_manual_override_wins_even_with_worse_duration(colliding_links, db):
    # Flip the BAD-duration link to manual. Despite its duration confidence
    # being lower, it should survive (admin assertion). The good-duration
    # auto-match should be the one cleared.
    with db.cursor() as cur:
        cur.execute(
            "UPDATE recording_release_streaming_links "
            "SET match_method = 'manual' WHERE id = %s",
            (LINK_BAD,),
        )
    db.commit()

    matcher = _make_matcher()
    with get_db_connection() as conn:
        cleared = matcher._resolve_release_track_collisions(conn, RELEASE_ID)
        conn.commit()

    assert cleared == 1
    with get_db_connection() as conn:
        assert _link_exists(conn, LINK_BAD), \
            "manual-override link must survive collision resolution"
        assert not _link_exists(conn, LINK_GOOD)


def test_two_manual_overrides_are_left_alone(colliding_links, db):
    # Edge case: admin asserted both. Don't auto-resolve — log a warning
    # and leave them alone.
    with db.cursor() as cur:
        cur.execute(
            "UPDATE recording_release_streaming_links "
            "SET match_method = 'manual' WHERE id IN (%s, %s)",
            (LINK_GOOD, LINK_BAD),
        )
    db.commit()

    matcher = _make_matcher()
    with get_db_connection() as conn:
        cleared = matcher._resolve_release_track_collisions(conn, RELEASE_ID)
        conn.commit()

    assert cleared == 0
    with get_db_connection() as conn:
        assert _link_exists(conn, LINK_GOOD)
        assert _link_exists(conn, LINK_BAD)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_does_not_delete(colliding_links):
    matcher = _make_matcher()
    matcher.dry_run = True
    with get_db_connection() as conn:
        cleared = matcher._resolve_release_track_collisions(conn, RELEASE_ID)
        conn.commit()
    # cleared count is still 0 in dry-run because no DELETE was issued.
    assert cleared == 0
    with get_db_connection() as conn:
        assert _link_exists(conn, LINK_GOOD)
        assert _link_exists(conn, LINK_BAD)
