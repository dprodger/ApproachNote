"""
Tests for the manual match-verification mechanism (issue: per-link
"this Spotify track is the right one, leave it alone" override).

Three things under test:

1. set_track_link_manual_override flips match_method correctly and is
   safely a no-op for non-existent IDs.

2. The admin POST /admin/duration-mismatches/links/<id>/verify endpoint
   - succeeds for an admin-authed request,
   - 404s for an unknown link,
   - 401/403s for unauthenticated/non-admin (via the existing gate).

3. The duration-mismatch read queries (both the matcher's
   get_*_with_duration_mismatches helpers in integrations/spotify/db.py
   and the admin /duration-mismatches/<song> page) exclude rows whose
   match_method is 'manual', and include them when include_verified=1.
"""

from __future__ import annotations

import pytest

from db_utils import get_db_connection
from integrations.spotify.db import (
    block_streaming_track,
    get_blocked_tracks_for_song,
    get_releases_with_duration_mismatches,
    get_songs_with_duration_mismatches,
    set_track_link_manual_override,
)


# ---------------------------------------------------------------------------
# Fixture UUIDs — pre-generated, fixture-only range.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-300000000{:03x}"

SONG_ID = _NS.format(0x001)
RECORDING_ID = _NS.format(0x010)
RELEASE_ID = _NS.format(0x040)
RR_ID = _NS.format(0x050)
LINK_ID = _NS.format(0x070)


def _cleanup(conn):
    with conn.cursor() as cur:
        # bad_streaming_matches has FK on song_id with ON DELETE CASCADE,
        # so deleting the song would also clear blocklist rows. We delete
        # explicitly for clarity and to match the order other tests use.
        cur.execute(
            "DELETE FROM bad_streaming_matches WHERE song_id = %s",
            (SONG_ID,),
        )
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (LINK_ID,),
        )
        cur.execute("DELETE FROM recording_releases WHERE id = %s", (RR_ID,))
        cur.execute("DELETE FROM recordings WHERE id = %s", (RECORDING_ID,))
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def mismatched_link(db):
    """A streaming link whose duration disagrees with the recording's
    canonical duration by more than 60s — i.e. exactly the kind of row
    the duration-mismatch admin page surfaces. The matcher recorded it
    via fuzzy_search; the admin can promote it to 'manual' to lock it."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Verify Test Song"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) VALUES (%s, %s, %s)",
            (RELEASE_ID, "Verify Test Album", "Verify Test Artist"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title, duration_ms) "
            "VALUES (%s, %s, %s, %s)",
            (RECORDING_ID, SONG_ID, "Verify Test Recording", 240_000),
        )
        cur.execute(
            "INSERT INTO recording_releases (id, recording_id, release_id) "
            "VALUES (%s, %s, %s)",
            (RR_ID, RECORDING_ID, RELEASE_ID),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms, match_method) "
            "VALUES (%s, %s, 'spotify', %s, %s, %s, 'fuzzy_search')",
            (LINK_ID, RR_ID, 'sp-track-1',
             'http://example.test/sp/track1', 600_000),  # 6:00, off by 6 min
        )
    db.commit()
    yield
    _cleanup(db)


def _link_method(conn, link_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT match_method FROM recording_release_streaming_links "
            "WHERE id = %s",
            (link_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row['match_method']


# ---------------------------------------------------------------------------
# set_track_link_manual_override — db helper
# ---------------------------------------------------------------------------

class TestSetTrackLinkManualOverride:
    def test_marks_existing_link_as_manual(self, mismatched_link, db):
        with get_db_connection() as conn:
            ok = set_track_link_manual_override(conn, LINK_ID, manual=True)
            conn.commit()
        assert ok is True
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) == 'manual'

    def test_unmark_restores_fuzzy_search(self, mismatched_link, db):
        with get_db_connection() as conn:
            set_track_link_manual_override(conn, LINK_ID, manual=True)
            conn.commit()
        with get_db_connection() as conn:
            ok = set_track_link_manual_override(conn, LINK_ID, manual=False)
            conn.commit()
        assert ok is True
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) == 'fuzzy_search'

    def test_unknown_link_returns_false(self):
        bogus_id = "00000000-0000-4000-8000-3ffffffffff0"
        with get_db_connection() as conn:
            ok = set_track_link_manual_override(conn, bogus_id, manual=True)
            conn.commit()
        assert ok is False


# ---------------------------------------------------------------------------
# Mismatch query exclusion of manual rows
# ---------------------------------------------------------------------------

class TestMismatchQueriesExcludeManual:
    def test_get_songs_with_duration_mismatches_excludes_manual(
        self, mismatched_link, db,
    ):
        # Before manual-override: our song shows up.
        songs = get_songs_with_duration_mismatches(threshold_ms=60_000)
        assert any(str(s['id']) == SONG_ID for s in songs)

        # Mark as manual → query should drop it.
        with get_db_connection() as conn:
            set_track_link_manual_override(conn, LINK_ID, manual=True)
            conn.commit()
        songs = get_songs_with_duration_mismatches(threshold_ms=60_000)
        assert not any(str(s['id']) == SONG_ID for s in songs)

    def test_get_releases_with_duration_mismatches_excludes_manual(
        self, mismatched_link, db,
    ):
        rels = get_releases_with_duration_mismatches(SONG_ID, threshold_ms=60_000)
        assert any(str(r['id']) == RELEASE_ID for r in rels)

        with get_db_connection() as conn:
            set_track_link_manual_override(conn, LINK_ID, manual=True)
            conn.commit()
        rels = get_releases_with_duration_mismatches(SONG_ID, threshold_ms=60_000)
        assert not any(str(r['id']) == RELEASE_ID for r in rels)


# ---------------------------------------------------------------------------
# Admin POST endpoint
# ---------------------------------------------------------------------------

def _grant_admin(db, user_id: str, is_admin: bool = True):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET is_admin = %s WHERE id = %s",
                    (is_admin, user_id))
    db.commit()


@pytest.fixture
def admin_user(register_user, db):
    body = register_user(
        email="verifyadmin@example.com",
        password="correct-horse-battery-staple",
        display_name="Admin",
    )
    _grant_admin(db, body["user"]["id"], True)
    return body


def _login(client, email="verifyadmin@example.com",
           password="correct-horse-battery-staple"):
    return client.post(
        "/admin/login", data={"email": email, "password": password},
    )


class TestVerifyEndpoint:
    def test_unauth_request_is_blocked(self, client, mismatched_link):
        resp = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/verify",
            json={"manual": True},
            headers={"Accept": "application/json"},
        )
        # Admin gate either 401s (no session) or 403s (CSRF). Either way,
        # the call must not succeed without admin auth.
        assert resp.status_code in (401, 403)
        # And the row is unchanged.
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) == 'fuzzy_search'

    def test_admin_verify_flips_to_manual(
        self, client, admin_user, mismatched_link,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        resp = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/verify",
            json={"manual": True},
            headers={
                "Accept": "application/json",
                "X-CSRF-Token": csrf,
            },
        )
        assert resp.status_code == 200, resp.get_json()
        body = resp.get_json()
        assert body['success'] is True
        assert body['match_method'] == 'manual'
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) == 'manual'

    def test_admin_unverify_restores_fuzzy(
        self, client, admin_user, mismatched_link,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        # Verify, then unverify.
        client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/verify",
            json={"manual": True},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        resp = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/verify",
            json={"manual": False},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert resp.status_code == 200
        assert resp.get_json()['match_method'] == 'fuzzy_search'

    def test_unknown_link_returns_404(
        self, client, admin_user,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        bogus = "00000000-0000-4000-8000-3ffffffffff0"
        resp = client.post(
            f"/admin/duration-mismatches/links/{bogus}/verify",
            json={"manual": True},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# block_streaming_track — db helper for the Reject action
# ---------------------------------------------------------------------------

class TestBlockStreamingTrack:
    def test_inserts_blocklist_row(self, mismatched_link, db):
        with get_db_connection() as conn:
            inserted = block_streaming_track(
                conn, SONG_ID, 'sp-track-1',
                service='spotify', reason='wrong artist',
            )
            conn.commit()
        assert inserted is True
        # Matcher's lookup helper now returns this track for this song.
        blocked = get_blocked_tracks_for_song(SONG_ID, service='spotify')
        assert 'sp-track-1' in blocked

    def test_duplicate_block_is_idempotent(self, mismatched_link):
        with get_db_connection() as conn:
            block_streaming_track(conn, SONG_ID, 'sp-track-1')
            conn.commit()
        with get_db_connection() as conn:
            inserted = block_streaming_track(conn, SONG_ID, 'sp-track-1')
            conn.commit()
        # Second call doesn't insert (unique constraint), but doesn't raise.
        assert inserted is False
        # Still only one row present.
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM bad_streaming_matches "
                    "WHERE song_id = %s AND service_id = %s",
                    (SONG_ID, 'sp-track-1'),
                )
                row = cur.fetchone()
                count = row['n'] if isinstance(row, dict) else row[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Reject endpoint
# ---------------------------------------------------------------------------

class TestRejectEndpoint:
    def test_unauth_request_is_blocked(self, client, mismatched_link):
        resp = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/reject",
            json={},
            headers={"Accept": "application/json"},
        )
        assert resp.status_code in (401, 403)
        # Link is unchanged, no blocklist row created.
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) == 'fuzzy_search'
        blocked = get_blocked_tracks_for_song(SONG_ID, service='spotify')
        assert 'sp-track-1' not in blocked

    def test_admin_reject_blocks_and_deletes(
        self, client, admin_user, mismatched_link,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        resp = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/reject",
            json={"reason": "wrong album"},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert resp.status_code == 200, resp.get_json()
        body = resp.get_json()
        assert body['success'] is True
        assert body['deleted'] is True
        assert body['blocked'] is True
        assert body['song_id'] == SONG_ID
        assert body['spotify_track_id'] == 'sp-track-1'

        # The streaming link is gone.
        with get_db_connection() as conn:
            assert _link_method(conn, LINK_ID) is None

        # And the (song, track) is now in the matcher's blocklist.
        blocked = get_blocked_tracks_for_song(SONG_ID, service='spotify')
        assert 'sp-track-1' in blocked

    def test_reason_text_is_persisted(
        self, client, admin_user, mismatched_link, db,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/reject",
            json={"reason": "different artist on the Spotify side"},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        with db.cursor() as cur:
            cur.execute(
                "SELECT reason FROM bad_streaming_matches "
                "WHERE song_id = %s AND service_id = %s",
                (SONG_ID, 'sp-track-1'),
            )
            row = cur.fetchone()
        reason = row[0] if not isinstance(row, dict) else row['reason']
        assert reason == "different artist on the Spotify side"

    def test_unknown_link_returns_404(
        self, client, admin_user,
    ):
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        bogus = "00000000-0000-4000-8000-3ffffffffff0"
        resp = client.post(
            f"/admin/duration-mismatches/links/{bogus}/reject",
            json={},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert resp.status_code == 404

    def test_reject_is_idempotent_against_repeat_calls(
        self, client, admin_user, mismatched_link, db,
    ):
        # Calling reject twice on the same link: first call deletes +
        # blocks, second call 404s because the link is gone (the block
        # itself stays in place, the unique constraint prevents
        # double-blocking).
        _login(client)
        csrf = client.get_cookie('admin_csrf', path='/admin').value
        first = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/reject",
            json={},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert first.status_code == 200

        second = client.post(
            f"/admin/duration-mismatches/links/{LINK_ID}/reject",
            json={},
            headers={"Accept": "application/json", "X-CSRF-Token": csrf},
        )
        assert second.status_code == 404

        # Still exactly one blocklist row.
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bad_streaming_matches "
                "WHERE song_id = %s AND service_id = %s",
                (SONG_ID, 'sp-track-1'),
            )
            row = cur.fetchone()
            count = row[0] if not isinstance(row, dict) else row['count']
        assert count == 1
