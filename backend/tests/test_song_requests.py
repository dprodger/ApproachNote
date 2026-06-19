"""
Integration tests for the song-request gating flow.

Covers:
- POST /v1/musicbrainz/request (authenticated users submit a pending request)
- duplicate / already-in-catalog conflict handling
- POST /v1/musicbrainz/import is now admin-only
- admin approve/reject under /admin/song-requests/*

The autouse fixtures in conftest.py TRUNCATE users (CASCADE, which clears
song_requests too). Songs created by approve/import are cleaned up here since
the `songs` table isn't auto-truncated.
"""

import pytest

# Distinct MB work ids per concern; reused across tests since cleanup runs
# after each test.
MBID = "11111111-1111-1111-1111-111111111111"
MBID_2 = "22222222-2222-2222-2222-222222222222"


def _grant_admin(db, user_id):
    with db.cursor() as cur:
        cur.execute("UPDATE users SET is_admin = true WHERE id = %s", (user_id,))
    db.commit()


@pytest.fixture
def admin_headers(register_user, db):
    """Bearer headers for an admin user (ops/admin path skips CSRF)."""
    body = register_user(email="admin-songreq@example.com")
    _grant_admin(db, body["user"]["id"])

    class _Headers(dict):
        pass

    headers = _Headers({"Authorization": f"Bearer {body['access_token']}"})
    headers.user = body["user"]
    return headers


@pytest.fixture(autouse=True)
def _clean_songs(db):
    """Remove any songs created by approve/import tests (songs isn't truncated)."""
    yield
    with db.cursor() as cur:
        cur.execute(
            "DELETE FROM songs WHERE musicbrainz_id = ANY(%s)",
            ([MBID, MBID_2],),
        )
    db.commit()


# ---------------------------------------------------------------------------
# Submitting requests
# ---------------------------------------------------------------------------

def test_submit_request_creates_pending(client, auth_headers, db):
    resp = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Test Tune", "composer": "A. Composer"},
    )
    assert resp.status_code == 201, resp.get_json()
    body = resp.get_json()
    assert body["success"] is True
    assert body["request"]["status"] == "pending"

    with db.cursor() as cur:
        cur.execute(
            "SELECT status, title, composer, requested_by "
            "FROM song_requests WHERE musicbrainz_id = %s",
            (MBID,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "pending"
    assert row[1] == "Test Tune"
    assert row[2] == "A. Composer"
    assert str(row[3]) == auth_headers.user["id"]


def test_submit_request_requires_auth(client):
    resp = client.post(
        "/v1/musicbrainz/request",
        json={"musicbrainz_id": MBID, "title": "Test Tune"},
    )
    assert resp.status_code == 401


def test_submit_request_validates_fields(client, auth_headers):
    resp = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": "", "title": ""},
    )
    assert resp.status_code == 400


def test_duplicate_pending_request_conflicts(client, auth_headers):
    payload = {"musicbrainz_id": MBID, "title": "Test Tune"}
    first = client.post("/v1/musicbrainz/request", headers=auth_headers, json=payload)
    assert first.status_code == 201
    second = client.post("/v1/musicbrainz/request", headers=auth_headers, json=payload)
    assert second.status_code == 409
    assert "request_id" in second.get_json()


def test_request_for_existing_song_conflicts(client, auth_headers, db):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (title, musicbrainz_id) VALUES (%s, %s)",
            ("Already Here", MBID),
        )
    db.commit()

    resp = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Already Here"},
    )
    assert resp.status_code == 409
    assert "catalog" in resp.get_json()["error"].lower()


# ---------------------------------------------------------------------------
# Import is now admin-only
# ---------------------------------------------------------------------------

def test_import_rejects_non_admin(client, auth_headers):
    resp = client.post(
        "/v1/musicbrainz/import",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Test Tune"},
    )
    assert resp.status_code == 403


def test_import_allows_admin(client, admin_headers, db):
    resp = client.post(
        "/v1/musicbrainz/import",
        headers=admin_headers,
        json={"musicbrainz_id": MBID, "title": "Test Tune"},
    )
    assert resp.status_code == 201, resp.get_json()
    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM songs WHERE musicbrainz_id = %s", (MBID,))
        assert cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Admin review
# ---------------------------------------------------------------------------

def test_admin_can_approve_request(client, auth_headers, admin_headers, db):
    submit = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Approve Me"},
    )
    request_id = submit.get_json()["request"]["id"]

    resp = client.post(
        f"/admin/song-requests/{request_id}/approve",
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["status"] == "approved"
    assert body["song_id"]

    with db.cursor() as cur:
        cur.execute(
            "SELECT status, created_song_id, reviewed_by FROM song_requests WHERE id = %s",
            (request_id,),
        )
        status, created_song_id, reviewed_by = cur.fetchone()
        assert status == "approved"
        assert created_song_id is not None
        assert reviewed_by is not None
        cur.execute("SELECT 1 FROM songs WHERE id = %s", (created_song_id,))
        assert cur.fetchone() is not None


def test_admin_can_reject_request(client, auth_headers, admin_headers, db):
    submit = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Reject Me"},
    )
    request_id = submit.get_json()["request"]["id"]

    resp = client.post(
        f"/admin/song-requests/{request_id}/reject",
        headers=admin_headers,
        json={"review_note": "Not a jazz standard"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "rejected"

    with db.cursor() as cur:
        cur.execute(
            "SELECT status, review_note FROM song_requests WHERE id = %s",
            (request_id,),
        )
        status, note = cur.fetchone()
    assert status == "rejected"
    assert note == "Not a jazz standard"

    # No song should have been created.
    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM songs WHERE musicbrainz_id = %s", (MBID,))
        assert cur.fetchone() is None


def test_approving_already_reviewed_request_conflicts(
    client, auth_headers, admin_headers, db
):
    submit = client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Double Approve"},
    )
    request_id = submit.get_json()["request"]["id"]

    first = client.post(
        f"/admin/song-requests/{request_id}/approve", headers=admin_headers
    )
    assert first.status_code == 200
    second = client.post(
        f"/admin/song-requests/{request_id}/approve", headers=admin_headers
    )
    assert second.status_code == 409


def test_admin_list_page_renders(client, auth_headers, admin_headers):
    # Empty state.
    empty = client.get("/admin/song-requests", headers=admin_headers)
    assert empty.status_code == 200
    assert b"Song Requests" in empty.data

    # With a pending request present.
    client.post(
        "/v1/musicbrainz/request",
        headers=auth_headers,
        json={"musicbrainz_id": MBID, "title": "Listed Tune", "composer": "C"},
    )
    populated = client.get("/admin/song-requests", headers=admin_headers)
    assert populated.status_code == 200
    assert b"Listed Tune" in populated.data
    assert b"pending" in populated.data


def test_admin_endpoints_require_admin(client, auth_headers):
    # A valid but non-admin bearer token must not reach the admin route.
    resp = client.post(
        "/admin/song-requests/00000000-0000-0000-0000-000000000000/approve",
        headers={**auth_headers, "Accept": "application/json"},
        json={},
    )
    assert resp.status_code in (401, 403)
