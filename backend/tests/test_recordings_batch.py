"""
Contract tests for GET /api/recordings/batch.

Why these tests exist
---------------------
The batch endpoint is the hydration primitive for the shell+hydrate
pattern: the iOS/Mac app loads shell rows via
``/songs/<id>/recordings/shell``, then calls this endpoint with the IDs
of rows that have scrolled into view to fill in cover art, full
performers, streaming links, and community data.

By contract, the per-row shape here matches the existing list endpoint
(``/songs/<id>/recordings``) so the Swift ``Recording`` decoder works on
both responses. This module pins that contract and the request-validation
behaviour (400s for bad input, cap on the number of IDs).

Key invariants:

* ``EXPECTED_BATCH_FIELDS`` equals ``EXPECTED_LIST_FIELDS`` from
  ``test_song_recordings.py``. We re-declare it here rather than import
  so each contract test file is self-documenting.
* Response envelope is ``{"recordings": [...]}``. No ``song_id`` — the
  batch is song-agnostic by design.
* Missing IDs are silently omitted (not a 404). A stale client with a
  deleted recording's ID should still be able to hydrate the rest.
* ID validation happens BEFORE the DB round-trip: missing query string,
  empty ids list, malformed UUID, oversize all return 400.
* IDs travel in the query string as a comma-separated list
  (``?ids=uuid1,uuid2,…``). Tests construct that string via helper
  rather than spelling it out in every call.

Fixture strategy mirrors the other tests: deterministic UUIDs with a
distinct prefix range, self-cleaning before and after each test.
"""

import pytest


# ---------------------------------------------------------------------------
# The contract — each row returned by GET /api/recordings/batch has exactly
# this top-level key set. Keep this in sync with EXPECTED_LIST_FIELDS in
# test_song_recordings.py: the list endpoint and the batch endpoint return
# the same per-row shape so a single Swift ``Recording`` decoder handles
# both.
# ---------------------------------------------------------------------------

EXPECTED_BATCH_FIELDS = frozenset({
    "id",
    "title",
    "album_title",
    "artist_credit",
    "recording_year",
    "is_canonical",
    "best_cover_art_small",
    "best_cover_art_medium",
    "best_cover_art_large",
    "best_cover_art_source",
    "best_cover_art_source_url",
    "back_cover_art_small",
    "back_cover_art_medium",
    "back_cover_art_large",
    "has_back_cover",
    "back_cover_source",
    "back_cover_source_url",
    "best_spotify_url",
    "has_streaming",
    "has_spotify",
    "has_apple_music",
    "has_youtube",
    "streaming_services",
    "performers",
    "authority_count",
    "authority_sources",
    "community_data",
})


# ---------------------------------------------------------------------------
# Fixture UUIDs — distinct prefix range from the other test suites.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-0000000b{:04x}"

SONG_ID = _NS.format(0x0001)
RECORDING_POPULATED_ID = _NS.format(0x0010)
RECORDING_BARE_ID = _NS.format(0x0011)
PERFORMER_ID = _NS.format(0x0020)
INSTRUMENT_ID = _NS.format(0x0030)
RELEASE_ID = _NS.format(0x0040)
RP_ID = _NS.format(0x0050)
RR_ID = _NS.format(0x0060)
RELEASE_IMAGERY_FRONT_ID = _NS.format(0x0070)
STREAMING_LINK_ID = _NS.format(0x0080)


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (STREAMING_LINK_ID,),
        )
        cur.execute(
            "DELETE FROM release_imagery WHERE id = %s",
            (RELEASE_IMAGERY_FRONT_ID,),
        )
        cur.execute("DELETE FROM recording_releases WHERE id = %s", (RR_ID,))
        cur.execute("DELETE FROM recording_performers WHERE id = %s", (RP_ID,))
        cur.execute(
            "DELETE FROM song_authority_recommendations WHERE song_id = %s",
            (SONG_ID,),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s)",
            (RECORDING_POPULATED_ID, RECORDING_BARE_ID),
        )
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute("DELETE FROM performers WHERE id = %s", (PERFORMER_ID,))
        cur.execute("DELETE FROM instruments WHERE id = %s", (INSTRUMENT_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def batch_fixture(db):
    """Insert a populated recording (with release, imagery, streaming link,
    and performer) and a bare recording (song link only). The batch
    endpoint must handle both without dropping the bare one.
    """
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Batch Contract Test Song"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (PERFORMER_ID, "Bud Testman", "Testman, Bud"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INSTRUMENT_ID, "piano"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) VALUES (%s, %s, %s)",
            (RELEASE_ID, "Batch Test Album", "Bud Testman Trio"),
        )
        cur.execute(
            """
            INSERT INTO recordings
                (id, song_id, title, recording_year, default_release_id, is_canonical)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                RECORDING_POPULATED_ID,
                SONG_ID,
                "Batch Test (populated)",
                1963,
                RELEASE_ID,
                True,
            ),
        )
        cur.execute(
            """
            INSERT INTO recording_performers
                (id, recording_id, performer_id, instrument_id, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (RP_ID, RECORDING_POPULATED_ID, PERFORMER_ID, INSTRUMENT_ID, "leader"),
        )
        cur.execute(
            """
            INSERT INTO recording_releases
                (id, recording_id, release_id, track_number)
            VALUES (%s, %s, %s, %s)
            """,
            (RR_ID, RECORDING_POPULATED_ID, RELEASE_ID, 1),
        )
        cur.execute(
            """
            INSERT INTO release_imagery
                (id, release_id, source, source_url, type,
                 image_url_small, image_url_medium, image_url_large)
            VALUES (%s, %s, 'MusicBrainz', 'http://example.test/ri', 'Front',
                    'http://example.test/s.jpg',
                    'http://example.test/m.jpg',
                    'http://example.test/l.jpg')
            """,
            (RELEASE_IMAGERY_FRONT_ID, RELEASE_ID),
        )
        cur.execute(
            """
            INSERT INTO recording_release_streaming_links
                (id, recording_release_id, service, service_url)
            VALUES (%s, %s, 'spotify', 'http://example.test/spotify/batch')
            """,
            (STREAMING_LINK_ID, RR_ID),
        )

        cur.execute(
            """
            INSERT INTO recordings (id, song_id, title, recording_year)
            VALUES (%s, %s, %s, %s)
            """,
            (RECORDING_BARE_ID, SONG_ID, "Batch Test (bare)", 1971),
        )
    db.commit()

    yield {
        "populated_recording_id": RECORDING_POPULATED_ID,
        "bare_recording_id": RECORDING_BARE_ID,
    }

    _cleanup(db)


# ---------------------------------------------------------------------------
# Happy-path contract tests
# ---------------------------------------------------------------------------

def _batch_url(ids):
    """Helper: build ``/api/recordings/batch?ids=<comma-joined>``.

    Tests spell the URL this way everywhere so the format is obvious in
    diffs, and so swapping to a different encoding (e.g. repeated
    ``?ids=a&ids=b``) would be a one-line change here rather than a
    sweep across every test.
    """
    return f"/v1/recordings/batch?ids={','.join(ids)}"


def test_response_envelope_is_recordings_array(client, batch_fixture):
    """Response top-level key is ``recordings`` and nothing else. No
    ``song_id`` (the batch is song-agnostic). No ``recording_count``
    either — the client already knows how many IDs it sent.
    """
    resp = client.get(_batch_url([batch_fixture["populated_recording_id"]]))
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()

    assert set(body.keys()) == {"recordings"}
    assert isinstance(body["recordings"], list)


def test_each_row_has_exact_expected_fields(client, batch_fixture):
    """Contract: every row has *exactly* EXPECTED_BATCH_FIELDS.

    This is the same set as EXPECTED_LIST_FIELDS in test_song_recordings.py,
    intentionally — the Swift ``Recording`` decoder has to handle both
    endpoints with one type.
    """
    resp = client.get(_batch_url([
        batch_fixture["populated_recording_id"],
        batch_fixture["bare_recording_id"],
    ]))
    assert resp.status_code == 200
    body = resp.get_json()
    assert len(body["recordings"]) == 2

    for i, rec in enumerate(body["recordings"]):
        actual = set(rec.keys())
        missing = EXPECTED_BATCH_FIELDS - actual
        unexpected = actual - EXPECTED_BATCH_FIELDS
        assert not missing and not unexpected, (
            f"row[{i}] drifted from the contract.\n"
            f"  missing:    {sorted(missing)}\n"
            f"  unexpected: {sorted(unexpected)}\n"
            "If intentional, update EXPECTED_BATCH_FIELDS here AND "
            "EXPECTED_LIST_FIELDS in test_song_recordings.py — they must "
            "stay identical so the Swift decoder can handle both endpoints."
        )


def test_populated_row_surfaces_full_data(client, batch_fixture):
    """The fully-linked recording returns the cover art URLs, performer
    with instrument, streaming flag, and everything else a list row
    needs to render — the whole point of the batch endpoint.
    """
    resp = client.get(_batch_url([batch_fixture["populated_recording_id"]]))
    body = resp.get_json()
    rec = body["recordings"][0]

    assert rec["album_title"] == "Batch Test Album"
    assert rec["artist_credit"] == "Bud Testman Trio"
    assert rec["recording_year"] == 1963
    assert rec["is_canonical"] is True
    assert rec["best_cover_art_medium"] == "http://example.test/m.jpg"
    assert rec["has_spotify"] is True
    assert rec["has_streaming"] is True
    assert rec["best_spotify_url"] == "http://example.test/spotify/batch"
    assert "spotify" in rec["streaming_services"]

    performers = rec["performers"]
    assert len(performers) == 1
    assert performers[0]["name"] == "Bud Testman"
    assert performers[0]["instrument"] == "piano"
    assert performers[0]["role"] == "leader"


def test_bare_row_fallthrough(client, batch_fixture):
    """A recording with no performers / releases / imagery still returns
    every contract field with the expected null / false / empty sentinel.
    Same INNER-JOIN protection as the list endpoint's CTE fall-through.
    """
    resp = client.get(_batch_url([batch_fixture["bare_recording_id"]]))
    body = resp.get_json()
    rec = body["recordings"][0]

    assert set(rec.keys()) == EXPECTED_BATCH_FIELDS
    assert rec["performers"] == []
    assert rec["authority_count"] == 0
    assert rec["authority_sources"] == []
    assert rec["streaming_services"] == []
    assert rec["has_streaming"] is False
    assert rec["has_spotify"] is False
    assert rec["has_back_cover"] is False
    assert rec["best_cover_art_medium"] is None
    assert rec["community_data"] is None


def test_unknown_ids_are_silently_omitted(client, batch_fixture):
    """A mix of known + unknown IDs returns only the known ones. A stale
    client (recording deleted server-side) should still hydrate whatever
    remains, not get a 404 that kills the whole batch.
    """
    import uuid
    unknown = str(uuid.uuid4())
    resp = client.get(_batch_url([
        batch_fixture["populated_recording_id"],
        unknown,
        batch_fixture["bare_recording_id"],
    ]))
    assert resp.status_code == 200
    body = resp.get_json()
    assert len(body["recordings"]) == 2
    returned_ids = {r["id"] for r in body["recordings"]}
    assert batch_fixture["populated_recording_id"] in returned_ids
    assert batch_fixture["bare_recording_id"] in returned_ids
    assert unknown not in returned_ids


def test_whitespace_around_ids_is_tolerated(client, batch_fixture):
    """Clients that get sloppy with URL construction (extra spaces, a
    trailing comma) should still succeed. Guards against a fragile
    split-and-strip regression on the server side.
    """
    populated = batch_fixture["populated_recording_id"]
    bare = batch_fixture["bare_recording_id"]
    # Leading/trailing spaces, a trailing comma, and a duplicate empty
    # field from a double comma — all should be cleaned up server-side.
    resp = client.get(f"/v1/recordings/batch?ids=  {populated} ,, {bare} ,")
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert len(body["recordings"]) == 2


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------

def test_missing_ids_param_returns_400(client):
    """No ``ids`` at all → 400. Don't let an empty request hit the DB."""
    resp = client.get("/v1/recordings/batch")
    assert resp.status_code == 400
    assert "ids" in resp.get_json()["error"].lower()


def test_empty_ids_value_returns_400(client):
    """``?ids=`` with no value → 400."""
    resp = client.get("/v1/recordings/batch?ids=")
    assert resp.status_code == 400


def test_only_separators_returns_400(client):
    """``?ids=,,,`` (all commas, no real IDs) → 400 after the strip pass."""
    resp = client.get("/v1/recordings/batch?ids=,,,")
    assert resp.status_code == 400


def test_malformed_uuid_returns_400(client):
    """A non-UUID entry → 400 with a message naming the bad value.
    Client-side bugs that construct bad IDs should surface clearly rather
    than as a cryptic psycopg error.
    """
    resp = client.get("/v1/recordings/batch?ids=not-a-uuid")
    assert resp.status_code == 400
    assert "uuid" in resp.get_json()["error"].lower()


def test_too_many_ids_returns_400(client):
    """Over BATCH_MAX_IDS → 400. Caps query cost and prevents clients
    from asking for a whole catalog at once.
    """
    import uuid
    from routes.recordings import BATCH_MAX_IDS
    ids = [str(uuid.uuid4()) for _ in range(BATCH_MAX_IDS + 1)]
    resp = client.get(_batch_url(ids))
    assert resp.status_code == 400
    assert "too many" in resp.get_json()["error"].lower()
