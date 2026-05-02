"""
Contract tests for GET /api/songs/<song_id>/recordings.

Why these tests exist
---------------------
This endpoint ships a trimmed payload shape to the iOS/Mac list views. It
has been refactored several times (CTE restructure, field trims), and each
refactor has risked drifting the response shape away from what the clients
expect. The Swift ``Recording`` struct is all-optional, so a missing or
renamed field here decodes to ``nil`` silently — which means problems only
surface as "why is my UI blank" after a ship.

This module pins the contract:

  * ``EXPECTED_LIST_FIELDS`` is the exact set of top-level keys each
    recording in the response must carry. Adding or removing a field here
    is the forcing function: a developer changing the endpoint has to
    consciously edit this constant, which makes them think about whether
    the iOS clients will render correctly with the new shape.
  * Fields are asserted as a *set equality*, not "contains" — so both
    additions and removals are caught.
  * ``DROPPED_LIST_FIELDS`` is a second constant listing fields we
    deliberately stripped from the list payload in favour of the detail
    endpoint carrying them. A redundant assertion by design — it's named
    for grep-ability when the next trim happens.
  * A separate test exercises the CTE fall-through path (recording with
    no releases, no imagery, no streaming, no performers, no community
    data) so that an INNER JOIN sneaking in for any of the pre-computed
    CTEs is caught immediately.

If you add a field to the list query, update ``EXPECTED_LIST_FIELDS``.
If you drop one, move it to ``DROPPED_LIST_FIELDS`` and verify no iOS
row-level view depends on it (detail views re-fetch via
``/api/recordings/<id>``, which carries the full payload).

Fixture strategy
----------------
The tests insert their own song/recordings/performers/releases using a
set of deterministic UUIDs (``00000000-…-0000-000000000001`` etc.) and
delete those specific rows before and after each test. This is compatible
with the existing conftest auth-table cleanup and won't touch data a
developer has locally in the ``jazz_test`` DB.
"""

import uuid

import pytest


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------

# Exact set of top-level keys each recording in the list response must carry.
# Every entry here corresponds to a field the iOS/Mac list-row views or the
# client-side recording filters read. Grep for the field name in apps/ before
# removing anything — some fields (e.g. community_data) are used only by the
# vocal/instrumental filter, not the row render itself.
EXPECTED_LIST_FIELDS = frozenset({
    "id",
    "title",
    "album_title",
    "artist_credit",
    "recording_year",
    "is_canonical",
    # Cover art — front (iOS row uses medium+small, Mac card uses large)
    "best_cover_art_small",
    "best_cover_art_medium",
    "best_cover_art_large",
    "best_cover_art_source",
    "best_cover_art_source_url",
    # Cover art — back (iOS row flip animation uses small+medium)
    "back_cover_art_small",
    "back_cover_art_medium",
    "back_cover_art_large",
    "has_back_cover",
    "back_cover_source",
    "back_cover_source_url",
    # Streaming (Mac StreamingButtons uses best_spotify_url; filters use flags)
    "best_spotify_url",
    "has_streaming",
    "has_spotify",
    "has_apple_music",
    "has_youtube",
    "streaming_services",
    # Performers — row shows leader; instrument filter reads all performers
    "performers",
    # Authority badge on row
    "authority_count",
    "authority_sources",
    # Community consensus drives the vocal/instrumental filter
    "community_data",
})

# Fields deliberately excluded from the list payload. Detail views re-fetch
# via /api/recordings/<id>, which carries these. Do not add them back here
# without first checking apps/ for a list-row or filter dependency.
DROPPED_LIST_FIELDS = frozenset({
    "musicbrainz_id",
    "default_release_id",
    "recording_date",
    "label",
    "notes",
})


# ---------------------------------------------------------------------------
# Fixture UUIDs — deterministic so cleanup is precise and test failures are
# easy to match to rows in the DB.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-00000000{:04x}"  # v4-shaped, fixture-only range

SONG_ID = _NS.format(0x0001)
RECORDING_POPULATED_ID = _NS.format(0x0010)
RECORDING_BARE_ID = _NS.format(0x0011)
PERFORMER_ID = _NS.format(0x0020)
INSTRUMENT_ID = _NS.format(0x0030)
RELEASE_ID = _NS.format(0x0040)
RECORDING_PERFORMER_ID = _NS.format(0x0050)
RECORDING_RELEASE_ID = _NS.format(0x0051)
RELEASE_IMAGERY_FRONT_ID = _NS.format(0x0060)
RELEASE_IMAGERY_BACK_ID = _NS.format(0x0061)
STREAMING_LINK_ID = _NS.format(0x0070)


def _cleanup(conn):
    """Delete all fixture rows. Safe to call before and after a test.

    Order matters: delete children before parents to satisfy FK constraints.
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (STREAMING_LINK_ID,),
        )
        cur.execute(
            "DELETE FROM release_imagery WHERE id IN (%s, %s)",
            (RELEASE_IMAGERY_FRONT_ID, RELEASE_IMAGERY_BACK_ID),
        )
        cur.execute(
            "DELETE FROM recording_releases WHERE id = %s",
            (RECORDING_RELEASE_ID,),
        )
        cur.execute(
            "DELETE FROM recording_performers WHERE id = %s",
            (RECORDING_PERFORMER_ID,),
        )
        cur.execute(
            "DELETE FROM song_authority_recommendations WHERE song_id = %s",
            (SONG_ID,),
        )
        cur.execute(
            "DELETE FROM recording_contributions WHERE recording_id IN (%s, %s)",
            (RECORDING_POPULATED_ID, RECORDING_BARE_ID),
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
def song_fixture(db):
    """Insert one song with two recordings:

    * ``RECORDING_POPULATED_ID`` — has a release, front+back imagery, a
      performer on an instrument, and a Spotify streaming link. Exercises
      every CTE in the list query.
    * ``RECORDING_BARE_ID`` — linked only to the song. No release, no
      imagery, no performers, no streaming, no community. Exercises the
      LEFT JOIN fall-through (fields should be present with null / false /
      empty values, not missing).

    Cleanup runs before AND after the test so a crashed previous run
    doesn't leave orphan rows that fail the next run.
    """
    _cleanup(db)  # clean any leftovers from a crashed previous run

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Contract Test Song"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (PERFORMER_ID, "Art Test", "Test, Art"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INSTRUMENT_ID, "piano"),
        )
        cur.execute(
            """
            INSERT INTO releases (id, title, artist_credit)
            VALUES (%s, %s, %s)
            """,
            (RELEASE_ID, "Contract Test Album", "Art Test"),
        )

        # Recording with all CTE inputs populated
        cur.execute(
            """
            INSERT INTO recordings
                (id, song_id, title, recording_year, default_release_id,
                 is_canonical)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                RECORDING_POPULATED_ID,
                SONG_ID,
                "Contract Test Recording (populated)",
                1957,
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
            (
                RECORDING_PERFORMER_ID,
                RECORDING_POPULATED_ID,
                PERFORMER_ID,
                INSTRUMENT_ID,
                "leader",
            ),
        )
        cur.execute(
            """
            INSERT INTO recording_releases
                (id, recording_id, release_id, track_number)
            VALUES (%s, %s, %s, %s)
            """,
            (RECORDING_RELEASE_ID, RECORDING_POPULATED_ID, RELEASE_ID, 1),
        )
        cur.execute(
            """
            INSERT INTO release_imagery
                (id, release_id, source, source_url, type,
                 image_url_small, image_url_medium, image_url_large)
            VALUES (%s, %s, 'MusicBrainz', 'http://example.test/ri-front', 'Front',
                    'http://example.test/front-small.jpg',
                    'http://example.test/front-medium.jpg',
                    'http://example.test/front-large.jpg')
            """,
            (RELEASE_IMAGERY_FRONT_ID, RELEASE_ID),
        )
        cur.execute(
            """
            INSERT INTO release_imagery
                (id, release_id, source, source_url, type,
                 image_url_small, image_url_medium, image_url_large)
            VALUES (%s, %s, 'MusicBrainz', 'http://example.test/ri-back', 'Back',
                    'http://example.test/back-small.jpg',
                    'http://example.test/back-medium.jpg',
                    'http://example.test/back-large.jpg')
            """,
            (RELEASE_IMAGERY_BACK_ID, RELEASE_ID),
        )
        cur.execute(
            """
            INSERT INTO recording_release_streaming_links
                (id, recording_release_id, service, service_url)
            VALUES (%s, %s, 'spotify', 'http://example.test/spotify/track/1')
            """,
            (STREAMING_LINK_ID, RECORDING_RELEASE_ID),
        )

        # Bare recording — same song, nothing else linked
        cur.execute(
            """
            INSERT INTO recordings (id, song_id, title, recording_year)
            VALUES (%s, %s, %s, %s)
            """,
            (
                RECORDING_BARE_ID,
                SONG_ID,
                "Contract Test Recording (bare)",
                1962,
            ),
        )
    db.commit()

    yield {
        "song_id": SONG_ID,
        "populated_recording_id": RECORDING_POPULATED_ID,
        "bare_recording_id": RECORDING_BARE_ID,
    }

    _cleanup(db)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_top_level_shape(client, song_fixture):
    """Top-level response envelope must carry song_id / recordings / count."""
    resp = client.get(f"/v1/songs/{song_fixture['song_id']}/recordings")
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()

    assert set(body.keys()) == {"song_id", "recordings", "recording_count"}
    assert body["song_id"] == song_fixture["song_id"]
    assert body["recording_count"] == 2
    assert len(body["recordings"]) == 2


def test_each_recording_has_exact_expected_fields(client, song_fixture):
    """Contract check: every recording carries *exactly* EXPECTED_LIST_FIELDS.

    This catches both:
      * accidental removal of a field the iOS row depends on (set mismatch),
      * accidental addition of a detail-only field to the list payload
        (set mismatch the other way, keeps the list endpoint lean).
    """
    resp = client.get(f"/v1/songs/{song_fixture['song_id']}/recordings")
    assert resp.status_code == 200
    body = resp.get_json()

    for i, rec in enumerate(body["recordings"]):
        actual = set(rec.keys())
        missing = EXPECTED_LIST_FIELDS - actual
        unexpected = actual - EXPECTED_LIST_FIELDS
        assert not missing and not unexpected, (
            f"recording[{i}] field set drifted from the contract.\n"
            f"  missing:    {sorted(missing)}\n"
            f"  unexpected: {sorted(unexpected)}\n"
            "If this is intentional, update EXPECTED_LIST_FIELDS at the top "
            "of this file and verify that no iOS/Mac list-row view or "
            "client-side filter depends on the removed field."
        )


def test_dropped_fields_stay_dropped(client, song_fixture):
    """Redundant with the contract, but explicit about intent.

    Named so ``grep musicbrainz_id backend/tests`` surfaces this test when
    someone considers re-adding a dropped field.
    """
    resp = client.get(f"/v1/songs/{song_fixture['song_id']}/recordings")
    body = resp.get_json()
    for rec in body["recordings"]:
        leaked = DROPPED_LIST_FIELDS & set(rec.keys())
        assert not leaked, (
            f"Dropped fields reappeared in list payload: {sorted(leaked)}. "
            "These were removed because detail view re-fetches via "
            "/recordings/<id>. Move back to EXPECTED_LIST_FIELDS only "
            "if a new iOS row-level usage requires it."
        )


def test_cte_fallthrough_for_bare_recording(client, song_fixture):
    """A recording with no releases/imagery/streaming/performers/community
    must still return every contract field — populated with null, false, or
    the empty-collection sentinel.

    This guards against any of the pre-computed CTEs being joined with an
    INNER JOIN (which would silently drop the row) or against a COALESCE
    being removed from a nullable column (which would change the wire type
    from ``bool`` to ``bool | null`` and break Swift decoding into ``Bool``
    for callers that have since un-optionaled it).
    """
    resp = client.get(f"/v1/songs/{song_fixture['song_id']}/recordings")
    body = resp.get_json()

    bare = next(
        r for r in body["recordings"]
        if r["id"] == song_fixture["bare_recording_id"]
    )

    # All contract fields present, even though nothing is joined in.
    assert set(bare.keys()) == EXPECTED_LIST_FIELDS

    # Spot-check the fallbacks — these are what the iOS filters / row
    # renders expect when a recording has no related data.
    assert bare["performers"] == []
    assert bare["authority_count"] == 0
    assert bare["authority_sources"] == []
    assert bare["streaming_services"] == []
    assert bare["has_streaming"] is False
    assert bare["has_spotify"] is False
    assert bare["has_apple_music"] is False
    assert bare["has_youtube"] is False
    assert bare["has_back_cover"] is False
    # Nullable-but-present: cover art URLs, community data, etc.
    assert bare["best_cover_art_medium"] is None
    assert bare["community_data"] is None


def test_populated_recording_surfaces_joined_data(client, song_fixture):
    """Counterpart to the bare-recording test: the fully-linked recording
    should expose the data from every CTE so refactors can't silently
    sever a JOIN.
    """
    resp = client.get(f"/v1/songs/{song_fixture['song_id']}/recordings")
    body = resp.get_json()

    rec = next(
        r for r in body["recordings"]
        if r["id"] == song_fixture["populated_recording_id"]
    )

    assert rec["album_title"] == "Contract Test Album"
    assert rec["artist_credit"] == "Art Test"
    assert rec["recording_year"] == 1957
    assert rec["is_canonical"] is True
    assert rec["best_cover_art_medium"] == "http://example.test/front-medium.jpg"
    assert rec["back_cover_art_medium"] == "http://example.test/back-medium.jpg"
    assert rec["has_back_cover"] is True
    assert rec["has_spotify"] is True
    assert rec["best_spotify_url"] == "http://example.test/spotify/track/1"
    assert "spotify" in rec["streaming_services"]

    performers = rec["performers"]
    assert len(performers) == 1
    assert performers[0]["name"] == "Art Test"
    assert performers[0]["instrument"] == "piano"
    assert performers[0]["role"] == "leader"


def test_name_sort_branch_returns_contract_shape(client, song_fixture):
    """The ``?sort=name`` branch swaps in a correlated subquery for
    ORDER BY (routes/songs.py). It's easy to break when editing the
    query, so exercise it explicitly with the same contract assertion.
    """
    resp = client.get(
        f"/v1/songs/{song_fixture['song_id']}/recordings?sort=name"
    )
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["recording_count"] == 2
    for rec in body["recordings"]:
        assert set(rec.keys()) == EXPECTED_LIST_FIELDS


def test_unknown_song_returns_empty_list(client):
    """Sanity: an unknown song ID returns 200 with zero recordings, not a
    500 or a 404. This matches the current handler behaviour and the iOS
    client's expectation (it shows an empty list rather than erroring).
    """
    unknown = str(uuid.uuid4())
    resp = client.get(f"/v1/songs/{unknown}/recordings")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["recording_count"] == 0
    assert body["recordings"] == []
