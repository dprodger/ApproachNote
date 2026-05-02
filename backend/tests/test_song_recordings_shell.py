"""
Contract tests for GET /api/songs/<song_id>/recordings/shell.

Why these tests exist
---------------------
The shell endpoint is half of a shell+hydrate pattern: the iOS/Mac app
loads it to render group headers, apply client-side filters, and show
skeleton rows; it then fills in cover art + full performer data via
GET /api/recordings/batch as rows scroll into view.

The contract here is narrower than the list endpoint's but has the same
brittle failure mode — a drift in the wire shape decodes silently to
``nil`` on the Swift side, which surfaces as "why is my group header
count wrong" or "why is the vocal filter dead" rather than a hard error.

This module pins the contract:

* ``EXPECTED_SHELL_FIELDS`` is the exact set of top-level keys each
  recording in the shell response must carry. Set equality, so additions
  and removals both fail the test.
* ``FORBIDDEN_SHELL_FIELDS`` is an explicit deny-list of "full" payload
  fields that MUST NOT appear on the shell — cover art, full performers
  arrays, community_data jsonb, etc. Catches accidental regressions where
  someone pastes in the full query and forgets to trim.
* A separate test exercises the CTE fall-through path (recording with no
  performers, no releases, no streaming, no community contributions, no
  authority recs) so the shell endpoint stays robust to missing data.
* Two tests cover the happy-path population cases: leader performer
  surfaces as a single-element ``performers`` array, sidemen's instruments
  are included in ``instruments_present`` even though sidemen are not in
  ``performers``.

If you add a field to the shell query, update ``EXPECTED_SHELL_FIELDS``.
If you add a field to ``FORBIDDEN_SHELL_FIELDS`` instead of
``EXPECTED_SHELL_FIELDS`` by mistake, both tests will fail — which is
a clear prompt to pick the right bucket.

Fixture strategy mirrors ``test_song_recordings.py``: deterministic UUIDs
in the ``00000000-0000-4000-8000-…`` range, self-cleaning before + after
every test so a crashed run can't orphan rows.
"""

import pytest


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------

EXPECTED_SHELL_FIELDS = frozenset({
    "id",
    "title",
    "album_title",
    "artist_credit",
    "recording_year",
    "is_canonical",
    # Leader performer is exposed as a single-element performers array so
    # RecordingRowView / RecordingCard can read performers.first(where: role=="leader")
    # without any client code change.
    "performers",
    # Flat list of instruments appearing ANYWHERE on the recording (leader
    # + sidemen). The iOS instrument-family filter reads this so hydration
    # doesn't have to have arrived for the filter to work.
    "instruments_present",
    # Community consensus bool, surfaced at top level (not wrapped in a
    # community_data jsonb). The iOS vocal/instrumental filter reads this.
    "is_instrumental",
    # Streaming flags drive the streaming-service filter. Full
    # streaming_services array and per-service URLs stay in the hydrated
    # batch response.
    "has_streaming",
    "has_spotify",
    "has_apple_music",
    "has_youtube",
    # Authority count drives the badge. The sources array stays in the
    # hydrated response.
    "authority_count",
})

# Fields that belong on the hydrated batch response but MUST NOT be on the
# shell. Having a separate assertion (rather than relying purely on set
# equality) makes the intent explicit when reading test output: if you
# see "back_cover_art_medium leaked into shell", you immediately know
# what happened.
FORBIDDEN_SHELL_FIELDS = frozenset({
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
    "streaming_services",
    "authority_sources",
    "community_data",
    # Fields the previous trim (189c099) removed from the list endpoint
    # should obviously not creep into the shell either.
    "musicbrainz_id",
    "default_release_id",
    "recording_date",
    "label",
    "notes",
})


# ---------------------------------------------------------------------------
# Fixture UUIDs — use a distinct range from test_song_recordings.py so the
# two suites never collide if they happen to run concurrently.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-0000000a{:04x}"

SONG_ID = _NS.format(0x0001)
RECORDING_POPULATED_ID = _NS.format(0x0010)
RECORDING_BARE_ID = _NS.format(0x0011)
LEADER_ID = _NS.format(0x0020)
SIDEMAN_ID = _NS.format(0x0021)
INSTRUMENT_PIANO_ID = _NS.format(0x0030)
INSTRUMENT_SAX_ID = _NS.format(0x0031)
RELEASE_ID = _NS.format(0x0040)
RP_LEADER_ID = _NS.format(0x0050)
RP_SIDEMAN_ID = _NS.format(0x0051)
RR_ID = _NS.format(0x0060)
STREAMING_LINK_ID = _NS.format(0x0070)


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (STREAMING_LINK_ID,),
        )
        cur.execute("DELETE FROM recording_releases WHERE id = %s", (RR_ID,))
        cur.execute(
            "DELETE FROM recording_performers WHERE id IN (%s, %s)",
            (RP_LEADER_ID, RP_SIDEMAN_ID),
        )
        cur.execute(
            "DELETE FROM recording_contributions WHERE recording_id IN (%s, %s)",
            (RECORDING_POPULATED_ID, RECORDING_BARE_ID),
        )
        cur.execute(
            "DELETE FROM song_authority_recommendations WHERE song_id = %s",
            (SONG_ID,),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s)",
            (RECORDING_POPULATED_ID, RECORDING_BARE_ID),
        )
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute(
            "DELETE FROM performers WHERE id IN (%s, %s)",
            (LEADER_ID, SIDEMAN_ID),
        )
        cur.execute(
            "DELETE FROM instruments WHERE id IN (%s, %s)",
            (INSTRUMENT_PIANO_ID, INSTRUMENT_SAX_ID),
        )
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def shell_fixture(db):
    """Insert a song with two recordings:

    * ``RECORDING_POPULATED_ID`` — leader on piano + sideman on saxophone,
      a release, a spotify streaming link. Exercises the leader CTE, the
      instruments_present aggregation (both leader's AND sideman's
      instruments should appear), and the streaming CTE.
    * ``RECORDING_BARE_ID`` — linked only to the song. Exercises all the
      LEFT JOIN fall-throughs.
    """
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Shell Contract Test Song"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (LEADER_ID, "Art Leader", "Leader, Art"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (SIDEMAN_ID, "Sid Man", "Man, Sid"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INSTRUMENT_PIANO_ID, "piano"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INSTRUMENT_SAX_ID, "saxophone"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) VALUES (%s, %s, %s)",
            (RELEASE_ID, "Shell Test Album", "Art Leader Quartet"),
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
                "Shell Test (populated)",
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
                RP_LEADER_ID,
                RECORDING_POPULATED_ID,
                LEADER_ID,
                INSTRUMENT_PIANO_ID,
                "leader",
            ),
        )
        cur.execute(
            """
            INSERT INTO recording_performers
                (id, recording_id, performer_id, instrument_id, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                RP_SIDEMAN_ID,
                RECORDING_POPULATED_ID,
                SIDEMAN_ID,
                INSTRUMENT_SAX_ID,
                "sideman",
            ),
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
            INSERT INTO recording_release_streaming_links
                (id, recording_release_id, service, service_url)
            VALUES (%s, %s, 'spotify', 'http://example.test/spotify/track/1')
            """,
            (STREAMING_LINK_ID, RR_ID),
        )

        cur.execute(
            """
            INSERT INTO recordings (id, song_id, title, recording_year)
            VALUES (%s, %s, %s, %s)
            """,
            (RECORDING_BARE_ID, SONG_ID, "Shell Test (bare)", 1962),
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

def test_top_level_shape(client, shell_fixture):
    """Shell response envelope matches the list envelope so the Swift
    ``SongRecordingsResponse`` decoder can be reused for both endpoints.
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell"
    )
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()

    assert set(body.keys()) == {"song_id", "recordings", "recording_count"}
    assert body["song_id"] == shell_fixture["song_id"]
    assert body["recording_count"] == 2
    assert len(body["recordings"]) == 2


def test_each_recording_has_exact_expected_fields(client, shell_fixture):
    """Contract check: every row carries *exactly* EXPECTED_SHELL_FIELDS.

    Fails loudly on both accidental addition (someone copies a full-row
    column into the shell SELECT) and accidental removal (someone edits
    the SELECT and forgets to update a consumer).
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell"
    )
    assert resp.status_code == 200
    body = resp.get_json()

    for i, rec in enumerate(body["recordings"]):
        actual = set(rec.keys())
        missing = EXPECTED_SHELL_FIELDS - actual
        unexpected = actual - EXPECTED_SHELL_FIELDS
        assert not missing and not unexpected, (
            f"recording[{i}] shell-field set drifted from the contract.\n"
            f"  missing:    {sorted(missing)}\n"
            f"  unexpected: {sorted(unexpected)}\n"
            "If intentional, update EXPECTED_SHELL_FIELDS at the top of "
            "this file. Double-check that iOS/Mac clients can still render "
            "and filter without the removed field, since shell fields are "
            "what drive group headers and filters."
        )


def test_forbidden_hydrate_only_fields_stay_out(client, shell_fixture):
    """Fields that belong on the hydrated batch response MUST NOT appear
    here. If they did, the whole point of the shell (being tiny) would be
    defeated and Swift decode would be harder to reason about.
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell"
    )
    body = resp.get_json()
    for rec in body["recordings"]:
        leaked = FORBIDDEN_SHELL_FIELDS & set(rec.keys())
        assert not leaked, (
            f"Hydrate-only fields leaked into shell: {sorted(leaked)}. "
            "These belong on GET /api/recordings/batch, not on the "
            "shell endpoint."
        )


def test_cte_fallthrough_for_bare_recording(client, shell_fixture):
    """A recording with no performers / releases / streaming / community /
    authority rows still returns every contract field with the expected
    null-or-empty sentinel. Guards against any of the LEFT JOINs being
    replaced with an INNER JOIN (which would silently drop the row from
    the response entirely — disastrous for the shell, since the row then
    doesn't appear in the group headers).
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell"
    )
    body = resp.get_json()

    bare = next(
        r for r in body["recordings"]
        if r["id"] == shell_fixture["bare_recording_id"]
    )

    assert set(bare.keys()) == EXPECTED_SHELL_FIELDS

    # Contract: empty list sentinels, not nil, where the iOS filter
    # iterates without a nil check.
    assert bare["performers"] == []
    assert bare["instruments_present"] == []
    # Contract: nullable bool for the vocal filter — the filter treats
    # null as "unknown", not "false", so the null must survive.
    assert bare["is_instrumental"] is None
    # Contract: booleans, never null — the filter treats false as "absent".
    assert bare["has_streaming"] is False
    assert bare["has_spotify"] is False
    assert bare["has_apple_music"] is False
    assert bare["has_youtube"] is False
    # Contract: int, never null — the UI's badge reads this with a > 0 check.
    assert bare["authority_count"] == 0


def test_populated_recording_leader_surfaces(client, shell_fixture):
    """A leader with a sideman: the shell's performers array contains ONLY
    the leader (one element), and instruments_present contains BOTH the
    leader's piano and the sideman's saxophone.

    This is the part that trips up hand-maintained queries most often:
    the performers-shape-for-display rule ("leader only") and the
    instruments-for-filter rule ("everyone") are different. Both matter.
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell"
    )
    body = resp.get_json()

    rec = next(
        r for r in body["recordings"]
        if r["id"] == shell_fixture["populated_recording_id"]
    )

    # performers: leader only, as a single-element array so
    # RecordingRowView's existing code (performers.first(where: role==leader))
    # works without modification.
    assert len(rec["performers"]) == 1
    leader = rec["performers"][0]
    assert leader["name"] == "Art Leader"
    assert leader["sort_name"] == "Leader, Art"
    assert leader["role"] == "leader"
    assert leader["instrument"] == "piano"

    # instruments_present: flat array with both leader and sideman
    # instruments, so the instrument-family filter can match by saxophone
    # even though no saxophonist is in `performers`.
    assert sorted(rec["instruments_present"]) == ["piano", "saxophone"]

    # Streaming flag: true because we inserted a spotify link.
    assert rec["has_spotify"] is True
    assert rec["has_streaming"] is True
    assert rec["has_apple_music"] is False
    assert rec["has_youtube"] is False

    # Album title + artist credit come from the default release.
    assert rec["album_title"] == "Shell Test Album"
    assert rec["artist_credit"] == "Art Leader Quartet"
    assert rec["recording_year"] == 1957
    assert rec["is_canonical"] is True


def test_name_sort_branch_returns_contract_shape(client, shell_fixture):
    """The ``?sort=name`` branch uses a different ORDER BY against the
    leader CTE. Exercise it with the same contract check so a typo there
    doesn't slip through.
    """
    resp = client.get(
        f"/v1/songs/{shell_fixture['song_id']}/recordings/shell?sort=name"
    )
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["recording_count"] == 2
    for rec in body["recordings"]:
        assert set(rec.keys()) == EXPECTED_SHELL_FIELDS


def test_unknown_song_returns_empty_list(client):
    """Matches behaviour of the list endpoint: unknown song → 200 with an
    empty recordings array, not a 404. iOS relies on this to render an
    empty state rather than erroring.
    """
    import uuid
    unknown = str(uuid.uuid4())
    resp = client.get(f"/v1/songs/{unknown}/recordings/shell")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["recording_count"] == 0
    assert body["recordings"] == []
