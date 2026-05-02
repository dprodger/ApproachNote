"""
Integration test: shell + batch equals the existing list endpoint.

The shell+hydrate pattern replaces a single call to
``GET /api/songs/<id>/recordings`` with two calls: the shell (for group
headers + filters + skeleton rows) and a batch hydration (for the render
data as rows scroll in). The two approaches must return semantically
equivalent data, row-for-row, or the iOS app will render the same song
differently depending on which code path it went down.

This test pins that equivalence by inserting a diverse fixture and
asserting:

* Same row count across all three endpoints.
* Same set of row IDs.
* For every field that has an obvious analogue across the three responses,
  the values match (has_spotify, recording_year, authority_count,
  leader name, instruments, vocal-consensus bool).
* Batch endpoint returns the exact same per-row shape as the list
  endpoint — i.e. the Swift decoder can treat them interchangeably.

A failure here is usually one of two things:
 1. Someone edited one of the three queries without updating the others.
    Fix: keep the CTE logic in sync.
 2. A new field was added to one endpoint but not the others. Fix: decide
    whether it belongs in shell / batch / both, and update the contract
    tests too.
"""

import pytest


_NS = "00000000-0000-4000-8000-0000000c{:04x}"

SONG_ID = _NS.format(0x0001)
REC_LEADER_ONLY_ID = _NS.format(0x0010)       # leader only, no other data
REC_FULL_ID = _NS.format(0x0011)              # leader + sideman + release + imagery + streaming
REC_BARE_ID = _NS.format(0x0012)              # nothing attached

LEADER_ID = _NS.format(0x0020)
SIDEMAN_ID = _NS.format(0x0021)
INST_PIANO_ID = _NS.format(0x0030)
INST_BASS_ID = _NS.format(0x0031)
RELEASE_ID = _NS.format(0x0040)
RP_LEADER_ONLY_ID = _NS.format(0x0050)
RP_FULL_LEADER_ID = _NS.format(0x0051)
RP_FULL_SIDEMAN_ID = _NS.format(0x0052)
RR_ID = _NS.format(0x0060)
RI_ID = _NS.format(0x0070)
SL_ID = _NS.format(0x0080)
RC_ID = _NS.format(0x0090)  # community contribution
USER_ID = _NS.format(0x00a0)


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links WHERE id = %s",
            (SL_ID,),
        )
        cur.execute("DELETE FROM release_imagery WHERE id = %s", (RI_ID,))
        cur.execute("DELETE FROM recording_releases WHERE id = %s", (RR_ID,))
        cur.execute(
            "DELETE FROM recording_performers WHERE id IN (%s, %s, %s)",
            (RP_LEADER_ONLY_ID, RP_FULL_LEADER_ID, RP_FULL_SIDEMAN_ID),
        )
        cur.execute(
            "DELETE FROM recording_contributions WHERE id = %s",
            (RC_ID,),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s, %s)",
            (REC_LEADER_ONLY_ID, REC_FULL_ID, REC_BARE_ID),
        )
        cur.execute("DELETE FROM releases WHERE id = %s", (RELEASE_ID,))
        cur.execute(
            "DELETE FROM performers WHERE id IN (%s, %s)",
            (LEADER_ID, SIDEMAN_ID),
        )
        cur.execute(
            "DELETE FROM instruments WHERE id IN (%s, %s)",
            (INST_PIANO_ID, INST_BASS_ID),
        )
        cur.execute("DELETE FROM users WHERE id = %s", (USER_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def three_recording_fixture(db):
    """One song with three recordings spanning the interesting shapes:

    * REC_LEADER_ONLY — leader performer but no release, no streaming,
      no community contribution. Catches regressions where a LEFT JOIN
      becomes INNER on one endpoint but not the other.
    * REC_FULL — leader + sideman + default release + front imagery +
      spotify link + community contribution. The "everything" row.
    * REC_BARE — only linked to the song. Catches the worst-case empty
      fall-through.
    """
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Integration Song"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (LEADER_ID, "Lead Player", "Player, Lead"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, sort_name) VALUES (%s, %s, %s)",
            (SIDEMAN_ID, "Side Kick", "Kick, Side"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INST_PIANO_ID, "piano"),
        )
        cur.execute(
            "INSERT INTO instruments (id, name) VALUES (%s, %s)",
            (INST_BASS_ID, "bass"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) VALUES (%s, %s, %s)",
            (RELEASE_ID, "Integration Album", "Lead Player Duo"),
        )
        # User for community contribution
        cur.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name)
            VALUES (%s, %s, %s, %s)
            """,
            (USER_ID, "integ@test.example", "x", "Integ User"),
        )

        # REC_LEADER_ONLY
        cur.execute(
            """
            INSERT INTO recordings (id, song_id, title, recording_year)
            VALUES (%s, %s, %s, %s)
            """,
            (REC_LEADER_ONLY_ID, SONG_ID, "Leader Only", 1955),
        )
        cur.execute(
            """
            INSERT INTO recording_performers
                (id, recording_id, performer_id, instrument_id, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (RP_LEADER_ONLY_ID, REC_LEADER_ONLY_ID, LEADER_ID, INST_PIANO_ID, "leader"),
        )

        # REC_FULL
        cur.execute(
            """
            INSERT INTO recordings
                (id, song_id, title, recording_year, default_release_id, is_canonical)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (REC_FULL_ID, SONG_ID, "Full Row", 1963, RELEASE_ID, True),
        )
        cur.execute(
            """
            INSERT INTO recording_performers
                (id, recording_id, performer_id, instrument_id, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (RP_FULL_LEADER_ID, REC_FULL_ID, LEADER_ID, INST_PIANO_ID, "leader"),
        )
        cur.execute(
            """
            INSERT INTO recording_performers
                (id, recording_id, performer_id, instrument_id, role)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (RP_FULL_SIDEMAN_ID, REC_FULL_ID, SIDEMAN_ID, INST_BASS_ID, "sideman"),
        )
        cur.execute(
            """
            INSERT INTO recording_releases
                (id, recording_id, release_id, track_number)
            VALUES (%s, %s, %s, %s)
            """,
            (RR_ID, REC_FULL_ID, RELEASE_ID, 1),
        )
        cur.execute(
            """
            INSERT INTO release_imagery
                (id, release_id, source, source_url, type,
                 image_url_small, image_url_medium, image_url_large)
            VALUES (%s, %s, 'MusicBrainz', 'http://t.test/ri', 'Front',
                    'http://t.test/s', 'http://t.test/m', 'http://t.test/l')
            """,
            (RI_ID, RELEASE_ID),
        )
        cur.execute(
            """
            INSERT INTO recording_release_streaming_links
                (id, recording_release_id, service, service_url)
            VALUES (%s, %s, 'spotify', 'http://t.test/spotify')
            """,
            (SL_ID, RR_ID),
        )
        cur.execute(
            """
            INSERT INTO recording_contributions
                (id, recording_id, user_id, is_instrumental)
            VALUES (%s, %s, %s, %s)
            """,
            (RC_ID, REC_FULL_ID, USER_ID, True),
        )

        # REC_BARE
        cur.execute(
            """
            INSERT INTO recordings (id, song_id, title, recording_year)
            VALUES (%s, %s, %s, %s)
            """,
            (REC_BARE_ID, SONG_ID, "Bare Row", 1971),
        )

    db.commit()
    yield {
        "song_id": SONG_ID,
        "leader_only_id": REC_LEADER_ONLY_ID,
        "full_id": REC_FULL_ID,
        "bare_id": REC_BARE_ID,
    }
    _cleanup(db)


# ---------------------------------------------------------------------------
# The integration check
# ---------------------------------------------------------------------------

def test_shell_plus_batch_matches_list_endpoint(client, three_recording_fixture):
    """The whole point of the shell+hydrate rewrite: two calls must yield
    the same rendering data as the one call they replace.

    Shape check: batch rows have the exact field set of list rows.
    Value check: cross-endpoint fields (IDs, year, has_*, authority_count,
    leader name, vocal consensus, instruments appearing anywhere) agree.
    """
    fx = three_recording_fixture

    # ---- 1. list endpoint (the existing, full payload) ----
    list_resp = client.get(f"/v1/songs/{fx['song_id']}/recordings")
    assert list_resp.status_code == 200
    list_body = list_resp.get_json()
    assert list_body["recording_count"] == 3

    # ---- 2. shell endpoint ----
    shell_resp = client.get(
        f"/v1/songs/{fx['song_id']}/recordings/shell"
    )
    assert shell_resp.status_code == 200
    shell_body = shell_resp.get_json()
    assert shell_body["recording_count"] == 3

    # ---- 3. batch endpoint with the IDs from shell ----
    shell_ids = [r["id"] for r in shell_body["recordings"]]
    batch_resp = client.get(
        f"/v1/recordings/batch?ids={','.join(shell_ids)}"
    )
    assert batch_resp.status_code == 200
    batch_body = batch_resp.get_json()
    assert len(batch_body["recordings"]) == 3

    # Row count + ID set agreement across all three endpoints.
    list_ids = {r["id"] for r in list_body["recordings"]}
    shell_id_set = set(shell_ids)
    batch_ids = {r["id"] for r in batch_body["recordings"]}
    assert list_ids == shell_id_set == batch_ids, (
        f"id-set drift: list={list_ids}, shell={shell_id_set}, batch={batch_ids}"
    )

    # ---- 4. batch per-row shape equals list per-row shape ----
    # This is the decoder-compatibility guarantee: the Swift Recording
    # decoder handles both endpoints because they return the same keys.
    list_keys = set(list_body["recordings"][0].keys())
    for rec in batch_body["recordings"]:
        assert set(rec.keys()) == list_keys, (
            f"batch row key set differs from list row key set — Swift "
            f"decoder will disagree across endpoints.\n"
            f"  batch only:  {sorted(set(rec.keys()) - list_keys)}\n"
            f"  list only:   {sorted(list_keys - set(rec.keys()))}"
        )

    # ---- 5. cross-endpoint value checks ----
    # Build lookup dicts so we can index by recording id across all three.
    list_by_id = {r["id"]: r for r in list_body["recordings"]}
    shell_by_id = {r["id"]: r for r in shell_body["recordings"]}
    batch_by_id = {r["id"]: r for r in batch_body["recordings"]}

    for rid in list_ids:
        lst = list_by_id[rid]
        sh = shell_by_id[rid]
        bt = batch_by_id[rid]

        # Fields that are literally the same column across all three:
        for field in ("recording_year", "title", "album_title",
                      "artist_credit", "is_canonical",
                      "has_streaming", "has_spotify", "has_apple_music",
                      "has_youtube", "authority_count"):
            assert lst[field] == sh[field] == bt[field], (
                f"{field} differs for row {rid}: "
                f"list={lst[field]!r}, shell={sh[field]!r}, batch={bt[field]!r}"
            )

        # Leader identity: shell's performers is [leader], list+batch's
        # performers includes leader + sidemen. Compare the leader entry.
        def leader_of(performers_list):
            if not performers_list:
                return None
            for p in performers_list:
                if p.get("role") == "leader":
                    return p
            return None

        shell_leader = leader_of(sh["performers"])
        list_leader = leader_of(lst["performers"])
        batch_leader = leader_of(bt["performers"])
        if shell_leader is None:
            assert list_leader is None and batch_leader is None, (
                f"shell has no leader but list/batch do for row {rid}"
            )
        else:
            assert (
                shell_leader["name"]
                == list_leader["name"]
                == batch_leader["name"]
            ), f"leader name disagrees for row {rid}"

        # Vocal-consensus: shell's top-level is_instrumental == the value
        # buried inside list/batch's community_data.consensus.is_instrumental.
        community = lst.get("community_data") or {}
        consensus = community.get("consensus") if community else None
        list_instrumental = (
            consensus.get("is_instrumental") if consensus else None
        )
        assert sh["is_instrumental"] == list_instrumental, (
            f"is_instrumental disagrees for row {rid}: "
            f"shell={sh['is_instrumental']!r}, list={list_instrumental!r}"
        )

        # instruments_present: shell's flat array should contain every
        # instrument appearing in list's full performers[]. This is how
        # the iOS instrument filter keeps working on pre-hydration rows.
        list_instruments = sorted({
            p["instrument"]
            for p in (lst.get("performers") or [])
            if p.get("instrument")
        })
        shell_instruments = sorted(sh["instruments_present"])
        assert list_instruments == shell_instruments, (
            f"instruments disagree for row {rid}: "
            f"list={list_instruments}, shell={shell_instruments}"
        )
