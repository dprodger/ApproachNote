"""
Regression tests for repertoire song serialization.

issue #197 follow-up: the repertoire endpoints select an explicit column
list, and `composed_year` had been left out. The iOS app decodes the same
`Song` model from `/songs/index` and the repertoire endpoints, so the
omission surfaced as a blank year on repertoire-filtered songs only. These
tests pin `composed_year` into all three repertoire SELECT paths:
  - GET /v1/repertoires/<id>/songs            (non-search)
  - GET /v1/repertoires/<id>/songs?search=... (search)
  - GET /v1/repertoires/<id>                  (detail, embedded songs)
"""

import uuid

import pytest


@pytest.fixture
def repertoire_with_song(db, auth_headers):
    """Create a repertoire owned by the auth_headers user and link a song with
    a known `composed_year`. Returns (repertoire_id, song_id, year).

    The autouse `_clean_auth_tables` fixture TRUNCATEs users CASCADE, which
    cleans up repertoires + repertoire_songs (both FK to users via the
    repertoire). `songs` isn't auth-related, so delete the song explicitly.
    """
    user_id = auth_headers.user["id"]
    repertoire_id = str(uuid.uuid4())
    song_id = str(uuid.uuid4())
    year = 1959

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title, composer, composed_year) "
            "VALUES (%s, %s, %s, %s)",
            (song_id, "So What", "Miles Davis", year),
        )
        cur.execute(
            "INSERT INTO repertoires (id, user_id, name) VALUES (%s, %s, %s)",
            (repertoire_id, user_id, "Test Repertoire"),
        )
        cur.execute(
            "INSERT INTO repertoire_songs (repertoire_id, song_id) "
            "VALUES (%s, %s)",
            (repertoire_id, song_id),
        )
    db.commit()

    yield repertoire_id, song_id, year

    with db.cursor() as cur:
        cur.execute("DELETE FROM songs WHERE id = %s", (song_id,))
    db.commit()


def test_repertoire_songs_includes_composed_year(client, auth_headers, repertoire_with_song):
    repertoire_id, song_id, year = repertoire_with_song

    resp = client.get(f"/v1/repertoires/{repertoire_id}/songs", headers=auth_headers)

    assert resp.status_code == 200, resp.get_json()
    songs = resp.get_json()
    assert len(songs) == 1
    assert songs[0]["id"] == song_id
    assert songs[0]["composed_year"] == year


def test_repertoire_songs_search_includes_composed_year(client, auth_headers, repertoire_with_song):
    repertoire_id, _song_id, year = repertoire_with_song

    resp = client.get(
        f"/v1/repertoires/{repertoire_id}/songs?search=So",
        headers=auth_headers,
    )

    assert resp.status_code == 200, resp.get_json()
    songs = resp.get_json()
    assert len(songs) == 1
    assert songs[0]["composed_year"] == year


def test_repertoire_detail_includes_composed_year(client, auth_headers, repertoire_with_song):
    repertoire_id, _song_id, year = repertoire_with_song

    resp = client.get(f"/v1/repertoires/{repertoire_id}", headers=auth_headers)

    assert resp.status_code == 200, resp.get_json()
    songs = resp.get_json()["songs"]
    assert len(songs) == 1
    assert songs[0]["composed_year"] == year
