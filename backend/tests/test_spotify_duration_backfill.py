"""
Tests for the Spotify duration backfill (issue #100):

  - research_worker/handlers/spotify.py::backfill_durations  — handler
  - core/spotify_duration_backfill.py                        — sweep enqueuer

Both touch real DB tables (recording_release_streaming_links,
recording_releases, recordings, songs) so we use deterministic fixture
UUIDs and explicit row-level cleanup, mirroring the pattern in
test_song_recordings.py.

The handler's outbound API call is mocked — we never hit Spotify.
"""

from __future__ import annotations

import logging
import uuid
from types import SimpleNamespace

import pytest

from core import spotify_duration_backfill, research_jobs
from db_utils import get_db_connection
from research_worker.errors import RetryableError
from research_worker.handlers import spotify as handler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Deterministic UUIDs in the v4-shape "fixture" range so cleanup can target
# them precisely without colliding with prod data.
_NS = "00000000-0000-4000-8000-1000000{:05x}"

SONG_A = _NS.format(0x00001)
SONG_B = _NS.format(0x00002)
RECORDING_A1 = _NS.format(0x00010)
RECORDING_A2 = _NS.format(0x00011)
RECORDING_A3 = _NS.format(0x00012)
RECORDING_B1 = _NS.format(0x00020)
RELEASE_A = _NS.format(0x00040)
RELEASE_B = _NS.format(0x00041)
RR_A1 = _NS.format(0x00050)
RR_A2 = _NS.format(0x00051)
RR_A3 = _NS.format(0x00052)
RR_B1 = _NS.format(0x00053)
LINK_A1_NULL = _NS.format(0x00070)   # song A, recording 1, duration NULL
LINK_A2_NULL = _NS.format(0x00071)   # song A, recording 2, duration NULL
LINK_A3_FILLED = _NS.format(0x00072)  # song A, recording 3, duration set
LINK_B1_NULL = _NS.format(0x00073)   # song B, recording 1, duration NULL


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM recording_release_streaming_links "
            "WHERE id IN (%s, %s, %s, %s)",
            (LINK_A1_NULL, LINK_A2_NULL, LINK_A3_FILLED, LINK_B1_NULL),
        )
        cur.execute(
            "DELETE FROM recording_releases WHERE id IN (%s, %s, %s, %s)",
            (RR_A1, RR_A2, RR_A3, RR_B1),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s, %s, %s)",
            (RECORDING_A1, RECORDING_A2, RECORDING_A3, RECORDING_B1),
        )
        cur.execute(
            "DELETE FROM releases WHERE id IN (%s, %s)",
            (RELEASE_A, RELEASE_B),
        )
        cur.execute(
            "DELETE FROM songs WHERE id IN (%s, %s)",
            (SONG_A, SONG_B),
        )
    conn.commit()


@pytest.fixture
def backfill_fixture(db):
    """Two songs.

      Song A — three recordings:
        * Recording A1 has one Spotify link with duration_ms NULL
          (LINK_A1_NULL).
        * Recording A2 has one Spotify link with duration_ms NULL
          (LINK_A2_NULL).
        * Recording A3 has one Spotify link with duration_ms already
          populated (LINK_A3_FILLED). The handler must not touch this row.

      Song B — one recording, one Spotify link with duration_ms NULL
        (LINK_B1_NULL).

    Note: the schema has UNIQUE(recording_release_id, service), so we
    can't put two Spotify links on the same recording_release — hence
    distinct recordings (and recording_releases) for each link.

    The shape exercises:
      - sweep candidate detection (A and B both qualify)
      - per-song handler scoping (a Song A run must NOT touch LINK_B1_NULL)
      - the "already populated" case (LINK_A3_FILLED stays untouched)
      - multi-row update (the same song's two NULL links get filled).
    """
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute("INSERT INTO songs (id, title) VALUES (%s, %s)",
                    (SONG_A, "Backfill Test Song A"))
        cur.execute("INSERT INTO songs (id, title) VALUES (%s, %s)",
                    (SONG_B, "Backfill Test Song B"))

        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) "
            "VALUES (%s, %s, %s)",
            (RELEASE_A, "Backfill Album A", "Artist A"),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit) "
            "VALUES (%s, %s, %s)",
            (RELEASE_B, "Backfill Album B", "Artist B"),
        )

        for rec_id, song_id in [
            (RECORDING_A1, SONG_A),
            (RECORDING_A2, SONG_A),
            (RECORDING_A3, SONG_A),
            (RECORDING_B1, SONG_B),
        ]:
            cur.execute(
                "INSERT INTO recordings (id, song_id, title) "
                "VALUES (%s, %s, %s)",
                (rec_id, song_id, "Backfill Test Recording"),
            )

        for rr_id, rec_id, rel_id in [
            (RR_A1, RECORDING_A1, RELEASE_A),
            (RR_A2, RECORDING_A2, RELEASE_A),
            (RR_A3, RECORDING_A3, RELEASE_A),
            (RR_B1, RECORDING_B1, RELEASE_B),
        ]:
            cur.execute(
                "INSERT INTO recording_releases "
                "(id, recording_id, release_id) VALUES (%s, %s, %s)",
                (rr_id, rec_id, rel_id),
            )

        # Streaming links. service_id is the Spotify track ID we'd query.
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms) "
            "VALUES (%s, %s, 'spotify', %s, %s, NULL)",
            (LINK_A1_NULL, RR_A1, 'spotify-a1', 'http://example.test/a1'),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms) "
            "VALUES (%s, %s, 'spotify', %s, %s, NULL)",
            (LINK_A2_NULL, RR_A2, 'spotify-a2', 'http://example.test/a2'),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms) "
            "VALUES (%s, %s, 'spotify', %s, %s, %s)",
            (LINK_A3_FILLED, RR_A3, 'spotify-a3-filled',
             'http://example.test/a3f', 240000),
        )
        cur.execute(
            "INSERT INTO recording_release_streaming_links "
            "(id, recording_release_id, service, service_id, service_url, "
            " duration_ms) "
            "VALUES (%s, %s, 'spotify', %s, %s, NULL)",
            (LINK_B1_NULL, RR_B1, 'spotify-b1', 'http://example.test/b1'),
        )

    db.commit()
    yield
    _cleanup(db)


def _link_duration(conn, link_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT duration_ms FROM recording_release_streaming_links "
            "WHERE id = %s",
            (link_id,),
        )
        row = cur.fetchone()
    return row['duration_ms'] if row else None


# ---------------------------------------------------------------------------
# Handler — backfill_durations
# ---------------------------------------------------------------------------

class FakeCtx:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'spotify'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_spotify_client(mocker):
    """Mock SpotifyClient so the handler doesn't hit the network."""
    instance = mocker.MagicMock()
    instance.get_tracks_batch = mocker.MagicMock()
    mocker.patch(
        'research_worker.handlers.spotify.SpotifyClient',
        return_value=instance,
    )
    return SimpleNamespace(instance=instance)


class TestHandlerSuccess:
    def test_fills_only_target_songs_null_links(
        self, backfill_fixture, db, patched_spotify_client,
    ):
        # Spotify returns durations for both of song A's NULL links.
        patched_spotify_client.instance.get_tracks_batch.return_value = {
            'spotify-a1': {'duration_ms': 200000},
            'spotify-a2': {'duration_ms': 210000},
        }

        result = handler.backfill_durations({}, FakeCtx(SONG_A))

        assert result['links_updated'] == 2
        assert result['links_found'] == 2
        assert result['batches'] == 1
        assert result['batches_failed'] == 0
        assert result['tracks_not_found'] == 0
        assert result['tracks_no_duration'] == 0

        with get_db_connection() as conn:
            assert _link_duration(conn, LINK_A1_NULL) == 200000
            assert _link_duration(conn, LINK_A2_NULL) == 210000
            # Already-filled row for song A: unchanged.
            assert _link_duration(conn, LINK_A3_FILLED) == 240000
            # Song B's link: untouched, even though it's NULL — it belongs
            # to a different song.
            assert _link_duration(conn, LINK_B1_NULL) is None

    def test_no_candidates_for_song_returns_clean_no_op(
        self, backfill_fixture, patched_spotify_client,
    ):
        # Song A's links all populated by hand → no work.
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE recording_release_streaming_links "
                    "SET duration_ms = 100000 "
                    "WHERE id IN (%s, %s)",
                    (LINK_A1_NULL, LINK_A2_NULL),
                )
            conn.commit()

        result = handler.backfill_durations({}, FakeCtx(SONG_A))

        assert result['links_updated'] == 0
        assert result['links_found'] == 0
        assert result['reason'] == 'no_candidates'
        # Spotify must not be called when there's nothing to do.
        patched_spotify_client.instance.get_tracks_batch.assert_not_called()

    def test_track_missing_from_spotify_response_is_counted(
        self, backfill_fixture, db, patched_spotify_client,
    ):
        # Spotify only returns one of the two requested tracks.
        patched_spotify_client.instance.get_tracks_batch.return_value = {
            'spotify-a1': {'duration_ms': 200000},
            # 'spotify-a2' deliberately missing
        }

        result = handler.backfill_durations({}, FakeCtx(SONG_A))

        assert result['links_updated'] == 1
        assert result['tracks_not_found'] == 1

        with get_db_connection() as conn:
            assert _link_duration(conn, LINK_A1_NULL) == 200000
            assert _link_duration(conn, LINK_A2_NULL) is None

    def test_track_with_no_duration_is_counted(
        self, backfill_fixture, db, patched_spotify_client,
    ):
        # Realistic shape: Spotify returns the track entry (so "not found"
        # is not the right bucket) but its duration_ms is null/0.
        patched_spotify_client.instance.get_tracks_batch.return_value = {
            'spotify-a1': {'duration_ms': 200000},
            'spotify-a2': {'name': 'A Local Track', 'duration_ms': None},
        }

        result = handler.backfill_durations({}, FakeCtx(SONG_A))

        assert result['links_updated'] == 1
        assert result['tracks_no_duration'] == 1


class TestHandlerErrorPaths:
    def test_total_batch_failure_raises_retryable(
        self, backfill_fixture, patched_spotify_client,
    ):
        # SpotifyClient.get_tracks_batch returns None on rate-limit /
        # network failure (after its own internal retries).
        patched_spotify_client.instance.get_tracks_batch.return_value = None

        with pytest.raises(RetryableError):
            handler.backfill_durations({}, FakeCtx(SONG_A))

    def test_partial_batch_failure_does_not_raise(
        self, backfill_fixture, db, mocker, patched_spotify_client,
    ):
        # Force two batches by monkey-patching the batch size down.
        mocker.patch(
            'research_worker.handlers.spotify._BATCH_SIZE', 1,
        )
        # First batch fails (None), second succeeds.
        patched_spotify_client.instance.get_tracks_batch.side_effect = [
            None,
            {'spotify-a2': {'duration_ms': 210000}},
        ]

        result = handler.backfill_durations({}, FakeCtx(SONG_A))

        # Partial success must not raise — the work that completed should
        # stick. Re-running the sweep later picks up whatever's still NULL.
        assert result['batches_failed'] == 1
        assert result['batches'] == 1
        assert result['links_updated'] == 1


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_find_candidate_song_ids_returns_distinct_songs(
        self, backfill_fixture,
    ):
        candidates = spotify_duration_backfill.find_candidate_song_ids()
        # Song A and Song B both have at least one NULL Spotify link.
        # (Other songs in the DB outside the fixture might also qualify,
        # but ours must appear.)
        assert SONG_A in candidates
        assert SONG_B in candidates
        # Distinctness: SONG_A appears once even though it owns two NULL
        # links (LINK_A1_NULL + LINK_A2_NULL).
        assert candidates.count(SONG_A) == 1

    def test_enqueue_sweep_creates_one_job_per_candidate(
        self, backfill_fixture, db,
    ):
        # The `db` fixture is a plain psycopg connection — cursor returns
        # tuples, not dict rows. Use positional access.

        # Snapshot the queue size for our two songs before the sweep.
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'spotify' AND job_type = 'backfill_durations' "
                "AND target_id IN (%s, %s)",
                (SONG_A, SONG_B),
            )
            before = cur.fetchone()[0]

        result = spotify_duration_backfill.enqueue_sweep()
        assert result['candidates'] >= 2
        assert result['enqueued'] >= 2
        assert result['errors'] == 0

        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id FROM research_jobs "
                "WHERE source = 'spotify' AND job_type = 'backfill_durations' "
                "AND target_id IN (%s, %s)",
                (SONG_A, SONG_B),
            )
            target_ids = {str(row[0]) for row in cur.fetchall()}

        assert SONG_A in target_ids
        assert SONG_B in target_ids
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'spotify' AND job_type = 'backfill_durations' "
                "AND target_id IN (%s, %s)",
                (SONG_A, SONG_B),
            )
            after = cur.fetchone()[0]
        assert after - before == 2

    def test_enqueue_sweep_is_idempotent(self, backfill_fixture, db):
        # Two consecutive sweeps must not double the row count for the
        # same songs — research_jobs' unique index on
        # (source, job_type, target_type, target_id) collapses dupes.
        spotify_duration_backfill.enqueue_sweep()
        spotify_duration_backfill.enqueue_sweep()

        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE source = 'spotify' AND job_type = 'backfill_durations' "
                "AND target_id IN (%s, %s) "
                "GROUP BY target_id",
                (SONG_A, SONG_B),
            )
            counts = {str(row[0]): row[1] for row in cur.fetchall()}

        assert counts.get(SONG_A) == 1
        assert counts.get(SONG_B) == 1

    def test_enqueue_sweep_respects_limit(self, backfill_fixture):
        result = spotify_duration_backfill.enqueue_sweep(limit=1)
        # At most one candidate even though the fixture has two qualifying
        # songs (and the wider DB may have more).
        assert result['candidates'] == 1
        assert result['enqueued'] == 1

    def test_no_candidates_returns_zero(self, db, mocker):
        # With no fixture loaded, force the candidate query to return
        # empty by mocking — keeps the test independent of whatever stale
        # rrsl rows the dev DB may carry.
        mocker.patch(
            'core.spotify_duration_backfill.find_candidate_song_ids',
            return_value=[],
        )
        result = spotify_duration_backfill.enqueue_sweep()
        assert result == {'candidates': 0, 'enqueued': 0, 'errors': 0}
