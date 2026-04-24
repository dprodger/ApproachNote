"""
Tests for core.song_research — the in-process orchestrator that runs
MusicBrainz import and fans out durable-queue jobs for Spotify / Apple
Music / YouTube matching.

The MB import and the song-metadata helpers are mocked; the enqueue
step hits the real research_jobs table so we verify the rows the worker
will actually see. Songs and recordings are inserted with deterministic
fixture UUIDs and cleaned up around each test.
"""

from __future__ import annotations

import logging

import pytest

from core import research_jobs, song_research


# ---------------------------------------------------------------------------
# Fixture UUIDs — deterministic so cleanup is precise.
# ---------------------------------------------------------------------------

_NS = "00000000-0000-4000-8000-00001000{:04x}"

SONG_ID = _NS.format(0x0001)
RECORDING_1_ID = _NS.format(0x0010)
RECORDING_2_ID = _NS.format(0x0011)


def _cleanup(conn):
    """Delete fixture rows. Safe to call before and after a test."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM research_jobs WHERE target_id IN (%s, %s, %s)",
            (SONG_ID, RECORDING_1_ID, RECORDING_2_ID),
        )
        cur.execute(
            "DELETE FROM recordings WHERE id IN (%s, %s)",
            (RECORDING_1_ID, RECORDING_2_ID),
        )
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    conn.commit()


@pytest.fixture
def song_with_recordings(db):
    """One song with two recordings. Yields (song_id, [recording_ids])."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Test Orchestration Song"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title) VALUES (%s, %s, %s)",
            (RECORDING_1_ID, SONG_ID, "Rec 1"),
        )
        cur.execute(
            "INSERT INTO recordings (id, song_id, title) VALUES (%s, %s, %s)",
            (RECORDING_2_ID, SONG_ID, "Rec 2"),
        )
    db.commit()

    yield SONG_ID, [RECORDING_1_ID, RECORDING_2_ID]

    _cleanup(db)


@pytest.fixture
def song_without_recordings(db):
    """A song with zero recordings — exercises the empty-fan-out path."""
    _cleanup(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO songs (id, title) VALUES (%s, %s)",
            (SONG_ID, "Lonely Song"),
        )
    db.commit()
    yield SONG_ID
    _cleanup(db)


def _jobs_for_target(db, target_id):
    """All research_jobs rows for a target, newest first, as dicts."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT source, job_type, target_type, target_id, "
            "       payload, priority, status "
            "FROM research_jobs WHERE target_id = %s "
            "ORDER BY id",
            (target_id,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# _enqueue_downstream_jobs — direct unit tests
# ---------------------------------------------------------------------------

class TestEnqueueDownstreamJobs:
    def test_enqueues_spotify_and_apple_at_priority_50(
        self, db, song_with_recordings,
    ):
        song_id, _ = song_with_recordings

        song_research._enqueue_downstream_jobs(song_id, force_refresh=True)

        rows = _jobs_for_target(db, song_id)
        by_source = {r['source']: r for r in rows}

        assert set(by_source.keys()) == {'spotify', 'apple'}
        for r in rows:
            assert r['job_type'] == 'match_song'
            assert r['target_type'] == 'song'
            assert r['priority'] == 50
            assert r['status'] == 'queued'

    def test_enqueues_youtube_job_per_recording(
        self, db, song_with_recordings,
    ):
        song_id, recording_ids = song_with_recordings

        song_research._enqueue_downstream_jobs(song_id, force_refresh=False)

        # One youtube job per recording, targeting the recording id.
        for rec_id in recording_ids:
            rows = _jobs_for_target(db, rec_id)
            yt_rows = [r for r in rows if r['source'] == 'youtube']
            assert len(yt_rows) == 1
            assert yt_rows[0]['job_type'] == 'match_recording'
            assert yt_rows[0]['target_type'] == 'recording'
            assert yt_rows[0]['priority'] == 50

    def test_rematch_payload_reflects_force_refresh(
        self, db, song_with_recordings,
    ):
        song_id, recording_ids = song_with_recordings

        song_research._enqueue_downstream_jobs(song_id, force_refresh=True)

        song_rows = _jobs_for_target(db, song_id)
        for r in song_rows:
            assert r['payload'] == {'rematch': True}

        rec_rows = _jobs_for_target(db, recording_ids[0])
        assert rec_rows[0]['payload'] == {'rematch': True}

    def test_force_refresh_false_sets_rematch_false(
        self, db, song_with_recordings,
    ):
        song_id, _ = song_with_recordings

        song_research._enqueue_downstream_jobs(song_id, force_refresh=False)

        for r in _jobs_for_target(db, song_id):
            assert r['payload'] == {'rematch': False}

    def test_no_recordings_means_no_youtube_jobs(
        self, db, song_without_recordings,
    ):
        song_id = song_without_recordings

        song_research._enqueue_downstream_jobs(song_id, force_refresh=True)

        # Spotify + Apple still queued; no YouTube rows anywhere.
        rows = _jobs_for_target(db, song_id)
        assert {r['source'] for r in rows} == {'spotify', 'apple'}

    def test_individual_enqueue_failure_does_not_abort_others(
        self, db, song_with_recordings, mocker,
    ):
        # When the Spotify enqueue raises, Apple + YouTube must still go
        # through. The orchestrator wraps each call in its own try so a
        # transient failure on one source doesn't starve the others.
        song_id, recording_ids = song_with_recordings

        real_enqueue = research_jobs.enqueue

        def flaky_enqueue(**kwargs):
            if kwargs.get('source') == research_jobs.SOURCE_SPOTIFY:
                raise RuntimeError('spotify enqueue blew up')
            return real_enqueue(**kwargs)

        mocker.patch.object(research_jobs, 'enqueue', side_effect=flaky_enqueue)
        # song_research imports enqueue through the research_jobs module,
        # so the patch above takes effect for its callsites.

        song_research._enqueue_downstream_jobs(song_id, force_refresh=True)

        song_rows = _jobs_for_target(db, song_id)
        sources = {r['source'] for r in song_rows}
        assert 'apple' in sources
        assert 'spotify' not in sources

        rec_rows = _jobs_for_target(db, recording_ids[0])
        assert any(r['source'] == 'youtube' for r in rec_rows)


# ---------------------------------------------------------------------------
# research_song — full orchestrator with MB import mocked
# ---------------------------------------------------------------------------

def _mock_mb_importer(mocker, *, success=True, stats=None, error=None):
    """Patch MBReleaseImporter so the constructor returns a mock whose
    import_releases() yields the canned result."""
    importer = mocker.MagicMock()
    result = {
        'success': success,
        'stats': stats or {
            'recordings_found': 2,
            'recordings_created': 2,
            'releases_created': 1,
            'releases_existing': 0,
            'performers_linked': 3,
            'errors': 0,
        },
    }
    if not success and error:
        result['error'] = error
    importer.import_releases.return_value = result

    mocker.patch('core.song_research.MBReleaseImporter', return_value=importer)
    # Metadata updaters are irrelevant to what we're testing; stub to no-op.
    mocker.patch('core.song_research.update_song_composer', return_value=False)
    mocker.patch('core.song_research.update_song_wikipedia_url', return_value=False)
    mocker.patch('core.song_research.update_song_composed_year', return_value=False)
    return importer


class TestResearchSong:
    def test_success_path_enqueues_downstream_jobs(
        self, db, song_with_recordings, mocker,
    ):
        song_id, recording_ids = song_with_recordings
        _mock_mb_importer(mocker, success=True)

        out = song_research.research_song(
            song_id, "Test Song", force_refresh=True,
        )

        assert out['success'] is True
        assert out['song_id'] == song_id
        assert 'musicbrainz' in out['stats']

        # Spotify + Apple jobs on the song, plus YouTube per recording.
        song_rows = _jobs_for_target(db, song_id)
        assert {r['source'] for r in song_rows} == {'spotify', 'apple'}

        for rec_id in recording_ids:
            rec_rows = _jobs_for_target(db, rec_id)
            assert any(r['source'] == 'youtube' for r in rec_rows)

    def test_mb_failure_returns_error_and_skips_enqueue(
        self, db, song_with_recordings, mocker,
    ):
        song_id, recording_ids = song_with_recordings
        _mock_mb_importer(mocker, success=False, error='MB down')

        out = song_research.research_song(
            song_id, "Test Song", force_refresh=True,
        )

        assert out['success'] is False
        assert 'MB down' in out['error']

        # Nothing enqueued anywhere.
        for tid in (song_id, *recording_ids):
            assert _jobs_for_target(db, tid) == []

    def test_importer_raising_is_caught_and_reported(
        self, db, song_with_recordings, mocker,
    ):
        # A crashing MBReleaseImporter must not propagate to the caller
        # (the research_queue worker thread would die on unhandled
        # exceptions). research_song swallows and surfaces as error dict.
        song_id, recording_ids = song_with_recordings

        importer = mocker.MagicMock()
        importer.import_releases.side_effect = RuntimeError('boom')
        mocker.patch('core.song_research.MBReleaseImporter', return_value=importer)
        mocker.patch('core.song_research.update_song_composer', return_value=False)
        mocker.patch('core.song_research.update_song_wikipedia_url', return_value=False)
        mocker.patch('core.song_research.update_song_composed_year', return_value=False)

        out = song_research.research_song(
            song_id, "Test Song", force_refresh=True,
        )

        assert out['success'] is False
        assert 'boom' in out['error']

        # No downstream jobs should have been enqueued.
        for tid in (song_id, *recording_ids):
            assert _jobs_for_target(db, tid) == []

    def test_respects_force_refresh_in_downstream_payload(
        self, db, song_with_recordings, mocker,
    ):
        song_id, _ = song_with_recordings
        _mock_mb_importer(mocker, success=True)

        song_research.research_song(
            song_id, "Test Song", force_refresh=False,
        )

        for r in _jobs_for_target(db, song_id):
            assert r['payload'] == {'rematch': False}
