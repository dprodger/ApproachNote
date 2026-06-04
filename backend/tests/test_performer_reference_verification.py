"""
Tests for the performer reference-verification backfill:

  - research_worker/handlers/musicbrainz.py::verify_performer_references — handler
  - core/performer_reference_verification.py                            — sweep enqueuer

Both touch real DB tables (performers, research_jobs) so we use
deterministic fixture UUIDs and explicit row-level cleanup, mirroring
test_release_label_backfill.py.

The Wikipedia and MusicBrainz client classes are mocked — we never hit the
network.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from core import performer_reference_verification as sweep_mod
from db_utils import get_db_connection
from research_worker.errors import PermanentError
from research_worker.handlers import musicbrainz as handler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Deterministic UUIDs in a "fixture" range so cleanup targets them precisely.
_NS = "00000000-0000-4000-8000-3000000{:05x}"

PERFORMER_MISSING_BOTH = _NS.format(0x00001)   # wiki NULL, mb NULL    (candidate)
PERFORMER_MISSING_WIKI = _NS.format(0x00002)   # wiki NULL, mb set     (candidate)
PERFORMER_MISSING_MB = _NS.format(0x00003)     # wiki set, mb NULL     (candidate)
PERFORMER_HAS_BOTH = _NS.format(0x00004)       # both set              (not candidate)
PERFORMER_WIKI_IN_LINKS = _NS.format(0x00005)  # wiki via external_links, mb set (not candidate)

_ALL_FIXTURE_IDS = (
    PERFORMER_MISSING_BOTH,
    PERFORMER_MISSING_WIKI,
    PERFORMER_MISSING_MB,
    PERFORMER_HAS_BOTH,
    PERFORMER_WIKI_IN_LINKS,
)

EXISTING_MBID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
EXISTING_WIKI = 'https://en.wikipedia.org/wiki/Existing'

# Song + recording used to exercise the song-scoped ingestion producer.
SONG_ID = _NS.format(0x0a001)
RECORDING_ID = _NS.format(0x0b001)


def _cleanup(conn):
    placeholders = ", ".join(["%s"] * len(_ALL_FIXTURE_IDS))
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM research_jobs WHERE target_id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
        cur.execute(
            f"DELETE FROM performers WHERE id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
    conn.commit()


@pytest.fixture
def perf_fixture(db):
    """Five performers exercising every candidate-eligibility case."""
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url, musicbrainz_id) "
            "VALUES (%s, %s, NULL, NULL)",
            (PERFORMER_MISSING_BOTH, "Missing Both"),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url, musicbrainz_id) "
            "VALUES (%s, %s, NULL, %s)",
            (PERFORMER_MISSING_WIKI, "Missing Wiki", EXISTING_MBID),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url, musicbrainz_id) "
            "VALUES (%s, %s, %s, NULL)",
            (PERFORMER_MISSING_MB, "Missing MB", EXISTING_WIKI),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url, musicbrainz_id) "
            "VALUES (%s, %s, %s, %s)",
            (PERFORMER_HAS_BOTH, "Has Both", EXISTING_WIKI, EXISTING_MBID),
        )
        cur.execute(
            "INSERT INTO performers "
            "(id, name, wikipedia_url, musicbrainz_id, external_links) "
            "VALUES (%s, %s, NULL, %s, %s::jsonb)",
            (
                PERFORMER_WIKI_IN_LINKS, "Wiki In Links", EXISTING_MBID,
                json.dumps({'wikipedia': EXISTING_WIKI}),
            ),
        )

    db.commit()
    yield
    _cleanup(db)


def _performer_refs(conn, performer_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wikipedia_url, musicbrainz_id FROM performers WHERE id = %s",
            (performer_id,),
        )
        row = cur.fetchone()
    return (row['wikipedia_url'], row['musicbrainz_id']) if row else (None, None)


# ---------------------------------------------------------------------------
# Handler — verify_performer_references
# ---------------------------------------------------------------------------

class FakeCtx:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'musicbrainz'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_searchers(mocker):
    """Mock both searcher classes so the handler never hits the network."""
    wiki = mocker.MagicMock()
    wiki.search_wikipedia = mocker.MagicMock(return_value=None)
    mocker.patch(
        'research_worker.handlers.musicbrainz.WikipediaSearcher',
        return_value=wiki,
    )

    mb = mocker.MagicMock()
    mb.search_musicbrainz_artist = mocker.MagicMock(return_value=[])
    mb.verify_musicbrainz_reference = mocker.MagicMock(
        return_value={'valid': True},
    )
    mocker.patch(
        'research_worker.handlers.musicbrainz.MusicBrainzSearcher',
        return_value=mb,
    )
    return SimpleNamespace(wiki=wiki, mb=mb)


class TestHandlerSuccess:
    def test_adds_wikipedia_when_missing(
        self, perf_fixture, patched_searchers,
    ):
        patched_searchers.wiki.search_wikipedia.return_value = (
            'https://en.wikipedia.org/wiki/Found'
        )

        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_WIKI),
        )

        assert result['updated'] is True
        assert result['wikipedia_added'] == 'https://en.wikipedia.org/wiki/Found'
        assert result['musicbrainz_added'] is None

        with get_db_connection() as conn:
            wiki, mb = _performer_refs(conn, PERFORMER_MISSING_WIKI)
        assert wiki == 'https://en.wikipedia.org/wiki/Found'
        # Existing MB id is left untouched.
        assert mb == EXISTING_MBID
        # MB search is skipped when the MB ref already exists.
        patched_searchers.mb.search_musicbrainz_artist.assert_not_called()

    def test_adds_musicbrainz_when_missing(
        self, perf_fixture, patched_searchers,
    ):
        patched_searchers.mb.search_musicbrainz_artist.return_value = [
            {'id': 'dddddddd-dddd-4ddd-8ddd-dddddddddddd', 'name': 'Missing MB'},
        ]
        patched_searchers.mb.verify_musicbrainz_reference.return_value = {
            'valid': True,
        }

        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_MB),
        )

        assert result['updated'] is True
        assert result['musicbrainz_added'] == 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'
        assert result['wikipedia_added'] is None

        with get_db_connection() as conn:
            wiki, mb = _performer_refs(conn, PERFORMER_MISSING_MB)
        assert mb == 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'
        assert wiki == EXISTING_WIKI
        # Wikipedia search is skipped when the wiki ref already exists.
        patched_searchers.wiki.search_wikipedia.assert_not_called()

    def test_adds_both_when_missing_both(
        self, perf_fixture, patched_searchers,
    ):
        patched_searchers.wiki.search_wikipedia.return_value = (
            'https://en.wikipedia.org/wiki/Both'
        )
        patched_searchers.mb.search_musicbrainz_artist.return_value = [
            {'id': 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee', 'name': 'Missing Both'},
        ]

        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_BOTH),
        )

        assert result['updated'] is True
        assert result['wikipedia_added'] == 'https://en.wikipedia.org/wiki/Both'
        assert result['musicbrainz_added'] == 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'

        with get_db_connection() as conn:
            wiki, mb = _performer_refs(conn, PERFORMER_MISSING_BOTH)
        assert wiki == 'https://en.wikipedia.org/wiki/Both'
        assert mb == 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'

    def test_mb_name_mismatch_is_not_written(
        self, perf_fixture, patched_searchers,
    ):
        # Search returns an artist whose name doesn't match — no exact match,
        # so nothing is written.
        patched_searchers.mb.search_musicbrainz_artist.return_value = [
            {'id': 'ffffffff-ffff-4fff-8fff-ffffffffffff', 'name': 'Someone Else'},
        ]

        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_MB),
        )

        assert result['updated'] is False
        assert result['reason'] == 'no_refs_found'
        patched_searchers.mb.verify_musicbrainz_reference.assert_not_called()

    def test_mb_unverified_match_is_not_written(
        self, perf_fixture, patched_searchers,
    ):
        # Exact name match, but verification rejects it.
        patched_searchers.mb.search_musicbrainz_artist.return_value = [
            {'id': 'ffffffff-ffff-4fff-8fff-ffffffffffff', 'name': 'Missing MB'},
        ]
        patched_searchers.mb.verify_musicbrainz_reference.return_value = {
            'valid': False,
        }

        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_MB),
        )

        assert result['updated'] is False
        assert result['reason'] == 'no_refs_found'

    def test_no_refs_found_records_noop(
        self, perf_fixture, patched_searchers,
    ):
        # Both searches come up empty (the default mock behaviour).
        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_MISSING_BOTH),
        )
        assert result['updated'] is False
        assert result['reason'] == 'no_refs_found'

        with get_db_connection() as conn:
            wiki, mb = _performer_refs(conn, PERFORMER_MISSING_BOTH)
        assert wiki is None
        assert mb is None

    def test_already_populated_short_circuits_without_search(
        self, perf_fixture, patched_searchers,
    ):
        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_HAS_BOTH),
        )
        assert result['updated'] is False
        assert result['reason'] == 'already_populated'
        patched_searchers.wiki.search_wikipedia.assert_not_called()
        patched_searchers.mb.search_musicbrainz_artist.assert_not_called()

    def test_external_links_wikipedia_counts_as_present(
        self, perf_fixture, patched_searchers,
    ):
        # Wiki lives in external_links and MB id is set, so the idempotency
        # guard must treat this performer as already populated.
        result = handler.verify_performer_references(
            {}, FakeCtx(PERFORMER_WIKI_IN_LINKS),
        )
        assert result['updated'] is False
        assert result['reason'] == 'already_populated'
        patched_searchers.wiki.search_wikipedia.assert_not_called()

    def test_force_refresh_payload_is_passed_to_searchers(
        self, perf_fixture, patched_searchers,
    ):
        handler.verify_performer_references(
            {'force_refresh': True}, FakeCtx(PERFORMER_MISSING_BOTH),
        )
        from research_worker.handlers.musicbrainz import (
            MusicBrainzSearcher, WikipediaSearcher,
        )
        WikipediaSearcher.assert_called_once_with(
            cache_days=7, force_refresh=True,
        )
        MusicBrainzSearcher.assert_called_once_with(force_refresh=True)


class TestHandlerReftypeScope:
    def test_wikipedia_only_skips_musicbrainz_lookup(
        self, perf_fixture, patched_searchers,
    ):
        # Performer missing BOTH refs, but a wiki-only job must not touch MB.
        patched_searchers.wiki.search_wikipedia.return_value = (
            'https://en.wikipedia.org/wiki/WikiOnly'
        )

        result = handler.verify_performer_references(
            {'reftypes': ['wikipedia']}, FakeCtx(PERFORMER_MISSING_BOTH),
        )

        assert result['updated'] is True
        assert result['wikipedia_added'] == 'https://en.wikipedia.org/wiki/WikiOnly'
        assert result['musicbrainz_added'] is None
        patched_searchers.mb.search_musicbrainz_artist.assert_not_called()

        with get_db_connection() as conn:
            wiki, mb = _performer_refs(conn, PERFORMER_MISSING_BOTH)
        assert wiki == 'https://en.wikipedia.org/wiki/WikiOnly'
        assert mb is None

    def test_wikipedia_only_noops_when_wiki_present_even_if_mb_missing(
        self, perf_fixture, patched_searchers,
    ):
        # PERFORMER_MISSING_MB has a wiki ref but no MB. A wiki-only job has
        # nothing to do and must not run any search.
        result = handler.verify_performer_references(
            {'reftypes': ['wikipedia']}, FakeCtx(PERFORMER_MISSING_MB),
        )
        assert result['updated'] is False
        assert result['reason'] == 'already_populated'
        patched_searchers.wiki.search_wikipedia.assert_not_called()
        patched_searchers.mb.search_musicbrainz_artist.assert_not_called()

    def test_musicbrainz_only_skips_wikipedia_lookup(
        self, perf_fixture, patched_searchers,
    ):
        patched_searchers.mb.search_musicbrainz_artist.return_value = [
            {'id': 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee', 'name': 'Missing Both'},
        ]

        result = handler.verify_performer_references(
            {'reftypes': ['musicbrainz']}, FakeCtx(PERFORMER_MISSING_BOTH),
        )

        assert result['updated'] is True
        assert result['musicbrainz_added'] == 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'
        assert result['wikipedia_added'] is None
        patched_searchers.wiki.search_wikipedia.assert_not_called()


class TestHandlerErrorPaths:
    def test_missing_performer_row_raises_permanent(
        self, perf_fixture, patched_searchers,
    ):
        bogus_id = '00000000-0000-4000-8000-300000099999'
        with pytest.raises(PermanentError):
            handler.verify_performer_references({}, FakeCtx(bogus_id))
        patched_searchers.wiki.search_wikipedia.assert_not_called()
        patched_searchers.mb.search_musicbrainz_artist.assert_not_called()


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_find_candidate_ids_includes_only_eligible_rows(self, perf_fixture):
        candidates = sweep_mod.find_candidate_performer_ids()
        assert PERFORMER_MISSING_BOTH in candidates
        assert PERFORMER_MISSING_WIKI in candidates
        assert PERFORMER_MISSING_MB in candidates
        # Both refs present (directly or via external_links) -> not a candidate.
        assert PERFORMER_HAS_BOTH not in candidates
        assert PERFORMER_WIKI_IN_LINKS not in candidates

    def test_wikipedia_only_candidates_exclude_performers_with_wiki(
        self, perf_fixture,
    ):
        candidates = sweep_mod.find_candidate_performer_ids(
            reftypes=['wikipedia'],
        )
        # Missing wiki -> candidates.
        assert PERFORMER_MISSING_BOTH in candidates
        assert PERFORMER_MISSING_WIKI in candidates
        # Has a wiki ref (directly or via external_links) -> not a wiki-only
        # candidate, even though it's missing MB.
        assert PERFORMER_MISSING_MB not in candidates
        assert PERFORMER_HAS_BOTH not in candidates
        assert PERFORMER_WIKI_IN_LINKS not in candidates

    def test_wikipedia_only_sweep_sets_reftypes_payload(self, perf_fixture, db):
        sweep_mod.enqueue_sweep(reftypes=['wikipedia'])
        with db.cursor() as cur:
            cur.execute(
                "SELECT payload FROM research_jobs "
                "WHERE source='musicbrainz' "
                "AND job_type='verify_performer_references' "
                "AND target_id = %s",
                (PERFORMER_MISSING_BOTH,),
            )
            payload = cur.fetchone()[0]
        assert payload == {'reftypes': ['wikipedia']}

    def test_unknown_reftype_raises(self):
        with pytest.raises(ValueError):
            sweep_mod.find_candidate_performer_ids(reftypes=['spotify'])

    def test_enqueue_sweep_creates_one_job_per_candidate(self, perf_fixture, db):
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'verify_performer_references' "
                "AND target_id IN (%s, %s, %s)",
                (PERFORMER_MISSING_BOTH, PERFORMER_MISSING_WIKI,
                 PERFORMER_MISSING_MB),
            )
            before = cur.fetchone()[0]

        result = sweep_mod.enqueue_sweep()
        assert result['candidates'] >= 3
        assert result['enqueued'] >= 3

        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'verify_performer_references' "
                "AND target_id IN (%s, %s, %s)",
                (PERFORMER_MISSING_BOTH, PERFORMER_MISSING_WIKI,
                 PERFORMER_MISSING_MB),
            )
            after = cur.fetchone()[0]
        assert after - before == 3

    def test_enqueue_sweep_is_idempotent(self, perf_fixture, db):
        sweep_mod.enqueue_sweep()
        sweep_mod.enqueue_sweep()

        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'verify_performer_references' "
                "AND target_id IN (%s, %s, %s) "
                "GROUP BY target_id",
                (PERFORMER_MISSING_BOTH, PERFORMER_MISSING_WIKI,
                 PERFORMER_MISSING_MB),
            )
            counts = {str(row[0]): row[1] for row in cur.fetchall()}

        assert counts.get(PERFORMER_MISSING_BOTH) == 1
        assert counts.get(PERFORMER_MISSING_WIKI) == 1
        assert counts.get(PERFORMER_MISSING_MB) == 1

    def test_enqueue_sweep_respects_limit(self, perf_fixture):
        result = sweep_mod.enqueue_sweep(limit=1)
        assert result['candidates'] == 1
        assert result['enqueued'] == 1

    def test_no_candidates_returns_zero(self, mocker):
        mocker.patch(
            'core.performer_reference_verification.find_candidate_performer_ids',
            return_value=[],
        )
        result = sweep_mod.enqueue_sweep()
        assert result == {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    def test_second_sweep_reports_skipped_via_dedup(self, perf_fixture):
        first = sweep_mod.enqueue_sweep()
        second = sweep_mod.enqueue_sweep()

        assert first['enqueued'] >= 3
        assert second['candidates'] == first['candidates']
        assert second['enqueued'] == 0
        assert second['skipped'] == second['candidates']


# ---------------------------------------------------------------------------
# Song-scoped producer (ingestion seam)
# ---------------------------------------------------------------------------

@pytest.fixture
def song_fixture(perf_fixture, db):
    """A song with one recording crediting two of the fixture performers:
    PERFORMER_MISSING_WIKI (a wiki candidate) and PERFORMER_HAS_BOTH (not).

    PERFORMER_MISSING_BOTH is intentionally NOT linked to the song, so tests
    can prove the producer is scoped to the song rather than the catalogue.
    """
    with db.cursor() as cur:
        cur.execute("DELETE FROM recording_performers WHERE recording_id = %s",
                    (RECORDING_ID,))
        cur.execute("DELETE FROM recordings WHERE id = %s", (RECORDING_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))

        cur.execute("INSERT INTO songs (id, title) VALUES (%s, %s)",
                    (SONG_ID, "Scoped Test Song"))
        cur.execute(
            "INSERT INTO recordings (id, song_id, title) VALUES (%s, %s, %s)",
            (RECORDING_ID, SONG_ID, "Scoped Test Recording"),
        )
        for performer_id in (PERFORMER_MISSING_WIKI, PERFORMER_HAS_BOTH):
            cur.execute(
                "INSERT INTO recording_performers (recording_id, performer_id) "
                "VALUES (%s, %s)",
                (RECORDING_ID, performer_id),
            )
    db.commit()
    yield
    with db.cursor() as cur:
        cur.execute("DELETE FROM recording_performers WHERE recording_id = %s",
                    (RECORDING_ID,))
        cur.execute("DELETE FROM recordings WHERE id = %s", (RECORDING_ID,))
        cur.execute("DELETE FROM songs WHERE id = %s", (SONG_ID,))
    db.commit()


class TestEnqueueForSong:
    def test_finds_only_song_performers_missing_wiki(self, song_fixture):
        ids = sweep_mod.find_song_performer_ids(SONG_ID, reftypes=['wikipedia'])
        # Linked to the song and missing wiki -> candidate.
        assert PERFORMER_MISSING_WIKI in ids
        # Linked but already has wiki -> excluded.
        assert PERFORMER_HAS_BOTH not in ids
        # A wiki candidate, but not credited on this song -> out of scope.
        assert PERFORMER_MISSING_BOTH not in ids

    def test_enqueues_one_wiki_job_for_song(self, song_fixture, db):
        result = sweep_mod.enqueue_for_song(SONG_ID, reftypes=['wikipedia'])
        assert result['candidates'] == 1
        assert result['enqueued'] == 1

        with db.cursor() as cur:
            cur.execute(
                "SELECT payload FROM research_jobs "
                "WHERE source='musicbrainz' "
                "AND job_type='verify_performer_references' "
                "AND target_id = %s",
                (PERFORMER_MISSING_WIKI,),
            )
            payload = cur.fetchone()[0]
        assert payload == {'reftypes': ['wikipedia']}

        # The unlinked catalogue candidate must NOT have been enqueued.
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE job_type='verify_performer_references' "
                "AND target_id = %s",
                (PERFORMER_MISSING_BOTH,),
            )
            assert cur.fetchone()[0] == 0

    def test_is_idempotent_for_song(self, song_fixture):
        first = sweep_mod.enqueue_for_song(SONG_ID, reftypes=['wikipedia'])
        second = sweep_mod.enqueue_for_song(SONG_ID, reftypes=['wikipedia'])

        assert first['enqueued'] == 1
        assert second['enqueued'] == 0
        assert second['skipped'] == 1

    def test_no_song_performers_returns_zero(self, perf_fixture):
        # A song id with no recordings/performers yields nothing to enqueue.
        result = sweep_mod.enqueue_for_song(SONG_ID, reftypes=['wikipedia'])
        assert result == {'candidates': 0, 'enqueued': 0, 'skipped': 0}
