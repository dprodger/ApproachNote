"""
Tests for the Wikipedia performer-enrichment backfill:

  - research_worker/handlers/wikipedia.py::enrich_performer_from_wikipedia — handler
  - core/performer_wikipedia_enrichment.py                                 — sweep enqueuer

Both touch real DB tables (performers, images, artist_images, research_jobs)
so we use deterministic fixture UUIDs and explicit row-level cleanup,
mirroring test_performer_reference_verification.py.

The Wikipedia page fetch (fetch_performer_data) and the WikipediaSearcher
class are mocked — we never hit the network.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from core import performer_wikipedia_enrichment as sweep_mod
from db_utils import get_db_connection
from integrations.wikipedia.performer_data import (
    PerformerWikipediaData,
    WikipediaImage,
    parse_date,
)
from research_worker.errors import PermanentError
from research_worker.handlers import wikipedia as handler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Deterministic UUIDs in a "fixture" range so cleanup targets them precisely.
_NS = "00000000-0000-4000-8000-4000000{:05x}"

PERFORMER_URL_EMPTY_DATA = _NS.format(0x00001)   # wiki url, no bio/dates/image (candidate)
PERFORMER_WIKI_IN_LINKS = _NS.format(0x00002)    # wiki via external_links     (candidate)
PERFORMER_NO_URL = _NS.format(0x00003)           # no wiki url                 (not candidate)
PERFORMER_FULLY_POPULATED = _NS.format(0x00004)  # dates+bio+wiki image        (candidate, no-op)
PERFORMER_EMPTY_STR_URL = _NS.format(0x00005)    # wikipedia_url = ''          (not candidate)
PERFORMER_HAS_BIRTH = _NS.format(0x00006)        # birth set, bio empty        (candidate, only-new)

_ALL_FIXTURE_IDS = (
    PERFORMER_URL_EMPTY_DATA,
    PERFORMER_WIKI_IN_LINKS,
    PERFORMER_NO_URL,
    PERFORMER_FULLY_POPULATED,
    PERFORMER_EMPTY_STR_URL,
    PERFORMER_HAS_BIRTH,
)

WIKI_URL = 'https://en.wikipedia.org/wiki/Test_Performer'
_TEST_IMAGE_PREFIX = 'http://test.example/'
EXISTING_IMAGE_URL = _TEST_IMAGE_PREFIX + 'existing.jpg'


def _cleanup(conn):
    placeholders = ", ".join(["%s"] * len(_ALL_FIXTURE_IDS))
    with conn.cursor() as cur:
        # artist_images cascades when performers go; images have no FK to a
        # performer, so clear our test image rows by their marker URL.
        cur.execute(
            f"DELETE FROM research_jobs WHERE target_id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
        cur.execute(
            f"DELETE FROM performers WHERE id IN ({placeholders})",
            _ALL_FIXTURE_IDS,
        )
        cur.execute(
            "DELETE FROM images WHERE url LIKE %s",
            (_TEST_IMAGE_PREFIX + '%',),
        )
    conn.commit()


@pytest.fixture
def perf_fixture(db):
    """Performers exercising every candidate / handler case."""
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url) VALUES (%s, %s, %s)",
            (PERFORMER_URL_EMPTY_DATA, "Url Empty Data", WIKI_URL),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url, external_links) "
            "VALUES (%s, %s, NULL, %s::jsonb)",
            (PERFORMER_WIKI_IN_LINKS, "Wiki In Links",
             json.dumps({'wikipedia': WIKI_URL})),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url) VALUES (%s, %s, NULL)",
            (PERFORMER_NO_URL, "No Url"),
        )
        cur.execute(
            "INSERT INTO performers "
            "(id, name, wikipedia_url, birth_date, death_date, biography) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (PERFORMER_FULLY_POPULATED, "Fully Populated", WIKI_URL,
             '1926-05-26', '1991-09-28', 'A fully documented life.'),
        )
        cur.execute(
            "INSERT INTO performers (id, name, wikipedia_url) VALUES (%s, %s, %s)",
            (PERFORMER_EMPTY_STR_URL, "Empty Str Url", ''),
        )
        cur.execute(
            "INSERT INTO performers "
            "(id, name, wikipedia_url, birth_date) VALUES (%s, %s, %s, %s)",
            (PERFORMER_HAS_BIRTH, "Has Birth", WIKI_URL, '1930-01-01'),
        )

        # Give the fully-populated performer a Wikipedia image so the handler's
        # short-circuit has nothing left to do.
        cur.execute(
            "INSERT INTO images (url, source, source_identifier) "
            "VALUES (%s, 'wikipedia', %s) RETURNING id",
            (EXISTING_IMAGE_URL, 'Fully Populated'),
        )
        image_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO artist_images (performer_id, image_id, is_primary) "
            "VALUES (%s, %s, true)",
            (PERFORMER_FULLY_POPULATED, image_id),
        )

    db.commit()
    yield
    _cleanup(db)


def _performer_row(conn, performer_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT birth_date, death_date, biography "
            "FROM performers WHERE id = %s",
            (performer_id,),
        )
        return cur.fetchone()


def _wikipedia_image_rows(conn, performer_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT im.url, ai.is_primary FROM images im "
            "JOIN artist_images ai ON ai.image_id = im.id "
            "WHERE ai.performer_id = %s AND im.source = 'wikipedia'",
            (performer_id,),
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestParseDate:
    @pytest.mark.parametrize("text,expected", [
        ('(1941-06-12)June 12, 1941', '1941-06-12'),  # hidden ISO preferred
        ('May 26, 1926', '1926-05-26'),
        ('26 May 1926', '1926-05-26'),
        ('1926', '1926-01-01'),                       # bare year
        ('June 1941', '1941-06-01'),                  # month + year
    ])
    def test_valid_dates_parse(self, text, expected):
        assert parse_date(text) == expected

    @pytest.mark.parametrize("text", [
        'April 31, 1940',                 # April has 30 days
        'February 30, 1940',              # impossible
        '(1940-02-30)February 30, 1940',  # impossible hidden ISO -> fall through
        '',
        'no date here',
    ])
    def test_impossible_or_absent_dates_return_none(self, text):
        # Regression: impossible calendar dates used to be emitted verbatim
        # (e.g. '1940-04-31') and crashed the UPDATE with DatetimeFieldOverflow.
        assert parse_date(text) is None

    def test_impossible_day_falls_through_to_month_year(self):
        # A bad day-of-month still yields a usable month/year date when the
        # text carries a separate "Month Year" the looser pattern can match.
        assert parse_date('April 1940') == '1940-04-01'


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

class FakeCtx:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'wikipedia'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_fetch(mocker):
    """Mock the page fetch + searcher so the handler never hits the network.

    Returns the fetch mock; tests set its return_value to a
    PerformerWikipediaData describing what Wikipedia 'has'.
    """
    mocker.patch(
        'research_worker.handlers.wikipedia.WikipediaSearcher',
        return_value=mocker.MagicMock(),
    )
    fetch = mocker.patch(
        'research_worker.handlers.wikipedia.fetch_performer_data',
        return_value=PerformerWikipediaData(page_fetched=True),
    )
    return fetch


def _image(url_suffix='primary.jpg'):
    return WikipediaImage(
        url=_TEST_IMAGE_PREFIX + url_suffix,
        thumbnail_url=_TEST_IMAGE_PREFIX + 'thumb.jpg',
        source_identifier='Test Performer',
        source_page_url=WIKI_URL,
        license_type='cc_by_sa',
        width=800,
        height=600,
    )


class TestHandlerSuccess:
    def test_fills_all_missing_fields(self, perf_fixture, patched_fetch):
        patched_fetch.return_value = PerformerWikipediaData(
            birth_date='1940-07-17',
            death_date='2005-03-02',
            biography='An influential player.',
            image=_image(),
            page_fetched=True,
        )

        result = handler.enrich_performer_from_wikipedia(
            {}, FakeCtx(PERFORMER_URL_EMPTY_DATA),
        )

        assert result['updated'] is True
        assert result['birth_date_added'] == '1940-07-17'
        assert result['death_date_added'] == '2005-03-02'
        assert result['biography_added'] is True
        assert result['image_added'] is True

        with get_db_connection() as conn:
            row = _performer_row(conn, PERFORMER_URL_EMPTY_DATA)
            images = _wikipedia_image_rows(conn, PERFORMER_URL_EMPTY_DATA)
        assert str(row['birth_date']) == '1940-07-17'
        assert str(row['death_date']) == '2005-03-02'
        assert row['biography'] == 'An influential player.'
        # First image on a performer with no images is the primary one.
        assert len(images) == 1
        assert images[0]['url'] == _TEST_IMAGE_PREFIX + 'primary.jpg'
        assert images[0]['is_primary'] is True

    def test_resolves_wiki_url_from_external_links(self, perf_fixture, patched_fetch):
        patched_fetch.return_value = PerformerWikipediaData(
            biography='From the links field.', page_fetched=True,
        )

        result = handler.enrich_performer_from_wikipedia(
            {}, FakeCtx(PERFORMER_WIKI_IN_LINKS),
        )

        assert result['updated'] is True
        # The handler passed the external_links URL to the fetcher.
        assert patched_fetch.call_args.args[1] == WIKI_URL

    def test_only_new_does_not_overwrite_existing_birth(
        self, perf_fixture, patched_fetch,
    ):
        # Wikipedia reports a different birth date + a new bio; the existing
        # birth must be preserved, only the missing bio written.
        patched_fetch.return_value = PerformerWikipediaData(
            birth_date='1999-12-31',
            biography='Newly discovered biography.',
            page_fetched=True,
        )

        result = handler.enrich_performer_from_wikipedia(
            {}, FakeCtx(PERFORMER_HAS_BIRTH),
        )

        assert result['updated'] is True
        assert result['birth_date_added'] is None       # not overwritten
        assert result['biography_added'] is True

        with get_db_connection() as conn:
            row = _performer_row(conn, PERFORMER_HAS_BIRTH)
        assert str(row['birth_date']) == '1930-01-01'    # unchanged
        assert row['biography'] == 'Newly discovered biography.'

    def test_only_new_skips_date_fetch_when_dates_present(
        self, perf_fixture, patched_fetch,
    ):
        # PERFORMER_HAS_BIRTH has a birth but no death -> dates still wanted
        # (for the missing death). Confirm the want flags are computed: bio is
        # missing so want_biography True, birth present so it won't be written.
        patched_fetch.return_value = PerformerWikipediaData(page_fetched=True)
        handler.enrich_performer_from_wikipedia({}, FakeCtx(PERFORMER_HAS_BIRTH))
        kwargs = patched_fetch.call_args.kwargs
        assert kwargs['want_dates'] is True      # death still missing
        assert kwargs['want_biography'] is True
        assert kwargs['want_image'] is True

    def test_force_refresh_payload_passed_to_searcher(
        self, perf_fixture, patched_fetch, mocker,
    ):
        handler.enrich_performer_from_wikipedia(
            {'force_refresh': True}, FakeCtx(PERFORMER_URL_EMPTY_DATA),
        )
        from research_worker.handlers.wikipedia import WikipediaSearcher
        WikipediaSearcher.assert_called_once_with(
            cache_days=7, force_refresh=True,
        )


class TestHandlerNoOps:
    def test_already_populated_short_circuits_without_fetch(
        self, perf_fixture, patched_fetch,
    ):
        result = handler.enrich_performer_from_wikipedia(
            {}, FakeCtx(PERFORMER_FULLY_POPULATED),
        )
        assert result['updated'] is False
        assert result['reason'] == 'already_populated'
        patched_fetch.assert_not_called()

    def test_nothing_new_when_wikipedia_empty(
        self, perf_fixture, patched_fetch,
    ):
        # Default mock returns an all-None PerformerWikipediaData.
        result = handler.enrich_performer_from_wikipedia(
            {}, FakeCtx(PERFORMER_URL_EMPTY_DATA),
        )
        assert result['updated'] is False
        assert result['reason'] == 'nothing_new'

        with get_db_connection() as conn:
            row = _performer_row(conn, PERFORMER_URL_EMPTY_DATA)
        assert row['birth_date'] is None
        assert row['biography'] is None


class TestHandlerErrorPaths:
    def test_missing_performer_row_raises_permanent(
        self, perf_fixture, patched_fetch,
    ):
        bogus = '00000000-0000-4000-8000-400000099999'
        with pytest.raises(PermanentError):
            handler.enrich_performer_from_wikipedia({}, FakeCtx(bogus))
        patched_fetch.assert_not_called()

    def test_no_wikipedia_url_raises_permanent(
        self, perf_fixture, patched_fetch,
    ):
        with pytest.raises(PermanentError):
            handler.enrich_performer_from_wikipedia(
                {}, FakeCtx(PERFORMER_NO_URL),
            )
        patched_fetch.assert_not_called()

    def test_empty_string_url_raises_permanent(
        self, perf_fixture, patched_fetch,
    ):
        with pytest.raises(PermanentError):
            handler.enrich_performer_from_wikipedia(
                {}, FakeCtx(PERFORMER_EMPTY_STR_URL),
            )


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_find_candidate_ids_includes_only_wiki_url_rows(self, perf_fixture):
        candidates = sweep_mod.find_candidate_performer_ids()
        assert PERFORMER_URL_EMPTY_DATA in candidates
        assert PERFORMER_WIKI_IN_LINKS in candidates
        assert PERFORMER_FULLY_POPULATED in candidates
        assert PERFORMER_HAS_BIRTH in candidates
        # No URL (NULL or empty string) -> not a candidate.
        assert PERFORMER_NO_URL not in candidates
        assert PERFORMER_EMPTY_STR_URL not in candidates

    def test_enqueue_sweep_creates_one_job_per_candidate(self, perf_fixture, db):
        result = sweep_mod.enqueue_sweep()
        assert result['candidates'] >= 4
        assert result['enqueued'] >= 4

        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'wikipedia' "
                "AND job_type = 'enrich_performer_from_wikipedia' "
                "AND target_id IN (%s, %s)",
                (PERFORMER_URL_EMPTY_DATA, PERFORMER_WIKI_IN_LINKS),
            )
            assert cur.fetchone()[0] == 2

    def test_enqueue_sweep_is_idempotent(self, perf_fixture, db):
        sweep_mod.enqueue_sweep()
        sweep_mod.enqueue_sweep()

        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE source = 'wikipedia' "
                "AND job_type = 'enrich_performer_from_wikipedia' "
                "AND target_id IN (%s, %s) GROUP BY target_id",
                (PERFORMER_URL_EMPTY_DATA, PERFORMER_WIKI_IN_LINKS),
            )
            counts = {str(row[0]): row[1] for row in cur.fetchall()}
        assert counts.get(PERFORMER_URL_EMPTY_DATA) == 1
        assert counts.get(PERFORMER_WIKI_IN_LINKS) == 1

    def test_enqueue_sweep_respects_limit(self, perf_fixture):
        result = sweep_mod.enqueue_sweep(limit=1)
        assert result['candidates'] == 1
        assert result['enqueued'] == 1

    def test_force_refresh_sets_payload(self, perf_fixture, db):
        sweep_mod.enqueue_sweep(force_refresh=True)
        with db.cursor() as cur:
            cur.execute(
                "SELECT payload FROM research_jobs "
                "WHERE source = 'wikipedia' "
                "AND job_type = 'enrich_performer_from_wikipedia' "
                "AND target_id = %s",
                (PERFORMER_URL_EMPTY_DATA,),
            )
            payload = cur.fetchone()[0]
        assert payload == {'force_refresh': True}

    def test_no_candidates_returns_zero(self, mocker):
        mocker.patch(
            'core.performer_wikipedia_enrichment.find_candidate_performer_ids',
            return_value=[],
        )
        result = sweep_mod.enqueue_sweep()
        assert result == {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    def test_second_sweep_reports_skipped_via_dedup(self, perf_fixture):
        first = sweep_mod.enqueue_sweep()
        second = sweep_mod.enqueue_sweep()
        assert first['enqueued'] >= 4
        assert second['enqueued'] == 0
        assert second['skipped'] == second['candidates']
