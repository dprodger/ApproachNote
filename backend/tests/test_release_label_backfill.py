"""
Tests for the MusicBrainz release-label backfill (issue #195):

  - research_worker/handlers/musicbrainz.py::backfill_release_label  — handler
  - core/release_label_backfill.py                                   — sweep enqueuer

Both touch real DB tables (releases, research_jobs) so we use deterministic
fixture UUIDs and explicit row-level cleanup, mirroring the pattern in
test_spotify_duration_backfill.py.

The MB client and the response parser are both mocked — we never hit
MusicBrainz, and the parser has its own coverage in test files for
integrations/musicbrainz/parsing.py.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from core import release_label_backfill
from db_utils import get_db_connection
from research_worker.errors import PermanentError, RetryableError
from research_worker.handlers import musicbrainz as handler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Deterministic UUIDs in the "fixture" range so cleanup can target them
# precisely without colliding with prod data.
_NS = "00000000-0000-4000-8000-2000000{:05x}"

RELEASE_NEEDS_BACKFILL_A = _NS.format(0x00001)   # label NULL, mbid set
RELEASE_NEEDS_BACKFILL_B = _NS.format(0x00002)   # label NULL, mbid set
RELEASE_ALREADY_HAS_LABEL = _NS.format(0x00003)  # label set, mbid set
RELEASE_NO_MBID = _NS.format(0x00004)            # label NULL, mbid NULL

MBID_A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
MBID_B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
MBID_HAS_LABEL = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'


def _cleanup(conn):
    fixture_ids = (
        RELEASE_NEEDS_BACKFILL_A,
        RELEASE_NEEDS_BACKFILL_B,
        RELEASE_ALREADY_HAS_LABEL,
        RELEASE_NO_MBID,
    )
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM research_jobs WHERE target_id IN (%s, %s, %s, %s)",
            fixture_ids,
        )
        cur.execute(
            "DELETE FROM releases WHERE id IN (%s, %s, %s, %s)",
            fixture_ids,
        )
    conn.commit()


@pytest.fixture
def backfill_fixture(db):
    """Four releases, exercising every candidate-eligibility case:

      A — label NULL, musicbrainz_release_id set        (candidate)
      B — label NULL, musicbrainz_release_id set        (candidate)
      ALREADY_HAS_LABEL — label set, mbid set           (not candidate)
      NO_MBID — label NULL, mbid NULL                   (not candidate)

    Tests assert that find_candidate_release_ids returns exactly the two
    candidates and that the handler treats each case correctly.
    """
    _cleanup(db)

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit, "
            " musicbrainz_release_id, label) "
            "VALUES (%s, %s, %s, %s, NULL)",
            (RELEASE_NEEDS_BACKFILL_A, "Backfill Release A", "Artist A", MBID_A),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit, "
            " musicbrainz_release_id, label) "
            "VALUES (%s, %s, %s, %s, NULL)",
            (RELEASE_NEEDS_BACKFILL_B, "Backfill Release B", "Artist B", MBID_B),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit, "
            " musicbrainz_release_id, label) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                RELEASE_ALREADY_HAS_LABEL, "Has Label", "Artist C",
                MBID_HAS_LABEL, "Blue Note",
            ),
        )
        cur.execute(
            "INSERT INTO releases (id, title, artist_credit, "
            " musicbrainz_release_id, label) "
            "VALUES (%s, %s, %s, NULL, NULL)",
            (RELEASE_NO_MBID, "No MBID", "Artist D"),
        )

    db.commit()
    yield
    _cleanup(db)


def _release_label_and_catalog(conn, release_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT label, catalog_number FROM releases WHERE id = %s",
            (release_id,),
        )
        row = cur.fetchone()
    return (row['label'], row['catalog_number']) if row else (None, None)


# ---------------------------------------------------------------------------
# Handler — backfill_release_label
# ---------------------------------------------------------------------------

class FakeCtx:
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.source = 'musicbrainz'
        self.log = logging.getLogger('test.fake_ctx')


@pytest.fixture
def patched_mb_client(mocker):
    """Mock MusicBrainzSearcher so the handler doesn't hit the network."""
    instance = mocker.MagicMock()
    instance.get_release_details = mocker.MagicMock()
    mocker.patch(
        'research_worker.handlers.musicbrainz.MusicBrainzSearcher',
        return_value=instance,
    )
    return SimpleNamespace(instance=instance)


@pytest.fixture
def patched_parse(mocker):
    """Stub the parser so tests can dictate (label, catalog_number) without
    constructing realistic MB JSON. The parser itself is covered by tests
    in test files for integrations/musicbrainz/parsing.py."""
    parse = mocker.patch(
        'research_worker.handlers.musicbrainz.parse_release_data',
    )
    return parse


class TestHandlerSuccess:
    def test_writes_label_and_catalog_number_from_mb(
        self, backfill_fixture, db, patched_mb_client, patched_parse,
    ):
        patched_mb_client.instance.get_release_details.return_value = {
            'id': MBID_A, 'title': 'whatever',
        }
        patched_parse.return_value = {
            'label': 'Impulse!', 'catalog_number': 'AS-77',
        }

        result = handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))

        assert result['updated'] is True
        assert result['label'] == 'Impulse!'
        assert result['catalog_number'] == 'AS-77'
        assert result['mbid'] == MBID_A

        with get_db_connection() as conn:
            label, catalog = _release_label_and_catalog(
                conn, RELEASE_NEEDS_BACKFILL_A,
            )
        assert label == 'Impulse!'
        assert catalog == 'AS-77'

        # Force-refresh must be True so we don't trust the stale cache
        # written before the `+labels` inc was added.
        from research_worker.handlers.musicbrainz import MusicBrainzSearcher
        MusicBrainzSearcher.assert_called_once_with(force_refresh=True)

    def test_writes_label_but_does_not_overwrite_existing_catalog_number(
        self, backfill_fixture, db, patched_mb_client, patched_parse,
    ):
        # Pre-set a catalog_number by hand on the candidate row.
        with db.cursor() as cur:
            cur.execute(
                "UPDATE releases SET catalog_number = %s WHERE id = %s",
                ('HAND-SET-001', RELEASE_NEEDS_BACKFILL_A),
            )
        db.commit()

        patched_mb_client.instance.get_release_details.return_value = {'id': MBID_A}
        patched_parse.return_value = {
            'label': 'Impulse!', 'catalog_number': 'AS-77',
        }

        handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))

        with get_db_connection() as conn:
            label, catalog = _release_label_and_catalog(
                conn, RELEASE_NEEDS_BACKFILL_A,
            )
        # Label gets backfilled, but the manually-set catalog_number stays.
        assert label == 'Impulse!'
        assert catalog == 'HAND-SET-001'

    def test_overlong_values_are_clamped_to_column_widths(
        self, backfill_fixture, db, patched_mb_client, patched_parse,
    ):
        # MB occasionally returns an overlong value (typically a concatenated
        # catalog_number); without clamping the UPDATE hits the column's
        # VARCHAR limit and raises StringDataRightTruncation. Clamp instead.
        long_label = 'L' * 400
        long_catalog = 'C' * 200
        patched_mb_client.instance.get_release_details.return_value = {'id': MBID_A}
        patched_parse.return_value = {
            'label': long_label, 'catalog_number': long_catalog,
        }

        result = handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))

        assert result['updated'] is True
        assert len(result['label']) == 255
        assert len(result['catalog_number']) == 100

        with get_db_connection() as conn:
            label, catalog = _release_label_and_catalog(
                conn, RELEASE_NEEDS_BACKFILL_A,
            )
        assert label == 'L' * 255
        assert catalog == 'C' * 100

    def test_already_populated_short_circuits_without_mb_call(
        self, backfill_fixture, patched_mb_client, patched_parse,
    ):
        result = handler.backfill_release_label(
            {}, FakeCtx(RELEASE_ALREADY_HAS_LABEL),
        )

        assert result['updated'] is False
        assert result['reason'] == 'already_populated'
        assert result['label'] == 'Blue Note'
        # No MB call — that's the whole point of the idempotency guard.
        patched_mb_client.instance.get_release_details.assert_not_called()
        patched_parse.assert_not_called()

    def test_mb_returns_data_but_no_label_records_no_op(
        self, backfill_fixture, db, patched_mb_client, patched_parse,
    ):
        patched_mb_client.instance.get_release_details.return_value = {'id': MBID_A}
        patched_parse.return_value = {'label': None, 'catalog_number': None}

        result = handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))

        assert result['updated'] is False
        assert result['reason'] == 'no_label_info'
        assert result['mbid'] == MBID_A

        # DB should not be modified.
        with get_db_connection() as conn:
            label, _ = _release_label_and_catalog(conn, RELEASE_NEEDS_BACKFILL_A)
        assert label is None


class TestHandlerErrorPaths:
    def test_missing_release_row_raises_permanent(
        self, backfill_fixture, patched_mb_client,
    ):
        bogus_id = '00000000-0000-4000-8000-200000099999'
        with pytest.raises(PermanentError):
            handler.backfill_release_label({}, FakeCtx(bogus_id))
        patched_mb_client.instance.get_release_details.assert_not_called()

    def test_missing_musicbrainz_id_raises_permanent(
        self, backfill_fixture, patched_mb_client,
    ):
        with pytest.raises(PermanentError):
            handler.backfill_release_label({}, FakeCtx(RELEASE_NO_MBID))
        patched_mb_client.instance.get_release_details.assert_not_called()

    def test_mb_returns_none_transient_raises_retryable(
        self, backfill_fixture, patched_mb_client,
    ):
        # None with last_release_status None means timeout/5xx — transient,
        # so worth a retry.
        patched_mb_client.instance.get_release_details.return_value = None
        patched_mb_client.instance.last_release_status = None

        with pytest.raises(RetryableError):
            handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))

    def test_mb_returns_none_404_raises_permanent(
        self, backfill_fixture, patched_mb_client,
    ):
        # None with status 404 means the MBID is deleted/merged — retrying
        # can't fix it, so it must go straight to 'dead'.
        patched_mb_client.instance.get_release_details.return_value = None
        patched_mb_client.instance.last_release_status = 404

        with pytest.raises(PermanentError):
            handler.backfill_release_label({}, FakeCtx(RELEASE_NEEDS_BACKFILL_A))


# ---------------------------------------------------------------------------
# Sweep enqueuer
# ---------------------------------------------------------------------------

class TestSweep:
    def test_find_candidate_release_ids_includes_only_eligible_rows(
        self, backfill_fixture,
    ):
        candidates = release_label_backfill.find_candidate_release_ids()
        # The two candidates with label=NULL AND mbid set must appear.
        assert RELEASE_NEEDS_BACKFILL_A in candidates
        assert RELEASE_NEEDS_BACKFILL_B in candidates
        # The two non-candidates must not.
        assert RELEASE_ALREADY_HAS_LABEL not in candidates
        assert RELEASE_NO_MBID not in candidates

    def test_enqueue_sweep_creates_one_job_per_candidate(
        self, backfill_fixture, db,
    ):
        # `db` cursor returns tuples (plain psycopg connection in the
        # fixture), so use positional access.
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'backfill_release_label' "
                "AND target_id IN (%s, %s)",
                (RELEASE_NEEDS_BACKFILL_A, RELEASE_NEEDS_BACKFILL_B),
            )
            before = cur.fetchone()[0]

        result = release_label_backfill.enqueue_sweep()
        assert result['candidates'] >= 2
        assert result['enqueued'] >= 2
        # First sweep: nothing should dedup-skip for our fresh fixture rows.
        # (`skipped` may be > 0 if the wider dev DB had stragglers, but our
        # fixture cleanup wiped any prior jobs for these target_ids.)
        assert 'skipped' in result

        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'backfill_release_label' "
                "AND target_id IN (%s, %s)",
                (RELEASE_NEEDS_BACKFILL_A, RELEASE_NEEDS_BACKFILL_B),
            )
            after = cur.fetchone()[0]
        assert after - before == 2

    def test_enqueue_sweep_is_idempotent(self, backfill_fixture, db):
        # Two consecutive sweeps must not double the row count for the
        # same releases — research_jobs' unique index on
        # (source, job_type, target_type, target_id) collapses dupes.
        release_label_backfill.enqueue_sweep()
        release_label_backfill.enqueue_sweep()

        with db.cursor() as cur:
            cur.execute(
                "SELECT target_id, COUNT(*) FROM research_jobs "
                "WHERE source = 'musicbrainz' "
                "AND job_type = 'backfill_release_label' "
                "AND target_id IN (%s, %s) "
                "GROUP BY target_id",
                (RELEASE_NEEDS_BACKFILL_A, RELEASE_NEEDS_BACKFILL_B),
            )
            counts = {str(row[0]): row[1] for row in cur.fetchall()}

        assert counts.get(RELEASE_NEEDS_BACKFILL_A) == 1
        assert counts.get(RELEASE_NEEDS_BACKFILL_B) == 1

    def test_enqueue_sweep_respects_limit(self, backfill_fixture):
        result = release_label_backfill.enqueue_sweep(limit=1)
        # At most one candidate even though the fixture has two qualifying
        # releases (and the wider DB may have more).
        assert result['candidates'] == 1
        assert result['enqueued'] == 1

    def test_no_candidates_returns_zero(self, mocker):
        # Force the candidate query to return empty so we don't depend on
        # whatever stale rows the dev DB carries outside the fixture.
        mocker.patch(
            'core.release_label_backfill.find_candidate_release_ids',
            return_value=[],
        )
        result = release_label_backfill.enqueue_sweep()
        assert result == {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    def test_second_sweep_reports_skipped_via_dedup(
        self, backfill_fixture, db,
    ):
        # First sweep enqueues both candidates; second sweep should report
        # them as skipped (dedup hit), not as new enqueues.
        first = release_label_backfill.enqueue_sweep()
        second = release_label_backfill.enqueue_sweep()

        assert first['enqueued'] >= 2
        # Second sweep finds the same candidates but ON CONFLICT DO NOTHING
        # collapses all inserts; the bulk path reports them as skipped.
        assert second['candidates'] == first['candidates']
        assert second['enqueued'] == 0
        assert second['skipped'] == second['candidates']
