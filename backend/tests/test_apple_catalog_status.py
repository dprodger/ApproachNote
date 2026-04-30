"""
Tests for core.apple_catalog_status — the read-only status snapshot
that powers /admin/apple-music-catalog.

The four sections are gathered independently so a failure in one
shouldn't blank out the others. Each test exercises one section in
isolation, mocking AppleMusicCatalog so we never touch DuckDB or the
filesystem during unit tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core import apple_catalog_status


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class TestConfiguration:
    def test_motherduck_override_detected(self, mocker, monkeypatch):
        # APPLE_MUSIC_CATALOG_DB starting with "md:" → motherduck mode
        monkeypatch.setenv('APPLE_MUSIC_CATALOG_DB', 'md:apple_music_feed')
        monkeypatch.setenv('MOTHERDUCK_TOKEN', 'tok123')
        cfg = apple_catalog_status._gather_configuration()
        assert cfg['mode'] == 'motherduck'
        assert cfg['details'] == 'md:apple_music_feed'
        assert cfg['motherduck_token_set'] is True

    def test_local_file_present_takes_precedence_over_motherduck_token(
        self, mocker, monkeypatch,
    ):
        # No override, default DB exists, MOTHERDUCK_TOKEN set →
        # local_file wins (matches AppleMusicCatalog's resolution order).
        monkeypatch.delenv('APPLE_MUSIC_CATALOG_DB', raising=False)
        monkeypatch.setenv('MOTHERDUCK_TOKEN', 'tok123')
        mocker.patch(
            'core.apple_catalog_status.Path.exists',
            return_value=True,
        )
        cfg = apple_catalog_status._gather_configuration()
        assert cfg['mode'] == 'local_file'
        assert cfg['default_db_path_exists'] is True

    def test_no_local_file_with_motherduck_token_falls_through_to_md(
        self, mocker, monkeypatch,
    ):
        monkeypatch.delenv('APPLE_MUSIC_CATALOG_DB', raising=False)
        monkeypatch.setenv('MOTHERDUCK_TOKEN', 'tok123')
        mocker.patch(
            'core.apple_catalog_status.Path.exists',
            return_value=False,
        )
        cfg = apple_catalog_status._gather_configuration()
        assert cfg['mode'] == 'motherduck'
        assert cfg['details'] == 'md:apple_music_feed'

    def test_no_db_no_token_is_parquet_fallback(self, mocker, monkeypatch):
        monkeypatch.delenv('APPLE_MUSIC_CATALOG_DB', raising=False)
        monkeypatch.delenv('MOTHERDUCK_TOKEN', raising=False)
        mocker.patch(
            'core.apple_catalog_status.Path.exists',
            return_value=False,
        )
        cfg = apple_catalog_status._gather_configuration()
        assert cfg['mode'] == 'parquet_fallback'
        assert cfg['motherduck_token_set'] is False


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------

class TestConnectivity:
    def test_select_1_success_records_elapsed_ms(self, mocker):
        # Mock AppleMusicCatalog so we don't hit DuckDB.
        fake_catalog = MagicMock()
        fake_conn = MagicMock()
        fake_conn.execute.return_value.fetchone.return_value = (1,)
        fake_catalog._conn = fake_conn

        def _set_conn():
            fake_catalog._conn = fake_conn
        fake_catalog._create_conn.side_effect = _set_conn

        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )

        result = apple_catalog_status._gather_connectivity()
        assert result['ok'] is True
        assert result['error'] is None
        assert isinstance(result['elapsed_ms'], (int, float))
        assert result['elapsed_ms'] >= 0

    def test_connection_failure_returns_structured_error(self, mocker):
        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            side_effect=RuntimeError('motherduck offline'),
        )
        result = apple_catalog_status._gather_connectivity()
        assert result['ok'] is False
        assert 'motherduck offline' in result['error']
        # elapsed_ms is recorded even on failure (we still timed how long
        # it took to fail).
        assert result['elapsed_ms'] is not None

    def test_select_failure_returns_structured_error(self, mocker):
        fake_catalog = MagicMock()
        fake_conn = MagicMock()
        fake_conn.execute.side_effect = RuntimeError('table not found')
        fake_catalog._conn = fake_conn
        fake_catalog._create_conn.return_value = None

        def _set_conn():
            fake_catalog._conn = fake_conn
        fake_catalog._create_conn.side_effect = _set_conn

        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )
        result = apple_catalog_status._gather_connectivity()
        assert result['ok'] is False
        assert 'table not found' in result['error']


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_returns_per_feed_metadata_from_catalog(self, mocker):
        fake_stats = {
            'songs':   {'export_date': '2025-12-18', 'total_records': 90_000_000,
                        'downloaded_at': '2025-12-18T03:14:15'},
            'albums':  {'export_date': '2025-12-18', 'total_records': 12_000_000,
                        'downloaded_at': '2025-12-18T03:00:00'},
            'artists': None,
        }
        fake_catalog = MagicMock()
        fake_catalog.get_catalog_stats.return_value = fake_stats
        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )

        result = apple_catalog_status._gather_freshness()
        assert result['error'] is None
        assert result['feeds'] == fake_stats

    def test_catalog_init_failure_returns_error(self, mocker):
        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            side_effect=ImportError('duckdb not installed'),
        )
        result = apple_catalog_status._gather_freshness()
        assert result['feeds'] is None
        assert 'duckdb' in result['error']


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------

class TestRowCounts:
    def test_indexed_db_counts_each_table(self, mocker):
        fake_catalog = MagicMock()
        fake_catalog._use_indexed_db = True
        fake_conn = MagicMock()
        # Different counts per table, fed in order.
        fake_conn.execute.return_value.fetchone.side_effect = [
            (90_000_000,),  # songs
            (12_000_000,),  # albums
            (3_000_000,),   # artists
        ]
        fake_catalog._conn = fake_conn

        def _set_conn():
            fake_catalog._conn = fake_conn
        fake_catalog._create_conn.side_effect = _set_conn

        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )
        result = apple_catalog_status._gather_row_counts()
        assert result['error'] is None
        assert result['mode'] == 'indexed'
        assert result['tables']['songs']['count'] == 90_000_000
        assert result['tables']['albums']['count'] == 12_000_000
        assert result['tables']['artists']['count'] == 3_000_000
        for v in result['tables'].values():
            assert v['error'] is None

    def test_parquet_fallback_skips_counting(self, mocker):
        fake_catalog = MagicMock()
        fake_catalog._use_indexed_db = False
        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )
        result = apple_catalog_status._gather_row_counts()
        assert result['mode'] == 'parquet_fallback_skipped'
        assert result['tables'] is None
        assert 'parquet' in result['note'].lower()
        # Skipped path must not even call _create_conn.
        fake_catalog._create_conn.assert_not_called()

    def test_per_table_failure_does_not_blank_other_tables(self, mocker):
        fake_catalog = MagicMock()
        fake_catalog._use_indexed_db = True
        fake_conn = MagicMock()

        # First call (songs) raises; second (albums) returns; third
        # (artists) raises again.
        def _execute(sql):
            if 'songs' in sql:
                raise RuntimeError('songs table missing')
            if 'albums' in sql:
                m = MagicMock()
                m.fetchone.return_value = (12_000_000,)
                return m
            if 'artists' in sql:
                raise RuntimeError('artists table missing')
            raise AssertionError(f'unexpected sql: {sql}')
        fake_conn.execute.side_effect = _execute
        fake_catalog._conn = fake_conn

        def _set_conn():
            fake_catalog._conn = fake_conn
        fake_catalog._create_conn.side_effect = _set_conn

        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            return_value=fake_catalog,
        )
        result = apple_catalog_status._gather_row_counts()
        assert result['error'] is None
        assert result['tables']['songs']['count'] is None
        assert 'songs table missing' in result['tables']['songs']['error']
        assert result['tables']['albums']['count'] == 12_000_000
        assert result['tables']['albums']['error'] is None
        assert result['tables']['artists']['count'] is None
        assert 'artists table missing' in result['tables']['artists']['error']

    def test_catalog_init_failure_returns_error(self, mocker):
        mocker.patch(
            'integrations.apple_music.feed.AppleMusicCatalog',
            side_effect=ImportError('duckdb'),
        )
        result = apple_catalog_status._gather_row_counts()
        assert result['tables'] is None
        assert 'duckdb' in result['error']


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

class TestGetCatalogStatus:
    def test_assembles_all_sections(self, mocker):
        # Mock every section gatherer so we just verify the shape.
        mocker.patch.object(
            apple_catalog_status, '_gather_configuration',
            return_value={'mode': 'local_file'},
        )
        mocker.patch.object(
            apple_catalog_status, '_gather_connectivity',
            return_value={'ok': True, 'elapsed_ms': 1.2, 'error': None},
        )
        mocker.patch.object(
            apple_catalog_status, '_gather_freshness',
            return_value={'error': None, 'feeds': {'songs': None}},
        )
        mocker.patch.object(
            apple_catalog_status, '_gather_row_counts',
            return_value={'error': None, 'mode': 'indexed', 'tables': {}},
        )
        mocker.patch.object(
            apple_catalog_status, '_gather_recent_refresh_jobs',
            return_value={'error': None, 'jobs': []},
        )

        result = apple_catalog_status.get_catalog_status()
        assert set(result.keys()) == {
            'configuration', 'connectivity', 'freshness', 'row_counts',
            'recent_refresh_jobs',
        }
