"""
Resource-limit safety on the Apple Music DuckDB connection.

The catalog reader runs alongside Spotify + YouTube workers in the same
process. DuckDB's defaults (80% of system RAM for the buffer pool, all
cores for threads) crowd out the other workers on a small instance, so
we apply PRAGMA memory_limit + threads to every connection we open.
These tests guard the limits against regression.
"""

from __future__ import annotations

import duckdb
import pytest

from integrations.apple_music.feed import AppleMusicCatalog


def _read_pragma(conn, name: str) -> str:
    """current_setting() returns the resolved PRAGMA value as text."""
    return conn.execute(f"SELECT current_setting('{name}')").fetchone()[0]


# DuckDB displays the resolved memory_limit in MiB (binary), but accepts MB
# (decimal) at input. So 512MB shows as 488.2 MiB. Parse the displayed value
# back to bytes so we can compare against the configured value tolerantly.
_UNIT_BYTES = {
    'KiB': 1024,
    'MiB': 1024 ** 2,
    'GiB': 1024 ** 3,
    'KB':  1000,
    'MB':  1000 ** 2,
    'GB':  1000 ** 3,
}


def _bytes_from_pragma(raw: str) -> int:
    num_part, unit = raw.split()
    return int(float(num_part) * _UNIT_BYTES[unit])


def test_default_memory_limit_is_applied(monkeypatch):
    # Strip any ambient overrides so the class defaults are what we test.
    monkeypatch.delenv('APPLE_DUCKDB_MEMORY_LIMIT', raising=False)
    monkeypatch.delenv('APPLE_DUCKDB_THREADS', raising=False)

    conn = duckdb.connect(':memory:')
    try:
        AppleMusicCatalog._apply_resource_limits(conn)
        observed = _bytes_from_pragma(_read_pragma(conn, 'memory_limit'))
        # Default was 512MB. Allow 1% drift for MiB/MB rounding.
        assert observed == pytest.approx(512 * 1000 ** 2, rel=0.01)
        assert int(_read_pragma(conn, 'threads')) == 2
    finally:
        conn.close()


def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv('APPLE_DUCKDB_MEMORY_LIMIT', '256MB')
    monkeypatch.setenv('APPLE_DUCKDB_THREADS', '1')

    conn = duckdb.connect(':memory:')
    try:
        AppleMusicCatalog._apply_resource_limits(conn)
        observed = _bytes_from_pragma(_read_pragma(conn, 'memory_limit'))
        assert observed == pytest.approx(256 * 1000 ** 2, rel=0.01)
        assert int(_read_pragma(conn, 'threads')) == 1
    finally:
        conn.close()


def test_apply_does_not_raise_on_unsupported_pragma(mocker, monkeypatch):
    # Some DuckDB targets (MotherDuck, older builds) reject PRAGMAs. The
    # wrapper logs and continues so we never lose a connection over tuning.
    monkeypatch.delenv('APPLE_DUCKDB_MEMORY_LIMIT', raising=False)
    monkeypatch.delenv('APPLE_DUCKDB_THREADS', raising=False)

    fake_conn = mocker.MagicMock()
    fake_conn.execute.side_effect = RuntimeError("PRAGMA not supported")

    # Should swallow the exception and log it. Failure here would surface
    # as an unhandled raise.
    AppleMusicCatalog._apply_resource_limits(fake_conn)

    # At least one execute attempt was made.
    assert fake_conn.execute.call_count >= 1
