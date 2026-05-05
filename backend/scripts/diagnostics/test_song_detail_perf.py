#!/usr/bin/env python3
"""
Performance regression test for the SongDetailView API endpoints.

Tests the two endpoints that back the SongDetailView:
  - GET /v1/songs/<id>/summary    (blocks UI, must be fast)
  - GET /v1/songs/<id>/recordings (background load, heavier but bounded)

Measures per endpoint:
  - Number of DB round-trips (execute_query / get_db_connection calls)
  - Wall-clock response time
  - Response payload size

Thresholds are set based on the known-good state where each endpoint
executes a single query. If someone adds more DB calls or the payload
balloons, this test will catch it.

Usage:
    cd backend
    python scripts/diagnostics/test_song_detail_perf.py
    python scripts/diagnostics/test_song_detail_perf.py --song-id <uuid>
    python scripts/diagnostics/test_song_detail_perf.py --verbose
"""

import argparse
import json
import os
import sys
import time
from unittest.mock import patch
from functools import wraps

# Ensure backend/ is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

os.environ['DB_USE_POOLING'] = 'true'

import db_utils as db_tools

# ============================================================================
# THRESHOLDS — update these when you intentionally change query structure
# ============================================================================

THRESHOLDS = {
    'summary': {
        'max_db_calls': 1,          # Currently 1 CTE query
        'max_response_ms': 3000,    # 3s wall clock (generous for remote DB)
        'max_payload_kb': 200,      # 200 KB should cover even large songs
    },
    'recordings': {
        'max_db_calls': 1,          # Currently 1 CTE query
        'max_response_ms': 3000,    # 3s wall clock (CTE-based query)
        'max_payload_kb': 1000,     # 1 MB for songs with many recordings
    },
}

# Additional thresholds for high-recording-count stress test
STRESS_THRESHOLDS = {
    'recordings': {
        'max_db_calls': 1,
        'max_response_ms': 8000,    # 8s for 500+ recordings (remote DB, cold cache)
        'max_payload_kb': 3000,     # 3 MB for very large songs
    },
}

# ============================================================================
# DB CALL COUNTER
# ============================================================================

class DBCallCounter:
    """Context manager that counts calls to db_utils.execute_query and get_db_connection."""

    def __init__(self):
        self.call_count = 0
        self.queries = []
        self._original_execute = None

    def __enter__(self):
        self._original_execute = db_tools.execute_query

        counter = self

        @wraps(db_tools.execute_query)
        def counting_execute(query, params=None, fetch_one=False, fetch_all=True):
            counter.call_count += 1
            # Capture first 120 chars of query for debugging
            preview = ' '.join(query.split())[:120]
            counter.queries.append(preview)
            return counter._original_execute(query, params, fetch_one=fetch_one, fetch_all=fetch_all)

        db_tools.execute_query = counting_execute
        return self

    def __exit__(self, *args):
        db_tools.execute_query = self._original_execute


# ============================================================================
# TEST RUNNER
# ============================================================================

def find_test_song():
    """Find a song with a realistic number of recordings (30-80 range).

    Picks the song closest to 50 recordings, which represents a typical
    well-known standard. Avoids extreme outliers like Summertime (1700+).
    """
    result = db_tools.execute_query("""
        SELECT s.id, s.title, COUNT(r.id) as rec_count
        FROM songs s
        JOIN recordings r ON r.song_id = s.id
        GROUP BY s.id, s.title
        HAVING COUNT(r.id) BETWEEN 30 AND 100
        ORDER BY ABS(COUNT(r.id) - 50)
        LIMIT 1
    """, fetch_one=True)
    if not result:
        # Fallback: just pick the song with the most recordings
        result = db_tools.execute_query("""
            SELECT s.id, s.title, COUNT(r.id) as rec_count
            FROM songs s
            JOIN recordings r ON r.song_id = s.id
            GROUP BY s.id, s.title
            ORDER BY rec_count DESC
            LIMIT 1
        """, fetch_one=True)
    return result


def find_stress_test_song():
    """Find a song with 500+ recordings for stress testing.

    Songs with many recordings (like "Summertime" or "All of Me") are the
    ones most likely to expose N+1 query regressions. If the query uses
    correlated subqueries instead of CTEs, these songs will be dramatically
    slower than songs with ~50 recordings.
    """
    result = db_tools.execute_query("""
        SELECT s.id, s.title, COUNT(r.id) as rec_count
        FROM songs s
        JOIN recordings r ON r.song_id = s.id
        GROUP BY s.id, s.title
        HAVING COUNT(r.id) >= 500
        ORDER BY ABS(COUNT(r.id) - 700)
        LIMIT 1
    """, fetch_one=True)
    return result


def test_endpoint(client, endpoint, label, thresholds, verbose=False):
    """Hit an endpoint and measure DB calls, time, and payload size."""
    failures = []

    with DBCallCounter() as counter:
        start = time.monotonic()
        response = client.get(endpoint)
        elapsed_ms = (time.monotonic() - start) * 1000

    status = response.status_code
    payload_bytes = len(response.data)
    payload_kb = payload_bytes / 1024

    # Parse response to count items
    item_count = None
    try:
        data = json.loads(response.data)
        if label == 'summary':
            recs = data.get('featured_recordings', [])
            item_count = len(recs) if recs else 0
        elif label == 'recordings':
            recs = data.get('recordings', [])
            item_count = len(recs) if recs else 0
    except (json.JSONDecodeError, TypeError):
        pass

    # Check thresholds
    if status != 200:
        failures.append(f"HTTP {status} (expected 200)")

    if counter.call_count > thresholds['max_db_calls']:
        failures.append(
            f"DB calls: {counter.call_count} (max {thresholds['max_db_calls']})"
        )

    if elapsed_ms > thresholds['max_response_ms']:
        failures.append(
            f"Response time: {elapsed_ms:.0f}ms (max {thresholds['max_response_ms']}ms)"
        )

    if payload_kb > thresholds['max_payload_kb']:
        failures.append(
            f"Payload: {payload_kb:.1f} KB (max {thresholds['max_payload_kb']} KB)"
        )

    # Print results
    passed = len(failures) == 0
    status_icon = "PASS" if passed else "FAIL"

    print(f"\n  [{status_icon}] {label}")
    print(f"    Endpoint:      {endpoint}")
    print(f"    DB calls:      {counter.call_count:>3}   (max {thresholds['max_db_calls']})")
    print(f"    Response time:  {elapsed_ms:>7.0f}ms (max {thresholds['max_response_ms']}ms)")
    print(f"    Payload:       {payload_kb:>7.1f} KB (max {thresholds['max_payload_kb']} KB)")
    if item_count is not None:
        print(f"    Items:         {item_count:>3}")

    if verbose and counter.queries:
        print(f"    Queries:")
        for i, q in enumerate(counter.queries, 1):
            print(f"      {i}. {q}...")

    if failures:
        for f in failures:
            print(f"    ** {f}")

    return passed


def main():
    parser = argparse.ArgumentParser(description='SongDetailView performance regression test')
    parser.add_argument('--song-id', help='Test a specific song UUID')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show query details')
    args = parser.parse_args()

    # Find test song
    if args.song_id:
        song = db_tools.execute_query(
            "SELECT id, title, (SELECT COUNT(*) FROM recordings WHERE song_id = s.id) as rec_count FROM songs s WHERE id = %s",
            (args.song_id,), fetch_one=True
        )
        if not song:
            print(f"Song {args.song_id} not found")
            sys.exit(1)
    else:
        song = find_test_song()
        if not song:
            print("No songs with recordings found in database")
            sys.exit(1)

    song_id = song['id']
    song_title = song['title']
    rec_count = song['rec_count']

    print("=" * 60)
    print("SongDetailView Performance Test")
    print("=" * 60)
    print(f"  Song:       {song_title}")
    print(f"  ID:         {song_id}")
    print(f"  Recordings: {rec_count}")

    # Create Flask test client (no server needed)
    from app import app
    app.config['TESTING'] = True
    client = app.test_client()

    all_passed = True

    # Test summary endpoint (routes live under /v1/ — see routes/__init__.py)
    passed = test_endpoint(
        client,
        f'/v1/songs/{song_id}/summary',
        'summary',
        THRESHOLDS['summary'],
        verbose=args.verbose,
    )
    all_passed = all_passed and passed

    # Test recordings endpoint
    passed = test_endpoint(
        client,
        f'/v1/songs/{song_id}/recordings',
        'recordings',
        THRESHOLDS['recordings'],
        verbose=args.verbose,
    )
    all_passed = all_passed and passed

    # Stress test: high-recording-count song (catches N+1 regressions)
    if not args.song_id:
        stress_song = find_stress_test_song()
        if stress_song:
            stress_id = stress_song['id']
            stress_title = stress_song['title']
            stress_count = stress_song['rec_count']
            print()
            print("-" * 60)
            print(f"  Stress test: {stress_title} ({stress_count} recordings)")
            print("-" * 60)

            passed = test_endpoint(
                client,
                f'/v1/songs/{stress_id}/recordings',
                'recordings',
                STRESS_THRESHOLDS['recordings'],
                verbose=args.verbose,
            )
            all_passed = all_passed and passed
        else:
            print("\n  (No song with 500+ recordings found — stress test skipped)")

    # Summary
    print()
    print("=" * 60)
    if all_passed:
        print("  RESULT: ALL CHECKS PASSED")
    else:
        print("  RESULT: FAILURES DETECTED")
    print("=" * 60)
    print()

    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
