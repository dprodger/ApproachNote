#!/usr/bin/env python3
"""
Diagnostic tool: runs EXPLAIN ANALYZE on the /songs/{id}/recordings query
and reports per-CTE timing breakdowns.

Usage:
    cd backend
    python scripts/diagnostics/explain_song_recordings.py <song_id>
    python scripts/diagnostics/explain_song_recordings.py <song_id> --raw
"""

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

os.environ['DB_USE_POOLING'] = 'true'

import db_utils as db_tools


def get_recording_count(song_id):
    result = db_tools.execute_query(
        "SELECT COUNT(*) as cnt FROM recordings WHERE song_id = %s",
        (song_id,), fetch_one=True
    )
    return result['cnt'] if result else 0


def run_explain(song_id, sort_by='year'):
    """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) on the recordings query."""

    if sort_by == 'name':
        recordings_order = """
            (
                SELECT COALESCE(p2.sort_name, p2.name, 'ZZZ')
                FROM recording_performers rp2
                JOIN performers p2 ON rp2.performer_id = p2.id
                WHERE rp2.recording_id = r.id AND rp2.role = 'leader'
                ORDER BY COALESCE(p2.sort_name, p2.name)
                LIMIT 1
            ) ASC NULLS LAST,
            r.recording_year ASC NULLS LAST
        """
    else:
        recordings_order = "r.recording_year ASC NULLS LAST"

    explain_query = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        WITH
        front_art AS (
            SELECT DISTINCT ON (sub.recording_id)
                sub.recording_id,
                sub.image_url_small, sub.image_url_medium, sub.image_url_large,
                sub.source, sub.source_url
            FROM (
                SELECT r.id as recording_id,
                       ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                       ri.source::text as source, ri.source_url,
                       1 as priority,
                       CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END as source_order
                FROM recordings r
                JOIN release_imagery ri ON ri.release_id = r.default_release_id AND ri.type = 'Front'
                WHERE r.song_id = %s
                UNION ALL
                SELECT r.id, ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                       ri.source::text, ri.source_url,
                       2 as priority,
                       CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
                FROM recordings r
                JOIN recording_releases rr ON rr.recording_id = r.id
                JOIN release_imagery ri ON ri.release_id = rr.release_id AND ri.type = 'Front'
                WHERE r.song_id = %s
            ) sub
            ORDER BY sub.recording_id, sub.priority, sub.source_order
        ),
        back_art AS (
            SELECT DISTINCT ON (r.id)
                r.id as recording_id,
                ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                ri.source::text as source, ri.source_url,
                TRUE as has_back_cover
            FROM recordings r
            JOIN release_imagery ri ON ri.release_id = r.default_release_id AND ri.type = 'Back'
            WHERE r.song_id = %s
        ),
        streaming AS (
            SELECT
                rr.recording_id,
                bool_or(TRUE) as has_streaming,
                bool_or(rrsl.service = 'spotify') as has_spotify,
                bool_or(rrsl.service = 'apple_music') as has_apple_music,
                bool_or(rrsl.service = 'youtube') as has_youtube,
                array_agg(DISTINCT rrsl.service) as streaming_services
            FROM recording_releases rr
            JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr.id
            WHERE rr.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            GROUP BY rr.recording_id
        ),
        spotify_urls AS (
            SELECT DISTINCT ON (rr.recording_id)
                rr.recording_id,
                rrsl.service_url as best_spotify_url
            FROM recording_releases rr
            JOIN recording_release_streaming_links rrsl
                ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
            WHERE rr.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            ORDER BY rr.recording_id,
                CASE WHEN rr.release_id = (
                    SELECT default_release_id FROM recordings WHERE id = rr.recording_id
                ) THEN 0 ELSE 1 END
        ),
        community AS (
            SELECT
                rc.recording_id,
                jsonb_build_object(
                    'consensus', jsonb_build_object(
                        'performance_key', (
                            SELECT performance_key FROM recording_contributions rc2
                            WHERE rc2.recording_id = rc.recording_id AND rc2.performance_key IS NOT NULL
                            GROUP BY performance_key ORDER BY COUNT(*) DESC, MAX(updated_at) DESC LIMIT 1
                        ),
                        'tempo_marking', (
                            SELECT tempo_marking FROM recording_contributions rc2
                            WHERE rc2.recording_id = rc.recording_id AND rc2.tempo_marking IS NOT NULL
                            GROUP BY tempo_marking ORDER BY COUNT(*) DESC, MAX(updated_at) DESC LIMIT 1
                        ),
                        'is_instrumental', (
                            SELECT is_instrumental FROM recording_contributions rc2
                            WHERE rc2.recording_id = rc.recording_id AND rc2.is_instrumental IS NOT NULL
                            GROUP BY is_instrumental ORDER BY COUNT(*) DESC, MAX(updated_at) DESC LIMIT 1
                        )
                    ),
                    'counts', jsonb_build_object(
                        'key', COUNT(*) FILTER (WHERE rc.performance_key IS NOT NULL),
                        'tempo', COUNT(*) FILTER (WHERE rc.tempo_marking IS NOT NULL),
                        'instrumental', COUNT(*) FILTER (WHERE rc.is_instrumental IS NOT NULL)
                    )
                ) as community_data
            FROM recording_contributions rc
            WHERE rc.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            GROUP BY rc.recording_id
        )
        SELECT
            r.id, r.title,
            def_rel.title as album_title,
            def_rel.artist_credit as artist_credit,
            r.recording_date, r.recording_year, r.default_release_id,
            su.best_spotify_url,
            fa.image_url_small as best_cover_art_small,
            fa.image_url_medium as best_cover_art_medium,
            fa.image_url_large as best_cover_art_large,
            fa.source as best_cover_art_source,
            fa.source_url as best_cover_art_source_url,
            ba.image_url_small as back_cover_art_small,
            ba.image_url_medium as back_cover_art_medium,
            ba.image_url_large as back_cover_art_large,
            COALESCE(ba.has_back_cover, FALSE) as has_back_cover,
            ba.source as back_cover_source,
            ba.source_url as back_cover_source_url,
            r.musicbrainz_id, r.is_canonical, r.notes,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', p.id, 'name', p.name, 'sort_name', p.sort_name,
                        'instrument', i.name, 'role', rp.role
                    ) ORDER BY
                        CASE rp.role WHEN 'leader' THEN 1 WHEN 'sideman' THEN 2 ELSE 3 END,
                        COALESCE(p.sort_name, p.name)
                ) FILTER (WHERE p.id IS NOT NULL),
                '[]'::json
            ) as performers,
            COUNT(DISTINCT sar.id) as authority_count,
            COALESCE(array_agg(DISTINCT sar.source) FILTER (WHERE sar.source IS NOT NULL), ARRAY[]::text[]) as authority_sources,
            COALESCE(st.has_streaming, FALSE) as has_streaming,
            COALESCE(st.has_spotify, FALSE) as has_spotify,
            COALESCE(st.has_apple_music, FALSE) as has_apple_music,
            COALESCE(st.has_youtube, FALSE) as has_youtube,
            COALESCE(st.streaming_services, ARRAY[]::varchar[]) as streaming_services,
            cm.community_data
        FROM recordings r
        LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
        LEFT JOIN recording_performers rp ON r.id = rp.recording_id
        LEFT JOIN performers p ON rp.performer_id = p.id
        LEFT JOIN instruments i ON rp.instrument_id = i.id
        LEFT JOIN song_authority_recommendations sar ON r.id = sar.recording_id
        LEFT JOIN front_art fa ON fa.recording_id = r.id
        LEFT JOIN back_art ba ON ba.recording_id = r.id
        LEFT JOIN streaming st ON st.recording_id = r.id
        LEFT JOIN spotify_urls su ON su.recording_id = r.id
        LEFT JOIN community cm ON cm.recording_id = r.id
        WHERE r.song_id = %s
        GROUP BY r.id, def_rel.title, def_rel.artist_credit, r.recording_date, r.recording_year,
                 r.default_release_id,
                 r.musicbrainz_id, r.is_canonical, r.notes,
                 su.best_spotify_url,
                 fa.image_url_small, fa.image_url_medium, fa.image_url_large, fa.source, fa.source_url,
                 ba.image_url_small, ba.image_url_medium, ba.image_url_large, ba.has_back_cover, ba.source, ba.source_url,
                 st.has_streaming, st.has_spotify, st.has_apple_music, st.has_youtube, st.streaming_services,
                 cm.community_data
        ORDER BY {recordings_order}
    """

    params = (song_id,) * 7
    return db_tools.execute_query(explain_query, params)


def parse_cte_timings(explain_rows):
    """Extract CTE and major node timings from EXPLAIN ANALYZE output."""
    timings = {}
    total_time = None
    current_cte = None

    for row in explain_rows:
        line = row.get('QUERY PLAN', '')

        # Total execution time
        m = re.search(r'Execution Time:\s+([\d.]+)\s*ms', line)
        if m:
            total_time = float(m.group(1))

        # Planning time
        m = re.search(r'Planning Time:\s+([\d.]+)\s*ms', line)
        if m:
            timings['Planning'] = float(m.group(1))

        # CTE scan nodes
        m = re.search(r'CTE (\w+)', line)
        if m:
            current_cte = m.group(1)

        # Actual time on nodes (take the last actual time for each CTE)
        m = re.search(r'actual time=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)', line)
        if m and current_cte:
            timings[f'CTE {current_cte}'] = {
                'startup_ms': float(m.group(1)),
                'total_ms': float(m.group(2)),
                'rows': int(m.group(3)),
            }

        # Top-level GroupAggregate or Sort
        if ('GroupAggregate' in line or 'Sort' in line) and 'actual time=' in line:
            m2 = re.search(r'actual time=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)', line)
            if m2:
                node_name = 'GroupAggregate' if 'GroupAggregate' in line else 'Sort'
                timings[node_name] = {
                    'startup_ms': float(m2.group(1)),
                    'total_ms': float(m2.group(2)),
                    'rows': int(m2.group(3)),
                }

    return timings, total_time


def main():
    parser = argparse.ArgumentParser(description='EXPLAIN ANALYZE for song recordings query')
    parser.add_argument('song_id', help='Song UUID to analyze')
    parser.add_argument('--sort', default='year', choices=['year', 'name'], help='Sort order')
    parser.add_argument('--raw', action='store_true', help='Print raw EXPLAIN output')
    args = parser.parse_args()

    # Get song info
    song = db_tools.execute_query(
        "SELECT title FROM songs WHERE id = %s", (args.song_id,), fetch_one=True
    )
    if not song:
        print(f"Song {args.song_id} not found")
        sys.exit(1)

    rec_count = get_recording_count(args.song_id)

    print("=" * 70)
    print(f"  EXPLAIN ANALYZE: /songs/{args.song_id}/recordings?sort={args.sort}")
    print(f"  Song: {song['title']} ({rec_count} recordings)")
    print("=" * 70)

    # Run EXPLAIN ANALYZE
    start = time.monotonic()
    rows = run_explain(args.song_id, args.sort)
    wall_time = (time.monotonic() - start) * 1000

    if args.raw:
        print()
        for row in rows:
            print(row.get('QUERY PLAN', ''))
        print()

    # Parse timings
    timings, exec_time = parse_cte_timings(rows)

    print(f"\n  Wall clock:      {wall_time:>8.0f} ms")
    if exec_time:
        print(f"  Execution time:  {exec_time:>8.1f} ms")
    if 'Planning' in timings:
        print(f"  Planning time:   {timings['Planning']:>8.1f} ms")

    print(f"\n  {'Node':<25} {'Time (ms)':>10} {'Rows':>8}")
    print(f"  {'-'*25} {'-'*10} {'-'*8}")

    for name, info in timings.items():
        if isinstance(info, dict):
            print(f"  {name:<25} {info['total_ms']:>10.1f} {info['rows']:>8}")

    print()
    print("=" * 70)


if __name__ == '__main__':
    main()
