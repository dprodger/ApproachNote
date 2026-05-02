"""
Spotify rematch helper: runs SpotifyMatcher(rematch_tracks=True) for a single
song and captures a before/after diff of Spotify state.

Used by both:
- backend/routes/admin.py (admin diagnostic page)
- backend/scripts/backfill_spotify_track_links.py (bulk backfill loop)

Run records are serialized to backend/data/spotify_rematch_runs/ so the admin
page can show run history.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from db_utils import get_db_connection
from integrations.spotify.db import find_song_by_id
from integrations.spotify.matcher import SpotifyMatcher

RUNS_DIR = Path(__file__).resolve().parent.parent / 'data' / 'spotify_rematch_runs'


def _snapshot_spotify_state(song_id: str) -> dict:
    """
    Capture Spotify-related state for every release/recording tied to this song.

    Captured tables: recording_release_streaming_links, release_streaming_links,
    release_imagery. These are the three places SpotifyMatcher may add, update,
    or clear data during rematch_tracks.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Releases associated with this song
            cur.execute("""
                SELECT DISTINCT r.id, r.title, r.release_year,
                       r.musicbrainz_release_id
                FROM releases r
                JOIN recording_releases rr ON rr.release_id = r.id
                JOIN recordings rec ON rec.id = rr.recording_id
                WHERE rec.song_id = %s
            """, (song_id,))
            releases = {str(row['id']): dict(row) for row in cur.fetchall()}

            if not releases:
                return {
                    'releases': {},
                    'tracks': {},
                    'release_links': {},
                    'imagery': {},
                    'recording_releases': {},
                }

            release_ids = list(releases.keys())

            # recording_releases context for enrichment
            cur.execute("""
                SELECT rr.id AS recording_release_id, rr.recording_id, rr.release_id,
                       rec.title AS recording_title,
                       (
                           SELECT p.name
                           FROM recording_performers rp
                           JOIN performers p ON p.id = rp.performer_id
                           WHERE rp.recording_id = rec.id AND rp.role = 'leader'
                           LIMIT 1
                       ) AS leader_name
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                WHERE rec.song_id = %s
            """, (song_id,))
            recording_releases = {str(row['recording_release_id']): dict(row) for row in cur.fetchall()}

            rr_ids = list(recording_releases.keys())

            # Track-level Spotify links
            if rr_ids:
                cur.execute("""
                    SELECT recording_release_id, service, service_id, service_url
                    FROM recording_release_streaming_links
                    WHERE recording_release_id = ANY(%s) AND service = 'spotify'
                """, (rr_ids,))
                tracks = {str(row['recording_release_id']): dict(row) for row in cur.fetchall()}
            else:
                tracks = {}

            # Release-level Spotify links
            cur.execute("""
                SELECT release_id, service, service_id, service_url
                FROM release_streaming_links
                WHERE release_id = ANY(%s) AND service = 'spotify'
            """, (release_ids,))
            release_links = {str(row['release_id']): dict(row) for row in cur.fetchall()}

            # Spotify imagery
            cur.execute("""
                SELECT release_id, source_id, source_url,
                       image_url_small, image_url_medium, image_url_large
                FROM release_imagery
                WHERE release_id = ANY(%s) AND source = 'Spotify'
            """, (release_ids,))
            imagery = {str(row['release_id']): dict(row) for row in cur.fetchall()}

    return {
        'releases': releases,
        'tracks': tracks,
        'release_links': release_links,
        'imagery': imagery,
        'recording_releases': recording_releases,
    }


def _release_info(release_id: Optional[str], before: dict, after: dict) -> dict:
    """Build a release descriptor preferring the after-snapshot."""
    if not release_id:
        return {}
    rel = after['releases'].get(release_id) or before['releases'].get(release_id) or {}
    return {
        'id': release_id,
        'title': rel.get('title'),
        'year': rel.get('release_year'),
        'musicbrainz_id': rel.get('musicbrainz_release_id'),
    }


def _diff_snapshots(before: dict, after: dict) -> list:
    """Produce a list of change records by diffing two snapshots."""
    changes = []

    # --- Track-level (recording_release_streaming_links, service='spotify') ---
    for rr_id in set(before['tracks']) | set(after['tracks']):
        b = before['tracks'].get(rr_id, {}).get('service_id')
        a = after['tracks'].get(rr_id, {}).get('service_id')
        if b == a:
            continue
        if b is None:
            action = 'track_added'
        elif a is None:
            action = 'track_removed'
        else:
            action = 'track_updated'
        ctx = before['recording_releases'].get(rr_id) or after['recording_releases'].get(rr_id) or {}
        rid = str(ctx['release_id']) if ctx.get('release_id') else None
        changes.append({
            'action': action,
            'recording': {
                'id': str(ctx['recording_id']) if ctx.get('recording_id') else None,
                'title': ctx.get('recording_title'),
                'leader': ctx.get('leader_name'),
            },
            'release': _release_info(rid, before, after),
            'recording_release_id': rr_id,
            'spotify_track_id': {'before': b, 'after': a},
        })

    # --- Release streaming links (release_streaming_links, service='spotify') ---
    for rid in set(before['release_links']) | set(after['release_links']):
        b = before['release_links'].get(rid, {}).get('service_id')
        a = after['release_links'].get(rid, {}).get('service_id')
        if b == a:
            continue
        if b is None:
            action = 'release_link_added'
        elif a is None:
            action = 'release_link_removed'
        else:
            action = 'release_link_updated'
        changes.append({
            'action': action,
            'release': _release_info(rid, before, after),
            'spotify_album_id': {'before': b, 'after': a},
        })

    # --- Release imagery (release_imagery, source='Spotify') ---
    for rid in set(before['imagery']) | set(after['imagery']):
        b_row = before['imagery'].get(rid)
        a_row = after['imagery'].get(rid)
        if b_row == a_row:
            continue
        if b_row is None:
            action = 'imagery_added'
            b_id, a_id = None, a_row.get('source_id')
        elif a_row is None:
            action = 'imagery_removed'
            b_id, a_id = b_row.get('source_id'), None
        else:
            action = 'imagery_updated'
            b_id, a_id = b_row.get('source_id'), a_row.get('source_id')
        changes.append({
            'action': action,
            'release': _release_info(rid, before, after),
            'spotify_album_id': {'before': b_id, 'after': a_id},
        })

    return changes


def _compute_post_run_buckets(after: dict) -> dict:
    """
    Classify per-release and per-recording-release state from the AFTER snapshot.

    Two diagnostic buckets:
      - unresolved_releases: releases that have a Spotify album ID set but no
        Spotify track link on ANY of their recording_releases for this song.
        Roughly correlates to the matcher's `releases_no_match` counter.
      - unmatched_recording_releases: recording_releases whose release has a
        Spotify album ID set but the recording_release itself has no Spotify
        track link. Roughly correlates to `tracks_no_match`.
    """
    # release_id -> list of rr_ids on that release for this song
    release_to_rrs: dict = {}
    for rr_id, ctx in after['recording_releases'].items():
        rid = str(ctx['release_id']) if ctx.get('release_id') else None
        if rid:
            release_to_rrs.setdefault(rid, []).append(rr_id)

    release_has_any_track = {
        rid: any(rr in after['tracks'] for rr in rr_ids)
        for rid, rr_ids in release_to_rrs.items()
    }

    unresolved_releases = []
    for rid, rel in after['releases'].items():
        album_id = after['release_links'].get(rid, {}).get('service_id')
        if not album_id:
            continue  # matcher skipped these (nothing to rematch)
        if release_has_any_track.get(rid, False):
            continue
        recordings = []
        for rr_id in release_to_rrs.get(rid, []):
            ctx = after['recording_releases'][rr_id]
            recordings.append({
                'recording_release_id': rr_id,
                'recording_id': str(ctx['recording_id']) if ctx.get('recording_id') else None,
                'title': ctx.get('recording_title'),
                'leader': ctx.get('leader_name'),
            })
        unresolved_releases.append({
            'release': {
                'id': rid,
                'title': rel.get('title'),
                'year': rel.get('release_year'),
                'musicbrainz_id': rel.get('musicbrainz_release_id'),
            },
            'spotify_album_id': album_id,
            'recordings': recordings,
        })

    unmatched_recording_releases = []
    for rr_id, ctx in after['recording_releases'].items():
        rid = str(ctx['release_id']) if ctx.get('release_id') else None
        if not rid:
            continue
        rel = after['releases'].get(rid, {})
        album_id = after['release_links'].get(rid, {}).get('service_id')
        if not album_id:
            continue  # release has no album ID, so no track-level rematch was attempted
        if rr_id in after['tracks']:
            continue
        unmatched_recording_releases.append({
            'recording_release_id': rr_id,
            'recording': {
                'id': str(ctx['recording_id']) if ctx.get('recording_id') else None,
                'title': ctx.get('recording_title'),
                'leader': ctx.get('leader_name'),
            },
            'release': {
                'id': rid,
                'title': rel.get('title'),
                'year': rel.get('release_year'),
                'musicbrainz_id': rel.get('musicbrainz_release_id'),
            },
            'spotify_album_id': album_id,
        })

    return {
        'unresolved_releases': unresolved_releases,
        'unmatched_recording_releases': unmatched_recording_releases,
    }


class _CaptureHandler(logging.Handler):
    """Capture matcher log lines while the run is in-flight."""

    def __init__(self):
        super().__init__()
        self.records: list = []

    def emit(self, record):
        try:
            self.records.append({
                'level': record.levelname,
                'message': record.getMessage(),
            })
        except Exception:  # pragma: no cover - never block the matcher on log capture
            pass


def run_spotify_rematch_for_song(song_id: str, *, logger: Optional[logging.Logger] = None) -> dict:
    """
    Run SpotifyMatcher(rematch_tracks=True) for one song, capturing:
      - a before/after diff of Spotify state (`changes`)
      - post-run DB-derived diagnostic buckets (`unresolved_releases`,
        `unmatched_recording_releases`)
      - every INFO+ log line the matcher emitted during the run (`log_lines`)

    Raises ValueError if the song is not found.
    """
    log = logger or logging.getLogger(__name__)

    song = find_song_by_id(song_id)
    if not song:
        raise ValueError(f"Song not found: {song_id}")

    before = _snapshot_spotify_state(song_id)

    # Dedicated child logger so we can attach a capture handler without
    # affecting the parent's handlers (and still propagating to the server log).
    run_id = uuid.uuid4().hex
    run_logger = log.getChild(f'rematch_{run_id}')
    run_logger.setLevel(logging.DEBUG)
    run_logger.propagate = True

    capture = _CaptureHandler()
    capture.setLevel(logging.INFO)
    run_logger.addHandler(capture)

    try:
        matcher = SpotifyMatcher(rematch_tracks=True, logger=run_logger)
        result = matcher.match_releases(song_id)
    finally:
        run_logger.removeHandler(capture)

    after = _snapshot_spotify_state(song_id)
    changes = _diff_snapshots(before, after)
    buckets = _compute_post_run_buckets(after)

    return {
        'run_id': run_id,
        'ran_at': datetime.now(timezone.utc).isoformat(),
        'song': {
            'id': str(song['id']),
            'title': song.get('title'),
            'composer': song.get('composer'),
        },
        'stats': dict(matcher.stats),
        'matcher_success': bool(result.get('success')),
        'matcher_error': result.get('error'),
        'changes': changes,
        'unresolved_releases': buckets['unresolved_releases'],
        'unmatched_recording_releases': buckets['unmatched_recording_releases'],
        'log_lines': capture.records,
    }


# ----------------------------------------------------------------------------
# Persistence
#
# Runs are serialized to JSON under RUNS_DIR. Filename encodes the timestamp
# first so directory listings sort newest-last lexicographically, and also
# includes song_id + run_id for direct lookup.
# ----------------------------------------------------------------------------

def _default_json(o):
    return str(o)


def _run_filename(run_record: dict) -> str:
    # Compact UTC timestamp, no colons, no dashes.
    ts = run_record['ran_at'].replace(':', '').replace('-', '').split('.')[0]
    return f"{ts}__{run_record['song']['id']}__{run_record['run_id']}.json"


def save_run(run_record: dict) -> Path:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    path = RUNS_DIR / _run_filename(run_record)
    path.write_text(json.dumps(run_record, indent=2, default=_default_json))
    return path


def _read_run_file(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def list_runs_for_song(song_id: str) -> list:
    """Summaries of all runs for a song, newest first."""
    if not RUNS_DIR.exists():
        return []
    summaries = []
    for path in sorted(RUNS_DIR.iterdir(), reverse=True):
        if f"__{song_id}__" not in path.name:
            continue
        data = _read_run_file(path)
        if data is None:
            continue
        summaries.append({
            'run_id': data.get('run_id'),
            'ran_at': data.get('ran_at'),
            'stats': data.get('stats', {}),
            'change_count': len(data.get('changes', [])),
        })
    return summaries


def list_all_runs(limit: int = 50) -> list:
    """Summaries of recent runs across all songs, newest first."""
    if not RUNS_DIR.exists():
        return []
    summaries = []
    for path in sorted(RUNS_DIR.iterdir(), reverse=True):
        data = _read_run_file(path)
        if data is None:
            continue
        summaries.append({
            'run_id': data.get('run_id'),
            'ran_at': data.get('ran_at'),
            'song': data.get('song', {}),
            'change_count': len(data.get('changes', [])),
        })
        if len(summaries) >= limit:
            break
    return summaries


def load_run(run_id: str) -> Optional[dict]:
    """Look up a run by run_id."""
    if not RUNS_DIR.exists():
        return None
    suffix = f"__{run_id}.json"
    for path in RUNS_DIR.iterdir():
        if path.name.endswith(suffix):
            return _read_run_file(path)
    return None
