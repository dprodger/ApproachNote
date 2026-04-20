"""
Spotify Matcher Diagnostics

Out-of-band recorders the matcher uses while it runs: the failure-cache
(skip known "album-matched-but-track-not-found" combos on reruns) and the
CSV audit loggers that let us review borderline matching decisions after a
batch completes.

Split out of SpotifyMatcher per #115 step 5 so step 3 (fold validation into
matching.py) can touch the scoring paths without also moving the loggers
they happen to call.

Shape: free functions that take `client` and/or `logger` explicitly. The
failure-cache functions use the SpotifyClient's cache directory and cache
validity check, so they take the client. The CSV loggers only need a
logger (for the warning-on-exception path) and a set of scalar values,
which is why they're plain append-a-row functions with no client handle.

CSV files are written relative to the current working directory, same as
before — callers running matcher scripts expect the files to appear next
to wherever they ran from.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Track-match failure cache
#
# When an album matches but no track inside it matches, we remember that for
# (song_id, release_id, spotify_album_id) so a re-run can skip the DB round
# trip and Spotify album-tracks fetch. Entries live under the client's
# cache dir in a `track_failures/` subdirectory as JSON blobs.
# ---------------------------------------------------------------------------

def get_track_match_failure_cache_path(client, song_id: str, release_id: str,
                                       spotify_album_id: str) -> Path:
    """Deterministic cache-file path for a (song, release, spotify album) tuple."""
    failure_cache_dir = client.cache_dir / 'track_failures'
    failure_cache_dir.mkdir(parents=True, exist_ok=True)

    # Convert UUIDs to strings so the filename is stable regardless of caller type
    filename = f"fail_{str(song_id)}_{str(release_id)}_{str(spotify_album_id)}.json"
    return failure_cache_dir / filename


def is_track_match_cached_failure(client, log: logging.Logger,
                                  song_id: str, release_id: str,
                                  spotify_album_id: str) -> bool:
    """
    True if we've already recorded this combination as a "no track match".
    Increments client.stats['cache_hits'] on a hit so aggregate stats stay
    consistent with other cache paths.
    """
    cache_path = get_track_match_failure_cache_path(client, song_id, release_id, spotify_album_id)

    if client.force_refresh:
        return False

    if not cache_path.exists():
        return False

    # Stale entries fall through to a fresh attempt.
    if client._is_cache_valid(cache_path):
        client.stats['cache_hits'] = client.stats.get('cache_hits', 0) + 1
        log.debug(f"    Track match failure cache hit")
        return True

    return False


def cache_track_match_failure(client, log: logging.Logger,
                              song_id: str, release_id: str,
                              spotify_album_id: str, song_title: str) -> None:
    """Record a "no track match" outcome so future runs skip the work."""
    cache_path = get_track_match_failure_cache_path(client, song_id, release_id, spotify_album_id)

    try:
        with open(cache_path, 'w') as f:
            json.dump({
                'song_id': str(song_id),
                'release_id': str(release_id),
                'spotify_album_id': str(spotify_album_id),
                'song_title': song_title,
                'result': 'no_track_match'
            }, f)
        log.debug(f"    Cached track match failure")
    except Exception as e:
        log.warning(f"    Failed to cache track match failure: {e}")


# ---------------------------------------------------------------------------
# CSV audit loggers
#
# Append-a-row outputs used for post-run review of borderline match decisions.
# Each creates the CSV with a header row on first write. Exceptions are
# swallowed with a warning so a disk-full or permissions issue can't kill an
# in-progress batch match.
# ---------------------------------------------------------------------------

def log_duration_rejection(log: logging.Logger, *,
                           song_title: str, recording_id: str, release_id: str,
                           spotify_track_id: str, spotify_track_name: str,
                           expected_ms: int, actual_ms: int,
                           confidence: float, title_score: float) -> None:
    """Log a track rejected for low duration confidence to spotify_duration_rejections.csv."""
    log_file = Path('spotify_duration_rejections.csv')
    file_exists = log_file.exists()

    try:
        with open(log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    'timestamp', 'song_title', 'recording_id', 'release_id',
                    'spotify_track_id', 'spotify_track_name', 'spotify_url',
                    'mb_duration_sec', 'spotify_duration_sec', 'diff_sec',
                    'confidence', 'title_score'
                ])
            diff_sec = abs(expected_ms - actual_ms) / 1000.0
            writer.writerow([
                datetime.now().isoformat(),
                song_title,
                str(recording_id),
                str(release_id),
                spotify_track_id,
                spotify_track_name,
                f'https://open.spotify.com/track/{spotify_track_id}',
                round(expected_ms / 1000.0, 1),
                round(actual_ms / 1000.0, 1),
                round(diff_sec, 1),
                confidence,
                title_score
            ])
    except Exception as e:
        log.warning(f"    Failed to log duration rejection: {e}")


def log_orphaned_track(log: logging.Logger, *,
                       release_id: str, recording_id: str,
                       spotify_track_url: str) -> None:
    """Log a stale track link cleared during rematch to spotify_orphaned_tracks.csv."""
    log_file = Path('spotify_orphaned_tracks.csv')
    file_exists = log_file.exists()

    try:
        with open(log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'release_id', 'recording_id', 'spotify_track_url'])
            writer.writerow([
                datetime.now().isoformat(),
                str(release_id),
                str(recording_id),
                spotify_track_url
            ])
    except Exception as e:
        log.warning(f"    Failed to log orphaned track: {e}")


def log_album_context_audit(log: logging.Logger, *,
                            song_title: str, recording_id: str, release_id: str,
                            spotify_track_id: str, spotify_track_name: str,
                            expected_ms: int, actual_ms: int,
                            confidence: float, title_score: float,
                            album_context: dict, would_rescue: bool) -> None:
    """Log an album-context rescue evaluation to album_context_audit.csv."""
    log_file = Path('album_context_audit.csv')
    file_exists = log_file.exists()

    try:
        with open(log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    'timestamp', 'song_title', 'recording_id', 'release_id',
                    'spotify_track_id', 'spotify_track_name', 'spotify_url',
                    'mb_duration_sec', 'spotify_duration_sec', 'diff_sec',
                    'duration_confidence', 'title_score',
                    'mb_track_count', 'spotify_track_count',
                    'tracklist_matched', 'tracklist_match_ratio',
                    'would_rescue',
                ])
            diff_sec = abs(expected_ms - actual_ms) / 1000.0
            writer.writerow([
                datetime.now().isoformat(),
                song_title,
                str(recording_id),
                str(release_id),
                spotify_track_id,
                spotify_track_name,
                f'https://open.spotify.com/track/{spotify_track_id}',
                round(expected_ms / 1000.0, 1),
                round(actual_ms / 1000.0, 1),
                round(diff_sec, 1),
                confidence,
                title_score,
                album_context['mb_track_count'],
                album_context['spotify_track_count'],
                album_context['matched_count'],
                round(album_context['match_ratio'], 2),
                would_rescue,
            ])
    except Exception as e:
        log.warning(f"    Failed to log album context audit: {e}")
