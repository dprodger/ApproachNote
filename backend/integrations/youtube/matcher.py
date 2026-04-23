"""
YouTubeMatcher: orchestrates per-recording YouTube match flow.

One recording in, one YouTube match (or none) out. Separate method for
matching every recording of a song. Callers:
- scripts/match_youtube_videos.py (CLI)
- eventually core/song_research.py once we're happy with match quality

Flow for a single recording:
1. Load row from db.py (song title, artist credit, duration, recording_release_id).
2. Skip if we already have a non-manual match and not in --rematch mode.
3. Build a search query ladder and fire the first that has the highest
   chance of success — but only ONE search.list (100 units) per recording
   by default. If --exhaustive is set, walk the ladder until a match is
   found.
4. Fetch video details for the top-N candidates (1 unit).
5. Score via matching.pick_best. Mode chosen based on whether MB duration
   is populated.
6. Write via db.upsert_youtube_for_recording (fans out across all
   recording_releases).
"""

import logging
from typing import Any, Dict, List, Optional

from integrations.spotify.matching import (
    extract_primary_artist,
    strip_ensemble_suffix,
)
from integrations.youtube import db as yt_db
from integrations.youtube.client import (
    YouTubeClient,
    YouTubeQuotaExceededError,
    build_youtube_video_url,
)
from integrations.youtube.matching import pick_best


class YouTubeMatcher:
    """Orchestrates per-recording matching. Stateless aside from stats + client."""

    def __init__(
        self,
        client: Optional[YouTubeClient] = None,
        dry_run: bool = False,
        rematch: bool = False,
        search_results: int = 8,
        logger: Optional[logging.Logger] = None,
        **client_kwargs: Any,
    ):
        """
        Args:
            client: A YouTubeClient instance. If None, one is built here.
            dry_run: Don't write to DB.
            rematch: Re-evaluate recordings that already have a non-manual
                youtube link. Manual overrides are always preserved.
            search_results: How many results to request per search.list.
            client_kwargs: Forwarded to YouTubeClient if we build one.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.client = client or YouTubeClient(logger=self.logger, **client_kwargs)
        self.dry_run = dry_run
        self.rematch = rematch
        self.search_results = search_results

        self.stats = {
            'recordings_processed': 0,
            'recordings_matched': 0,
            'recordings_no_match': 0,
            'recordings_skipped': 0,
            'recordings_skipped_no_default_release': 0,
            'recordings_skipped_manual': 0,
            'errors': 0,
            # Mirrored from the client at end of run:
            'cache_hits': 0,
            'api_calls': 0,
            'quota_units': 0,
        }

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def match_recording(self, recording_id: str) -> Dict[str, Any]:
        """Match a single recording. Returns {success, recording, match?, detail}."""
        row = yt_db.load_recording(recording_id)
        if not row:
            return {'success': False, 'message': f'Recording not found: {recording_id}'}
        return self._process_recording(row)

    def match_song(self, song_id: str) -> Dict[str, Any]:
        """Match every recording of a song. Returns aggregate {success, stats, results}."""
        rows = yt_db.load_recordings_for_song(song_id)
        if not rows:
            return {'success': False, 'message': f'No recordings found for song {song_id}'}

        self.logger.info(f"Processing {len(rows)} recording(s) for song {song_id}")
        results: List[Dict[str, Any]] = []
        for i, row in enumerate(rows, 1):
            self.logger.info(
                f"\n[{i}/{len(rows)}] {row['recording_title'] or row['song_title']} "
                f"— {row.get('default_release_artist') or '?'} "
                f"— {row.get('duration_ms')}ms"
            )
            try:
                results.append(self._process_recording(row))
            except YouTubeQuotaExceededError as e:
                self.logger.error(f"  Quota exhausted, stopping: {e}")
                self.stats['errors'] += 1
                break
            except Exception as e:
                self.logger.exception(f"  Error processing recording: {e}")
                self.stats['errors'] += 1

        self._aggregate_client_stats()
        return {
            'success': True,
            'song_id': song_id,
            'stats': self.stats,
            'results': results,
        }

    # ------------------------------------------------------------------
    # Per-recording flow
    # ------------------------------------------------------------------

    def _process_recording(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Return a result dict for one recording. Always safe to keep iterating."""
        self.stats['recordings_processed'] += 1
        recording_id = str(row['recording_id'])
        song_title = row['song_title']
        expected_artist = row.get('default_release_artist') or ''
        expected_duration_ms = row.get('duration_ms')

        if not row.get('default_recording_release_id'):
            self.logger.info("  → skip: no default recording_release row")
            self.stats['recordings_skipped'] += 1
            self.stats['recordings_skipped_no_default_release'] += 1
            return {
                'recording_id': recording_id, 'matched': False,
                'skipped': 'no_default_release',
            }

        if row.get('has_youtube') and not self.rematch:
            self.logger.info("  → skip: already has youtube link (use --rematch to re-evaluate)")
            self.stats['recordings_skipped'] += 1
            return {'recording_id': recording_id, 'matched': False, 'skipped': 'has_youtube'}

        # Build the query ladder, fire every query, collect unique video IDs,
        # score them all together. This is *always* exhaustive by design —
        # a later query can yield a much higher-confidence match (e.g. a
        # primary-artist-only query finding a Topic channel) and first-match
        # leaves that on the table. Deduping IDs across queries caps the
        # videos.list cost at 1 unit no matter how many searches fire.
        queries = self._build_queries(song_title, expected_artist)
        self.logger.debug(f"  query ladder: {queries}")

        unique_ids: List[str] = []
        seen_ids = set()
        for query in queries:
            try:
                hits = self.client.search(query, max_results=self.search_results)
            except YouTubeQuotaExceededError:
                raise
            if not hits:
                self.logger.debug(f"    no results for: {query}")
                continue
            for h in hits:
                vid = h.get('videoId')
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    unique_ids.append(vid)

        if not unique_ids:
            self.stats['recordings_no_match'] += 1
            self.logger.info("  ✗ no candidates returned")
            return {'recording_id': recording_id, 'matched': False, 'rejected': []}

        details = self.client.get_videos(unique_ids)
        best_match, best_scored = pick_best(
            details, song_title, expected_artist, expected_duration_ms
        )

        if not best_match:
            self.stats['recordings_no_match'] += 1
            self.logger.info(f"  ✗ no match across {len(details)} candidate(s):")
            for c in best_scored[:3]:
                self.logger.info(
                    f"      - {(c.get('video_title') or '')[:60]} "
                    f"(channel={c.get('channel')}) → {c.get('rejected_reason')}"
                )
            return {
                'recording_id': recording_id, 'matched': False,
                'rejected': best_scored[:5],
            }

        self.logger.info(
            f"  ✓ match: {best_match['title'][:60]} "
            f"(channel={best_match['channelTitle']!r}, conf={best_match['_match_confidence']:.2f})"
        )

        # Write.
        mode = 'duration_match' if expected_duration_ms else 'conservative'
        rows_written = yt_db.upsert_youtube_for_recording(
            recording_id=recording_id,
            video_id=best_match['videoId'],
            video_title=best_match['title'],
            duration_ms=best_match.get('duration_ms'),
            match_confidence=best_match['_match_confidence'],
            match_method=f'youtube_{mode}',
            dry_run=self.dry_run,
            log=self.logger,
        )
        self.stats['recordings_matched'] += 1
        return {
            'recording_id': recording_id,
            'matched': True,
            'video_id': best_match['videoId'],
            'video_url': build_youtube_video_url(best_match['videoId']),
            'video_title': best_match['title'],
            'channel': best_match['channelTitle'],
            'confidence': best_match['_match_confidence'],
            'rows_written': rows_written,
            'match_detail': best_match['_match_detail'],
        }

    def _build_queries(self, song_title: str, expected_artist: str) -> List[str]:
        """
        Ladder of search queries, broadest-to-narrowest. Every query is
        fired (see _process_recording); this just controls the spread of
        candidates we'll see at scoring time.

        Earlier experimentation tried appending ' - Topic' to nudge YouTube
        toward Topic channels — it didn't work (the string is treated as
        a literal search term, not a channel filter), so we dropped it.
        Scoring picks up Topic channels organically via the channelTitle
        suffix check.
        """
        queries: List[str] = []
        if expected_artist:
            queries.append(f'"{song_title}" "{expected_artist}"')

            primary = extract_primary_artist(expected_artist)
            if primary and primary != expected_artist:
                queries.append(f'"{song_title}" "{primary}"')

            stripped = strip_ensemble_suffix(expected_artist)
            if stripped and stripped not in (expected_artist, primary or ''):
                queries.append(f'"{song_title}" "{stripped}"')
        else:
            queries.append(f'"{song_title}"')

        # De-dupe while preserving order.
        seen = set()
        deduped = []
        for q in queries:
            if q not in seen:
                seen.add(q)
                deduped.append(q)
        return deduped

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def _aggregate_client_stats(self) -> None:
        self.stats['cache_hits'] = self.client.stats.get('cache_hits', 0)
        self.stats['api_calls'] = self.client.stats.get('api_calls', 0)
        self.stats['quota_units'] = self.client.stats.get('quota_units', 0)
