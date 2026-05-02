-- ============================================================================
-- Migration: backfill release_streaming_links from releases.spotify_album_id
-- Description:
--   Phase A of the legacy-column cleanup (issue #181 / spinoff from #177).
--
--   Background: release_streaming_links is the normalized table holding
--   per-service album-level matches. The pre-normalization code wrote
--   only to releases.spotify_album_id, leaving ~27.5k releases with the
--   legacy column populated but no normalized row. The current matcher
--   dual-writes both, so newly matched releases stay in sync — but the
--   legacy data was never backfilled.
--
--   This migration copies every populated releases.spotify_album_id into
--   release_streaming_links (service='spotify', match_method='legacy_backfill')
--   where a normalized row doesn't already exist. INSERT ... NOT EXISTS,
--   so it's idempotent: re-running is a no-op.
--
--   Scope: ~27,500 rows on the production DB at the time of writing.
--
--   What this does NOT do:
--   - Drop the legacy column. Many readers still consume it; that's
--     Phase B (flip readers) and Phase C (drop the column).
--   - Touch recording-level (track) streaming links. Those are already
--     in recording_release_streaming_links and don't need a backfill.
--
--   match_method='legacy_backfill' is deliberate so a future cleanup can
--   identify these rows (e.g., to re-validate the album_ids by hitting
--   Spotify's API). matched_at is set to the migration time, not to the
--   long-lost original match time which we don't have.
--
-- Run: psql "$DATABASE_URL" -f sql/migrations/016_backfill_release_streaming_links_spotify.sql
-- ============================================================================

BEGIN;

DO $$
DECLARE
    pre_count   integer;
    post_count  integer;
    inserted    integer;
BEGIN
    -- Snapshot the "before" count so we can report what changed.
    SELECT COUNT(*) INTO pre_count
    FROM release_streaming_links
    WHERE service = 'spotify';

    INSERT INTO release_streaming_links (
        release_id,
        service,
        service_id,
        service_url,
        match_method,
        matched_at,
        created_at,
        updated_at
    )
    SELECT
        rel.id,
        'spotify',
        rel.spotify_album_id,
        'https://open.spotify.com/album/' || rel.spotify_album_id,
        'legacy_backfill',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM releases rel
    WHERE rel.spotify_album_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM release_streaming_links rsl
          WHERE rsl.release_id = rel.id
            AND rsl.service = 'spotify'
      );

    GET DIAGNOSTICS inserted = ROW_COUNT;

    SELECT COUNT(*) INTO post_count
    FROM release_streaming_links
    WHERE service = 'spotify';

    RAISE NOTICE 'release_streaming_links spotify rows: % before, % after (% inserted)',
        pre_count, post_count, inserted;
END $$;

COMMIT;
