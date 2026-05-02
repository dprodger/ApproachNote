-- ============================================================================
-- Migration: drop the legacy releases.spotify_album_id column
-- Description:
--   Phase C of the legacy-column cleanup (issue #183).
--
--   Phase A (016) backfilled release_streaming_links from the legacy column.
--   Phase B flipped every reader to consume release_streaming_links.
--   This migration drops the now-orphaned column and its index.
--
--   The dual-writes in update_release_spotify_data and
--   clear_release_spotify_data have already been removed from the app, so
--   no live writers reference this column at the moment of the drop.
--
-- Run: psql "$DATABASE_URL" -f sql/migrations/017_drop_releases_spotify_album_id.sql
-- ============================================================================

BEGIN;

DROP INDEX IF EXISTS idx_releases_spotify_album_id;
ALTER TABLE releases DROP COLUMN IF EXISTS spotify_album_id;

COMMIT;
