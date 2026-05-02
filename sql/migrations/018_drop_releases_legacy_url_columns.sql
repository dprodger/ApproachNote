-- ============================================================================
-- Migration: drop legacy unused URL columns from releases (issue #181)
-- Description:
--   releases.cover_art_url, releases.amazon_url, and releases.discogs_url are
--   leftover columns from an early MusicBrainz import shape. No app code
--   writes or reads them today — release artwork lives in release_imagery and
--   external storefront links are not currently surfaced. The only references
--   in the repo were a handful of {% if %} display blocks on the admin
--   release-detail page, which have been removed in the same change.
--
-- Run: psql "$DATABASE_URL" -f sql/migrations/018_drop_releases_legacy_url_columns.sql
-- ============================================================================

BEGIN;

ALTER TABLE releases DROP COLUMN IF EXISTS cover_art_url;
ALTER TABLE releases DROP COLUMN IF EXISTS amazon_url;
ALTER TABLE releases DROP COLUMN IF EXISTS discogs_url;

COMMIT;
