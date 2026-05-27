-- Migration: Drop label column from recordings table
-- Date: 2026-05-27
--
-- The recordings.label column is being removed in favor of always reading the
-- label from the release being displayed (recordings.default_release_id ->
-- releases.label, or whichever release the user has selected in the UI).
--
-- Labels are an attribute of a release in MusicBrainz, not of a recording —
-- a recording reissued on a different label has a different label depending
-- on which release you're looking at. Storing a single denormalized value
-- on the recording invited the kind of mistake this migration corrects:
-- the column was never populated, and any backfill would have had to pick
-- an arbitrary "winning" release.
--
-- Before running this migration, ensure all code references to
-- recordings.label have been updated to read from releases.label instead.

-- ============================================================================
-- PRE-MIGRATION CHECK: Report any rows that have a label set
-- ============================================================================

DO $$
DECLARE
    populated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO populated_count
    FROM recordings
    WHERE label IS NOT NULL;

    IF populated_count > 0 THEN
        RAISE NOTICE '% recordings have a non-null label that will be dropped', populated_count;
    ELSE
        RAISE NOTICE 'No recordings have a populated label — column was never used';
    END IF;
END $$;

-- ============================================================================
-- MIGRATION: Drop the column
-- ============================================================================

ALTER TABLE recordings DROP COLUMN IF EXISTS label CASCADE;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'recordings' AND column_name = 'label'
    ) THEN
        RAISE EXCEPTION 'Migration failed: label column still exists';
    ELSE
        RAISE NOTICE 'Migration successful: label column has been dropped from recordings table';
    END IF;
END $$;


