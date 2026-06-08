-- ============================================================================
-- Migration: Commons imagery enrichment
-- Description:
--   Adds the producer/handler support for the ('commons',
--   'enrich_performer_imagery') research job:
--
--     1. performers.last_imagery_check — when the performer was last swept for
--        Commons imagery. The producer (core/performer_commons_imagery.py)
--        treats NULL or "older than the staleness window" as due; the handler
--        (research_worker/handlers/commons.py) stamps it now() on every
--        completion (even a no-op).
--
--     2. A 'commons' daily quota row in source_quotas, used by the handler to
--        cap the number of paid Claude vision-rerank calls per day. One unit =
--        one image reranked. When the budget is spent the worker reschedules
--        the job for the next reset (QuotaExhausted).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + INSERT ... ON CONFLICT DO NOTHING, so
-- safe to re-run.
--
-- Run: psql $DATABASE_URL -f sql/migrations/add_commons_imagery_enrichment.sql
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. performers.last_imagery_check
-- ----------------------------------------------------------------------------

ALTER TABLE performers
    ADD COLUMN IF NOT EXISTS last_imagery_check TIMESTAMPTZ;

COMMENT ON COLUMN performers.last_imagery_check IS
    'When this performer was last swept for Commons imagery by the '
    '(commons, enrich_performer_imagery) research job. NULL = never checked. '
    'The producer enqueues performers that are NULL or older than the '
    'staleness window (default 90 days); the handler stamps now() on every '
    'completion.';

-- Supports the producer''s "due" scan (NULL-first / oldest-first).
CREATE INDEX IF NOT EXISTS idx_performers_last_imagery_check
    ON performers (last_imagery_check NULLS FIRST);

-- ----------------------------------------------------------------------------
-- 2. 'commons' daily quota (caps paid Claude rerank calls/day)
-- ----------------------------------------------------------------------------
-- units_limit = max images reranked per day across all performers. At the
-- handler default rerank_cap of 12 images/performer, 2000 covers ~166
-- performers/day. Tune with:
--   UPDATE source_quotas SET units_limit = <n> WHERE source = 'commons';
-- resets_at uses the default 'day' window (next UTC midnight), matching
-- research_worker/quota.py's _DEFAULT_RESET_SQL.

INSERT INTO source_quotas (source, window_name, units_used, units_limit, resets_at)
VALUES (
    'commons',
    'day',
    0,
    2000,
    (date_trunc('day', now()) + interval '1 day')
)
ON CONFLICT (source, window_name) DO NOTHING;

COMMIT;

-- ============================================================================
-- ROLLBACK (manual)
-- ============================================================================
-- BEGIN;
--   DROP INDEX IF EXISTS idx_performers_last_imagery_check;
--   ALTER TABLE performers DROP COLUMN IF EXISTS last_imagery_check;
--   DELETE FROM source_quotas WHERE source = 'commons';
-- COMMIT;
