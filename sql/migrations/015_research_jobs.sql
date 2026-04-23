-- ============================================================================
-- Migration: research_jobs + source_quotas
-- Description:
--   Introduces a durable, Postgres-backed job queue for background research
--   tasks (YouTube search, eventually Spotify/Apple/MusicBrainz matching).
--   Replaces the in-process `core.research_queue` thread with a separate
--   worker service that claims jobs via SELECT ... FOR UPDATE SKIP LOCKED.
--
--   This migration only adds the tables. Producer/worker code lives in
--   backend/core/research_jobs.py and backend/research_worker/.
--
--   First consumer: YouTube. Other sources move over piecemeal.
--
-- Run: psql $DATABASE_URL -f sql/migrations/015_research_jobs.sql
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- research_jobs: one row per queued/in-flight/finished unit of work.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS research_jobs (
    id            BIGSERIAL PRIMARY KEY,

    source        TEXT        NOT NULL,                    -- 'youtube', 'spotify', ...
    job_type      TEXT        NOT NULL,                    -- 'search_song', 'fetch_video_meta', ...
    target_type   TEXT        NOT NULL,                    -- 'song' | 'recording' | 'performer' | 'release'
    target_id     UUID        NOT NULL,                    -- UUID of the target row

    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    result        JSONB,                                   -- handler return value, on success

    status        TEXT        NOT NULL DEFAULT 'queued',
    priority      SMALLINT    NOT NULL DEFAULT 100,        -- lower runs sooner
    attempts      SMALLINT    NOT NULL DEFAULT 0,
    max_attempts  SMALLINT    NOT NULL DEFAULT 5,

    run_after     TIMESTAMPTZ NOT NULL DEFAULT now(),      -- earliest eligible time
    claimed_at    TIMESTAMPTZ,
    claimed_by    TEXT,                                    -- worker id (host:pid:thread)
    finished_at   TIMESTAMPTZ,
    last_error    TEXT,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT research_jobs_status_chk
        CHECK (status IN ('queued', 'running', 'done', 'failed', 'dead'))
);

COMMENT ON TABLE research_jobs IS
    'Durable background job queue for per-source research work (YouTube, Spotify, etc). '
    'Worker claims via SELECT ... FOR UPDATE SKIP LOCKED filtered by source.';

COMMENT ON COLUMN research_jobs.status IS
    'queued: waiting to be claimed. running: claimed by a worker. done: succeeded. '
    'failed: handler raised; will be retried if attempts < max_attempts. '
    'dead: exhausted retries OR cancelled by an admin.';

COMMENT ON COLUMN research_jobs.run_after IS
    'Earliest time this job is eligible to be claimed. Used for backoff '
    '(set to now() + delay on retry) and quota deferral (set to quota '
    'reset time when source is exhausted).';

COMMENT ON COLUMN research_jobs.claimed_by IS
    'Worker identity of the form "<host>:<pid>:<thread>". Set when status '
    'transitions to running so the janitor can reap stuck jobs.';

-- Hot path: claim oldest eligible job for a given source.
CREATE INDEX IF NOT EXISTS research_jobs_claim_idx
    ON research_jobs (source, run_after, priority)
    WHERE status = 'queued';

-- Idempotency: at most one queued/running job per (source, type, target).
-- Producers use ON CONFLICT DO NOTHING so duplicate enqueues collapse silently.
CREATE UNIQUE INDEX IF NOT EXISTS research_jobs_dedup_idx
    ON research_jobs (source, job_type, target_type, target_id)
    WHERE status IN ('queued', 'running');

-- Per-target lookup powers the client-facing research_status endpoint.
CREATE INDEX IF NOT EXISTS research_jobs_target_idx
    ON research_jobs (target_type, target_id, source, finished_at DESC);

-- Janitor sweeps for stuck `running` jobs and old `done`/`dead` rows.
CREATE INDEX IF NOT EXISTS research_jobs_janitor_idx
    ON research_jobs (status, claimed_at)
    WHERE status = 'running';


-- ----------------------------------------------------------------------------
-- source_quotas: token-bucket state per (source, window).
-- One row per quota window we want to enforce. For YouTube we only care
-- about the daily quota (10,000 units, resets at midnight Pacific Time).
-- Per-second/per-100s quotas can be added as additional rows later.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS source_quotas (
    source       TEXT        NOT NULL,
    window_name  TEXT        NOT NULL,                     -- 'day' | 'minute' | 'second'
    units_used   INTEGER     NOT NULL DEFAULT 0,
    units_limit  INTEGER     NOT NULL,
    resets_at    TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (source, window_name),

    CONSTRAINT source_quotas_units_chk CHECK (units_used >= 0)
);

COMMENT ON TABLE source_quotas IS
    'Per-source quota counters. Worker decrements via a single UPDATE that '
    'returns zero rows when the budget would be exceeded; that signals '
    'QuotaExhausted and the worker reschedules the job for resets_at.';

COMMENT ON COLUMN source_quotas.resets_at IS
    'When this window rolls over. For YouTube ''day'' this is the next '
    'midnight Pacific Time. The worker resets units_used to 0 and pushes '
    'resets_at forward when it observes resets_at <= now().';

-- Seed the YouTube daily quota row. resets_at is set to next midnight
-- Pacific Time using the date math; AT TIME ZONE handles DST correctly.
INSERT INTO source_quotas (source, window_name, units_used, units_limit, resets_at)
VALUES (
    'youtube',
    'day',
    0,
    10000,
    -- Today's midnight in Los Angeles, plus one day, expressed as UTC.
    ((date_trunc('day', now() AT TIME ZONE 'America/Los_Angeles')
      + interval '1 day') AT TIME ZONE 'America/Los_Angeles')
)
ON CONFLICT (source, window_name) DO NOTHING;


-- ----------------------------------------------------------------------------
-- updated_at triggers — keep both tables honest without app code remembering.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION research_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS research_jobs_set_updated_at ON research_jobs;
CREATE TRIGGER research_jobs_set_updated_at
    BEFORE UPDATE ON research_jobs
    FOR EACH ROW
    EXECUTE FUNCTION research_set_updated_at();

DROP TRIGGER IF EXISTS source_quotas_set_updated_at ON source_quotas;
CREATE TRIGGER source_quotas_set_updated_at
    BEFORE UPDATE ON source_quotas
    FOR EACH ROW
    EXECUTE FUNCTION research_set_updated_at();

COMMIT;

-- ============================================================================
-- ROLLBACK (if needed)
-- ============================================================================
-- BEGIN;
-- DROP TRIGGER IF EXISTS source_quotas_set_updated_at ON source_quotas;
-- DROP TRIGGER IF EXISTS research_jobs_set_updated_at ON research_jobs;
-- DROP FUNCTION IF EXISTS research_set_updated_at();
-- DROP TABLE IF EXISTS source_quotas;
-- DROP TABLE IF EXISTS research_jobs;
-- COMMIT;
