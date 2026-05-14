-- ----------------------------------------------------------------------------
-- 019_youtube_quota_increase.sql
--
-- Bump the YouTube daily quota from 10,000 to 1,000,000 units after Google
-- approved a quota increase request for the YouTube Data API v3 project
-- used by the research worker. The original seed in 015_research_jobs.sql
-- stays at 10k so a fresh DB rebuild starts at the default tier; this
-- migration carries forward the grant.
-- ----------------------------------------------------------------------------

UPDATE source_quotas
SET units_limit = 1000000,
    updated_at  = now()
WHERE source = 'youtube'
  AND window_name = 'day';
