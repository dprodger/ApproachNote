-- ============================================================================
-- DB forensics + audit-posture check
--
-- Queries to answer "who is deleting things from my database, and do I have
-- the logging in place to catch it next time?"
--
-- Usage:
--     psql $DATABASE_URL -f sql/diagnostics/db_forensics.sql
--
-- Or paste into the Supabase SQL Editor one section at a time (the
-- editor only displays results from one statement per run, so the
-- section-header SELECTs would crowd out the data query — paste just
-- the data SELECT after each `-- SECTION N` comment block).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- SECTION 1: Stats reset timestamp
--
-- pg_stat_statements and pg_stat_user_tables are useful only for activity
-- since their last reset. If stats_reset is recent (e.g. right when you
-- restored the DB), counters before that are gone forever.
-- ----------------------------------------------------------------------------

SELECT '=== 1. Stats reset timestamp ===' AS "label";

SELECT datname,
       stats_reset,
       NOW() - stats_reset AS age
FROM pg_stat_database
WHERE datname = current_database();


-- ----------------------------------------------------------------------------
-- SECTION 2: pg_stat_statements — retrospective search for destructive SQL
--
-- If the original wipe statement is still in the cache, it'll show up
-- here. pg_stat_statements doesn't timestamp individual calls, but a
-- non-zero `calls` with a normalized TRUNCATE / DELETE text is a smoking
-- gun.
--
-- The extension is enabled by default on Supabase. If this section
-- returns "relation pg_stat_statements does not exist", run:
--     CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- and then come back.
-- ----------------------------------------------------------------------------

SELECT '=== 2. pg_stat_statements: destructive queries ===' AS "label";

SELECT calls,
       rows AS rows_affected,
       ROUND(total_exec_time::numeric, 1) AS total_ms,
       ROUND(mean_exec_time::numeric, 2) AS mean_ms,
       LEFT(query, 200) AS query_excerpt
FROM pg_stat_statements
WHERE query ILIKE '%truncate%'
   OR query ILIKE '%delete from recording_release_streaming%'
   OR query ILIKE '%delete from release_streaming%'
   OR query ILIKE '%delete from recording_releases%'
   OR query ILIKE '%delete from releases%'
ORDER BY calls DESC, rows_affected DESC
LIMIT 50;


-- ----------------------------------------------------------------------------
-- SECTION 3: Per-table DML activity on sensitive tables
--
-- Cumulative insert / update / delete counts since the last stats reset.
-- n_live_tup only means something after an ANALYZE has run (see
-- last_analyze column). The column to watch is n_tup_del — a sudden
-- spike in the hundreds or thousands is the watchdog signal.
-- ----------------------------------------------------------------------------

SELECT '=== 3. DML activity on sensitive tables ===' AS "label";

SELECT relname,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       n_live_tup,
       last_vacuum,
       last_autovacuum,
       last_analyze,
       last_autoanalyze
FROM pg_stat_user_tables
WHERE relname IN (
    'recording_release_streaming_links',
    'release_streaming_links',
    'recording_releases',
    'releases',
    'recordings',
    'songs'
)
ORDER BY relname;


-- ----------------------------------------------------------------------------
-- SECTION 4: Which roles can DELETE / TRUNCATE the sensitive tables
--
-- If anything more than the app's expected role has these privileges,
-- that's worth knowing. Supabase's default roles are `anon`,
-- `authenticated`, `service_role`, `authenticator`, `postgres`, plus any
-- you've added yourself.
-- ----------------------------------------------------------------------------

SELECT '=== 4. DELETE/TRUNCATE privileges on streaming_links tables ===' AS "label";

SELECT table_name,
       grantee,
       privilege_type,
       is_grantable
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name IN (
      'recording_release_streaming_links',
      'release_streaming_links',
      'recording_releases',
      'releases'
  )
  AND privilege_type IN ('DELETE', 'TRUNCATE', 'ALL')
ORDER BY table_name, grantee, privilege_type;


-- ----------------------------------------------------------------------------
-- SECTION 5: Current logging configuration
--
-- What statement logging is currently on. If any of these look too quiet
-- for a forensics posture, Section 7 has the ALTER SYSTEM commands to
-- turn them up.
-- ----------------------------------------------------------------------------

SELECT '=== 5. Current logging configuration ===' AS "label";

SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE name IN (
    'log_statement',             -- none/ddl/mod/all
    'log_min_duration_statement',
    'log_connections',
    'log_disconnections',
    'log_hostname',
    'log_line_prefix',
    'log_destination',
    'logging_collector'
)
ORDER BY name;


-- ----------------------------------------------------------------------------
-- SECTION 6: Is pgAudit installed?
-- ----------------------------------------------------------------------------

SELECT '=== 6. pgAudit extension status ===' AS "label";

SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('pgaudit', 'pg_stat_statements')
ORDER BY extname;


-- ----------------------------------------------------------------------------
-- SECTION 7: Suggested follow-up actions (commented out — intentionally)
--
-- Uncomment any of these, review them, and run them if you want to
-- harden the logging posture. None of them delete data.
-- ----------------------------------------------------------------------------

SELECT '=== 7. Suggested hardening (not executed) ===' AS "label";

-- Turn on statement-level logging for every mutating statement.
--   ALTER SYSTEM SET log_statement = 'mod';
--
-- Log every new connection + disconnection + host, so "who ran that
-- statement" is answerable by IP + app_name.
--   ALTER SYSTEM SET log_connections = 'on';
--   ALTER SYSTEM SET log_disconnections = 'on';
--   ALTER SYSTEM SET log_hostname = 'on';
--
-- Prefix every log line with timestamp, pid, user, db, application_name,
-- and client_addr. Makes grepping the Supabase log UI much more useful.
--   ALTER SYSTEM SET log_line_prefix = '%t [%p] user=%u,db=%d,app=%a,client=%h ';
--
-- After any ALTER SYSTEM above, reload to apply without restart:
--   SELECT pg_reload_conf();
--
-- If you want structured audit logging beyond log_statement, enable
-- pgAudit and tell it what categories to capture:
--   CREATE EXTENSION IF NOT EXISTS pgaudit;
--   ALTER SYSTEM SET pgaudit.log = 'write, ddl, role';
--   SELECT pg_reload_conf();
--
-- If Section 2 showed pg_stat_statements was missing:
--   CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
