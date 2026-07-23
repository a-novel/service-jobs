CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Retention purge: delete settled jobs once their retention window has passed. expires_at is stamped
-- on settle and on reaper-abandon, seven days out — the window in which a retry may still arrive, not
-- a forensics window. The statement carries no logic a Go loop would, so pg_cron runs it in the
-- database. The predicate is served by jobs_retention_idx, so a sweep that finds nothing costs an
-- index probe.
--
-- The job name is service-scoped on purpose: pg_cron job names are global to the PostgreSQL instance,
-- so a generic 'purge' would collide with any other service sharing it. cron.schedule upserts by
-- name, so re-running this never creates a second job.
--
-- This is scheduled here, in the image's init SQL, rather than in a migration. cron.database_name
-- names exactly one database, but postgres.RunDBTest clones a fresh database per test with no cron
-- schema, so a migration calling cron.schedule would fail every DAO test. Init SQL runs once on the
-- real database and never on a test clone. The tradeoff — the schedule does not travel with the
-- migrations, so a bring-your-own-Postgres deployment must schedule it itself — is recorded in
-- CONTRIBUTING.md.
SELECT
  cron.schedule (
    'service-jobs-purge-settled',
    '*/10 * * * *',
    $$DELETE FROM jobs WHERE expires_at IS NOT NULL AND expires_at < clock_timestamp()$$
  );
