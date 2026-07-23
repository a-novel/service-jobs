-- The guard is the whole safety of settle: id, the worker that holds the claim, and the claimed
-- status together. A job the reaper already recovered has a different claimed_by, or is no longer
-- claimed, so a worker settling stale work matches no row and overwrites nothing.
--
-- Setting a terminal status while clearing the lease and stamping settled_at and expires_at is what
-- the jobs_lease_matches_status and jobs_terminal_fields constraints jointly require; a partial
-- write is rejected by the database. expires_at is the retention horizon the scheduled purge reads.
UPDATE jobs
SET
  status = ?2::job_status,
  result = ?3,
  error = ?4,
  claimed_by = NULL,
  lease_expires_at = NULL,
  settled_at = clock_timestamp(),
  expires_at = clock_timestamp() + make_interval(days => ?5),
  updated_at = clock_timestamp()
WHERE
  id = ?0
  AND claimed_by = ?1
  AND status = 'claimed'
RETURNING
  *;
