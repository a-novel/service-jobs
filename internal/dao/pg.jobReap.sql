-- Lease-driven recovery: every job whose claim has lapsed, in one sweep. The predicate matches
-- jobs_lease_idx, so the scan does not grow with the settled rows around it.
--
-- The fate of each reaped job turns on whether an attempt remains. attempt was incremented at claim,
-- so a job at its cap has already used every run it was granted: it settles abandoned, stamping
-- settled_at and expires_at and taking the supplied error. A job with an attempt left returns to
-- pending, immediately claimable, with settled_at and expires_at left null. The CASE expressions
-- keep both branches inside the jobs_terminal_fields constraint, which ties those two timestamps to
-- a terminal status.
UPDATE jobs
SET
  status = CASE
    WHEN attempt < max_attempts THEN 'pending'
    ELSE 'abandoned'
  END::job_status,
  claimed_by = NULL,
  lease_expires_at = NULL,
  run_at = CASE
    WHEN attempt < max_attempts THEN clock_timestamp()
    ELSE run_at
  END,
  settled_at = CASE
    WHEN attempt < max_attempts THEN NULL
    ELSE clock_timestamp()
  END,
  expires_at = CASE
    WHEN attempt < max_attempts THEN NULL
    ELSE clock_timestamp() + make_interval(days => ?1)
  END,
  error = CASE
    WHEN attempt < max_attempts THEN error
    ELSE ?0
  END,
  updated_at = clock_timestamp()
WHERE
  status = 'claimed'
  AND lease_expires_at < clock_timestamp()
RETURNING
  *;
