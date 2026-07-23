-- The claimable set is selected FOR UPDATE SKIP LOCKED so two workers polling at once take disjoint
-- rows and neither blocks on the other's locks. The predicate matches jobs_dispatch_idx exactly, so
-- the scan never grows with the terminal rows piling up beside it.
--
-- attempt is incremented here, at the start of a run, so it counts runs begun rather than runs
-- finished. A job stranded after its only permitted attempt is therefore already at the cap when the
-- reaper finds it, and settles abandoned rather than looping.
--
-- lease_expires_at is clock_timestamp() plus the visibility timeout, computed in the database so
-- worker clock skew never enters lease arithmetic. Setting status and the lease together is what the
-- jobs_lease_matches_status constraint requires.
WITH
  claimable AS (
    SELECT
      id
    FROM
      jobs
    WHERE
      status = 'pending'
      AND run_at <= clock_timestamp()
      AND kind IN (?0)
    ORDER BY
      run_at
    LIMIT
      ?1
    FOR UPDATE
      SKIP LOCKED
  )
UPDATE jobs
SET
  status = 'claimed',
  attempt = attempt + 1,
  claimed_by = ?2,
  lease_expires_at = clock_timestamp() + make_interval(secs => ?3),
  updated_at = clock_timestamp()
FROM
  claimable
WHERE
  jobs.id = claimable.id
RETURNING
  jobs.*;
