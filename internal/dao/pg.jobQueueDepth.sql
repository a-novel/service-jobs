-- Backlog probe: how many jobs are due to run and still unclaimed, and how long the oldest of them
-- has waited. The predicate matches jobs_dispatch_idx — (run_at) partial on status = 'pending' — so
-- the probe is an index range over the pending partition rather than a scan of the terminal rows
-- piling up beside it.
--
-- run_at <= clock_timestamp() scopes the count to work a worker could claim right now: a job
-- scheduled for the future is not backlog, and its age would be negative. min(run_at) is the oldest
-- such job, and the age is measured from the database clock so worker skew never enters it.
-- oldest_pending_age is null when nothing is pending, which the caller reports as an absent age
-- rather than a zero one.
SELECT
  count(*) AS pending,
  extract(
    epoch
    FROM
      clock_timestamp() - min(run_at)
  ) AS oldest_pending_age
FROM
  jobs
WHERE
  status = 'pending'
  AND run_at <= clock_timestamp();
