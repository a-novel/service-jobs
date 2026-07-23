-- Reads a job by id alone, with no owner scope. This is the worker-side read: settle and its retry
-- decision are keyed by the job id and the claim, not by an owner, because a worker running a job
-- does not carry the owner's identity. Owner-scoped reads go through pg.jobGet.sql instead.
SELECT
  *
FROM
  jobs
WHERE
  id = ?0;
