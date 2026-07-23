-- A worker handing a job back voluntarily, before its lease expires. The guard is the same as
-- settle's — id, holding worker, claimed status — so a worker cannot requeue work the reaper has
-- already recovered or another worker now holds.
--
-- attempt is left as it is: the run that began still counts, so a job handed back does not earn a
-- free extra attempt. run_at is reset to now, so it is immediately claimable again.
UPDATE jobs
SET
  status = 'pending',
  claimed_by = NULL,
  lease_expires_at = NULL,
  run_at = clock_timestamp(),
  updated_at = clock_timestamp()
WHERE
  id = ?0
  AND claimed_by = ?1
  AND status = 'claimed'
RETURNING
  *;
