-- Both aggregates are served by jobs_dispatch_idx, the partial index on run_at where the status is
-- pending, so the query never touches a claimed or settled row. The count alone cannot tell a queue
-- absorbing a burst from a stalled one; the age of the oldest pending job is what separates them.
--
-- The age is returned as epoch seconds against the database clock, so no application-server time
-- enters it. coalesce keeps it meaningful on an empty queue: min over no rows is null, which reads
-- back as an age of zero rather than a missing value.
SELECT
  count(*) AS pending,
  COALESCE(
    EXTRACT(
      EPOCH
      FROM
        clock_timestamp() - min(run_at)
    ),
    0
  ) AS oldest_age_seconds
FROM
  jobs
WHERE
  status = 'pending';
