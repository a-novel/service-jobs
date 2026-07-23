package dao_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobQueueDepth(t *testing.T) {
	t.Parallel()

	t.Run("CountsDuePending", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// Four due-now pending jobs; claim the oldest so it is no longer pending. A fifth pushed into
			// the future is pending but not due. The probe counts the three that remain due-and-unclaimed
			// and none of the others.
			for i := range 4 {
				enqueueJob(ctx, t, seedID(i), 1)
			}

			claimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: fixtureWorkerID, Limit: 1, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Len(t, claimed, 1)

			futureID := seedID(9)
			enqueueJob(ctx, t, futureID, 1)

			pg, err := postgres.GetContext(ctx)
			require.NoError(t, err)

			_, err = pg.NewRaw(
				"UPDATE jobs SET run_at = clock_timestamp() + interval '1 hour' WHERE id = ?",
				uuid.MustParse(futureID),
			).Exec(ctx)
			require.NoError(t, err)

			depth, err := dao.NewJobQueueDepth().Exec(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(3), depth.Pending)
			require.NotNil(t, depth.OldestPendingAge)
			require.GreaterOrEqual(t, *depth.OldestPendingAge, time.Duration(0))
		})
	})

	t.Run("EmptyQueue", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// No pending rows: a zero count and an absent age, not a zero one.
			depth, err := dao.NewJobQueueDepth().Exec(ctx)
			require.NoError(t, err)
			require.Zero(t, depth.Pending)
			require.Nil(t, depth.OldestPendingAge)
		})
	})

	t.Run("UsesDispatchIndex", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			for i := range 5 {
				enqueueJob(ctx, t, seedID(i), 1)
			}

			// EXPLAIN the query the DAO actually embeds, so the assertion cannot drift from it.
			query, err := os.ReadFile("pg.jobQueueDepth.sql")
			require.NoError(t, err)

			pg, err := postgres.GetContext(ctx)
			require.NoError(t, err)

			bunDB, ok := pg.(*bun.DB)
			require.True(t, ok)

			// enable_seqscan off forces the planner to prefer an index where one applies, so the plan
			// proves the query is servable by jobs_dispatch_idx independent of the planner's size-based
			// choice on a small test table. SET LOCAL needs a transaction.
			err = bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
				_, txErr := tx.NewRaw("SET LOCAL enable_seqscan = off").Exec(ctx)
				if txErr != nil {
					return txErr
				}

				var plan string

				txErr = tx.NewRaw("EXPLAIN (FORMAT JSON)\n"+string(query)).Scan(ctx, &plan)
				if txErr != nil {
					return txErr
				}

				require.Contains(t, plan, "jobs_dispatch_idx")

				return nil
			})
			require.NoError(t, err)
		})
	})
}
