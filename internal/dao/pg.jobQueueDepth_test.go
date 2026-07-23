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

// scheduleFuture pushes a job's run_at an hour ahead, so it is pending but not yet due.
func scheduleFuture(ctx context.Context, t *testing.T, id string) {
	t.Helper()

	pg, err := postgres.GetContext(ctx)
	require.NoError(t, err)

	_, err = pg.NewRaw(
		"UPDATE jobs SET run_at = clock_timestamp() + interval '1 hour' WHERE id = ?",
		uuid.MustParse(id),
	).Exec(ctx)
	require.NoError(t, err)
}

// claimAll claims every due pending job of the fixture kind, so a case can move rows out of the
// pending state.
func claimAll(ctx context.Context, t *testing.T, limit int) {
	t.Helper()

	claimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
		Kinds: []string{"generate"}, WorkerID: fixtureWorkerID, Limit: limit, LeaseSeconds: 60,
	})
	require.NoError(t, err)
	require.Len(t, claimed, limit)
}

func TestJobQueueDepth(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		// seed sets up the queue state for the case, inside the test's own database.
		seed func(ctx context.Context, t *testing.T)

		expectPending int64
		// expectAge is whether an oldest-pending age is reported: present exactly when something is
		// pending, absent (nil) otherwise.
		expectAge bool
	}{
		{
			// Four due-now pending; the oldest is claimed away, and a fifth is scheduled for the future.
			// Three remain due-and-unclaimed — the only ones that count.
			name: "Backlog",

			seed: func(ctx context.Context, t *testing.T) {
				t.Helper()

				for i := range 4 {
					enqueueJob(ctx, t, seedID(i), 1)
				}

				claimAll(ctx, t, 1)
				scheduleFuture(ctx, t, seedID(9))
			},

			expectPending: 3,
			expectAge:     true,
		},
		{
			name: "Empty",

			seed: func(_ context.Context, t *testing.T) { t.Helper() },

			expectPending: 0,
			expectAge:     false,
		},
		{
			// A claimed job is not pending, so nothing is due.
			name: "OnlyClaimed",

			seed: func(ctx context.Context, t *testing.T) {
				t.Helper()

				enqueueJob(ctx, t, seedID(0), 1)
				claimAll(ctx, t, 1)
			},

			expectPending: 0,
			expectAge:     false,
		},
		{
			// A pending job scheduled for the future is not yet backlog.
			name: "OnlyFutureScheduled",

			seed: func(ctx context.Context, t *testing.T) {
				t.Helper()

				enqueueJob(ctx, t, seedID(0), 1)
				scheduleFuture(ctx, t, seedID(0))
			},

			expectPending: 0,
			expectAge:     false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				testCase.seed(ctx, t)

				depth, err := dao.NewJobQueueDepth().Exec(ctx)
				require.NoError(t, err)
				require.Equal(t, testCase.expectPending, depth.Pending)

				if testCase.expectAge {
					require.NotNil(t, depth.OldestPendingAge)
					require.GreaterOrEqual(t, *depth.OldestPendingAge, time.Duration(0))
				} else {
					require.Nil(t, depth.OldestPendingAge)
				}
			})
		})
	}

	// The plan assertion checks the query plan rather than a count, so it stays its own test rather
	// than a row in the table above.
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
