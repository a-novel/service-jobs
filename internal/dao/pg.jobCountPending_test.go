package dao_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobCountPending(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		// enqueued is how many pending jobs to seed; claimOne moves one out of the pending set.
		enqueued int
		claimOne bool

		expectPending int
	}{
		{
			name: "CountsOnlyPending",

			enqueued: 3,
			claimOne: true,

			expectPending: 2,
		},
		{
			name: "EmptyQueue",

			enqueued: 0,

			expectPending: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				for i := range testCase.enqueued {
					enqueueJob(ctx, t, seedID(i), 1)
				}

				if testCase.claimOne {
					_, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
						Kinds: []string{"generate"}, WorkerID: "worker-a", Limit: 1, LeaseSeconds: 60,
					})
					require.NoError(t, err)
				}

				stats, err := dao.NewJobCountPending().Exec(ctx)
				require.NoError(t, err)

				require.Equal(t, testCase.expectPending, stats.Pending)
				// The oldest-pending age is a non-negative duration on the database clock; on an empty
				// queue it coalesces to zero rather than a null.
				require.GreaterOrEqual(t, stats.OldestAge, time.Duration(0))

				if testCase.expectPending == 0 {
					require.Zero(t, stats.OldestAge)
				}
			})
		})
	}
}
