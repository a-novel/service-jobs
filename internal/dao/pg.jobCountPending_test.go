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

	t.Run("CountsOnlyPending", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			enqueueJob(ctx, t, "00000000-0000-0000-0000-000000000001", 1)
			enqueueJob(ctx, t, "00000000-0000-0000-0000-000000000002", 1)
			enqueueJob(ctx, t, "00000000-0000-0000-0000-000000000003", 1)

			// Claiming one moves it out of the pending set, so the count drops to two.
			_, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-a", Limit: 1, LeaseSeconds: 60,
			})
			require.NoError(t, err)

			stats, err := dao.NewJobCountPending().Exec(ctx)
			require.NoError(t, err)

			require.Equal(t, 2, stats.Pending)
			// The oldest pending job has waited a non-negative amount; the age is the DB clock's.
			require.GreaterOrEqual(t, stats.OldestAge, time.Duration(0))
		})
	})

	t.Run("EmptyQueue", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			stats, err := dao.NewJobCountPending().Exec(ctx)
			require.NoError(t, err)

			require.Equal(t, 0, stats.Pending)
			require.Zero(t, stats.OldestAge)
		})
	})
}
