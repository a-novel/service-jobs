package dao_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobRequeue(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse(fixtureJobID)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimed := claimJob(ctx, t, 2, 60)
			require.EqualValues(t, 1, claimed.Attempt)

			requeued, err := dao.NewJobRequeue().Exec(ctx, &dao.JobRequeueRequest{
				ID:       id,
				WorkerID: fixtureWorkerID,
			})
			require.NoError(t, err)

			require.Equal(t, dao.JobStatusPending, requeued.Status)
			require.Nil(t, requeued.ClaimedBy)
			require.Nil(t, requeued.LeaseExpiresAt)
			// The attempt that began still counts — a hand-back does not refund it.
			require.EqualValues(t, 1, requeued.Attempt)

			// The job is pending again, so another worker can claim it.
			reclaimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-b", Limit: 10, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Len(t, reclaimed, 1)
			require.EqualValues(t, 2, reclaimed[0].Attempt)
		})
	})

	t.Run("Error/WrongWorker", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimJob(ctx, t, 2, 60)

			_, err := dao.NewJobRequeue().Exec(ctx, &dao.JobRequeueRequest{
				ID:       id,
				WorkerID: "worker-b",
			})
			require.ErrorIs(t, err, dao.ErrJobRequeueNotClaimed)
		})
	})
}
