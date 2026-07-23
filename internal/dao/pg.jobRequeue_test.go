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

	testCases := []struct {
		name string

		// requeueWorker is the worker that asks to hand the job back. It matches the claiming worker on
		// the success path and differs from it to exercise the guard.
		requeueWorker string

		expectErr error
	}{
		{
			name: "Success",

			requeueWorker: fixtureWorkerID,
		},
		{
			name: "Error/WrongWorker",

			requeueWorker: "worker-b",

			expectErr: dao.ErrJobRequeueNotClaimed,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				// max_attempts 2 so a requeued job is still claimable afterwards.
				claimed := claimJob(ctx, t, 2, 60)
				require.EqualValues(t, 1, claimed.Attempt)

				requeued, err := dao.NewJobRequeue().Exec(ctx, &dao.JobRequeueRequest{
					ID:       id,
					WorkerID: testCase.requeueWorker,
				})
				require.ErrorIs(t, err, testCase.expectErr)

				if testCase.expectErr != nil {
					require.Nil(t, requeued)

					return
				}

				require.Equal(t, dao.JobStatusPending, requeued.Status)
				require.Nil(t, requeued.ClaimedBy)
				require.Nil(t, requeued.LeaseExpiresAt)
				// The attempt that began still counts — a hand-back does not refund it.
				require.EqualValues(t, 1, requeued.Attempt)

				// The job is pending again, so another worker can claim it, advancing the attempt.
				reclaimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
					Kinds: []string{"generate"}, WorkerID: "worker-b", Limit: 10, LeaseSeconds: 60,
				})
				require.NoError(t, err)
				require.Len(t, reclaimed, 1)
				require.EqualValues(t, 2, reclaimed[0].Attempt)
			})
		})
	}
}
