package dao_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobSettle(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse(fixtureJobID)

	testCases := []struct {
		name string

		// settleWorker is the worker that settles. It matches the claim on the success paths and
		// differs to exercise the guard.
		settleWorker string
		status       dao.JobStatus
		result       json.RawMessage
		jobErr       json.RawMessage

		// preSettle, when set, settles the job once before the case's own settle, so the second one
		// finds nothing still claimed.
		preSettle bool

		expectStatus dao.JobStatus
		expectResult json.RawMessage
		expectErr    error
	}{
		{
			name: "Succeeded",

			settleWorker: fixtureWorkerID,
			status:       dao.JobStatusSucceeded,
			result:       json.RawMessage(`{"ok":true}`),

			expectStatus: dao.JobStatusSucceeded,
			expectResult: json.RawMessage(`{"ok":true}`),
		},
		{
			name: "Failed",

			settleWorker: fixtureWorkerID,
			status:       dao.JobStatusFailed,
			jobErr:       json.RawMessage(`{"reason":"boom"}`),

			expectStatus: dao.JobStatusFailed,
		},
		{
			name: "Error/WrongWorker",

			settleWorker: "worker-b",
			status:       dao.JobStatusSucceeded,

			expectErr: dao.ErrJobSettleNotClaimed,
		},
		{
			name: "Error/AlreadySettled",

			settleWorker: fixtureWorkerID,
			status:       dao.JobStatusSucceeded,
			preSettle:    true,

			expectErr: dao.ErrJobSettleNotClaimed,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				claimJob(ctx, t, 1, 60)

				settle := dao.NewJobSettle()

				if testCase.preSettle {
					_, err := settle.Exec(ctx, &dao.JobSettleRequest{
						ID: id, WorkerID: fixtureWorkerID, Status: dao.JobStatusSucceeded, RetentionDays: 7,
					})
					require.NoError(t, err)
				}

				settled, err := settle.Exec(ctx, &dao.JobSettleRequest{
					ID:            id,
					WorkerID:      testCase.settleWorker,
					Status:        testCase.status,
					Result:        testCase.result,
					Error:         testCase.jobErr,
					RetentionDays: 7,
				})
				require.ErrorIs(t, err, testCase.expectErr)

				if testCase.expectErr != nil {
					require.Nil(t, settled)

					return
				}

				require.Equal(t, testCase.expectStatus, settled.Status)
				require.Nil(t, settled.ClaimedBy)
				require.Nil(t, settled.LeaseExpiresAt)
				// A terminal settle stamps both timestamps, which the retention purge later reads.
				require.NotNil(t, settled.SettledAt)
				require.NotNil(t, settled.ExpiresAt)
				require.True(t, settled.ExpiresAt.After(*settled.SettledAt))

				if testCase.expectResult != nil {
					require.JSONEq(t, string(testCase.expectResult), string(settled.Result))
				}

				if testCase.jobErr != nil {
					require.JSONEq(t, string(testCase.jobErr), string(settled.Error))
				}
			})
		})
	}
}
