package dao_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobReap(t *testing.T) {
	t.Parallel()

	reapErr := json.RawMessage(`{"reason":"lease expired"}`)

	testCases := []struct {
		name string

		// maxAttempts and leaseSeconds set up the claim: a negative lease lands it in the past so the
		// job is immediately reapable, a large one keeps the claim live.
		maxAttempts  int16
		leaseSeconds int

		expectReaped bool
		expectStatus dao.JobStatus
	}{
		{
			// attempt 1 < max 2: an attempt remains, so the lapsed claim returns to pending.
			name: "RequeuesWhenAttemptsRemain",

			maxAttempts:  2,
			leaseSeconds: -1,

			expectReaped: true,
			expectStatus: dao.JobStatusPending,
		},
		{
			// attempt 1, max 1: no attempt remains, so the lapsed claim settles abandoned.
			name: "AbandonsWhenNoAttemptsRemain",

			maxAttempts:  1,
			leaseSeconds: -1,

			expectReaped: true,
			expectStatus: dao.JobStatusAbandoned,
		},
		{
			// A live lease is not the reaper's business.
			name: "LeavesLiveLeasesAlone",

			maxAttempts:  1,
			leaseSeconds: 3600,

			expectReaped: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				claimJob(ctx, t, testCase.maxAttempts, testCase.leaseSeconds)

				reaped, err := dao.NewJobReap().Exec(ctx, &dao.JobReapRequest{Error: reapErr, RetentionDays: 7})
				require.NoError(t, err)

				if !testCase.expectReaped {
					require.Empty(t, reaped)

					return
				}

				require.Len(t, reaped, 1)

				job := reaped[0]
				require.Equal(t, testCase.expectStatus, job.Status)
				require.Nil(t, job.ClaimedBy)
				require.Nil(t, job.LeaseExpiresAt)

				if testCase.expectStatus == dao.JobStatusAbandoned {
					// A terminal status stamps both settle timestamps and takes the reaper's error.
					require.NotNil(t, job.SettledAt)
					require.NotNil(t, job.ExpiresAt)
					require.JSONEq(t, string(reapErr), string(job.Error))
				} else {
					// Back to pending: no settle timestamps, immediately claimable again.
					require.Nil(t, job.SettledAt)
					require.Nil(t, job.ExpiresAt)
				}
			})
		})
	}
}
