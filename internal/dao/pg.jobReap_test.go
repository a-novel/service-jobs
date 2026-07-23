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

	t.Run("RequeuesWhenAttemptsRemain", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// max_attempts 2, claimed once (attempt 1) with a lease already in the past.
			claimJob(ctx, t, 2, -1)

			reaped, err := dao.NewJobReap().Exec(ctx, &dao.JobReapRequest{Error: reapErr, RetentionDays: 7})
			require.NoError(t, err)
			require.Len(t, reaped, 1)

			job := reaped[0]
			require.Equal(t, dao.JobStatusPending, job.Status)
			require.Nil(t, job.ClaimedBy)
			require.Nil(t, job.LeaseExpiresAt)
			require.Nil(t, job.SettledAt)
			require.Nil(t, job.ExpiresAt)
			// The consumed attempt is kept; the re-claim advances it.
			require.EqualValues(t, 1, job.Attempt)
		})
	})

	t.Run("AbandonsWhenNoAttemptsRemain", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// max_attempts 1, claimed once (attempt 1) — no attempt left when the lease lapses.
			claimJob(ctx, t, 1, -1)

			reaped, err := dao.NewJobReap().Exec(ctx, &dao.JobReapRequest{Error: reapErr, RetentionDays: 7})
			require.NoError(t, err)
			require.Len(t, reaped, 1)

			job := reaped[0]
			require.Equal(t, dao.JobStatusAbandoned, job.Status)
			require.Nil(t, job.ClaimedBy)
			require.Nil(t, job.LeaseExpiresAt)
			require.NotNil(t, job.SettledAt)
			require.NotNil(t, job.ExpiresAt)
			require.JSONEq(t, string(reapErr), string(job.Error))
		})
	})

	t.Run("LeavesLiveLeasesAlone", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// A healthy claim with a lease well in the future is not the reaper's business.
			claimJob(ctx, t, 1, 3600)

			reaped, err := dao.NewJobReap().Exec(ctx, &dao.JobReapRequest{Error: reapErr, RetentionDays: 7})
			require.NoError(t, err)
			require.Empty(t, reaped)
		})
	})
}
