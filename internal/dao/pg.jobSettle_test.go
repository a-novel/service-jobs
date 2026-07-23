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

	t.Run("Succeeded", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimJob(ctx, t, 1, 60)

			settled, err := dao.NewJobSettle().Exec(ctx, &dao.JobSettleRequest{
				ID:            id,
				WorkerID:      fixtureWorkerID,
				Status:        dao.JobStatusSucceeded,
				Result:        json.RawMessage(`{"ok":true}`),
				RetentionDays: 7,
			})
			require.NoError(t, err)

			require.Equal(t, dao.JobStatusSucceeded, settled.Status)
			require.JSONEq(t, `{"ok":true}`, string(settled.Result))
			require.Nil(t, settled.ClaimedBy)
			require.Nil(t, settled.LeaseExpiresAt)
			require.NotNil(t, settled.SettledAt)
			require.NotNil(t, settled.ExpiresAt)
			// The retention horizon is the settle time plus the requested window.
			require.True(t, settled.ExpiresAt.After(*settled.SettledAt))
		})
	})

	t.Run("Failed", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimJob(ctx, t, 1, 60)

			settled, err := dao.NewJobSettle().Exec(ctx, &dao.JobSettleRequest{
				ID:            id,
				WorkerID:      fixtureWorkerID,
				Status:        dao.JobStatusFailed,
				Error:         json.RawMessage(`{"reason":"boom"}`),
				RetentionDays: 7,
			})
			require.NoError(t, err)

			require.Equal(t, dao.JobStatusFailed, settled.Status)
			require.JSONEq(t, `{"reason":"boom"}`, string(settled.Error))
			require.NotNil(t, settled.SettledAt)
		})
	})

	t.Run("Error/WrongWorker", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimJob(ctx, t, 1, 60)

			// Another worker cannot settle a job it does not hold: the guard matches nothing.
			_, err := dao.NewJobSettle().Exec(ctx, &dao.JobSettleRequest{
				ID:            id,
				WorkerID:      "worker-b",
				Status:        dao.JobStatusSucceeded,
				RetentionDays: 7,
			})
			require.ErrorIs(t, err, dao.ErrJobSettleNotClaimed)
		})
	})

	t.Run("Error/AlreadySettled", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			claimJob(ctx, t, 1, 60)

			settle := dao.NewJobSettle()

			_, err := settle.Exec(ctx, &dao.JobSettleRequest{
				ID: id, WorkerID: fixtureWorkerID, Status: dao.JobStatusSucceeded, RetentionDays: 7,
			})
			require.NoError(t, err)

			// The job is no longer claimed, so a second settle finds nothing to move.
			_, err = settle.Exec(ctx, &dao.JobSettleRequest{
				ID: id, WorkerID: fixtureWorkerID, Status: dao.JobStatusSucceeded, RetentionDays: 7,
			})
			require.ErrorIs(t, err, dao.ErrJobSettleNotClaimed)
		})
	})
}
