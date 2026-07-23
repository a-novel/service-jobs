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

func TestJobGetByID(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			seeded := enqueueJob(ctx, t, fixtureJobID, 1)

			// Reads by id alone — no owner argument, unlike the owner-scoped read.
			got, err := dao.NewJobGetByID().Exec(ctx, &dao.JobGetByIDRequest{ID: uuid.MustParse(fixtureJobID)})
			require.NoError(t, err)
			require.Equal(t, seeded, got)
		})
	})

	t.Run("Error/NotFound", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			_, err := dao.NewJobGetByID().Exec(ctx, &dao.JobGetByIDRequest{
				ID: uuid.MustParse("00000000-0000-0000-0000-0000000000ff"),
			})
			require.ErrorIs(t, err, dao.ErrJobGetByIDNotFound)
		})
	})
}
