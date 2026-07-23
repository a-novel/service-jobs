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

	testCases := []struct {
		name string

		// seed enqueues the fixture job before the read, so a case that expects a hit has something to
		// find and one that expects a miss does not.
		seed bool

		requestID string

		expectFound bool
		expectErr   error
	}{
		{
			name: "Success",

			seed:      true,
			requestID: fixtureJobID,

			expectFound: true,
		},
		{
			name: "Error/NotFound",

			requestID: "00000000-0000-0000-0000-0000000000ff",

			expectErr: dao.ErrJobGetByIDNotFound,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
				t.Helper()

				var seeded *dao.Job
				if testCase.seed {
					seeded = enqueueJob(ctx, t, fixtureJobID, 1)
				}

				// Reads by id alone — no owner argument, unlike the owner-scoped read.
				got, err := dao.NewJobGetByID().Exec(ctx, &dao.JobGetByIDRequest{ID: uuid.MustParse(testCase.requestID)})
				require.ErrorIs(t, err, testCase.expectErr)

				if testCase.expectFound {
					require.Equal(t, seeded, got)
				} else {
					require.Nil(t, got)
				}
			})
		})
	}
}
