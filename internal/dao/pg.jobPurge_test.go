package dao_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

// purgeStatement extracts the DELETE that builds/database.sql schedules via pg_cron — the text
// between its dollar-quote markers — so this test exercises exactly what the cron job runs rather
// than a copy that could drift from it.
func purgeStatement(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile("../../builds/database.sql")
	require.NoError(t, err)

	sql := string(data)

	open := strings.Index(sql, "$$")
	require.GreaterOrEqual(t, open, 0, "database.sql has no dollar-quoted command")

	rest := sql[open+2:]
	end := strings.Index(rest, "$$")
	require.GreaterOrEqual(t, end, 0, "database.sql dollar quote is not closed")

	return strings.TrimSpace(rest[:end])
}

// insertSettled inserts a terminal (succeeded) job whose retention expires expiresIn from now, and
// returns its id. A negative expiresIn is already past retention.
func insertSettled(ctx context.Context, t *testing.T, expiresIn time.Duration) uuid.UUID {
	t.Helper()

	pg, err := postgres.GetContext(ctx)
	require.NoError(t, err)

	var id uuid.UUID

	err = pg.NewRaw(
		`INSERT INTO jobs (kind, owner_id, request_fingerprint, status, attempt, max_attempts, settled_at, expires_at)
		 VALUES ('purge', gen_random_uuid(), '\x01'::bytea, 'succeeded', 1, 1,
		         clock_timestamp(), clock_timestamp() + make_interval(secs => ?))
		 RETURNING id`,
		expiresIn.Seconds(),
	).Scan(ctx, &id)
	require.NoError(t, err)

	return id
}

// insertClaimed inserts a claimed job — which has no expires_at — and returns its id.
func insertClaimed(ctx context.Context, t *testing.T) uuid.UUID {
	t.Helper()

	pg, err := postgres.GetContext(ctx)
	require.NoError(t, err)

	var id uuid.UUID

	err = pg.NewRaw(
		`INSERT INTO jobs (kind, owner_id, request_fingerprint, status, attempt, max_attempts, claimed_by, lease_expires_at)
		 VALUES ('purge', gen_random_uuid(), '\x01'::bytea, 'claimed', 1, 1, 'worker', clock_timestamp() + interval '1 hour')
		 RETURNING id`,
	).Scan(ctx, &id)
	require.NoError(t, err)

	return id
}

func jobExists(ctx context.Context, t *testing.T, id uuid.UUID) bool {
	t.Helper()

	pg, err := postgres.GetContext(ctx)
	require.NoError(t, err)

	var count int

	err = pg.NewRaw("SELECT count(*) FROM jobs WHERE id = ?", id).Scan(ctx, &count)
	require.NoError(t, err)

	return count > 0
}

func TestJobPurge(t *testing.T) {
	t.Parallel()

	postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
		t.Helper()

		// A settled job past its retention window is deleted; one still inside the window is kept; a
		// claimed job — which carries no expires_at — is never touched.
		expired := insertSettled(ctx, t, -time.Hour)
		inWindow := insertSettled(ctx, t, time.Hour)
		claimed := insertClaimed(ctx, t)

		pg, err := postgres.GetContext(ctx)
		require.NoError(t, err)

		_, err = pg.NewRaw(purgeStatement(t)).Exec(ctx)
		require.NoError(t, err)

		require.False(t, jobExists(ctx, t, expired), "a settled job past expires_at should be purged")
		require.True(t, jobExists(ctx, t, inWindow), "a settled job inside its window should be kept")
		require.True(t, jobExists(ctx, t, claimed), "a claimed job should never be purged")
	})
}
