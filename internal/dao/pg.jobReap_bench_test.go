package dao_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

// seedExpiredClaims inserts n rows already claimed with a lease an hour in the past, in one
// statement, so the benchmark's setup cost stays O(1) round trips rather than O(n) enqueues. attempt
// equals max_attempts, so the reap abandons each row (a terminal status) rather than requeuing it —
// the rows leave the claimed set and never re-enter the next iteration's sweep.
func seedExpiredClaims(ctx context.Context, b *testing.B, n int) {
	b.Helper()

	db, err := postgres.GetContext(ctx)
	if err != nil {
		b.Fatalf("get database: %v", err)
	}

	_, err = db.NewRaw(
		`INSERT INTO jobs (kind, owner_id, request_fingerprint, status, attempt, max_attempts, claimed_by, lease_expires_at)
		 SELECT
		   'bench', gen_random_uuid(), '\x01'::bytea, 'claimed', 1, 1, 'bench-worker',
		   clock_timestamp() - interval '1 hour'
		 FROM generate_series(1, ?)`, n,
	).Exec(ctx)
	if err != nil {
		b.Fatalf("seed expired claims: %v", err)
	}
}

// BenchmarkJobReap measures one reaper sweep recovering n stranded claims — the number that answers
// "can a single sweep keep up after a mass worker crash". Each iteration seeds n immediately-reapable
// claims with the timer stopped, then times one reap that abandons them. The sweep is served by the
// jobs_lease_idx partial index, so its cost tracks n rather than the abandoned rows piling up beside
// them across iterations.
//
// Run it against a live database (the reap is a real statement, not a mock):
//
//	POSTGRES_DSN=... go test -bench=BenchmarkJobReap -benchmem -benchtime=20x -run=^$ ./internal/dao/...
func BenchmarkJobReap(b *testing.B) {
	ctx, err := postgres.NewContext(context.Background(), configtest.PostgresPreset)
	if err != nil {
		b.Fatalf("open database: %v", err)
	}

	err = postgres.RunMigrationsContext(ctx, migrations.Migrations)
	if err != nil {
		b.Fatalf("run migrations: %v", err)
	}

	reapErr := json.RawMessage(`{"reason":"lease expired"}`)
	reap := dao.NewJobReap()

	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("expired=%d", n), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				seedExpiredClaims(ctx, b, n)
				b.StartTimer()

				reaped, err := reap.Exec(ctx, &dao.JobReapRequest{Error: reapErr, RetentionDays: 7})
				if err != nil {
					b.Fatalf("reap: %v", err)
				}

				if len(reaped) != n {
					b.Fatalf("reaped %d, want %d", len(reaped), n)
				}
			}
		})
	}
}
