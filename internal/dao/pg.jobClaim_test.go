package dao_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

func TestJobClaim(t *testing.T) {
	t.Parallel()

	owner := uuid.MustParse("00000000-0000-0000-0000-00000000a11c")

	// seed enqueues one pending job per (id, kind) pair, so a case states only what it needs.
	seed := func(ctx context.Context, t *testing.T, enqueue *dao.JobEnqueue, id, kind string) *dao.Job {
		t.Helper()

		entity, err := enqueue.Exec(ctx, &dao.JobEnqueueRequest{
			ID:                 uuid.MustParse(id),
			Kind:               kind,
			Payload:            json.RawMessage(`{}`),
			OwnerID:            owner,
			IdempotencyKey:     lo.ToPtr(id),
			RequestFingerprint: []byte{0x01},
			MaxAttempts:        1,
		})
		require.NoError(t, err)

		return entity
	}

	t.Run("ClaimsMatchingKindsOnly", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			enqueue := dao.NewJobEnqueue()
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000001", "generate")
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000002", "generate")
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000003", "consolidate")

			claimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds:        []string{"generate"},
				WorkerID:     "worker-a",
				Limit:        10,
				LeaseSeconds: 60,
			})
			require.NoError(t, err)

			require.Len(t, claimed, 2)

			for _, job := range claimed {
				require.Equal(t, "generate", job.Kind)
				require.Equal(t, dao.JobStatusClaimed, job.Status)
				require.EqualValues(t, 1, job.Attempt)
				require.Equal(t, lo.ToPtr("worker-a"), job.ClaimedBy)
				require.NotNil(t, job.LeaseExpiresAt)
				require.True(t, job.LeaseExpiresAt.After(job.CreatedAt))
			}
		})
	})

	t.Run("LimitAndReClaim", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			enqueue := dao.NewJobEnqueue()
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000001", "generate")
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000002", "generate")
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000003", "generate")

			claim := dao.NewJobClaim()

			first, err := claim.Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-a", Limit: 2, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Len(t, first, 2)

			// The claimed rows are no longer pending, so a second claim takes only the remaining one.
			second, err := claim.Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-b", Limit: 10, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Len(t, second, 1)

			// A third claim finds nothing and returns an empty slice, not an error.
			third, err := claim.Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-c", Limit: 10, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Empty(t, third)
		})
	})

	t.Run("ConcurrentClaimsAreDisjoint", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			enqueue := dao.NewJobEnqueue()

			ids := []string{
				"00000000-0000-0000-0000-000000000001",
				"00000000-0000-0000-0000-000000000002",
				"00000000-0000-0000-0000-000000000003",
				"00000000-0000-0000-0000-000000000004",
			}
			for _, id := range ids {
				seed(ctx, t, enqueue, id, "generate")
			}

			claim := dao.NewJobClaim()

			// Two claims run concurrently on separate pool connections. SKIP LOCKED is what keeps them
			// from returning the same row: without it, one would block on the other's locks.
			var (
				wg         sync.WaitGroup
				resA, resB []*dao.Job
				errA, errB error
			)

			wg.Add(2)
			go func() {
				defer wg.Done()

				resA, errA = claim.Exec(ctx, &dao.JobClaimRequest{
					Kinds: []string{"generate"}, WorkerID: "worker-a", Limit: 2, LeaseSeconds: 60,
				})
			}()
			go func() {
				defer wg.Done()

				resB, errB = claim.Exec(ctx, &dao.JobClaimRequest{
					Kinds: []string{"generate"}, WorkerID: "worker-b", Limit: 2, LeaseSeconds: 60,
				})
			}()

			wg.Wait()

			require.NoError(t, errA)
			require.NoError(t, errB)

			seen := map[uuid.UUID]struct{}{}
			for _, job := range append(append([]*dao.Job{}, resA...), resB...) {
				_, dup := seen[job.ID]
				require.False(t, dup, "job %s claimed by both workers", job.ID)
				seen[job.ID] = struct{}{}
			}
		})
	})

	t.Run("EmptyKindsClaimsNothing", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			enqueue := dao.NewJobEnqueue()
			seed(ctx, t, enqueue, "00000000-0000-0000-0000-000000000001", "generate")

			// A worker that serves no kinds claims nothing — fail-safe rather than fail-open. A worker
			// misconfigured with an empty kind set sits idle, visible in the queue depth, instead of
			// claiming work it has no handler for and burning the attempt failing it.
			claimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{}, WorkerID: "worker-a", Limit: 10, LeaseSeconds: 60,
			})
			require.NoError(t, err)
			require.Empty(t, claimed)
		})
	})

	// Stress the claim path under real contention: many workers draining one queue at once. The
	// point is not throughput but correctness — SKIP LOCKED must hand every job to exactly one
	// worker and never deadlock, which a single-threaded test cannot exercise. Run it under -race to
	// catch a data race in the path as well.
	t.Run("StressConcurrentDrain", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			const (
				jobCount = 60
				workers  = 8
			)

			enqueue := dao.NewJobEnqueue()

			for i := range jobCount {
				id := uuid.New()
				_, err := enqueue.Exec(ctx, &dao.JobEnqueueRequest{
					ID:                 id,
					Kind:               "generate",
					Payload:            json.RawMessage(`{}`),
					OwnerID:            uuid.MustParse("00000000-0000-0000-0000-00000000a11c"),
					IdempotencyKey:     lo.ToPtr(id.String()),
					RequestFingerprint: []byte{byte(i)},
					MaxAttempts:        1,
				})
				require.NoError(t, err)
			}

			claim := dao.NewJobClaim()
			settle := dao.NewJobSettle()

			var (
				mu      sync.Mutex
				settled = map[uuid.UUID]int{} // id -> how many times it was settled
				errs    []error               // failures collected off the worker goroutines
				wg      sync.WaitGroup
			)

			// record is the only thing the workers write through, so require lives on the test
			// goroutine alone — a require.FailNow inside a goroutine would leak a runtime.Goexit and
			// could hang the run rather than fail it.
			record := func(id uuid.UUID, err error) {
				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					errs = append(errs, err)

					return
				}

				settled[id]++
			}

			for w := range workers {
				wg.Add(1)

				go func(workerID string) {
					defer wg.Done()

					// Each worker claims and settles in a loop until the queue is drained. The
					// iteration cap is a deadlock backstop: a correct run needs far fewer, but a
					// livelock should fail the test rather than hang it.
					for range jobCount * 2 {
						batch, err := claim.Exec(ctx, &dao.JobClaimRequest{
							Kinds: []string{"generate"}, WorkerID: workerID, Limit: 3, LeaseSeconds: 60,
						})
						if err != nil {
							record(uuid.Nil, err)

							return
						}

						if len(batch) == 0 {
							return
						}

						for _, job := range batch {
							_, err := settle.Exec(ctx, &dao.JobSettleRequest{
								ID: job.ID, WorkerID: workerID, Status: dao.JobStatusSucceeded, RetentionDays: 7,
							})
							record(job.ID, err)
						}
					}
				}(fmt.Sprintf("worker-%d", w))
			}

			wg.Wait()

			require.Empty(t, errs)

			// Every job was settled exactly once: none dropped, none processed twice.
			require.Len(t, settled, jobCount)

			for id, count := range settled {
				require.Equalf(t, 1, count, "job %s settled %d times", id, count)
			}
		})
	})
}
