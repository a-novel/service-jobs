package dao_test

import (
	"context"
	"encoding/json"
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
}
