package lib_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/dao"
	"github.com/a-novel/service-jobs/internal/lib"
	"github.com/a-novel/service-jobs/internal/models/migrations"
)

// fakeReapService is a JobReapService whose result is fixed per test and whose call count is
// observable, so the loop's cadence can be asserted without a database or wall-clock coupling.
type fakeReapService struct {
	mu    sync.Mutex
	calls int
	jobs  []*core.Job
	err   error
}

func (f *fakeReapService) Exec(_ context.Context) ([]*core.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	return f.jobs, f.err
}

func (f *fakeReapService) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// enqueueJob seeds one pending job under a distinct owner and key, so no dedup collides.
func enqueueJob(ctx context.Context, t *testing.T, id uuid.UUID, maxAttempts int16) {
	t.Helper()

	_, err := dao.NewJobEnqueue().Exec(ctx, &dao.JobEnqueueRequest{
		ID:                 id,
		Kind:               "generate",
		Payload:            json.RawMessage(`{}`),
		OwnerID:            uuid.New(),
		IdempotencyKey:     lo.ToPtr(id.String()),
		RequestFingerprint: []byte{0x01},
		MaxAttempts:        maxAttempts,
	})
	require.NoError(t, err)
}

func TestReaper(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")

	t.Run("Sweep/Recovered", func(t *testing.T) {
		t.Parallel()

		fake := &fakeReapService{jobs: []*core.Job{{}, {}, {}}}

		recovered, err := lib.NewReaper(fake, time.Minute).Sweep(t.Context())
		require.NoError(t, err)
		require.Equal(t, 3, recovered)
	})

	t.Run("Sweep/Error", func(t *testing.T) {
		t.Parallel()

		fake := &fakeReapService{err: errFoo}

		recovered, err := lib.NewReaper(fake, time.Minute).Sweep(t.Context())
		require.Error(t, err)
		require.Zero(t, recovered)
	})

	t.Run("Run/StopsOnCancel", func(t *testing.T) {
		t.Parallel()

		fake := &fakeReapService{}
		reaper := lib.NewReaper(fake, 20*time.Millisecond)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			defer close(done)

			reaper.Run(ctx)
		}()

		// One immediate sweep at start, then one per tick: at least two means the ticker fired, so the
		// loop is genuinely looping rather than sweeping once.
		require.Eventually(t, func() bool { return fake.callCount() >= 2 }, 2*time.Second, 5*time.Millisecond)

		cancel()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reaper did not stop after cancel")
		}

		// Once Run has returned no tick fires again, so the count is frozen. A later change would mean
		// the loop outlived its context.
		settled := fake.callCount()

		time.Sleep(50 * time.Millisecond)
		require.Equal(t, settled, fake.callCount())
	})

	t.Run("Sweep/Integration", func(t *testing.T) {
		t.Parallel()

		postgres.RunDBTest(t, configtest.PostgresPreset, migrations.Migrations, func(ctx context.Context, t *testing.T) {
			t.Helper()

			// Two jobs of one kind, both claimed with an already-lapsed lease (LeaseSeconds -1). One has
			// an attempt left and one is at its cap, so a single sweep through the loop must requeue the
			// first and abandon the second — the outcome asserted through the loop, not the DAO alone.
			requeueID := uuid.New()
			abandonID := uuid.New()

			enqueueJob(ctx, t, requeueID, 2)
			enqueueJob(ctx, t, abandonID, 1)

			_, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
				Kinds: []string{"generate"}, WorkerID: "worker-x", Limit: 10, LeaseSeconds: -1,
			})
			require.NoError(t, err)

			reaper := lib.NewReaper(core.NewJobReap(dao.NewJobReap(), 7), time.Minute)

			recovered, err := reaper.Sweep(ctx)
			require.NoError(t, err)
			require.Equal(t, 2, recovered)

			requeued, err := dao.NewJobGetByID().Exec(ctx, &dao.JobGetByIDRequest{ID: requeueID})
			require.NoError(t, err)
			require.Equal(t, dao.JobStatusPending, requeued.Status)

			abandoned, err := dao.NewJobGetByID().Exec(ctx, &dao.JobGetByIDRequest{ID: abandonID})
			require.NoError(t, err)
			require.Equal(t, dao.JobStatusAbandoned, abandoned.Status)
		})
	})
}
