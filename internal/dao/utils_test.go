package dao_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/dao"
)

// testOwner is the owner every worker-operation fixture is enqueued under. Ownership is exercised in
// the enqueue and get tests; these operations are keyed by job id and worker, not by owner.
var testOwner = uuid.MustParse("00000000-0000-0000-0000-00000000a11c")

// fixtureJobID is the id claimJob enqueues under, and fixtureWorkerID the worker it claims for.
// Both are shared so a settle/requeue/reap case can address the claim it set up — and name a
// different worker to exercise the "not yours" guard — without repeating the literals.
const (
	fixtureJobID    = "00000000-0000-0000-0000-000000000001"
	fixtureWorkerID = "worker-a"
)

// enqueueJob seeds one pending job. maxAttempts lets a case choose whether a reaped job requeues or
// abandons.
func enqueueJob(ctx context.Context, t *testing.T, id string, maxAttempts int16) *dao.Job {
	t.Helper()

	entity, err := dao.NewJobEnqueue().Exec(ctx, &dao.JobEnqueueRequest{
		ID:                 uuid.MustParse(id),
		Kind:               "generate",
		Payload:            json.RawMessage(`{}`),
		OwnerID:            testOwner,
		IdempotencyKey:     lo.ToPtr(id),
		RequestFingerprint: []byte{0x01},
		MaxAttempts:        maxAttempts,
	})
	require.NoError(t, err)

	return entity
}

// claimJob enqueues the fixture job and claims it for workerID, returning the claimed row. A
// negative leaseSeconds lands the lease in the past, so the job is immediately reapable — which is
// how the reaper tests avoid depending on wall-clock timing.
func claimJob(ctx context.Context, t *testing.T, maxAttempts int16, leaseSeconds int) *dao.Job {
	t.Helper()

	enqueueJob(ctx, t, fixtureJobID, maxAttempts)

	claimed, err := dao.NewJobClaim().Exec(ctx, &dao.JobClaimRequest{
		Kinds:        []string{"generate"},
		WorkerID:     fixtureWorkerID,
		Limit:        10,
		LeaseSeconds: leaseSeconds,
	})
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	return claimed[0]
}
