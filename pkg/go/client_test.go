package servicejobs_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	golibproto "github.com/a-novel-kit/golib/grpcf/proto/gen"

	"github.com/a-novel/service-jobs/internal/config/env"
	servicejobs "github.com/a-novel/service-jobs/pkg/go"
)

// These run against the standalone-grpc image the test-pkg lane boots, so they drive the real server
// through the published client rather than a mock.

func newClient(t *testing.T) servicejobs.Client {
	t.Helper()

	client, err := servicejobs.NewClient(env.GrpcUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(client.Close)

	return client
}

func TestClientEcho(t *testing.T) {
	t.Parallel()

	_, err := newClient(t).UnaryEcho(t.Context(), &golibproto.UnaryEchoRequest{})
	require.NoError(t, err)
}

func TestClientEnqueueAndGet(t *testing.T) {
	t.Parallel()

	client := newClient(t)
	owner := uuid.NewString()
	key := uuid.NewString()

	enqueue := func() *servicejobs.JobEnqueueResponse {
		t.Helper()

		resp, err := client.JobEnqueue(t.Context(), &servicejobs.JobEnqueueRequest{
			Kind:           "generate",
			Payload:        []byte(`{"seed":"an idea"}`),
			OwnerId:        owner,
			IdempotencyKey: &key,
			MaxAttempts:    1,
		})
		require.NoError(t, err)

		return resp
	}

	first := enqueue()
	require.True(t, first.GetCreated())
	require.Equal(t, servicejobs.JobStatusPending, first.GetJob().GetStatus())

	// A second enqueue under the same key is an idempotent replay: the same job, created=false.
	replay := enqueue()
	require.False(t, replay.GetCreated())
	require.Equal(t, first.GetJob().GetId(), replay.GetJob().GetId())

	// The owner reads its own job back.
	got, err := client.JobGet(t.Context(), &servicejobs.JobGetRequest{Id: first.GetJob().GetId(), OwnerId: owner})
	require.NoError(t, err)
	require.Equal(t, first.GetJob().GetId(), got.GetJob().GetId())

	// A different owner reading the same id gets NOT_FOUND — never a distinguishable access error.
	_, err = client.JobGet(t.Context(), &servicejobs.JobGetRequest{
		Id: first.GetJob().GetId(), OwnerId: uuid.NewString(),
	})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestClientEnqueueConflict(t *testing.T) {
	t.Parallel()

	client := newClient(t)
	owner := uuid.NewString()
	key := uuid.NewString()

	_, err := client.JobEnqueue(t.Context(), &servicejobs.JobEnqueueRequest{
		Kind: "generate", Payload: []byte(`{"a":1}`), OwnerId: owner, IdempotencyKey: &key, MaxAttempts: 1,
	})
	require.NoError(t, err)

	// The same key with a different payload reused it for different work: ALREADY_EXISTS, not a replay.
	_, err = client.JobEnqueue(t.Context(), &servicejobs.JobEnqueueRequest{
		Kind: "generate", Payload: []byte(`{"a":2}`), OwnerId: owner, IdempotencyKey: &key, MaxAttempts: 1,
	})
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestClientClaimAndSettle(t *testing.T) {
	t.Parallel()

	client := newClient(t)
	owner := uuid.NewString()
	worker := uuid.NewString()
	// A worker-unique kind so the claim only takes this test's job, not a parallel test's.
	kind := "settle-" + worker
	key := uuid.NewString()

	enqueued, err := client.JobEnqueue(t.Context(), &servicejobs.JobEnqueueRequest{
		Kind: kind, Payload: []byte(`{}`), OwnerId: owner, IdempotencyKey: &key, MaxAttempts: 1,
	})
	require.NoError(t, err)

	claimed, err := client.JobClaim(t.Context(), &servicejobs.JobClaimRequest{
		Kinds: []string{kind}, WorkerId: worker, Limit: 10, LeaseSeconds: 60,
	})
	require.NoError(t, err)
	require.Len(t, claimed.GetJobs(), 1)
	require.Equal(t, enqueued.GetJob().GetId(), claimed.GetJobs()[0].GetId())

	settled, err := client.JobSettle(t.Context(), &servicejobs.JobSettleRequest{
		Id:       enqueued.GetJob().GetId(),
		WorkerId: worker,
		Outcome:  &servicejobs.JobSettleResult{Result: []byte(`{"done":true}`)},
	})
	require.NoError(t, err)
	require.Equal(t, servicejobs.JobStatusSucceeded, settled.GetJob().GetStatus())
}
