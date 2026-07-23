package handlers_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers"
	handlersmocks "github.com/a-novel/service-jobs/internal/handlers/mocks"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// fakeWatchStream is a ServerStreamingServer that records what the handler sends. It embeds a nil
// ServerStream: the handler only calls Send and Context, so the rest is never reached.
type fakeWatchStream struct {
	grpc.ServerStream

	// A gRPC stream carries its own context, so a fake of one holds it too; this is not the
	// context-in-struct the linter warns about in production code.
	ctx  context.Context //nolint:containedctx
	mu   sync.Mutex
	sent []*protogen.JobWatchResponse
}

func (s *fakeWatchStream) Context() context.Context { return s.ctx }

func (s *fakeWatchStream) Send(response *protogen.JobWatchResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sent = append(s.sent, response)

	return nil
}

func (s *fakeWatchStream) snapshots() []*protogen.JobWatchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*protogen.JobWatchResponse{}, s.sent...)
}

func TestJobWatch(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	owner := uuid.New()

	t.Run("StreamsUntilTerminal", func(t *testing.T) {
		t.Parallel()

		service := handlersmocks.NewMockJobWatchService(t)
		getReq := &core.JobGetRequest{ID: id, OwnerID: owner}

		// The job is claimed on the first read and succeeded on the second, so the stream sends two
		// snapshots and then stops on the terminal status.
		service.EXPECT().Exec(mock.Anything, getReq).
			Return(&core.Job{ID: id, OwnerID: owner, Status: core.JobStatusClaimed}, nil).Once()
		service.EXPECT().Exec(mock.Anything, getReq).
			Return(&core.Job{ID: id, OwnerID: owner, Status: core.JobStatusSucceeded}, nil).Once()

		stream := &fakeWatchStream{ctx: t.Context()}
		handler := handlers.NewJobWatch(service, time.Millisecond)

		err := handler.JobWatch(&protogen.JobWatchRequest{Id: id.String(), OwnerId: owner.String()}, stream)
		require.NoError(t, err)

		sent := stream.snapshots()
		require.Len(t, sent, 2)
		require.Equal(t, protogen.JobStatus_JOB_STATUS_CLAIMED, sent[0].GetJob().GetStatus())
		require.Equal(t, protogen.JobStatus_JOB_STATUS_SUCCEEDED, sent[1].GetJob().GetStatus())

		service.AssertExpectations(t)
	})

	t.Run("Error/NotFound", func(t *testing.T) {
		t.Parallel()

		service := handlersmocks.NewMockJobWatchService(t)
		service.EXPECT().Exec(mock.Anything, &core.JobGetRequest{ID: id, OwnerID: owner}).
			Return(nil, core.ErrJobNotFound)

		stream := &fakeWatchStream{ctx: t.Context()}
		handler := handlers.NewJobWatch(service, time.Millisecond)

		err := handler.JobWatch(&protogen.JobWatchRequest{Id: id.String(), OwnerId: owner.String()}, stream)
		require.Equal(t, codes.NotFound, status.Code(err))
		require.Empty(t, stream.snapshots())

		service.AssertExpectations(t)
	})

	t.Run("Error/InvalidID", func(t *testing.T) {
		t.Parallel()

		handler := handlers.NewJobWatch(handlersmocks.NewMockJobWatchService(t), time.Millisecond)

		err := handler.JobWatch(&protogen.JobWatchRequest{Id: "bad", OwnerId: owner.String()},
			&fakeWatchStream{ctx: t.Context()})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}
