package handlers_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/config/configtest"
	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers"
	handlersmocks "github.com/a-novel/service-jobs/internal/handlers/mocks"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

func TestStatus(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")
	age := 5 * time.Minute

	type queueMock struct {
		resp *core.QueueDepth
		err  error
	}

	testCases := []struct {
		name string

		skipPostgres bool
		queueMock    queueMock

		expect *protogen.StatusResponse
	}{
		{
			name: "Success",

			queueMock: queueMock{resp: &core.QueueDepth{Pending: 3, OldestPendingAge: &age}},

			expect: &protogen.StatusResponse{
				Postgres: &protogen.DependencyHealth{Status: protogen.DependencyStatus_DEPENDENCY_STATUS_UP},
				Queue:    &protogen.QueueDepth{Pending: 3, OldestPendingAge: durationpb.New(age)},
			},
		},
		{
			// An empty queue reports a zero count and no age — the absence, not a zero duration.
			name: "Success/EmptyQueue",

			queueMock: queueMock{resp: &core.QueueDepth{Pending: 0}},

			expect: &protogen.StatusResponse{
				Postgres: &protogen.DependencyHealth{Status: protogen.DependencyStatus_DEPENDENCY_STATUS_UP},
				Queue:    &protogen.QueueDepth{Pending: 0},
			},
		},
		{
			// A degraded database fails the ping and the backlog query alike, so postgres reports down and
			// the queue is absent. Comparing the whole response guards the shape: it fails if a raw error
			// string is ever attached back onto either message.
			name: "Success/Degraded",

			skipPostgres: true,
			queueMock:    queueMock{err: errFoo},

			expect: &protogen.StatusResponse{
				Postgres: &protogen.DependencyHealth{Status: protogen.DependencyStatus_DEPENDENCY_STATUS_DOWN},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			queueDepth := handlersmocks.NewMockQueueDepthService(t)
			queueDepth.EXPECT().Exec(mock.Anything).Return(testCase.queueMock.resp, testCase.queueMock.err)

			handler := handlers.NewGrpcStatus(queueDepth, time.Minute)

			ctx := t.Context()

			if !testCase.skipPostgres {
				var err error

				ctx, err = postgres.NewContext(ctx, configtest.PostgresPreset)
				require.NoError(t, err)
			}

			res, err := handler.Status(ctx, new(protogen.StatusRequest))
			require.NoError(t, err)
			require.Equal(t, testCase.expect, res)

			queueDepth.AssertExpectations(t)
		})
	}

	t.Run("Cache/HitWithinTTL", func(t *testing.T) {
		t.Parallel()

		queueDepth := handlersmocks.NewMockQueueDepthService(t)
		// Called once despite two probes: the second is served from cache within the TTL. A second call
		// to the mock would be unexpected and fail the test.
		queueDepth.EXPECT().Exec(mock.Anything).Return(&core.QueueDepth{Pending: 7}, nil).Once()

		handler := handlers.NewGrpcStatus(queueDepth, time.Hour)

		ctx, err := postgres.NewContext(t.Context(), configtest.PostgresPreset)
		require.NoError(t, err)

		for range 2 {
			res, err := handler.Status(ctx, new(protogen.StatusRequest))
			require.NoError(t, err)
			require.Equal(t, int64(7), res.GetQueue().GetPending())
		}

		queueDepth.AssertExpectations(t)
	})
}
