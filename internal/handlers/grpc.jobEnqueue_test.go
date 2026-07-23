package handlers_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers"
	handlersmocks "github.com/a-novel/service-jobs/internal/handlers/mocks"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

func TestJobEnqueue(t *testing.T) {
	t.Parallel()

	owner := uuid.New()

	type serviceMock struct {
		resp *core.JobEnqueueResult
		err  error
	}

	testCases := []struct {
		name string

		request *protogen.JobEnqueueRequest

		serviceMock *serviceMock

		expectCreated bool
		expectCode    codes.Code
	}{
		{
			name: "Created",

			request: &protogen.JobEnqueueRequest{Kind: "generate", OwnerId: owner.String(), MaxAttempts: 1},

			serviceMock: &serviceMock{
				resp: &core.JobEnqueueResult{Job: &core.Job{ID: uuid.New(), OwnerID: owner}, Created: true},
			},

			expectCreated: true,
		},
		{
			name: "Replay",

			request: &protogen.JobEnqueueRequest{Kind: "generate", OwnerId: owner.String(), MaxAttempts: 1},

			serviceMock: &serviceMock{
				resp: &core.JobEnqueueResult{Job: &core.Job{ID: uuid.New(), OwnerID: owner}, Created: false},
			},

			expectCreated: false,
		},
		{
			// A bad owner id fails at the boundary, before the service is asked.
			name: "Error/InvalidOwner",

			request:    &protogen.JobEnqueueRequest{Kind: "generate", OwnerId: "not-a-uuid"},
			expectCode: codes.InvalidArgument,
		},
		{
			name: "Error/Conflict",

			request: &protogen.JobEnqueueRequest{Kind: "generate", OwnerId: owner.String(), MaxAttempts: 1},

			serviceMock: &serviceMock{err: core.ErrJobConflict},
			expectCode:  codes.AlreadyExists,
		},
		{
			name: "Error/Invalid",

			request: &protogen.JobEnqueueRequest{Kind: "generate", OwnerId: owner.String()},

			serviceMock: &serviceMock{err: core.ErrInvalidRequest},
			expectCode:  codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			service := handlersmocks.NewMockJobEnqueueService(t)

			if testCase.serviceMock != nil {
				service.EXPECT().
					Exec(mock.Anything, mock.MatchedBy(func(r *core.JobEnqueueRequest) bool {
						return r.Kind == testCase.request.GetKind() && r.OwnerID == owner
					})).
					Return(testCase.serviceMock.resp, testCase.serviceMock.err)
			}

			handler := handlers.NewJobEnqueue(service)

			resp, err := handler.JobEnqueue(t.Context(), testCase.request)
			require.Equal(t, testCase.expectCode, status.Code(err))

			if testCase.expectCode == codes.OK {
				require.Equal(t, testCase.expectCreated, resp.GetCreated())
			}

			service.AssertExpectations(t)
		})
	}
}
