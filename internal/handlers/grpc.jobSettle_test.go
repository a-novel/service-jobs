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

func TestJobSettle(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	worker := "worker-a"

	testCases := []struct {
		name string

		request *protogen.JobSettleRequest

		// expectReq is the core request the oneof should decode to, or nil when the boundary rejects
		// the call before the service is reached.
		expectReq  *core.JobSettleRequest
		serviceErr error

		expectCode codes.Code
	}{
		{
			// A result arm decodes to a success: nil Failure, Result carried through.
			name: "Success",

			request: &protogen.JobSettleRequest{
				Id: id.String(), WorkerId: worker,
				Outcome: &protogen.JobSettleRequest_Result{Result: []byte(`{"ok":true}`)},
			},

			expectReq: &core.JobSettleRequest{ID: id, WorkerID: worker, Result: []byte(`{"ok":true}`)},
		},
		{
			// A failure arm decodes to a failure carrying the retryable flag.
			name: "Failure",

			request: &protogen.JobSettleRequest{
				Id: id.String(), WorkerId: worker,
				Outcome: &protogen.JobSettleRequest_Failure{
					Failure: &protogen.JobFailure{Error: []byte(`{"reason":"boom"}`), Retryable: true},
				},
			},

			expectReq: &core.JobSettleRequest{
				ID: id, WorkerID: worker,
				Failure: &core.JobFailure{Error: []byte(`{"reason":"boom"}`), Retryable: true},
			},
		},
		{
			// A worker acting on a job it no longer holds maps to FAILED_PRECONDITION.
			name: "Error/NotClaimed",

			request: &protogen.JobSettleRequest{
				Id: id.String(), WorkerId: worker,
				Outcome: &protogen.JobSettleRequest_Result{Result: []byte(`{}`)},
			},

			expectReq:  &core.JobSettleRequest{ID: id, WorkerID: worker, Result: []byte(`{}`)},
			serviceErr: core.ErrJobNotClaimed,
			expectCode: codes.FailedPrecondition,
		},
		{
			name: "Error/InvalidID",

			request:    &protogen.JobSettleRequest{Id: "bad", WorkerId: worker},
			expectCode: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			service := handlersmocks.NewMockJobSettleService(t)

			if testCase.expectReq != nil {
				service.EXPECT().
					Exec(mock.Anything, testCase.expectReq).
					Return(&core.Job{ID: id, Status: core.JobStatusSucceeded}, testCase.serviceErr)
			}

			handler := handlers.NewJobSettle(service)

			_, err := handler.JobSettle(t.Context(), testCase.request)
			require.Equal(t, testCase.expectCode, status.Code(err))

			service.AssertExpectations(t)
		})
	}
}
