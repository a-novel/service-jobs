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

func TestJobGet(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	owner := uuid.New()

	type serviceMock struct {
		resp *core.Job
		err  error
	}

	testCases := []struct {
		name string

		request *protogen.JobGetRequest

		serviceMock *serviceMock

		expectCode codes.Code
	}{
		{
			name: "Success",

			request:     &protogen.JobGetRequest{Id: id.String(), OwnerId: owner.String()},
			serviceMock: &serviceMock{resp: &core.Job{ID: id, OwnerID: owner, Status: core.JobStatusPending}},
		},
		{
			// A cross-owner read reports not-found, which the handler maps to NOT_FOUND — never a
			// distinguishable access error.
			name: "Error/NotFound",

			request:     &protogen.JobGetRequest{Id: id.String(), OwnerId: owner.String()},
			serviceMock: &serviceMock{err: core.ErrJobNotFound},
			expectCode:  codes.NotFound,
		},
		{
			name: "Error/InvalidID",

			request:    &protogen.JobGetRequest{Id: "nope", OwnerId: owner.String()},
			expectCode: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			service := handlersmocks.NewMockJobGetService(t)

			if testCase.serviceMock != nil {
				service.EXPECT().
					Exec(mock.Anything, &core.JobGetRequest{ID: id, OwnerID: owner}).
					Return(testCase.serviceMock.resp, testCase.serviceMock.err)
			}

			handler := handlers.NewJobGet(service)

			_, err := handler.JobGet(t.Context(), testCase.request)
			require.Equal(t, testCase.expectCode, status.Code(err))

			service.AssertExpectations(t)
		})
	}
}
