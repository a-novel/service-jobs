package handlers_test

import (
	"errors"
	"testing"
	"time"

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

func TestJobClaim(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")

	providerCallID := "provider-xyz"
	settledAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	expiresAt := settledAt.Add(7 * 24 * time.Hour)

	// The first job carries every optional field so the batch mapping exercises jobToProto's nullable
	// translators end to end. The handler is a pure translator, so this field combination need not be a
	// state the queue itself produces.
	claimed := []*core.Job{
		{
			ID: uuid.New(), OwnerID: uuid.New(), Kind: "generate", Status: core.JobStatusClaimed,
			ProviderCallID: &providerCallID, SettledAt: &settledAt, ExpiresAt: &expiresAt,
		},
		{ID: uuid.New(), OwnerID: uuid.New(), Kind: "generate", Status: core.JobStatusClaimed},
	}

	// The handler passes the request straight through, so every case reaches the service with the same
	// translated request.
	request := &protogen.JobClaimRequest{Kinds: []string{"generate"}, WorkerId: "worker-a", Limit: 10, LeaseSeconds: 60}
	coreRequest := &core.JobClaimRequest{Kinds: []string{"generate"}, WorkerID: "worker-a", Limit: 10, LeaseSeconds: 60}

	type serviceMock struct {
		resp []*core.Job
		err  error
	}

	testCases := []struct {
		name string

		serviceMock serviceMock

		expectCode codes.Code
	}{
		{
			name: "Success",

			serviceMock: serviceMock{resp: claimed},
		},
		{
			// A claim that takes nothing is a success with an empty batch, not an error.
			name: "Success/EmptyBatch",

			serviceMock: serviceMock{resp: []*core.Job{}},
		},
		{
			// The core layer rejects a bad request — no worker, a non-positive limit — and the handler
			// maps that to INVALID_ARGUMENT.
			name: "Error/InvalidArgument",

			serviceMock: serviceMock{err: core.ErrInvalidRequest},
			expectCode:  codes.InvalidArgument,
		},
		{
			name: "Error/Internal",

			serviceMock: serviceMock{err: errFoo},
			expectCode:  codes.Internal,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			service := handlersmocks.NewMockJobClaimService(t)
			service.EXPECT().Exec(mock.Anything, coreRequest).Return(testCase.serviceMock.resp, testCase.serviceMock.err)

			handler := handlers.NewJobClaim(service)

			res, err := handler.JobClaim(t.Context(), request)
			require.Equal(t, testCase.expectCode, status.Code(err))

			if testCase.expectCode == codes.OK {
				// The batch is mapped one job per row, in order.
				require.Len(t, res.GetJobs(), len(testCase.serviceMock.resp))

				for i, job := range testCase.serviceMock.resp {
					require.Equal(t, job.ID.String(), res.GetJobs()[i].GetId())
				}
			}

			// The fully-populated first job pins jobToProto's nullable translators: the provider call id
			// and the RFC3339-formatted settle timestamps.
			if len(res.GetJobs()) > 0 {
				first := res.GetJobs()[0]
				require.Equal(t, providerCallID, first.GetProviderCallId())
				require.Equal(t, settledAt.Format(time.RFC3339), first.GetSettledAt())
				require.Equal(t, expiresAt.Format(time.RFC3339), first.GetExpiresAt())
			}

			service.AssertExpectations(t)
		})
	}
}
