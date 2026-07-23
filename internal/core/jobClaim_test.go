package core_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobClaim(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")

	testCases := []struct {
		name string

		request *core.JobClaimRequest

		daoResp   []*dao.Job
		daoErr    error
		callsDao  bool
		expectLen int
		expectErr error
	}{
		{
			name: "Success",

			request:  &core.JobClaimRequest{Kinds: []string{"generate"}, WorkerID: "w", Limit: 5, LeaseSeconds: 60},
			daoResp:  []*dao.Job{{ID: uuid.New()}, {ID: uuid.New()}},
			callsDao: true,

			expectLen: 2,
		},
		{
			name: "Error/MissingWorker",

			request:   &core.JobClaimRequest{Kinds: []string{"generate"}, Limit: 5, LeaseSeconds: 60},
			expectErr: core.ErrInvalidRequest,
		},
		{
			name: "Error/NonPositiveLimit",

			request:   &core.JobClaimRequest{Kinds: []string{"generate"}, WorkerID: "w", Limit: 0, LeaseSeconds: 60},
			expectErr: core.ErrInvalidRequest,
		},
		{
			name: "Error/Dao",

			request:   &core.JobClaimRequest{Kinds: []string{"generate"}, WorkerID: "w", Limit: 5, LeaseSeconds: 60},
			daoErr:    errFoo,
			callsDao:  true,
			expectErr: errFoo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockDao := coremocks.NewMockJobClaimDao(t)

			if testCase.callsDao {
				mockDao.EXPECT().
					Exec(mock.Anything, &dao.JobClaimRequest{
						Kinds:        testCase.request.Kinds,
						WorkerID:     testCase.request.WorkerID,
						Limit:        testCase.request.Limit,
						LeaseSeconds: testCase.request.LeaseSeconds,
					}).
					Return(testCase.daoResp, testCase.daoErr)
			}

			service := core.NewJobClaim(mockDao)

			resp, err := service.Exec(t.Context(), testCase.request)
			require.ErrorIs(t, err, testCase.expectErr)
			require.Len(t, resp, testCase.expectLen)

			mockDao.AssertExpectations(t)
		})
	}
}
