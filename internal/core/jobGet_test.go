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

func TestJobGet(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	owner := uuid.MustParse("00000000-0000-0000-0000-00000000a11c")
	errFoo := errors.New("foo")

	type daoMock struct {
		resp *dao.Job
		err  error
	}

	testCases := []struct {
		name string

		request *core.JobGetRequest

		daoMock *daoMock

		expectErr error
	}{
		{
			name: "Success",

			request: &core.JobGetRequest{ID: id, OwnerID: owner},
			daoMock: &daoMock{resp: &dao.Job{ID: id, OwnerID: owner, Status: dao.JobStatusPending}},
		},
		{
			// The data-access not-found sentinel is translated to the core one, which the handler maps
			// to NOT_FOUND — a cross-owner read reports the same, never access-denied.
			name: "Error/NotFound",

			request:   &core.JobGetRequest{ID: id, OwnerID: owner},
			daoMock:   &daoMock{err: dao.ErrJobGetNotFound},
			expectErr: core.ErrJobNotFound,
		},
		{
			name: "Error/MissingOwner",

			request:   &core.JobGetRequest{ID: id},
			expectErr: core.ErrInvalidRequest,
		},
		{
			name: "Error/Dao",

			request:   &core.JobGetRequest{ID: id, OwnerID: owner},
			daoMock:   &daoMock{err: errFoo},
			expectErr: errFoo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockDao := coremocks.NewMockJobGetDao(t)

			if testCase.daoMock != nil {
				mockDao.EXPECT().
					Exec(mock.Anything, &dao.JobGetRequest{ID: testCase.request.ID, OwnerID: testCase.request.OwnerID}).
					Return(testCase.daoMock.resp, testCase.daoMock.err)
			}

			service := core.NewJobGet(mockDao)

			_, err := service.Exec(t.Context(), testCase.request)
			require.ErrorIs(t, err, testCase.expectErr)

			mockDao.AssertExpectations(t)
		})
	}
}
