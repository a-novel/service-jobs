package core_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobReap(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")

	// The service always reaps with the same abandon error and the retention horizon it was built
	// with, regardless of what lapsed. The expected request pins both.
	expectRequest := &dao.JobReapRequest{
		Error:         json.RawMessage(`{"reason":"lease expired"}`),
		RetentionDays: 7,
	}

	type daoMock struct {
		resp []*dao.Job
		err  error
	}

	testCases := []struct {
		name string

		daoMock *daoMock

		expectLen int
		expectErr error
	}{
		{
			// A sweep that recovers two lapsed claims maps both to the core view.
			name: "Success/Reaped",

			daoMock: &daoMock{
				resp: []*dao.Job{
					{ID: uuid.New(), Status: dao.JobStatusPending},
					{ID: uuid.New(), Status: dao.JobStatusAbandoned},
				},
			},

			expectLen: 2,
		},
		{
			// An idle sweep finds nothing and is not an error.
			name: "Success/Empty",

			daoMock: &daoMock{resp: []*dao.Job{}},

			expectLen: 0,
		},
		{
			name: "Error/Dao",

			daoMock: &daoMock{err: errFoo},

			expectErr: errFoo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			daoJobReap := coremocks.NewMockJobReapDao(t)
			daoJobReap.EXPECT().Exec(mock.Anything, expectRequest).Return(testCase.daoMock.resp, testCase.daoMock.err)

			service := core.NewJobReap(daoJobReap, 7)

			jobs, err := service.Exec(t.Context())

			if testCase.expectErr != nil {
				require.ErrorIs(t, err, testCase.expectErr)
				require.Nil(t, jobs)
			} else {
				require.NoError(t, err)
				require.Len(t, jobs, testCase.expectLen)
			}

			daoJobReap.AssertExpectations(t)
		})
	}
}
