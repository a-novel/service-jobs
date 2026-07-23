package core_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobCountPending(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")

	type daoMock struct {
		resp *dao.JobPendingStats
		err  error
	}

	testCases := []struct {
		name string

		daoMock *daoMock

		expect    *core.JobPendingStats
		expectErr error
	}{
		{
			name: "Success",

			daoMock: &daoMock{resp: &dao.JobPendingStats{Pending: 3, OldestAge: 90 * time.Second}},

			expect: &core.JobPendingStats{Pending: 3, OldestAge: 90 * time.Second},
		},
		{
			name: "Error/Dao",

			daoMock:   &daoMock{err: errFoo},
			expectErr: errFoo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockDao := coremocks.NewMockJobCountPendingDao(t)

			if testCase.daoMock != nil {
				mockDao.EXPECT().Exec(mock.Anything).Return(testCase.daoMock.resp, testCase.daoMock.err)
			}

			service := core.NewJobCountPending(mockDao)

			stats, err := service.Exec(t.Context())
			require.ErrorIs(t, err, testCase.expectErr)
			require.Equal(t, testCase.expect, stats)

			mockDao.AssertExpectations(t)
		})
	}
}
