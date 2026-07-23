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

func TestJobQueueDepth(t *testing.T) {
	t.Parallel()

	errFoo := errors.New("foo")
	age := 3 * time.Minute

	type daoMock struct {
		resp *dao.QueueDepth
		err  error
	}

	testCases := []struct {
		name string

		daoMock *daoMock

		expect    *core.QueueDepth
		expectErr error
	}{
		{
			name: "Success/Backlog",

			daoMock: &daoMock{resp: &dao.QueueDepth{Pending: 5, OldestPendingAge: &age}},

			expect: &core.QueueDepth{Pending: 5, OldestPendingAge: &age},
		},
		{
			// An empty queue carries a zero count and a nil age, which the service passes through as-is.
			name: "Success/Empty",

			daoMock: &daoMock{resp: &dao.QueueDepth{Pending: 0}},

			expect: &core.QueueDepth{Pending: 0},
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

			daoQueueDepth := coremocks.NewMockJobQueueDepthDao(t)
			daoQueueDepth.EXPECT().Exec(mock.Anything).Return(testCase.daoMock.resp, testCase.daoMock.err)

			service := core.NewJobQueueDepth(daoQueueDepth)

			depth, err := service.Exec(t.Context())

			if testCase.expectErr != nil {
				require.ErrorIs(t, err, testCase.expectErr)
				require.Nil(t, depth)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expect, depth)
			}

			daoQueueDepth.AssertExpectations(t)
		})
	}
}
