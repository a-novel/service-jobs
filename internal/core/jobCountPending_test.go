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

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		mockDao := coremocks.NewMockJobCountPendingDao(t)
		mockDao.EXPECT().
			Exec(mock.Anything).
			Return(&dao.JobPendingStats{Pending: 3, OldestAge: 90 * time.Second}, nil)

		service := core.NewJobCountPending(mockDao)

		stats, err := service.Exec(t.Context())
		require.NoError(t, err)
		require.Equal(t, 3, stats.Pending)
		require.Equal(t, 90*time.Second, stats.OldestAge)

		mockDao.AssertExpectations(t)
	})

	t.Run("Error/Dao", func(t *testing.T) {
		t.Parallel()

		errFoo := errors.New("foo")

		mockDao := coremocks.NewMockJobCountPendingDao(t)
		mockDao.EXPECT().Exec(mock.Anything).Return(nil, errFoo)

		service := core.NewJobCountPending(mockDao)

		_, err := service.Exec(t.Context())
		require.ErrorIs(t, err, errFoo)

		mockDao.AssertExpectations(t)
	})
}
