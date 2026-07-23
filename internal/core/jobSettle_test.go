package core_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/transaction/transactiontest"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobSettle(t *testing.T) {
	t.Parallel()

	const (
		jobID  = "00000000-0000-0000-0000-000000000001"
		worker = "worker-a"
	)

	id := uuid.MustParse(jobID)

	claimedBy := func(w string) *dao.Job {
		return &dao.Job{ID: id, Status: dao.JobStatusClaimed, ClaimedBy: lo.ToPtr(w), Attempt: 1, MaxAttempts: 2}
	}

	t.Run("Succeeded", func(t *testing.T) {
		t.Parallel()

		settleDao := coremocks.NewMockJobSettleSettleDao(t)
		settleDao.EXPECT().
			Exec(mock.Anything, &dao.JobSettleRequest{
				ID: id, WorkerID: worker, Status: dao.JobStatusSucceeded,
				Result: json.RawMessage(`{"ok":true}`), RetentionDays: 7,
			}).
			Return(&dao.Job{ID: id, Status: dao.JobStatusSucceeded}, nil)

		service := core.NewJobSettle(settleDao,
			coremocks.NewMockJobSettleRequeueDao(t), coremocks.NewMockJobSettleGetDao(t),
			transactiontest.NewTransactor(), 7)

		result, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Result: json.RawMessage(`{"ok":true}`),
		})
		require.NoError(t, err)
		require.Equal(t, dao.JobStatusSucceeded, result.Status)

		settleDao.AssertExpectations(t)
	})

	t.Run("FailedNonRetryable", func(t *testing.T) {
		t.Parallel()

		// A non-retryable failure is one terminal write: the get and requeue daos are never touched,
		// and the transactor is never entered.
		settleDao := coremocks.NewMockJobSettleSettleDao(t)
		settleDao.EXPECT().
			Exec(mock.Anything, &dao.JobSettleRequest{
				ID: id, WorkerID: worker, Status: dao.JobStatusFailed,
				Error: json.RawMessage(`{"reason":"boom"}`), RetentionDays: 7,
			}).
			Return(&dao.Job{ID: id, Status: dao.JobStatusFailed}, nil)

		transactor := transactiontest.NewTransactor()
		service := core.NewJobSettle(settleDao,
			coremocks.NewMockJobSettleRequeueDao(t), coremocks.NewMockJobSettleGetDao(t), transactor, 7)

		_, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Failure: &core.JobFailure{Error: json.RawMessage(`{"reason":"boom"}`)},
		})
		require.NoError(t, err)
		require.Zero(t, transactor.Calls())

		settleDao.AssertExpectations(t)
	})

	t.Run("RetryableWithAttemptsRequeues", func(t *testing.T) {
		t.Parallel()

		// attempt 1 < max 2: an attempt remains, so a retryable failure returns the job to pending.
		getDao := coremocks.NewMockJobSettleGetDao(t)
		getDao.EXPECT().Exec(mock.Anything, &dao.JobGetByIDRequest{ID: id}).Return(claimedBy(worker), nil)

		requeueDao := coremocks.NewMockJobSettleRequeueDao(t)
		requeueDao.EXPECT().
			Exec(mock.Anything, &dao.JobRequeueRequest{ID: id, WorkerID: worker}).
			Return(&dao.Job{ID: id, Status: dao.JobStatusPending}, nil)

		transactor := transactiontest.NewTransactor()
		service := core.NewJobSettle(coremocks.NewMockJobSettleSettleDao(t), requeueDao, getDao, transactor, 7)

		result, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
		})
		require.NoError(t, err)
		require.Equal(t, dao.JobStatusPending, result.Status)
		require.Equal(t, 1, transactor.Calls())

		getDao.AssertExpectations(t)
		requeueDao.AssertExpectations(t)
	})

	t.Run("RetryableWithoutAttemptsFails", func(t *testing.T) {
		t.Parallel()

		// attempt 2, max 2: no attempt remains, so even a retryable failure settles failed.
		exhausted := &dao.Job{ID: id, Status: dao.JobStatusClaimed, ClaimedBy: lo.ToPtr(worker), Attempt: 2, MaxAttempts: 2}

		getDao := coremocks.NewMockJobSettleGetDao(t)
		getDao.EXPECT().Exec(mock.Anything, &dao.JobGetByIDRequest{ID: id}).Return(exhausted, nil)

		settleDao := coremocks.NewMockJobSettleSettleDao(t)
		settleDao.EXPECT().
			Exec(mock.Anything, &dao.JobSettleRequest{
				ID: id, WorkerID: worker, Status: dao.JobStatusFailed, Error: json.RawMessage(`{}`), RetentionDays: 7,
			}).
			Return(&dao.Job{ID: id, Status: dao.JobStatusFailed}, nil)

		service := core.NewJobSettle(settleDao, coremocks.NewMockJobSettleRequeueDao(t), getDao,
			transactiontest.NewTransactor(), 7)

		result, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
		})
		require.NoError(t, err)
		require.Equal(t, dao.JobStatusFailed, result.Status)

		getDao.AssertExpectations(t)
		settleDao.AssertExpectations(t)
	})

	t.Run("Error/RetryableNotClaimedByWorker", func(t *testing.T) {
		t.Parallel()

		// The reaper recovered the job — another worker holds it now, so this one cannot settle it.
		getDao := coremocks.NewMockJobSettleGetDao(t)
		getDao.EXPECT().Exec(mock.Anything, &dao.JobGetByIDRequest{ID: id}).Return(claimedBy("worker-b"), nil)

		service := core.NewJobSettle(coremocks.NewMockJobSettleSettleDao(t),
			coremocks.NewMockJobSettleRequeueDao(t), getDao, transactiontest.NewTransactor(), 7)

		_, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
		})
		require.ErrorIs(t, err, core.ErrJobNotClaimed)

		getDao.AssertExpectations(t)
	})

	t.Run("Error/RetryDecisionIsInsideTheTransaction", func(t *testing.T) {
		t.Parallel()

		// The whole point of the transactor: the read and the write are one unit. A failing transactor
		// never opens the scope, so neither the get nor the requeue dao is reached — proving the retry
		// decision runs inside WithinTx rather than merely near it. Mocks with no expectations fail on
		// any call.
		errTx := errors.New("transaction refused")

		service := core.NewJobSettle(coremocks.NewMockJobSettleSettleDao(t),
			coremocks.NewMockJobSettleRequeueDao(t), coremocks.NewMockJobSettleGetDao(t),
			transactiontest.NewFailingTransactor(errTx), 7)

		_, err := service.Exec(t.Context(), &core.JobSettleRequest{
			ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
		})
		require.ErrorIs(t, err, errTx)
	})
}
