package core_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel-kit/golib/transaction"
	"github.com/a-novel-kit/golib/transaction/transactiontest"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobSettle(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	worker := "worker-a"
	errTx := errors.New("transaction refused")

	// One mock struct per dependency: a nil pointer means that dependency must not be called, so the
	// mock is left with no registered expectation and fails on any call.
	type getMock struct {
		resp *dao.Job
		err  error
	}

	type settleMock struct {
		req  *dao.JobSettleRequest
		resp *dao.Job
	}

	type requeueMock struct {
		req  *dao.JobRequeueRequest
		resp *dao.Job
	}

	claimed := func(w string, attempt, maxAttempts int16) *dao.Job {
		return &dao.Job{
			ID: id, Status: dao.JobStatusClaimed, ClaimedBy: lo.ToPtr(w), Attempt: attempt, MaxAttempts: maxAttempts,
		}
	}

	testCases := []struct {
		name string

		request *core.JobSettleRequest

		// transactorErr, when set, makes the injected transactor refuse to open a scope.
		transactorErr error

		getMock     *getMock
		settleMock  *settleMock
		requeueMock *requeueMock

		expectStatus dao.JobStatus
		expectErr    error
	}{
		{
			// Success is one guarded write; the get and requeue daos are never touched.
			name: "Succeeded",

			request: &core.JobSettleRequest{ID: id, WorkerID: worker, Result: json.RawMessage(`{"ok":true}`)},

			settleMock: &settleMock{
				req: &dao.JobSettleRequest{
					ID: id, WorkerID: worker, Status: dao.JobStatusSucceeded,
					Result: json.RawMessage(`{"ok":true}`), RetentionDays: 7,
				},
				resp: &dao.Job{ID: id, Status: dao.JobStatusSucceeded},
			},

			expectStatus: dao.JobStatusSucceeded,
		},
		{
			// A non-retryable failure is also one write — no read, no transaction.
			name: "FailedNonRetryable",

			request: &core.JobSettleRequest{
				ID: id, WorkerID: worker, Failure: &core.JobFailure{Error: json.RawMessage(`{"reason":"boom"}`)},
			},

			settleMock: &settleMock{
				req: &dao.JobSettleRequest{
					ID: id, WorkerID: worker, Status: dao.JobStatusFailed,
					Error: json.RawMessage(`{"reason":"boom"}`), RetentionDays: 7,
				},
				resp: &dao.Job{ID: id, Status: dao.JobStatusFailed},
			},

			expectStatus: dao.JobStatusFailed,
		},
		{
			// attempt 1 < max 2: an attempt remains, so a retryable failure returns the job to pending.
			name: "RetryableWithAttemptsRequeues",

			request: &core.JobSettleRequest{
				ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
			},

			getMock: &getMock{resp: claimed(worker, 1, 2)},
			requeueMock: &requeueMock{
				req:  &dao.JobRequeueRequest{ID: id, WorkerID: worker},
				resp: &dao.Job{ID: id, Status: dao.JobStatusPending},
			},

			expectStatus: dao.JobStatusPending,
		},
		{
			// attempt 2, max 2: no attempt remains, so even a retryable failure settles failed.
			name: "RetryableWithoutAttemptsFails",

			request: &core.JobSettleRequest{
				ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
			},

			getMock: &getMock{resp: claimed(worker, 2, 2)},
			settleMock: &settleMock{
				req: &dao.JobSettleRequest{
					ID: id, WorkerID: worker, Status: dao.JobStatusFailed, Error: json.RawMessage(`{}`), RetentionDays: 7,
				},
				resp: &dao.Job{ID: id, Status: dao.JobStatusFailed},
			},

			expectStatus: dao.JobStatusFailed,
		},
		{
			// The reaper recovered the job — another worker holds it now — so this one cannot settle it.
			name: "Error/RetryableNotClaimedByWorker",

			request: &core.JobSettleRequest{
				ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
			},

			getMock: &getMock{resp: claimed("worker-b", 1, 2)},

			expectErr: core.ErrJobNotClaimed,
		},
		{
			// The transactor never opens the scope, so neither the get nor the requeue dao is reached:
			// the retry decision runs inside WithinTx rather than merely near it.
			name: "Error/RetryDecisionIsInsideTheTransaction",

			request: &core.JobSettleRequest{
				ID: id, WorkerID: worker, Failure: &core.JobFailure{Retryable: true, Error: json.RawMessage(`{}`)},
			},

			transactorErr: errTx,

			expectErr: errTx,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			getDao := coremocks.NewMockJobSettleGetDao(t)
			if testCase.getMock != nil {
				getDao.EXPECT().
					Exec(mock.Anything, &dao.JobGetByIDRequest{ID: id}).
					Return(testCase.getMock.resp, testCase.getMock.err)
			}

			settleDao := coremocks.NewMockJobSettleSettleDao(t)
			if testCase.settleMock != nil {
				settleDao.EXPECT().Exec(mock.Anything, testCase.settleMock.req).Return(testCase.settleMock.resp, nil)
			}

			requeueDao := coremocks.NewMockJobSettleRequeueDao(t)
			if testCase.requeueMock != nil {
				requeueDao.EXPECT().Exec(mock.Anything, testCase.requeueMock.req).Return(testCase.requeueMock.resp, nil)
			}

			var transactor transaction.Transactor = transactiontest.NewTransactor()
			if testCase.transactorErr != nil {
				transactor = transactiontest.NewFailingTransactor(testCase.transactorErr)
			}

			service := core.NewJobSettle(settleDao, requeueDao, getDao, transactor, 7)

			result, err := service.Exec(t.Context(), testCase.request)
			require.ErrorIs(t, err, testCase.expectErr)

			if testCase.expectErr == nil {
				require.Equal(t, testCase.expectStatus, result.Status)
			}

			getDao.AssertExpectations(t)
			settleDao.AssertExpectations(t)
			requeueDao.AssertExpectations(t)
		})
	}
}
