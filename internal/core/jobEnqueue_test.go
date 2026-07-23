package core_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/core"
	coremocks "github.com/a-novel/service-jobs/internal/core/mocks"
	"github.com/a-novel/service-jobs/internal/dao"
)

func TestJobEnqueue(t *testing.T) {
	t.Parallel()

	owner := uuid.MustParse("00000000-0000-0000-0000-00000000a11c")
	errFoo := errors.New("foo")

	payload := json.RawMessage(`{"seed":"an idea"}`)
	fingerprint := sha256.Sum256(payload)

	// daoReturns is the row the DAO hands back. sameID makes the returned id match the one core mints
	// (a creation); a fixed different id makes it a replay of an existing row.
	type daoMock struct {
		sameID bool
		row    *dao.Job // row minus its id, which the test stamps per sameID
		err    error
	}

	testCases := []struct {
		name string

		request *core.JobEnqueueRequest

		daoMock *daoMock

		expectCreated bool
		expectErr     error
	}{
		{
			name: "Created",

			request: &core.JobEnqueueRequest{
				Kind: "generate", Payload: payload, OwnerID: owner, IdempotencyKey: lo.ToPtr("k1"), MaxAttempts: 1,
			},

			daoMock: &daoMock{
				sameID: true,
				row:    &dao.Job{Kind: "generate", OwnerID: owner, RequestFingerprint: fingerprint[:]},
			},

			expectCreated: true,
		},
		{
			// The returned id differs from the minted one and the stored fingerprint matches: a genuine
			// idempotent replay, so the caller attaches to the existing job.
			name: "Replay",

			request: &core.JobEnqueueRequest{
				Kind: "generate", Payload: payload, OwnerID: owner, IdempotencyKey: lo.ToPtr("k1"), MaxAttempts: 1,
			},

			daoMock: &daoMock{
				sameID: false,
				row:    &dao.Job{Kind: "generate", OwnerID: owner, RequestFingerprint: fingerprint[:]},
			},

			expectCreated: false,
		},
		{
			// Same key, different stored fingerprint: the key was reused for different work.
			name: "Error/FingerprintConflict",

			request: &core.JobEnqueueRequest{
				Kind: "generate", Payload: payload, OwnerID: owner, IdempotencyKey: lo.ToPtr("k1"), MaxAttempts: 1,
			},

			daoMock: &daoMock{
				sameID: false,
				row:    &dao.Job{Kind: "generate", OwnerID: owner, RequestFingerprint: []byte("a different digest")},
			},

			expectErr: core.ErrJobConflict,
		},
		{
			name: "Error/MissingKind",

			request:   &core.JobEnqueueRequest{Payload: payload, OwnerID: owner, MaxAttempts: 1},
			expectErr: core.ErrInvalidRequest,
		},
		{
			name: "Error/Dao",

			request: &core.JobEnqueueRequest{
				Kind: "generate", Payload: payload, OwnerID: owner, MaxAttempts: 1,
			},

			daoMock:   &daoMock{err: errFoo},
			expectErr: errFoo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockDao := coremocks.NewMockJobEnqueueDao(t)

			if testCase.daoMock != nil {
				mockDao.EXPECT().
					Exec(mock.Anything, mock.MatchedBy(func(r *dao.JobEnqueueRequest) bool {
						// The service mints the id and derives the fingerprint; the mock only asserts the
						// fields it controls, and echoes the minted id back per sameID.
						return r.Kind == testCase.request.Kind && r.OwnerID == testCase.request.OwnerID
					})).
					RunAndReturn(func(_ context.Context, r *dao.JobEnqueueRequest) (*dao.Job, error) {
						if testCase.daoMock.err != nil {
							return nil, testCase.daoMock.err
						}

						row := *testCase.daoMock.row
						if testCase.daoMock.sameID {
							row.ID = r.ID
						} else {
							row.ID = uuid.MustParse("00000000-0000-0000-0000-0000000000ff")
						}

						return &row, nil
					})
			}

			service := core.NewJobEnqueue(mockDao)

			result, err := service.Exec(t.Context(), testCase.request)
			require.ErrorIs(t, err, testCase.expectErr)

			if testCase.expectErr != nil {
				require.Nil(t, result)
			} else {
				require.Equal(t, testCase.expectCreated, result.Created)
			}

			mockDao.AssertExpectations(t)
		})
	}
}
