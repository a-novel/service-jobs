package env_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a-novel/service-jobs/internal/config/env"
)

func TestReaperInterval(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		raw string

		expect    time.Duration
		expectErr bool
	}{
		{
			// Unset is the common case: the default cadence, no error.
			name:   "Empty/Default",
			raw:    "",
			expect: env.ReaperIntervalDefault,
		},
		{
			name:   "Valid",
			raw:    "45s",
			expect: 45 * time.Second,
		},
		{
			// The trap the strict parser exists for: "30" without a unit parses to nothing usable, so it
			// must fail rather than silently fall back to the default the way config.LoadEnv would.
			name:      "Malformed/NoUnit",
			raw:       "30",
			expectErr: true,
		},
		{
			name:      "Malformed/Garbage",
			raw:       "soon",
			expectErr: true,
		},
		{
			// A zero or negative cadence is not a cadence: reject it rather than spin the loop.
			name:      "NonPositive/Zero",
			raw:       "0s",
			expectErr: true,
		},
		{
			name:      "NonPositive/Negative",
			raw:       "-5s",
			expectErr: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			interval, err := env.ReaperInterval(testCase.raw)

			if testCase.expectErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expect, interval)
		})
	}
}
