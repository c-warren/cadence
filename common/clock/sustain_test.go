package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type check struct {
	seconds int
	value   bool
}

func TestCheckAndReset(t *testing.T) {
	cases := []struct {
		name     string
		duration time.Duration
		calls    []check
		expected []bool
	}{
		{
			name:     "simple case",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{10, true},
			},
			expected: []bool{
				false,
				true,
			},
		},
		{
			name:     "intermediate successes",
			duration: 10 * time.Second,
			calls: []check{
				{0, true},
				{2, true},
				{2, true},
				{2, true},
				{2, true},
				{2, true},
			},
			expected: []bool{
				false,
				false,
				false,
				false,
				false,
				true,
			},
		},
		{
			name:     "resets after success",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{10, true},
				{0, true},
			},
			expected: []bool{
				false,
				true,
				false,
			},
		},
		{
			name:     "resets after false",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{1, false},
				{1, true},
				{9, true},
				{1, true},
			},
			expected: []bool{
				false,
				false,
				false,
				false,
				true,
			},
		},
		{
			name:     "duration = 0",
			duration: 0,
			calls: []check{
				{0, true},
			},
			expected: []bool{
				true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := NewMockedTimeSource()
			sus := NewSustain(clock, func() time.Duration {
				return tc.duration
			})
			require.Equal(t, len(tc.calls), len(tc.expected))
			for i, c := range tc.calls {
				expected := tc.expected[i]
				clock.Advance(time.Duration(c.seconds) * time.Second)
				actual := sus.CheckAndReset(c.value)
				assert.Equal(t, expected, actual, "check %d", i)
			}
		})
	}
}

func TestCheck(t *testing.T) {
	cases := []struct {
		name     string
		duration time.Duration
		calls    []check
		expected []bool
	}{
		{
			name:     "simple case",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{10, true},
			},
			expected: []bool{
				false,
				true,
			},
		},
		{
			name:     "intermediate successes",
			duration: 10 * time.Second,
			calls: []check{
				{0, true},
				{2, true},
				{2, true},
				{2, true},
				{2, true},
				{2, true},
			},
			expected: []bool{
				false,
				false,
				false,
				false,
				false,
				true,
			},
		},
		{
			name:     "stays after success",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{10, true},
				{0, true},
			},
			expected: []bool{
				false,
				true,
				true,
			},
		},
		{
			name:     "resets after false",
			duration: time.Second * 10,
			calls: []check{
				{0, true},
				{1, false},
				{1, true},
				{9, true},
				{1, true},
			},
			expected: []bool{
				false,
				false,
				false,
				false,
				true,
			},
		},
		{
			name:     "duration = 0",
			duration: 0,
			calls: []check{
				{0, true},
			},
			expected: []bool{
				true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := NewMockedTimeSource()
			sus := NewSustain(clock, func() time.Duration {
				return tc.duration
			})
			require.Equal(t, len(tc.calls), len(tc.expected))
			for i, c := range tc.calls {
				expected := tc.expected[i]
				clock.Advance(time.Duration(c.seconds) * time.Second)
				actual := sus.Check(c.value)
				assert.Equal(t, expected, actual, "check %d", i)
			}
		})
	}
}
