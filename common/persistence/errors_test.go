package persistence

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsDuplicateRequestError(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		expectedErr *DuplicateRequestError
		ok          bool
	}{
		{
			name:        "unwrapped",
			err:         &DuplicateRequestError{RunID: "a"},
			expectedErr: &DuplicateRequestError{RunID: "a"},
			ok:          true,
		},
		{
			name:        "wrapped",
			err:         fmt.Errorf("%w", &DuplicateRequestError{RunID: "b"}),
			expectedErr: &DuplicateRequestError{RunID: "b"},
			ok:          true,
		},
		{
			name: "not same type",
			err:  fmt.Errorf("adasdf"),
			ok:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e, ok := AsDuplicateRequestError(tc.err)
			assert.Equal(t, tc.ok, ok)
			if ok {
				assert.Equal(t, tc.expectedErr, e)
			}
		})
	}
}
