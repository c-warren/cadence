package definition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWorkflowIdentifierSize verifies the Size method of WorkflowIdentifier.
func TestWorkflowIdentifierSize(t *testing.T) {
	tests := []struct {
		name     string
		wi       WorkflowIdentifier
		expected uint64
	}{
		{
			name:     "non-empty fields",
			wi:       NewWorkflowIdentifier("domain", "workflow", "run"),
			expected: uint64(len("domain") + len("workflow") + len("run") + 3*16),
		},
		{
			name:     "empty fields",
			wi:       NewWorkflowIdentifier("", "", ""),
			expected: uint64(3 * 16),
		},
		{
			name:     "short fields",
			wi:       NewWorkflowIdentifier("a", "b", "c"),
			expected: uint64(3 + 3*16),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			size := test.wi.ByteSize()
			assert.Equal(t, test.expected, size)
		})
	}
}
