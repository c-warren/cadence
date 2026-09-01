package definition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSystemBoolKey(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		{"IsCron", true},
		{"StartTime", false},
	}

	for _, test := range tests {
		actualResult := IsSystemBoolKey(test.key)
		assert.Equal(t, test.expected, actualResult)
	}
}
