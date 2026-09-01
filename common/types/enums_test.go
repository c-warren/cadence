package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_EventTypeValues(t *testing.T) {
	result := EventTypeValues()
	require.Equal(t, 42, len(result))
}

func Test_DecisionTypeValues(t *testing.T) {
	result := DecisionTypeValues()
	require.Equal(t, 13, len(result))
}
