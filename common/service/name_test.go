package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceNames(t *testing.T) {
	shortName := "frontend"
	fullName := "cadence-frontend"

	assert.Equal(t, shortName, ShortName(shortName))
	assert.Equal(t, shortName, ShortName(fullName))

	assert.Equal(t, fullName, FullName(shortName))
	assert.Equal(t, fullName, FullName(fullName))

	assert.Equal(t, []string{"cadence-frontend", "cadence-history", "cadence-matching", "cadence-worker"}, List)
	assert.Equal(t, []string{"frontend", "history", "matching", "worker"}, ShortNames(List))
}
