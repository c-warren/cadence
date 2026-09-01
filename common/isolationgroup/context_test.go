package isolationgroup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	partitionConfig := map[string]string{"test": "abc"}
	assert.Equal(t, partitionConfig, ConfigFromContext(ContextWithConfig(context.Background(), partitionConfig)))

	isolationGroup := "zone1"
	assert.Equal(t, isolationGroup, IsolationGroupFromContext(ContextWithIsolationGroup(context.Background(), isolationGroup)))
}
