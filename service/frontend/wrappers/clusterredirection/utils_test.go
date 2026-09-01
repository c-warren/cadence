package clusterredirection

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/yarpc/yarpctest"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/types"
)

func TestGetRequestedConsistencyLevelFromContext(t *testing.T) {
	tests := []struct {
		name         string
		featureFlags apiv1.FeatureFlags
		expected     types.QueryConsistencyLevel
	}{
		{
			name:         "empty feature flags",
			featureFlags: apiv1.FeatureFlags{},
			expected:     types.QueryConsistencyLevelEventual,
		},
		{
			name:         "auto forwarding disabled",
			featureFlags: apiv1.FeatureFlags{AutoforwardingEnabled: false},
			expected:     types.QueryConsistencyLevelEventual,
		},
		{
			name:         "autoforwarding enabled",
			featureFlags: apiv1.FeatureFlags{AutoforwardingEnabled: true},
			expected:     types.QueryConsistencyLevelStrong,
		},
		{
			name: "no autoforwarding field",
			featureFlags: apiv1.FeatureFlags{
				WorkflowExecutionAlreadyCompletedErrorEnabled: true,
			},
			expected: types.QueryConsistencyLevelEventual,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := yarpctest.ContextWithCall(context.Background(), &yarpctest.Call{
				Headers: map[string]string{common.ClientFeatureFlagsHeaderName: client.FeatureFlagsHeader(tt.featureFlags)},
			})

			result := getRequestedConsistencyLevelFromContext(ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}
