package ratelimited

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/service/history/workflowcache"
)

func TestAllowWfID(t *testing.T) {
	tests := []struct {
		workflowIDCacheAllow bool
		expected             bool
	}{
		{
			workflowIDCacheAllow: true,
			expected:             true,
		},
		{
			workflowIDCacheAllow: false,
			expected:             false,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("workflowIDCacheAllow: %t", tt.workflowIDCacheAllow), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			workflowIDCacheMock := workflowcache.NewMockWFCache(ctrl)
			workflowIDCacheMock.EXPECT().AllowExternal(testDomainID, testWorkflowID).Return(tt.workflowIDCacheAllow).Times(1)

			h := &historyHandler{
				workflowIDCache: workflowIDCacheMock,
				logger:          log.NewNoop(),
			}

			got := h.allowWfID(testDomainID, testWorkflowID)

			assert.Equal(t, tt.expected, got)
		})
	}
}
