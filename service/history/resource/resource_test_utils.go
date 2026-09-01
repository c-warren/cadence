package resource

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/quotas/global/algorithm"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	// Test is the test implementation used for testing
	Test struct {
		*resource.Test
		EventCache           *events.MockCache
		ratelimiterAlgorithm algorithm.RequestWeighted
		archiverClient       archiver.Client
	}
)

var _ Resource = (*Test)(nil)

// NewTest returns a new test resource instance
func NewTest(
	t *testing.T,
	controller *gomock.Controller,
	serviceMetricsIndex metrics.ServiceIdx,
) *Test {
	return &Test{
		Test:           resource.NewTest(t, controller, serviceMetricsIndex),
		EventCache:     events.NewMockCache(controller),
		archiverClient: archiver.NewMockClient(controller),
	}
}

// GetEventCache for testing
func (s *Test) GetEventCache() events.Cache {
	return s.EventCache
}

func (s *Test) GetRatelimiterAlgorithm() algorithm.RequestWeighted {
	return s.ratelimiterAlgorithm
}

func (s *Test) GetArchiverClient() archiver.Client {
	return s.archiverClient
}
