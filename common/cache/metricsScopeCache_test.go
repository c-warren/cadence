package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/metrics"
)

type domainMetricsCacheSuite struct {
	suite.Suite
	*require.Assertions

	metricsClient metrics.Client
	metricsCache  DomainMetricsScopeCache
}

func TestDomainMetricsCacheSuite(t *testing.T) {
	s := new(domainMetricsCacheSuite)
	suite.Run(t, s)
}

func (s *domainMetricsCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Frontend, metrics.MigrationConfig{})

	metricsCache := NewDomainMetricsScopeCache().(*domainMetricsScopeCache)
	metricsCache.flushDuration = 100 * time.Millisecond
	s.metricsCache = metricsCache

	s.metricsCache.Start()
}

func (s *domainMetricsCacheSuite) TearDownTest() {
	s.metricsCache.Stop()
}

func (s *domainMetricsCacheSuite) TestGetMetricsScope() {
	var found bool

	tests := []struct {
		scopeID  metrics.ScopeIdx
		domainID string
	}{
		{1, "A"},
		{2, "B"},
		{1, "C"},
	}

	for _, t := range tests {
		mockMetricsScope := s.metricsClient.Scope(t.scopeID)
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	time.Sleep(110 * time.Millisecond)

	metricsScope, found := s.metricsCache.Get("A", 1)
	testMetricsScope := s.metricsClient.Scope(1)
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	_, found = s.metricsCache.Get("B", 2)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("C", 1)
	testMetricsScope = s.metricsClient.Scope(3)
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("D", 3)
	testMetricsScope = s.metricsClient.Scope(3)
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)
}

func (s *domainMetricsCacheSuite) TestGetMetricsScopeMultipleFlushLoop() {
	var found bool

	tests := []struct {
		scopeID  metrics.ScopeIdx
		domainID string
	}{
		{1, "A"},
		{2, "B"},
		{1, "C"},
		{5, "D"},
		{3, "E"},
	}

	for i := 0; i < 3; i++ {
		t := tests[i]
		mockMetricsScope := s.metricsClient.Scope(t.scopeID)
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	time.Sleep(110 * time.Millisecond)

	for i := 3; i < len(tests); i++ {
		t := tests[i]
		mockMetricsScope := s.metricsClient.Scope(t.scopeID)
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	metricsScope, found := s.metricsCache.Get("A", 1)
	testMetricsScope := s.metricsClient.Scope(1)
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	_, found = s.metricsCache.Get("B", 2)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("C", 1)
	testMetricsScope = s.metricsClient.Scope(3)
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("D", 5)
	testMetricsScope = s.metricsClient.Scope(5)
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)

	metricsScope, found = s.metricsCache.Get("E", 3)
	testMetricsScope = s.metricsClient.Scope(3)
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)

	time.Sleep(200 * time.Millisecond)

	metricsScope, found = s.metricsCache.Get("D", 5)
	testMetricsScope = s.metricsClient.Scope(5)
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("E", 3)
	testMetricsScope = s.metricsClient.Scope(3)
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)
}

func (s *domainMetricsCacheSuite) TestConcurrentMetricsScopeAccess() {

	ch := make(chan struct{})
	var wg sync.WaitGroup
	var metricsScope, testMetricsScope metrics.Scope
	var found bool

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		// concurrent get and put
		go func(scopeIdx metrics.ScopeIdx) {
			defer wg.Done()

			<-ch

			s.metricsCache.Get("test_domain", scopeIdx)
			s.metricsCache.Put("test_domain", scopeIdx, s.metricsClient.Scope(metrics.ScopeIdx(int(scopeIdx)%int(metrics.NumServices))))
		}(metrics.ScopeIdx(i))
	}

	close(ch)
	wg.Wait()

	time.Sleep(120 * time.Millisecond)

	for i := 0; i < 1000; i++ {
		metricsScope, found = s.metricsCache.Get("test_domain", metrics.ScopeIdx(i))
		testMetricsScope = s.metricsClient.Scope(metrics.ScopeIdx(i % int(metrics.NumServices)))

		s.Equal(true, found)
		s.Equal(testMetricsScope, metricsScope)
	}
}
