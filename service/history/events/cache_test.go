package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

type (
	eventsCacheSuite struct {
		suite.Suite
		*require.Assertions

		logger             log.Logger
		mockHistoryManager *mocks.HistoryV2Manager

		cache       *cacheImpl
		ctrl        *gomock.Controller
		domainCache *cache.MockDomainCache
	}
)

func TestEventsCacheSuite(t *testing.T) {
	s := new(eventsCacheSuite)
	suite.Run(t, s)
}

func (s *eventsCacheSuite) SetupSuite() {

}

func (s *eventsCacheSuite) TearDownSuite() {

}

func (s *eventsCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.logger = testlogger.New(s.Suite.T())
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.mockHistoryManager = &mocks.HistoryV2Manager{}
	s.ctrl = gomock.NewController(s.T())
	s.domainCache = cache.NewMockDomainCache(s.ctrl)
	s.cache = s.newTestEventsCache()
}

func (s *eventsCacheSuite) TearDownTest() {
	s.mockHistoryManager.AssertExpectations(s.T())
	s.ctrl.Finish()
}

func (s *eventsCacheSuite) newTestEventsCache() *cacheImpl {
	return newCacheWithOption(common.IntPtr(10), 16, 32, time.Minute, s.mockHistoryManager, false, s.logger,
		metrics.NewClient(tally.NoopScope, metrics.History, metrics.MigrationConfig{}), dynamicproperties.GetBoolPropertyFn(false), dynamicproperties.GetIntPropertyFn(1000), s.domainCache)
}

func (s *eventsCacheSuite) TestEventsCacheHitSuccess() {
	domainID := "events-cache-hit-success-domain"
	workflowID := "events-cache-hit-success-workflow-id"
	runID := "events-cache-hit-success-run-id"
	eventID := int64(23)
	event := &types.HistoryEvent{
		ID:                                 eventID,
		EventType:                          types.EventTypeActivityTaskStarted.Ptr(),
		ActivityTaskStartedEventAttributes: &types.ActivityTaskStartedEventAttributes{},
	}

	s.cache.PutEvent(domainID, workflowID, runID, eventID, event)
	actualEvent, err := s.cache.GetEvent(context.Background(), 0, domainID, workflowID, runID, eventID, eventID, nil)
	s.Nil(err)
	s.Equal(event, actualEvent)
}

func (s *eventsCacheSuite) TestEventsCacheMissMultiEventsBatchV2Success() {
	domainID := "events-cache-miss-multi-events-batch-v2-success-domain"
	workflowID := "events-cache-miss-multi-events-batch-v2-success-workflow-id"
	runID := "events-cache-miss-multi-events-batch-v2-success-run-id"
	domainName := "events-cache-miss-multi-events-batch-v2-success-domainName"
	event1 := &types.HistoryEvent{
		ID:                                   11,
		EventType:                            types.EventTypeDecisionTaskCompleted.Ptr(),
		DecisionTaskCompletedEventAttributes: &types.DecisionTaskCompletedEventAttributes{},
	}
	event2 := &types.HistoryEvent{
		ID:                                   12,
		EventType:                            types.EventTypeActivityTaskScheduled.Ptr(),
		ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{},
	}
	event3 := &types.HistoryEvent{
		ID:                                   13,
		EventType:                            types.EventTypeActivityTaskScheduled.Ptr(),
		ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{},
	}
	event4 := &types.HistoryEvent{
		ID:                                   14,
		EventType:                            types.EventTypeActivityTaskScheduled.Ptr(),
		ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{},
	}
	event5 := &types.HistoryEvent{
		ID:                                   15,
		EventType:                            types.EventTypeActivityTaskScheduled.Ptr(),
		ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{},
	}
	event6 := &types.HistoryEvent{
		ID:                                   16,
		EventType:                            types.EventTypeActivityTaskScheduled.Ptr(),
		ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{},
	}

	shardID := common.IntPtr(10)
	s.mockHistoryManager.On("ReadHistoryBranch", mock.Anything, &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    event1.ID,
		MaxEventID:    event6.ID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
		DomainName:    domainName,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*types.HistoryEvent{event1, event2, event3, event4, event5, event6},
		NextPageToken:    nil,
		LastFirstEventID: event1.ID,
	}, nil)

	s.domainCache.EXPECT().GetDomainName(gomock.Any()).Return(domainName, nil).AnyTimes()
	s.cache.PutEvent(domainID, workflowID, runID, event2.ID, event2)
	actualEvent, err := s.cache.GetEvent(context.Background(), *shardID, domainID, workflowID, runID, event1.ID, event6.ID, []byte("store_token"))
	s.Nil(err)
	s.Equal(event6, actualEvent)
}

func (s *eventsCacheSuite) TestEventsCacheMissV2Failure() {
	domainID := "events-cache-miss-failure-domain"
	workflowID := "events-cache-miss-failure-workflow-id"
	runID := "events-cache-miss-failure-run-id"
	shardID := common.IntPtr(10)
	domainName := "events-cache-miss-failure-domainName"
	expectedErr := errors.New("persistence call failed")
	s.mockHistoryManager.On("ReadHistoryBranch", mock.Anything, &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    int64(11),
		MaxEventID:    int64(15),
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
		DomainName:    domainName,
	}).Return(nil, expectedErr)

	s.domainCache.EXPECT().GetDomainName(gomock.Any()).Return(domainName, nil).AnyTimes()
	actualEvent, err := s.cache.GetEvent(context.Background(), *shardID, domainID, workflowID, runID, int64(11), int64(14), []byte("store_token"))
	s.Nil(actualEvent)
	s.Equal(expectedErr, err)
}

func (s *eventsCacheSuite) TestEventsCacheDisableSuccess() {
	domainID := "events-cache-disable-success-domain"
	workflowID := "events-cache-disable-success-workflow-id"
	runID := "events-cache-disable-success-run-id"
	shardID := common.IntPtr(10)
	domainName := "events-cache-disable-success-domainName"
	event1 := &types.HistoryEvent{
		ID:                                 23,
		EventType:                          types.EventTypeActivityTaskStarted.Ptr(),
		ActivityTaskStartedEventAttributes: &types.ActivityTaskStartedEventAttributes{},
	}
	event2 := &types.HistoryEvent{
		ID:                                 32,
		EventType:                          types.EventTypeActivityTaskStarted.Ptr(),
		ActivityTaskStartedEventAttributes: &types.ActivityTaskStartedEventAttributes{},
	}

	s.mockHistoryManager.On("ReadHistoryBranch", mock.Anything, &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("store_token"),
		MinEventID:    event2.ID,
		MaxEventID:    event2.ID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
		DomainName:    domainName,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*types.HistoryEvent{event2},
		NextPageToken:    nil,
		LastFirstEventID: event2.ID,
	}, nil)

	s.domainCache.EXPECT().GetDomainName(gomock.Any()).Return(domainName, nil).AnyTimes()
	s.cache.PutEvent(domainID, workflowID, runID, event1.ID, event1)
	s.cache.PutEvent(domainID, workflowID, runID, event2.ID, event2)
	s.cache.disabled = true
	actualEvent, err := s.cache.GetEvent(context.Background(), *shardID, domainID, workflowID, runID, event2.ID, event2.ID, []byte("store_token"))
	s.Nil(err)
	s.Equal(event2, actualEvent)
}
