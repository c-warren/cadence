package persistencetests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/wrappers/sampled"
	"github.com/uber/cadence/common/types"
)

type VisibilitySamplingSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	client       p.VisibilityManager
	persistence  *mocks.VisibilityManager
	metricClient *mmocks.Client
}

var (
	testDomainUUID        = "fb15e4b5-356f-466d-8c6d-a29223e5c536"
	testDomain            = "test-domain-name"
	testWorkflowExecution = types.WorkflowExecution{
		WorkflowID: "visibility-workflow-test",
		RunID:      "843f6fc7-102a-4c63-a2d4-7c653b01bf52",
	}
	testWorkflowTypeName = "visibility-workflow"

	listErrMsg = "Persistence Max QPS Reached for List Operations."
)

func TestVisibilitySamplingSuite(t *testing.T) {
	suite.Run(t, new(VisibilitySamplingSuite))
}

func (s *VisibilitySamplingSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.persistence = &mocks.VisibilityManager{}
	config := &sampled.Config{
		VisibilityOpenMaxQPS:   dynamicproperties.GetIntPropertyFilteredByDomain(1),
		VisibilityClosedMaxQPS: dynamicproperties.GetIntPropertyFilteredByDomain(10),
		VisibilityListMaxQPS:   dynamicproperties.GetIntPropertyFilteredByDomain(1),
	}
	s.metricClient = &mmocks.Client{}
	s.client = sampled.NewVisibilityManager(s.persistence, sampled.Params{
		Config:                 config,
		MetricClient:           s.metricClient,
		Logger:                 testlogger.New(s.T()),
		TimeSource:             clock.NewRealTimeSource(),
		RateLimiterFactoryFunc: sampled.NewDomainToBucketMap,
	})
}

func (s *VisibilitySamplingSuite) TearDownTest() {
	s.persistence.AssertExpectations(s.T())
	s.metricClient.AssertExpectations(s.T())
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionStarted() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	request := &p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		StartTimestamp:   time.Now().UnixNano(),
	}
	s.persistence.On("RecordWorkflowExecutionStarted", mock.Anything, request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(ctx, request))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(ctx, request))
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionClosed() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	request := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           types.WorkflowExecutionCloseStatusCompleted,
	}
	request2 := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           types.WorkflowExecutionCloseStatusFailed,
	}

	s.persistence.On("RecordWorkflowExecutionClosed", mock.Anything, request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(ctx, request))
	s.persistence.On("RecordWorkflowExecutionClosed", mock.Anything, request2).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(ctx, request2))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(ctx, request))
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(ctx, request2))
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutions() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	request := &p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	s.persistence.On("ListOpenWorkflowExecutions", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutions(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutions(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutions() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	request := &p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	s.persistence.On("ListClosedWorkflowExecutions", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutions(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutions(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByType() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.persistence.On("ListOpenWorkflowExecutionsByType", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutionsByType(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutionsByType(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByType() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowTypeName:              testWorkflowTypeName,
	}
	s.persistence.On("ListClosedWorkflowExecutionsByType", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByType(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByType(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListOpenWorkflowExecutionsByWorkflowID() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowID(),
	}
	s.persistence.On("ListOpenWorkflowExecutionsByWorkflowID", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByWorkflowID() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListWorkflowExecutionsByWorkflowIDRequest{
		ListWorkflowExecutionsRequest: req,
		WorkflowID:                    testWorkflowExecution.GetWorkflowID(),
	}
	s.persistence.On("ListClosedWorkflowExecutionsByWorkflowID", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}

func (s *VisibilitySamplingSuite) TestListClosedWorkflowExecutionsByStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()

	req := p.ListWorkflowExecutionsRequest{
		DomainUUID: testDomainUUID,
		Domain:     testDomain,
	}
	request := &p.ListClosedWorkflowExecutionsByStatusRequest{
		ListWorkflowExecutionsRequest: req,
		Status:                        types.WorkflowExecutionCloseStatusFailed,
	}
	s.persistence.On("ListClosedWorkflowExecutionsByStatus", mock.Anything, request).Return(nil, nil).Once()
	_, err := s.client.ListClosedWorkflowExecutionsByStatus(ctx, request)
	s.NoError(err)

	// no remaining tokens
	_, err = s.client.ListClosedWorkflowExecutionsByStatus(ctx, request)
	s.Error(err)
	errDetail, ok := err.(*types.ServiceBusyError)
	s.True(ok)
	s.Equal(listErrMsg, errDetail.Message)
}
