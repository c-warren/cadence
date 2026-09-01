package parentclosepolicy

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	cclient "go.uber.org/cadence/client"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

//go:generate mockgen -package=$GOPACKAGE -destination=client_mock.go -self_package=github.com/uber/cadence/service/worker/parentclosepolicy github.com/uber/cadence/service/worker/parentclosepolicy Client

type (

	// Client is used to send request to processor workflow
	Client interface {
		SendParentClosePolicyRequest(context.Context, Request) error
	}

	clientImpl struct {
		metricsClient metrics.Client
		logger        log.Logger
		cadenceClient cclient.Client
		numWorkflows  int
	}
)

var _ Client = (*clientImpl)(nil)

const (
	signalTimeout    = 400 * time.Millisecond
	workflowIDPrefix = "parent-close-policy-workflow"
)

// NewClient creates a new Client
func NewClient(
	metricsClient metrics.Client,
	logger log.Logger,
	publicClient workflowserviceclient.Interface,
	numWorkflows int,
) Client {
	return &clientImpl{
		metricsClient: metricsClient,
		logger:        logger,
		cadenceClient: cclient.NewClient(publicClient, constants.SystemLocalDomainName, &cclient.Options{}),
		numWorkflows:  numWorkflows,
	}
}

func (c *clientImpl) SendParentClosePolicyRequest(
	ctx context.Context,
	request Request,
) error {
	randomID := rand.Intn(c.numWorkflows)
	workflowID := fmt.Sprintf("%v-%v", workflowIDPrefix, randomID)
	workflowOptions := cclient.StartWorkflowOptions{
		ID:                              workflowID,
		TaskList:                        processorTaskListName,
		ExecutionStartToCloseTimeout:    infiniteDuration,
		DecisionTaskStartToCloseTimeout: time.Minute,
		WorkflowIDReusePolicy:           cclient.WorkflowIDReusePolicyAllowDuplicate,
	}
	signalCtx, cancel := context.WithTimeout(ctx, signalTimeout)
	defer cancel()
	_, err := c.cadenceClient.SignalWithStartWorkflow(signalCtx, workflowID, processorChannelName, request, workflowOptions, processorWFTypeName, nil)
	return err
}
