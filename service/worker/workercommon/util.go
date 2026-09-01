package workercommon

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/client"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/resource"
)

func StartWorkflowWithRetry(
	workflowType string,
	startUpDelay time.Duration,
	resource resource.Resource,
	startWorkflow func(client client.Client) error,
) error {
	// let history / matching service warm up
	time.Sleep(startUpDelay)
	sdkClient := client.NewClient(
		resource.GetSDKClient(),
		constants.SystemLocalDomainName,
		nil, /* &client.Options{} */
	)
	policy := backoff.NewExponentialRetryPolicy(time.Second)
	policy.SetMaximumInterval(time.Minute)
	policy.SetExpirationInterval(backoff.NoInterval)
	throttleRetry := backoff.NewThrottleRetry(
		backoff.WithRetryPolicy(policy),
		backoff.WithRetryableError(func(_ error) bool { return true }),
	)
	err := throttleRetry.Do(context.Background(), func(ctx context.Context) error {
		return startWorkflow(sdkClient)
	})
	if err != nil {
		panic(fmt.Sprintf("unreachable: %#v", err))
	} else {
		resource.GetLogger().Info("starting workflow", tag.WorkflowType(workflowType))
	}
	return err
}
