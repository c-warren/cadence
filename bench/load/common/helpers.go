package common

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/.gen/go/shared"

	"github.com/uber/cadence/bench/lib"
)

const (
	taskListPrefix = "cadence-bench-tl"
)

// GetTaskListName returns the task list name for the given task list id
func GetTaskListName(taskListNumber int) string {
	return fmt.Sprintf("%s-%v", taskListPrefix, taskListNumber)
}

// GetActivityServiceConfig returns the service config from activity context
// Returns nil if the context does not contain the service config
func GetActivityServiceConfig(ctx context.Context) *lib.RuntimeContext {
	val := ctx.Value(lib.CtxKeyRuntimeContext)
	if val == nil {
		return nil
	}
	return val.(*lib.RuntimeContext)
}

// IsServiceBusyError returns if the err is a ServiceBusyError
func IsServiceBusyError(err error) bool {
	_, ok := err.(*shared.ServiceBusyError)
	return ok
}

// IsCancellationAlreadyRequestedError returns if the err is a CancellationAlreadyRequestedError
func IsCancellationAlreadyRequestedError(err error) bool {
	_, ok := err.(*shared.CancellationAlreadyRequestedError)
	return ok
}

// IsEntityNotExistsError returns if the err is a EntityNotExistsError
func IsEntityNotExistsError(err error) bool {
	_, ok := err.(*shared.EntityNotExistsError)
	return ok
}

// IsNonRetryableError return true if the err is considered non-retryable
func IsNonRetryableError(err error) bool {
	if err == context.DeadlineExceeded || err == context.Canceled {
		return true
	}

	if IsEntityNotExistsError(err) {
		return true
	}

	return false
}

// RetryOp retries an operation based on the default retry policy
func RetryOp(
	op func() error,
	isNonRetryableError func(error) bool,
) error {
	var err error
	for retryCount := 0; retryCount < DefaultMaxRetryCount; retryCount++ {
		if err = op(); err == nil {
			return nil
		}

		if isNonRetryableError != nil && isNonRetryableError(err) {
			return err
		}

		if retryCount < DefaultMaxRetryCount-1 {
			time.Sleep(DefaultRetryBackoffDuration)
		}
	}

	return err
}
