package nosql

import (
	"fmt"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

// ShardingError represents invalid shard
type ShardingError struct {
	Message string
}

func (e *ShardingError) Error() string {
	return e.Message
}

func convertCommonErrors(errChecker nosqlplugin.ClientErrorChecker, operation string, err error) error {
	if errChecker.IsNotFoundError(err) {
		return &types.EntityNotExistsError{
			Message: fmt.Sprintf("%v failed. Error: %v ", operation, err),
		}
	}

	if errChecker.IsTimeoutError(err) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("%v timed out. Error: %v", operation, err)}
	}

	if errChecker.IsThrottlingError(err) {
		return &types.ServiceBusyError{
			Message: fmt.Sprintf("%v operation failed. Error: %v", operation, err),
		}
	}

	if errChecker.IsDBUnavailableError(err) {
		return &persistence.DBUnavailableError{
			Msg: fmt.Sprintf("%v operation failed. Error: %v", operation, err),
		}
	}

	return &types.InternalServiceError{
		Message: fmt.Sprintf("%v operation failed. Error: %v", operation, err),
	}
}
