package thrift

import (
	"github.com/uber/cadence/common/types/mapper/errorutils"
)

// FromError convert error to Thrift type if it comes as its internal equivalent
func FromError(err error) error {
	if err == nil {
		return nil
	}

	var (
		ok       bool
		typedErr error
	)
	if ok, typedErr = errorutils.ConvertError(err, FromAccessDeniedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromBadRequestError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromCancellationAlreadyRequestedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromClientVersionNotSupportedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromFeatureNotEnabledError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromCurrentBranchChangedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromDomainAlreadyExistsError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromDomainNotActiveError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromEntityNotExistsError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromWorkflowExecutionAlreadyCompletedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromInternalDataInconsistencyError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromInternalServiceError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromLimitExceededError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromQueryFailedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromRemoteSyncMatchedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromRetryTaskV2Error); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromServiceBusyError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromWorkflowExecutionAlreadyStartedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromShardOwnershipLostError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromEventAlreadyStartedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromStickyWorkerUnavailableError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, FromTaskListNotOwnedByHostError); ok {
		return typedErr
	}

	return err
}

// ToError convert error to internal type if it comes as its thrift equivalent
func ToError(err error) error {
	if err == nil {
		return nil
	}

	var (
		ok       bool
		typedErr error
	)
	if ok, typedErr = errorutils.ConvertError(err, ToAccessDeniedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToBadRequestError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToCancellationAlreadyRequestedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToClientVersionNotSupportedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToFeatureNotEnabledError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToCurrentBranchChangedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToDomainAlreadyExistsError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToDomainNotActiveError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToEntityNotExistsError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToWorkflowExecutionAlreadyCompletedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToInternalDataInconsistencyError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToInternalServiceError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToLimitExceededError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToQueryFailedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToRemoteSyncMatchedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToRetryTaskV2Error); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToServiceBusyError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToWorkflowExecutionAlreadyStartedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToShardOwnershipLostError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToEventAlreadyStartedError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToStickyWorkerUnavailableError); ok {
		return typedErr
	} else if ok, typedErr = errorutils.ConvertError(err, ToTaskListNotOwnedByHostError); ok {
		return typedErr
	}

	return err
}
