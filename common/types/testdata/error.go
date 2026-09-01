package testdata

import (
	"github.com/uber/cadence/common"
	cadence_errors "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/types"
)

const (
	ErrorMessage = "ErrorMessage"
)

var (
	AccessDeniedError = types.AccessDeniedError{
		Message: ErrorMessage,
	}
	BadRequestError = types.BadRequestError{
		Message: ErrorMessage,
	}
	CancellationAlreadyRequestedError = types.CancellationAlreadyRequestedError{
		Message: ErrorMessage,
	}
	ClientVersionNotSupportedError = types.ClientVersionNotSupportedError{
		FeatureVersion:    FeatureVersion,
		ClientImpl:        ClientImpl,
		SupportedVersions: SupportedVersions,
	}
	FeatureNotEnabledError = types.FeatureNotEnabledError{
		FeatureFlag: FeatureFlag,
	}
	CurrentBranchChangedError = types.CurrentBranchChangedError{
		Message:            ErrorMessage,
		CurrentBranchToken: BranchToken,
	}
	DomainAlreadyExistsError = types.DomainAlreadyExistsError{
		Message: ErrorMessage,
	}
	DomainNotActiveError = types.DomainNotActiveError{
		Message:        ErrorMessage,
		DomainName:     DomainName,
		CurrentCluster: ClusterName1,
		ActiveCluster:  ClusterName2,
		ActiveClusters: []string{ClusterName2},
	}
	EntityNotExistsError = types.EntityNotExistsError{
		Message:        ErrorMessage,
		CurrentCluster: ClusterName1,
		ActiveCluster:  ClusterName2,
		ActiveClusters: []string{ClusterName2},
	}
	WorkflowExecutionAlreadyCompletedError = types.WorkflowExecutionAlreadyCompletedError{
		Message: ErrorMessage,
	}
	EventAlreadyStartedError = types.EventAlreadyStartedError{
		Message: ErrorMessage,
	}
	InternalDataInconsistencyError = types.InternalDataInconsistencyError{
		Message: ErrorMessage,
	}
	InternalServiceError = types.InternalServiceError{
		Message: ErrorMessage,
	}
	LimitExceededError = types.LimitExceededError{
		Message: ErrorMessage,
	}
	QueryFailedError = types.QueryFailedError{
		Message: ErrorMessage,
	}
	RemoteSyncMatchedError = types.RemoteSyncMatchedError{
		Message: ErrorMessage,
	}
	RetryTaskV2Error = types.RetryTaskV2Error{
		Message:           ErrorMessage,
		DomainID:          DomainID,
		WorkflowID:        WorkflowID,
		RunID:             RunID,
		StartEventID:      common.Int64Ptr(EventID1),
		StartEventVersion: common.Int64Ptr(Version1),
		EndEventID:        common.Int64Ptr(EventID2),
		EndEventVersion:   common.Int64Ptr(Version2),
	}
	ServiceBusyError = types.ServiceBusyError{
		Message: ErrorMessage,
		Reason:  ErrorReason,
	}
	ShardOwnershipLostError = types.ShardOwnershipLostError{
		Message: ErrorMessage,
		Owner:   HostName,
	}
	WorkflowExecutionAlreadyStartedError = types.WorkflowExecutionAlreadyStartedError{
		Message:        ErrorMessage,
		StartRequestID: RequestID,
		RunID:          RunID,
	}
	StickyWorkerUnavailableError = types.StickyWorkerUnavailableError{
		Message: ErrorMessage,
	}
	ReadOnlyPartitionError = types.ReadOnlyPartitionError{
		Message: ErrorMessage,
	}
	TaskListNotOwnedByHostError = cadence_errors.TaskListNotOwnedByHostError{
		OwnedByIdentity: HostName,
		MyIdentity:      HostName2,
		TasklistName:    TaskListName,
	}
)

var Errors = []error{
	&AccessDeniedError,
	&BadRequestError,
	&CancellationAlreadyRequestedError,
	&ClientVersionNotSupportedError,
	&FeatureNotEnabledError,
	&CurrentBranchChangedError,
	&DomainAlreadyExistsError,
	&DomainNotActiveError,
	&EntityNotExistsError,
	&WorkflowExecutionAlreadyCompletedError,
	&EventAlreadyStartedError,
	&InternalDataInconsistencyError,
	&InternalServiceError,
	&LimitExceededError,
	&QueryFailedError,
	&RemoteSyncMatchedError,
	&RetryTaskV2Error,
	&ServiceBusyError,
	&ShardOwnershipLostError,
	&WorkflowExecutionAlreadyStartedError,
	&StickyWorkerUnavailableError,
	&ReadOnlyPartitionError,
	&TaskListNotOwnedByHostError,
}
