package domaindeprecation

import "time"

const (
	// ErrDomainDoesNotExistNonRetryable is error reason used for Cadence non-retryable errors
	ErrDomainDoesNotExistNonRetryable = "domain does not exist"
	// ErrAccessDeniedNonRetryable is permission error that require manual intervention
	ErrAccessDeniedNonRetryable = "AccessDeniedError"
	// ErrWorkflowAlreadyCompletedNonRetryable is error that indicates workflow execution already completed
	ErrWorkflowAlreadyCompletedNonRetryable = "WorkflowExecutionAlreadyCompletedError"
	// ErrActivePollersNonRetryable indicates the domain has active pollers and cannot be deprecated without force
	ErrActivePollersNonRetryable = "domain has active pollers"

	// DefaultRPS is the default RPS
	DefaultRPS = 50
	// DefaultConcurrency is the default concurrency
	DefaultConcurrency = 5
	// DefaultPageSize is the default page size
	DefaultPageSize = 1000
	// DefaultAttemptsOnRetryableError is the default value for AttemptsOnRetryableError
	DefaultAttemptsOnRetryableError = 50
	// DefaultActivityHeartBeatTimeout is the default value for ActivityHeartBeatTimeout
	DefaultActivityHeartBeatTimeout = time.Second * 10
	// DefaultMaxActivityRetries is the default value for MaxActivityRetries
	DefaultMaxActivityRetries = 4
)
