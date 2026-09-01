package common

import "time"

const (
	// DefaultAvailabilityThreshold is the default threshold for availability
	DefaultAvailabilityThreshold = 0.99

	// DefaultContextTimeout is the default context timeout for RPC calls
	DefaultContextTimeout = 3 * time.Second
)

const (
	// DefaultMaxRetryCount is the default max retry count
	DefaultMaxRetryCount = 5
	// DefaultRetryBackoffDuration is the default backoff duration for retry
	DefaultRetryBackoffDuration = 50 * time.Millisecond
)

const (
	// ErrReasonValidationFailed is the failure reason for validation failure
	ErrReasonValidationFailed = "validation failed"
	// ErrReasonWorkflowNotExist is the error reason for workflow not exist
	ErrReasonWorkflowNotExist = "workflow not exist"
)
