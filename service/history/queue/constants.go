package queue

import "time"

const (
	defaultProcessingQueueLevel = 0

	// gracefulShutdownTimeout is the the hardcoded timeout for queue components to wrap up shutting down.
	// This is not ideal because we should have a top level deadline for shutdown propagating down to all components.
	gracefulShutdownTimeout = time.Minute
)
