package task

import "time"

const (
	loadDomainEntryForTaskRetryDelay = 100 * time.Millisecond

	activeTaskResubmitMaxAttempts = 10

	defaultTaskEventLoggerSize = 100

	stickyTaskMaxRetryCount = 100

	// noPriority is the value returned if no priority is ever assigned to the task
	noPriority = -1
)
