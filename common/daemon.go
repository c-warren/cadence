package common

import "context"

const (
	// used for background threads

	// DaemonStatusInitialized coroutine pool initialized
	DaemonStatusInitialized int32 = 0
	// DaemonStatusStarted coroutine pool started
	DaemonStatusStarted int32 = 1
	// DaemonStatusStopped coroutine pool stopped
	DaemonStatusStopped int32 = 2
)

type (
	// Daemon is the base interfaces implemented by
	// background tasks within Cadence
	//
	// Deprecated: Use DaemonV2 instead for context-aware lifecycle management
	Daemon interface {
		Start()
		Stop()
	}

	// DaemonV2 is the context-aware version of Daemon
	// for background tasks that need graceful shutdown coordination
	DaemonV2 interface {
		Start(context.Context) error
		Stop(context.Context) error
	}
)
