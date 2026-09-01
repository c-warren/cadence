package lib

type (
	// Runnable is an interface for anything that exposes a Run method
	Runnable interface {
		Run() error
	}
)
