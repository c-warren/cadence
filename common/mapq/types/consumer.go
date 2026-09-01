package types

import "context"

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination consumer_mock.go -package types github.com/uber/cadence/common/mapq/types ConsumerFactory,Consumer

type ConsumerFactory interface {
	// New creates a new consumer with the given partitions or returns an existing consumer
	// to process the given partitions
	// Consumer lifecycle is managed by the factory so the returned consumer must be started.
	New(ItemPartitions) (Consumer, error)

	// Stop stops all consumers created by this factory
	Stop(context.Context) error
}

type Consumer interface {
	Start(context.Context) error
	Stop(context.Context) error
	Process(context.Context, Item) error
}
