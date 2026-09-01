package messaging

import "context"

type noopProducer struct{}

// NewNoopProducer returns a no-op message producer
func NewNoopProducer() Producer {
	return &noopProducer{}
}

func (p noopProducer) Publish(
	_ context.Context,
	_ interface{},
) error {
	return nil
}
