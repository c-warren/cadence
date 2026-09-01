//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/common/asyncworkflow/queue/provider

package provider

import (
	"fmt"

	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/syncmap"
	"github.com/uber/cadence/common/types"
)

type (
	Params struct {
		Logger         log.Logger
		MetricsClient  metrics.Client
		FrontendClient frontend.Client
	}

	Decoder interface {
		Decode(any) error
	}

	Consumer interface {
		Start() error
		Stop()
	}

	// Queue is an interface for async queue
	Queue interface {
		ID() string
		CreateConsumer(*Params) (Consumer, error)
		CreateProducer(*Params) (messaging.Producer, error)
	}

	QueueConstructor func(Decoder) (Queue, error)

	DecoderConstructor func(*types.DataBlob) Decoder
)

var (
	queueConstructors   = syncmap.New[string, QueueConstructor]()
	decoderConstructors = syncmap.New[string, DecoderConstructor]()
)

// RegisterQueueProvider registers a queue constructor for a given queue type
func RegisterQueueProvider(queueType string, queueConstructor QueueConstructor) error {
	inserted := queueConstructors.Put(queueType, queueConstructor)
	if !inserted {
		return fmt.Errorf("queue type %v already registered", queueType)
	}
	return nil
}

// GetQueueProvider returns a queue constructor for a given queue type
func GetQueueProvider(queueType string) (QueueConstructor, bool) {
	return queueConstructors.Get(queueType)
}

// RegisterDecoder registers a decoder constructor for a given queue type
func RegisterDecoder(queueType string, decoderConstructor DecoderConstructor) error {
	inserted := decoderConstructors.Put(queueType, decoderConstructor)
	if !inserted {
		return fmt.Errorf("decoder type %v already registered", queueType)
	}
	return nil
}

// GetDecoder returns a decoder constructor for a given queue type
func GetDecoder(queueType string) (DecoderConstructor, bool) {
	return decoderConstructors.Get(queueType)
}
