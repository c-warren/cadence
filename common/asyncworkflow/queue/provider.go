package queue

import (
	"fmt"

	"github.com/uber/cadence/common/asyncworkflow/queue/provider"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/types"
)

type (
	providerImpl struct {
		queues map[string]provider.Queue
	}
)

// NewAsyncQueueProvider returns a new async queue provider
func NewAsyncQueueProvider(cfg map[string]config.AsyncWorkflowQueueProvider) (Provider, error) {
	p := &providerImpl{
		queues: make(map[string]provider.Queue),
	}
	for queueName, queueCfg := range cfg {
		queueConstructor, ok := provider.GetQueueProvider(queueCfg.Type)
		if !ok {
			return nil, fmt.Errorf("queue type %v not registered", queueCfg.Type)
		}
		queue, err := queueConstructor(queueCfg.Config)
		if err != nil {
			return nil, err
		}
		p.queues[queueName] = queue
	}
	return p, nil
}

func (p *providerImpl) GetPredefinedQueue(name string) (provider.Queue, error) {
	queue, ok := p.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %v not found", name)
	}
	return queue, nil
}

func (p *providerImpl) GetQueue(queueType string, queueConfig *types.DataBlob) (provider.Queue, error) {
	queueConfigDecoder, ok := provider.GetDecoder(queueType)
	if !ok {
		return nil, fmt.Errorf("queue type %v not registered", queueType)
	}
	decoder := queueConfigDecoder(queueConfig)
	queueConstructor, ok := provider.GetQueueProvider(queueType)
	if !ok {
		return nil, fmt.Errorf("queue type %v not registered", queueType)
	}
	return queueConstructor(decoder)
}
