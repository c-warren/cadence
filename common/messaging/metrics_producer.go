package messaging

import (
	"context"

	"github.com/uber/cadence/common/metrics"
)

type MetricsProducer struct {
	producer Producer
	scope    metrics.Scope
	tags     []metrics.Tag
}

type MetricProducerOptions func(*MetricsProducer)

func WithMetricTags(tags ...metrics.Tag) MetricProducerOptions {
	return func(p *MetricsProducer) {
		p.tags = tags
	}
}

// NewMetricProducer creates a new instance of producer that emits metrics
func NewMetricProducer(
	producer Producer,
	metricsClient metrics.Client,
	opts ...MetricProducerOptions,
) Producer {
	p := &MetricsProducer{
		producer: producer,
	}

	for _, opt := range opts {
		opt(p)
	}

	p.scope = metricsClient.Scope(metrics.MessagingClientPublishScope, p.tags...)
	return p
}

func (p *MetricsProducer) Publish(ctx context.Context, msg interface{}) error {
	p.scope.IncCounter(metrics.CadenceClientRequests)

	sw := p.scope.StartTimerWithExponentialHistogram(metrics.CadenceClientLatency, metrics.CadenceClientLatencyHistogram)
	err := p.producer.Publish(ctx, msg)
	sw.Stop()

	if err != nil {
		p.scope.IncCounter(metrics.CadenceClientFailures)
	}
	return err
}

func (p *MetricsProducer) Close() error {
	if closeableProducer, ok := p.producer.(CloseableProducer); ok {
		return closeableProducer.Close()
	}

	return nil
}
