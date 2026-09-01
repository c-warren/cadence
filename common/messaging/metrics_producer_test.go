package messaging

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/metrics/mocks"
)

func TestPublish(t *testing.T) {
	tests := []struct {
		desc                string
		tags                []metrics.Tag
		producerFails       bool
		metricsClientMockFn func() *mocks.Client
	}{
		{
			desc:          "success",
			producerFails: false,
			tags: []metrics.Tag{
				metrics.TopicTag("test-topic-1"),
			},
			metricsClientMockFn: func() *mocks.Client {
				metricsClient := &mocks.Client{}
				metricsScope := &mocks.Scope{}
				metricsClient.
					On("Scope", metrics.MessagingClientPublishScope, []metrics.Tag{metrics.TopicTag("test-topic-1")}).
					Return(metricsScope).
					Once()
				metricsScope.On("IncCounter", metrics.CadenceClientRequests).Once()

				sw := metrics.NoopScope.StartTimer(-1)
				metricsScope.On("StartTimerWithExponentialHistogram", metrics.CadenceClientLatency, metrics.CadenceClientLatencyHistogram).Return(sw).Once()
				return metricsClient
			},
		},
		{
			desc:          "failure",
			producerFails: true,
			tags: []metrics.Tag{
				metrics.TopicTag("test-topic-2"),
			},
			metricsClientMockFn: func() *mocks.Client {
				metricsClient := &mocks.Client{}
				metricsScope := &mocks.Scope{}
				metricsClient.
					On("Scope", metrics.MessagingClientPublishScope, []metrics.Tag{metrics.TopicTag("test-topic-2")}).
					Return(metricsScope).
					Once()
				metricsScope.On("IncCounter", metrics.CadenceClientRequests).Once()
				metricsScope.On("IncCounter", metrics.CadenceClientFailures).Once()

				sw := metrics.NoopScope.StartTimer(-1)
				metricsScope.On("StartTimerWithExponentialHistogram", metrics.CadenceClientLatency, metrics.CadenceClientLatencyHistogram).Return(sw).Once()
				return metricsClient
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// setup
			ctrl := gomock.NewController(t)
			mockProducer := NewMockProducer(ctrl)
			msg := "custom-message"
			if tc.producerFails {
				mockProducer.EXPECT().Publish(gomock.Any(), msg).Return(errors.New("publish failed")).Times(1)
			} else {
				mockProducer.EXPECT().Publish(gomock.Any(), msg).Return(nil).Times(1)
			}
			metricsClient := tc.metricsClientMockFn()

			// create producer and call publish
			p := NewMetricProducer(mockProducer, metricsClient, WithMetricTags(tc.tags...))
			err := p.Publish(context.Background(), msg)

			// validations
			if tc.producerFails != (err != nil) {
				t.Errorf("expected producer to fail: %v, got: %v", tc.producerFails, err)
			}
			if err != nil {
				return
			}

			metricsClient.AssertExpectations(t)
		})
	}
}
