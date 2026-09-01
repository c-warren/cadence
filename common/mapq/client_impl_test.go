package mapq

import (
	"context"
	"testing"

	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/mapq/types"
	"github.com/uber/cadence/common/metrics"
)

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)

	tests := []struct {
		name    string
		opts    []Options
		wantErr bool
	}{
		{
			name: "success",
			opts: []Options{
				WithPersister(types.NewMockPersister(ctrl)),
				WithConsumerFactory(types.NewMockConsumerFactory(ctrl)),
			},
		},
		{
			name:    "no persister",
			wantErr: true,
			opts: []Options{
				WithConsumerFactory(types.NewMockConsumerFactory(ctrl)),
			},
		},
		{
			name:    "no consumer factoru",
			wantErr: true,
			opts: []Options{
				WithPersister(types.NewMockPersister(ctrl)),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := testlogger.New(t)
			scope := metrics.NoopScope
			cl, err := New(logger, scope, tc.opts...)
			if (err != nil) != tc.wantErr {
				t.Errorf("New() error: %v, wantErr: %v", err, tc.wantErr)
			}

			if err != nil {
				return
			}

			_, ok := cl.(*clientImpl)
			if !ok {
				t.Errorf("New() = %T, want *clientImpl", cl)
			}
		})
	}
}

func TestStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctrl := gomock.NewController(t)
	consumerFactory := types.NewMockConsumerFactory(ctrl)
	consumer := types.NewMockConsumer(ctrl)
	consumerFactory.EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
	consumerFactory.EXPECT().New(gomock.Any()).Return(consumer, nil).Times(1)
	opts := []Options{
		WithPersister(types.NewMockPersister(ctrl)),
		WithConsumerFactory(consumerFactory),
	}
	logger := testlogger.New(t)
	scope := metrics.NoopScope
	cl, err := New(logger, scope, opts...)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	cl.Start(context.Background())
	defer cl.Stop(context.Background())
}

func TestAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	consumerFactory := types.NewMockConsumerFactory(ctrl)
	consumer := types.NewMockConsumer(ctrl)
	consumerFactory.EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
	consumerFactory.EXPECT().New(gomock.Any()).Return(consumer, nil).Times(1)
	opts := []Options{
		WithPersister(types.NewMockPersister(ctrl)),
		WithConsumerFactory(consumerFactory),
	}
	logger := testlogger.New(t)
	scope := metrics.NoopScope
	cl, err := New(logger, scope, opts...)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	cl.Start(context.Background())
	defer cl.Stop(context.Background())

	err = cl.Ack(context.Background(), nil)
	if err == nil || err.Error() != "not implemented" {
		t.Errorf("Ack() error: %q, want %q", err, "not implemented")
	}
}

func TestNack(t *testing.T) {
	ctrl := gomock.NewController(t)
	consumerFactory := types.NewMockConsumerFactory(ctrl)
	consumer := types.NewMockConsumer(ctrl)
	consumerFactory.EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
	consumerFactory.EXPECT().New(gomock.Any()).Return(consumer, nil).Times(1)
	opts := []Options{
		WithPersister(types.NewMockPersister(ctrl)),
		WithConsumerFactory(consumerFactory),
	}
	logger := testlogger.New(t)
	scope := metrics.NoopScope
	cl, err := New(logger, scope, opts...)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	cl.Start(context.Background())
	defer cl.Stop(context.Background())

	err = cl.Nack(context.Background(), nil)
	if err == nil || err.Error() != "not implemented" {
		t.Errorf("Ack() error: %q, want %q", err, "not implemented")
	}
}
