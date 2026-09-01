package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
)

type (
	fakePeerTransport struct{}
	fakePeer          struct{}
)

func (t *fakePeerTransport) RetainPeer(peer.Identifier, peer.Subscriber) (peer.Peer, error) {
	return &fakePeer{}, nil
}
func (t *fakePeerTransport) ReleasePeer(peer.Identifier, peer.Subscriber) error {
	return nil
}

func (p *fakePeer) Identifier() string  { return "fakePeer" }
func (p *fakePeer) Status() peer.Status { return peer.Status{ConnectionStatus: peer.Available} }
func (p *fakePeer) StartRequest()       {}
func (p *fakePeer) EndRequest()         {}

func TestDNSPeerChooserFactory(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := log.NewNoop()
	ctx := context.Background()
	interval := 10 * time.Millisecond

	factory := NewDNSPeerChooserFactory(interval, logger)
	peerTransport := &fakePeerTransport{}

	// Ensure invalid address returns error
	_, err := factory.CreatePeerChooser(peerTransport, PeerChooserOptions{Address: "invalid address"})
	assert.EqualError(t, err, "incorrect DNS:Port format")

	chooser, err := factory.CreatePeerChooser(peerTransport, PeerChooserOptions{Address: "localhost:1234"})
	require.NoError(t, err)

	require.NoError(t, chooser.Start())
	defer chooser.Stop()

	require.True(t, chooser.IsRunning())

	// Poll until the DNS refresh populates the peer list.
	var gotPeer peer.Peer
	require.Eventually(t, func() bool {
		attemptCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		p, _, err := chooser.Choose(attemptCtx, &transport.Request{})
		if err != nil {
			return false
		}
		gotPeer = p
		return true
	}, 500*time.Millisecond, interval)
	require.NotNil(t, gotPeer)
	assert.Equal(t, "fakePeer", gotPeer.Identifier())
}

func TestDirectPeerChooserFactory(t *testing.T) {
	logger := testlogger.New(t)
	metricCl := metrics.NewNoopMetricsClient()
	serviceName := "service"
	pcf := NewDirectPeerChooserFactory(serviceName, logger, metricCl)
	directConnRetainFn := func(opts ...dynamicproperties.FilterOption) bool { return false }
	grpcTransport := grpc.NewTransport()
	chooser, err := pcf.CreatePeerChooser(grpcTransport, PeerChooserOptions{
		ServiceName:                            serviceName,
		EnableConnectionRetainingDirectChooser: directConnRetainFn,
	})
	if err != nil {
		t.Fatalf("Failed to create direct peer chooser: %v", err)
	}
	if chooser == nil {
		t.Fatal("Failed to create direct peer chooser: nil")
	}

	if _, dc := chooser.(*directPeerChooser); !dc {
		t.Fatalf("Want chooser be of type (*directPeerChooser), got %d", chooser)
	}
}
