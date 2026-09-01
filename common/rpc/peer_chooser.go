//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination peer_chooser_mock.go -self_package github.com/uber/cadence/common/rpc

package rpc

import (
	"context"
	"time"

	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/roundrobin"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
)

const defaultDNSRefreshInterval = time.Second * 10

type (
	PeerChooserOptions struct {
		// Address is the target dns address. Used by dns peer chooser.
		Address string

		// ServiceName is the name of service. Used by direct peer chooser.
		ServiceName string

		// EnableConnectionRetainingDirectChooser is used by direct peer chooser.
		// If false, yarpc's own default direct peer chooser will be used which doesn't retain connections.
		// If true, cadence's own direct peer chooser will be used which retains connections.
		EnableConnectionRetainingDirectChooser dynamicproperties.BoolPropertyFn
	}
	PeerChooserFactory interface {
		CreatePeerChooser(transport peer.Transport, opts PeerChooserOptions) (PeerChooser, error)
	}

	PeerChooser interface {
		peer.Chooser

		// UpdatePeers updates the list of peers if needed.
		UpdatePeers(serviceName string, members []membership.HostInfo)
	}

	dnsPeerChooserFactory struct {
		interval time.Duration
		logger   log.Logger
	}

	directPeerChooserFactory struct {
		serviceName string
		logger      log.Logger
		metricsCl   metrics.Client
		choosers    []*directPeerChooser
	}
)

type defaultPeerChooser struct {
	actual peer.Chooser
	onStop func()
}

// UpdatePeers is a no-op for defaultPeerChooser. It is added to satisfy the PeerChooser interface.
func (d *defaultPeerChooser) UpdatePeers(string, []membership.HostInfo) {}

// Choose a Peer for the next call, block until a peer is available (or timeout)
func (d *defaultPeerChooser) Choose(ctx context.Context, req *transport.Request) (peer peer.Peer, onFinish func(error), err error) {
	return d.actual.Choose(ctx, req)
}

func (d *defaultPeerChooser) Start() error {
	return d.actual.Start()
}

func (d *defaultPeerChooser) Stop() error {
	if d.onStop != nil {
		d.onStop()
	}
	return d.actual.Stop()
}

func (d *defaultPeerChooser) IsRunning() bool {
	return d.actual.IsRunning()
}

func NewDNSPeerChooserFactory(interval time.Duration, logger log.Logger) PeerChooserFactory {
	if interval <= 0 {
		interval = defaultDNSRefreshInterval
	}

	return &dnsPeerChooserFactory{
		interval: interval,
		logger:   logger,
	}
}

func (f *dnsPeerChooserFactory) CreatePeerChooser(transport peer.Transport, opts PeerChooserOptions) (PeerChooser, error) {
	peerList := roundrobin.New(transport)
	peerListUpdater, err := newDNSUpdater(peerList, opts.Address, f.interval, f.logger)
	if err != nil {
		return nil, err
	}
	peerListUpdater.Start()
	return &defaultPeerChooser{
		actual: peerList,
		onStop: peerListUpdater.Stop,
	}, nil
}

func NewDirectPeerChooserFactory(serviceName string, logger log.Logger, metricsCl metrics.Client) PeerChooserFactory {
	return &directPeerChooserFactory{
		serviceName: serviceName,
		logger:      logger,
		metricsCl:   metricsCl,
	}
}

func (f *directPeerChooserFactory) CreatePeerChooser(transport peer.Transport, opts PeerChooserOptions) (PeerChooser, error) {
	c := newDirectChooser(f.serviceName, transport, f.logger, f.metricsCl, opts.EnableConnectionRetainingDirectChooser)
	f.choosers = append(f.choosers, c)
	return c, nil
}
