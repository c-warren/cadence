//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination factory_mock.go -self_package github.com/uber/cadence/common/rpc

package rpc

import (
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"

	"github.com/uber/cadence/common/membership"
)

// Factory Creates a dispatcher that knows how to transport requests.
type Factory interface {
	GetDispatcher() *yarpc.Dispatcher
	GetMaxMessageSize() int
	Start(PeerLister) error
	GetTChannel() tchannel.Channel
	Stop() error
}

type PeerLister interface {
	Subscribe(service, name string, notifyChannel chan<- *membership.ChangedEvent) error
	Unsubscribe(service, name string) error
	Members(service string) ([]membership.HostInfo, error)
}
