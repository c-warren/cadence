package membership

import "github.com/uber/cadence/common"

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination=singleprovider_mock.go SingleProvider

type SingleProvider interface {
	common.Daemon
	Lookup(key string) (HostInfo, error)
	// LookupN returns up to n hosts responsible for the given key, enabling
	// redundant workers across multiple hosts for the same key.
	LookupN(key string, n int) ([]HostInfo, error)
	Subscribe(name string, channel chan<- *ChangedEvent) error
	AddressToHost(owner string) (HostInfo, error)
	Unsubscribe(name string) error
	Members() []HostInfo
	MemberCount() int
	Refresh() error
}
