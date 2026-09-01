package host

import (
	"errors"
	"fmt"

	"github.com/uber/cadence/common/membership"
)

type simpleResolver struct {
	hostInfo  membership.HostInfo
	resolvers map[string]*simpleHashring
}

// NewSimpleResolver returns a membership resolver interface
func NewSimpleResolver(serviceName string, hosts map[string][]membership.HostInfo, currentHost membership.HostInfo) membership.Resolver {
	resolvers := make(map[string]*simpleHashring, len(hosts))
	for service, hostList := range hosts {
		resolvers[service] = newSimpleHashring(hostList)
	}
	return &simpleResolver{
		hostInfo:  currentHost,
		resolvers: resolvers,
	}
}

func (s *simpleResolver) Start() {
}

func (s *simpleResolver) Stop() {
}

func (s *simpleResolver) EvictSelf() error {
	return nil
}

func (s *simpleResolver) WhoAmI() (membership.HostInfo, error) {
	return s.hostInfo, nil
}

func (s *simpleResolver) Subscribe(service string, name string, notifyChannel chan<- *membership.ChangedEvent) error {
	return nil
}

func (s *simpleResolver) Unsubscribe(service string, name string) error {
	return nil
}

func (s *simpleResolver) Lookup(service string, key string) (membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return membership.HostInfo{}, fmt.Errorf("cannot lookup host for service %q", service)
	}
	return resolver.Lookup(key)
}

func (s *simpleResolver) LookupN(service string, key string, n int) ([]membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, fmt.Errorf("cannot lookup host for service %q", service)
	}
	return resolver.LookupN(key, n)
}

func (s *simpleResolver) MemberCount(service string) (int, error) {
	members, err := s.Members(service)
	return len(members), err
}

func (s *simpleResolver) Members(service string) ([]membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return nil, fmt.Errorf("cannot lookup host for service %q", service)
	}
	return resolver.Members(), nil
}

func (s *simpleResolver) LookupByAddress(service string, address string) (membership.HostInfo, error) {
	resolver, ok := s.resolvers[service]
	if !ok {
		return membership.HostInfo{}, fmt.Errorf("cannot lookup host for service %q", service)
	}
	for _, m := range resolver.Members() {
		if belongs, err := m.Belongs(address); err == nil && belongs {
			return m, nil
		}
	}

	return membership.HostInfo{}, errors.New("host not found")
}
