package peerprovider

import (
	"fmt"

	"go.uber.org/yarpc/transport/tchannel"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/config/yaml"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/syncmap"
)

const key = "peerprovider"

// Container is passed to peer provider plugin
type Container struct {
	Service string
	// Channel is required by ringpop
	Channel tchannel.Channel
	Logger  log.Logger
	Portmap membership.PortMap
}

type constructorFn func(cfg *yaml.Node, container Container) (membership.PeerProvider, error)

var plugins = syncmap.New[string, plugin]()

type plugin struct {
	fn        constructorFn
	configKey string
}

type Provider struct {
	config    config.PeerProvider
	container Container
}

func New(config config.PeerProvider, container Container) *Provider {
	return &Provider{
		config:    config,
		container: container,
	}
}

func Register(configKey string, constructor constructorFn) error {

	inserted := plugins.Put(key, plugin{
		fn:        constructor,
		configKey: configKey,
	})

	// only one plugin is allowed to be registered
	if !inserted {
		registeredPlugin, _ := plugins.Get(key)
		return fmt.Errorf("cannot register %q provider, %q is already registered", configKey, registeredPlugin.configKey)
	}

	return nil
}

func (p *Provider) Provider() (membership.PeerProvider, error) {
	registeredPlugin, found := plugins.Get(key)

	if !found {
		return nil, fmt.Errorf("no configured peer providers found")
	}

	for configKey, cfg := range p.config {
		if configKey == registeredPlugin.configKey {
			return registeredPlugin.fn(cfg, p.container)
		}
	}

	return nil, fmt.Errorf("no configuration for %q peer provider found", registeredPlugin.configKey)
}
