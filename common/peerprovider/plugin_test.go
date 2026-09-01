package peerprovider

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/config/yaml"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/syncmap"
)

func TestProviderRetrunsErrorWhenNoProviderRegistered(t *testing.T) {
	// Reset plugins
	plugins = syncmap.New[string, plugin]()
	a := Provider{
		config:    nil,
		container: Container{},
	}
	p, err := a.Provider()
	assert.Nil(t, p)
	assert.EqualError(t, err, "no configured peer providers found")
}

func TestProviderRetrunsErrorWhenPluginAlreadyRegistered(t *testing.T) {
	// Reset plugins
	plugins = syncmap.New[string, plugin]()
	err := Register("provider1", func(cfg *yaml.Node, container Container) (membership.PeerProvider, error) {
		return nil, nil
	})
	assert.NoError(t, err)
	err = Register("provider2", func(cfg *yaml.Node, container Container) (membership.PeerProvider, error) {
		return nil, nil
	})
	assert.Error(t, err)
}

func TestConfigIsPickedUp(t *testing.T) {
	// Reset plugins
	plugins = syncmap.New[string, plugin]()

	peerProviderConfig := map[string]*yaml.Node{}
	peerProviderConfig["provider1"] = &yaml.Node{}

	pp := New(peerProviderConfig, Container{})
	err := Register("provider1", func(cfg *yaml.Node, container Container) (membership.PeerProvider, error) {
		return nil, nil
	})
	assert.NoError(t, err)
	_, err = pp.Provider()
	assert.NoError(t, err)
}

func TestErrorWhenConfigIsNotProvided(t *testing.T) {
	// Reset plugins
	plugins = syncmap.New[string, plugin]()
	pp := New(config.PeerProvider{}, Container{})
	err := Register("provider1", func(cfg *yaml.Node, container Container) (membership.PeerProvider, error) {
		return nil, nil
	})
	p, err := pp.Provider()
	assert.Nil(t, p)
	assert.EqualError(t, err, "no configuration for \"provider1\" peer provider found")
}
