// Package openfeatureprovider is a plugin registry for OpenFeature providers.
// Each provider (flagd, unleash, etc.) lives in its own subpackage, defines its own
// config struct, and self-registers a Constructor via init() - see the unleash
// subpackage for the reference implementation. Binaries opt into a provider with a
// blank import (e.g. cmd/server/main.go), same convention as
// common/asyncworkflow/queue/provider and common/archiver/provider.
package openfeatureprovider

import (
	"fmt"

	"github.com/open-feature/go-sdk/openfeature"

	"github.com/uber/cadence/common/syncmap"
)

type (
	// Decoder decodes a provider's own configuration. *yaml.Node (from
	// common/config/yaml) satisfies this structurally, so this package never
	// needs to import common/config.
	Decoder interface {
		Decode(out any) error
	}

	// Constructor builds an OpenFeature provider from its own config, decoded via cfg.
	Constructor func(cfg Decoder) (openfeature.FeatureProvider, error)
)

var constructors = syncmap.New[string, Constructor]()

// Register registers a named OpenFeature provider constructor. Intended to be called
// from a provider's own package init(), e.g.
// common/dynamicconfig/openfeatureprovider/unleash.
func Register(name string, constructor Constructor) error {
	if !constructors.Put(name, constructor) {
		return fmt.Errorf("openfeature provider %q already registered", name)
	}
	return nil
}

// Get returns the constructor registered for name, if any.
func Get(name string) (Constructor, bool) {
	return constructors.Get(name)
}
