package gcloud

import (
	"fmt"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/gcloud/connector"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/config/yaml"
)

func init() {
	// register default providers, ideally remove this and trigger manually during startup

	must := func(err error) {
		if err != nil {
			panic(fmt.Errorf("failed to register gcloud archivers: %w", err))
		}
	}

	must(provider.RegisterHistoryArchiver(URIScheme, ConfigKey, func(cfg *yaml.Node, container *archiver.HistoryBootstrapContainer) (archiver.HistoryArchiver, error) {
		var out connector.Config
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return NewHistoryArchiver(container, out)
	}))
	must(provider.RegisterVisibilityArchiver(URIScheme, ConfigKey, func(cfg *yaml.Node, container *archiver.VisibilityBootstrapContainer) (archiver.VisibilityArchiver, error) {
		var out connector.Config
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return NewVisibilityArchiver(container, out)
	}))
}
