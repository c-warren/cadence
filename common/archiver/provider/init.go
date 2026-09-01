package provider

import (
	"fmt"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/filestore"
	"github.com/uber/cadence/common/archiver/s3store"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/config/yaml"
)

func init() {
	// TODO: ideally remove this and handle per-instance registration during startup somehow,
	// as globals and inits have consistently caused issues.
	//
	// For now though, it's replacing a hard-coded switch statement, so an init func
	// is the most straightforward and should-be-identical conversion.

	must := func(err error) {
		if err != nil {
			panic(fmt.Errorf("failed to register default provider: %w", err))
		}
	}

	must(RegisterHistoryArchiver(filestore.URIScheme, config.FilestoreConfig, func(cfg *yaml.Node, container *archiver.HistoryBootstrapContainer) (archiver.HistoryArchiver, error) {
		var out *config.FilestoreArchiver
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return filestore.NewHistoryArchiver(container, out)
	}))
	// s3store handles both the plain bucket scheme ("s3") and the access point scheme ("s3-ap").
	// Both register under the same S3storeConfig YAML node.
	s3HistoryConstructor := func(cfg *yaml.Node, container *archiver.HistoryBootstrapContainer) (archiver.HistoryArchiver, error) {
		var out *config.S3Archiver
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return s3store.NewHistoryArchiver(container, out)
	}
	must(RegisterHistoryArchiver(s3store.URIScheme, config.S3storeConfig, s3HistoryConstructor))
	must(RegisterHistoryArchiver(s3store.URISchemeAccessPoint, config.S3storeConfig, s3HistoryConstructor))

	must(RegisterVisibilityArchiver(filestore.URIScheme, config.FilestoreConfig, func(cfg *yaml.Node, container *archiver.VisibilityBootstrapContainer) (archiver.VisibilityArchiver, error) {
		var out *config.FilestoreArchiver
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return filestore.NewVisibilityArchiver(container, out)
	}))
	s3VisibilityConstructor := func(cfg *yaml.Node, container *archiver.VisibilityBootstrapContainer) (archiver.VisibilityArchiver, error) {
		var out *config.S3Archiver
		if err := cfg.Decode(&out); err != nil {
			return nil, fmt.Errorf("bad config: %w", err)
		}
		return s3store.NewVisibilityArchiver(container, out)
	}
	must(RegisterVisibilityArchiver(s3store.URIScheme, config.S3storeConfig, s3VisibilityConstructor))
	must(RegisterVisibilityArchiver(s3store.URISchemeAccessPoint, config.S3storeConfig, s3VisibilityConstructor))
}
