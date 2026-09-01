package archiverfx

import (
	"go.uber.org/fx"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig"
)

// Module provides archival components for fx application.
var Module = fx.Module("archiverfx",
	fx.Provide(NewArchivalMetadata),
	fx.Provide(NewArchiverProvider),
)

type archivalMetadataParams struct {
	fx.In

	DynamicCollection *dynamicconfig.Collection
	Config            config.Config
}

// NewArchivalMetadata creates an ArchivalMetadata instance via dependency injection.
func NewArchivalMetadata(p archivalMetadataParams) archiver.ArchivalMetadata {
	return archiver.NewArchivalMetadata(
		p.DynamicCollection,
		p.Config.Archival.History.Status,
		p.Config.Archival.History.EnableRead,
		p.Config.Archival.Visibility.Status,
		p.Config.Archival.Visibility.EnableRead,
		&p.Config.DomainDefaults.Archival,
	)
}

type archiverProviderParams struct {
	fx.In

	Config config.Config
}

// NewArchiverProvider creates an ArchiverProvider instance via dependency injection.
func NewArchiverProvider(p archiverProviderParams) provider.ArchiverProvider {
	return provider.NewArchiverProvider(
		p.Config.Archival.History.Provider,
		p.Config.Archival.Visibility.Provider,
	)
}
