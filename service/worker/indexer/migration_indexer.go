package indexer

import (
	"github.com/uber/cadence/common/constants"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
)

// NewMigrationDualIndexer create a new Indexer that will be used during visibility migration
// When migrate from ES to OS, we will have this indexer to index to both ES and OS
func NewMigrationDualIndexer(config *Config,
	client messaging.Client,
	primaryClient es.GenericClient,
	secondaryClient es.GenericClient,
	primaryVisibilityName string,
	secondaryVisibilityName string,
	primaryConsumerName string,
	secondaryConsumerName string,
	logger log.Logger,
	metricsClient metrics.Client) *DualIndexer {

	logger = logger.WithTags(tag.ComponentIndexer)

	visibilityProcessor, err := newESProcessor(processorName, config, primaryClient, logger, metricsClient)
	if err != nil {
		logger.Fatal("Index ES processor state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	if primaryConsumerName == "" {
		primaryConsumerName = getConsumerName(primaryVisibilityName)
	}
	consumer, err := client.NewConsumer(constants.VisibilityAppName, primaryConsumerName)
	if err != nil {
		logger.Fatal("Index consumer state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	sourceIndexer := &Indexer{
		config:              config,
		esIndexName:         primaryVisibilityName,
		consumer:            consumer,
		logger:              logger.WithTags(tag.ComponentIndexerProcessor),
		scope:               metricsClient.Scope(metrics.IndexProcessorScope),
		shutdownCh:          make(chan struct{}),
		visibilityProcessor: visibilityProcessor,
		msgEncoder:          defaultEncoder,
	}

	secondaryVisibilityProcessor, err := newESProcessor(migrationProcessorName, config, secondaryClient, logger, metricsClient)
	if err != nil {
		logger.Fatal("Migration Index ES processor state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	if secondaryConsumerName == "" {
		secondaryConsumerName = getConsumerName(primaryVisibilityName)
	}
	secondaryConsumer, err := client.NewConsumer(constants.VisibilityAppName, secondaryConsumerName)
	if err != nil {
		logger.Fatal("Migration Index consumer state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	destIndexer := &Indexer{
		config:              config,
		esIndexName:         secondaryVisibilityName,
		consumer:            secondaryConsumer,
		logger:              logger.WithTags(tag.ComponentIndexerProcessor),
		scope:               metricsClient.Scope(metrics.IndexProcessorScope),
		shutdownCh:          make(chan struct{}),
		visibilityProcessor: secondaryVisibilityProcessor,
		msgEncoder:          defaultEncoder,
	}

	return &DualIndexer{
		SourceIndexer: sourceIndexer,
		DestIndexer:   destIndexer,
	}
}

func (i *DualIndexer) Start() error {
	if err := i.SourceIndexer.Start(); err != nil {
		i.SourceIndexer.Stop()
		return err
	}

	if err := i.DestIndexer.Start(); err != nil {
		i.DestIndexer.Stop()
		return err
	}

	return nil
}

func (i *DualIndexer) Stop() {
	i.SourceIndexer.Stop()
	i.DestIndexer.Stop()
}
