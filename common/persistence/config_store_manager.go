package persistence

import (
	"context"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/log"
)

type (

	// configStoreManagerImpl implements ConfigStoreManager based on ConfigStore and PayloadSerializer
	configStoreManagerImpl struct {
		serializer  PayloadSerializer
		persistence ConfigStore
		logger      log.Logger
		timeSrc     clock.TimeSource
	}
)

var _ ConfigStoreManager = (*configStoreManagerImpl)(nil)

// NewConfigStoreManagerImpl returns new ConfigStoreManager
func NewConfigStoreManagerImpl(persistence ConfigStore, logger log.Logger) ConfigStoreManager {
	return &configStoreManagerImpl{
		serializer:  NewPayloadSerializer(),
		persistence: persistence,
		logger:      logger,
		timeSrc:     clock.NewRealTimeSource(),
	}
}

func (m *configStoreManagerImpl) Close() {
	m.persistence.Close()
}

func (m *configStoreManagerImpl) FetchDynamicConfig(ctx context.Context, cfgType ConfigType) (*FetchDynamicConfigResponse, error) {
	values, err := m.persistence.FetchConfig(ctx, cfgType)
	if err != nil || values == nil {
		return nil, err
	}

	config, err := m.serializer.DeserializeDynamicConfigBlob(values.Values)
	if err != nil {
		return nil, err
	}

	return &FetchDynamicConfigResponse{Snapshot: &DynamicConfigSnapshot{
		Version: values.Version,
		Values:  config,
	}}, nil
}

func (m *configStoreManagerImpl) UpdateDynamicConfig(ctx context.Context, request *UpdateDynamicConfigRequest, cfgType ConfigType) error {
	blob, err := m.serializer.SerializeDynamicConfigBlob(request.Snapshot.Values, constants.EncodingTypeThriftRW)
	if err != nil {
		return err
	}

	entry := &InternalConfigStoreEntry{
		RowType:   int(cfgType),
		Version:   request.Snapshot.Version,
		Timestamp: m.timeSrc.Now(),
		Values:    blob,
	}

	return m.persistence.UpdateConfig(ctx, entry)
}
