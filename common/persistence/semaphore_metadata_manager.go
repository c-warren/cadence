package persistence

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
)

// DefaultSemaphoreBucketSize is the per-bucket token budget applied when a
// CreateSemaphoreRequest leaves BucketSize unset. N = ceil(size / bucket_size).
const DefaultSemaphoreBucketSize = 100

type semaphoreMetadataManagerImpl struct {
	persistence SemaphoreMetadataStore
	logger      log.Logger
	timeSrc     clock.TimeSource
}

// NewSemaphoreMetadataManagerImpl returns a new SemaphoreMetadataManager
func NewSemaphoreMetadataManagerImpl(persistence SemaphoreMetadataStore, logger log.Logger) SemaphoreMetadataManager {
	return &semaphoreMetadataManagerImpl{
		persistence: persistence,
		logger:      logger,
		timeSrc:     clock.NewRealTimeSource(),
	}
}

func (m *semaphoreMetadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *semaphoreMetadataManagerImpl) Close() {
	m.persistence.Close()
}

func (m *semaphoreMetadataManagerImpl) CreateSemaphore(
	ctx context.Context,
	request *CreateSemaphoreRequest,
) (*CreateSemaphoreResponse, error) {
	if request.DomainID == "" {
		return nil, fmt.Errorf("DomainID is required")
	}
	if request.SemaphoreName == "" {
		return nil, fmt.Errorf("SemaphoreName is required")
	}
	if request.Size <= 0 {
		return nil, fmt.Errorf("Size must be positive, got %d", request.Size)
	}

	if request.BucketSize < 0 {
		return nil, fmt.Errorf("BucketSize must not be negative, got %d", request.BucketSize)
	}
	// BucketSize is optional: an unset (zero) value falls back to the default.
	bucketSize := request.BucketSize
	if bucketSize == 0 {
		bucketSize = DefaultSemaphoreBucketSize
	}

	semaphore := &SemaphoreMetadata{
		DomainID:      request.DomainID,
		SemaphoreName: request.SemaphoreName,
		Size:          request.Size,
		BucketSize:    bucketSize,
		CreatedTime:   m.timeSrc.Now().UTC(),
	}

	if err := m.persistence.CreateSemaphore(ctx, semaphore); err != nil {
		return nil, err
	}
	return &CreateSemaphoreResponse{Semaphore: semaphore}, nil
}

func (m *semaphoreMetadataManagerImpl) GetSemaphore(
	ctx context.Context,
	request *GetSemaphoreRequest,
) (*GetSemaphoreResponse, error) {
	semaphore, err := m.persistence.GetSemaphore(ctx, request)
	if err != nil {
		return nil, err
	}
	return &GetSemaphoreResponse{Semaphore: semaphore}, nil
}

func (m *semaphoreMetadataManagerImpl) ListSemaphores(
	ctx context.Context,
	request *ListSemaphoresRequest,
) (*ListSemaphoresResponse, error) {
	return m.persistence.ListSemaphores(ctx, request)
}
