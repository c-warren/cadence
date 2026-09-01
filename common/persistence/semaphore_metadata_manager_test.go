package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log/testlogger"
)

func newTestSemaphoreMetadataManager(store SemaphoreMetadataStore, timeSrc clock.TimeSource, t *testing.T) *semaphoreMetadataManagerImpl {
	return &semaphoreMetadataManagerImpl{
		persistence: store,
		logger:      testlogger.New(t),
		timeSrc:     timeSrc,
	}
}

func TestSemaphoreManagerCreateSemaphore(t *testing.T) {
	fixedTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		request    *CreateSemaphoreRequest
		setupMock  func(store *MockSemaphoreMetadataStore)
		wantErr    bool
		wantResult *SemaphoreMetadata
	}{
		{
			name: "success with explicit bucket size",
			request: &CreateSemaphoreRequest{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
				BucketSize:    10,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {
				store.EXPECT().CreateSemaphore(gomock.Any(), &SemaphoreMetadata{
					DomainID:      "domain-1",
					SemaphoreName: "sem-1",
					Size:          100,
					BucketSize:    10,
					CreatedTime:   fixedTime,
				}).Return(nil).Times(1)
			},
			wantResult: &SemaphoreMetadata{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
				BucketSize:    10,
				CreatedTime:   fixedTime,
			},
		},
		{
			name: "success with default bucket size",
			request: &CreateSemaphoreRequest{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {
				store.EXPECT().CreateSemaphore(gomock.Any(), &SemaphoreMetadata{
					DomainID:      "domain-1",
					SemaphoreName: "sem-1",
					Size:          100,
					BucketSize:    DefaultSemaphoreBucketSize,
					CreatedTime:   fixedTime,
				}).Return(nil).Times(1)
			},
			wantResult: &SemaphoreMetadata{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
				BucketSize:    DefaultSemaphoreBucketSize,
				CreatedTime:   fixedTime,
			},
		},
		{
			name: "missing domain id",
			request: &CreateSemaphoreRequest{
				SemaphoreName: "sem-1",
				Size:          100,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {},
			wantErr:   true,
		},
		{
			name: "missing semaphore name",
			request: &CreateSemaphoreRequest{
				DomainID: "domain-1",
				Size:     100,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {},
			wantErr:   true,
		},
		{
			name: "non-positive size",
			request: &CreateSemaphoreRequest{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          0,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {},
			wantErr:   true,
		},
		{
			name: "negative bucket size",
			request: &CreateSemaphoreRequest{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
				BucketSize:    -1,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {},
			wantErr:   true,
		},
		{
			name: "store error is propagated",
			request: &CreateSemaphoreRequest{
				DomainID:      "domain-1",
				SemaphoreName: "sem-1",
				Size:          100,
			},
			setupMock: func(store *MockSemaphoreMetadataStore) {
				store.EXPECT().CreateSemaphore(gomock.Any(), gomock.Any()).Return(errors.New("store failed")).Times(1)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockSemaphoreMetadataStore(ctrl)
			tc.setupMock(store)

			m := newTestSemaphoreMetadataManager(store, clock.NewMockedTimeSourceAt(fixedTime), t)

			resp, err := m.CreateSemaphore(context.Background(), tc.request)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantResult, resp.Semaphore)
		})
	}
}

func TestSemaphoreManagerGetSemaphore(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSemaphoreMetadataStore(ctrl)

	want := &SemaphoreMetadata{DomainID: "domain-1", SemaphoreName: "sem-1", Size: 100, BucketSize: 10}
	req := &GetSemaphoreRequest{DomainID: "domain-1", SemaphoreName: "sem-1"}
	store.EXPECT().GetSemaphore(gomock.Any(), req).Return(want, nil).Times(1)

	m := newTestSemaphoreMetadataManager(store, clock.NewMockedTimeSource(), t)
	resp, err := m.GetSemaphore(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, want, resp.Semaphore)
}

func TestSemaphoreManagerGetSemaphoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSemaphoreMetadataStore(ctrl)

	req := &GetSemaphoreRequest{DomainID: "domain-1", SemaphoreName: "sem-1"}
	store.EXPECT().GetSemaphore(gomock.Any(), req).Return(nil, errors.New("not found")).Times(1)

	m := newTestSemaphoreMetadataManager(store, clock.NewMockedTimeSource(), t)
	resp, err := m.GetSemaphore(context.Background(), req)
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestSemaphoreManagerListSemaphores(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSemaphoreMetadataStore(ctrl)

	req := &ListSemaphoresRequest{DomainID: "domain-1", PageSize: 10}
	want := &ListSemaphoresResponse{
		Semaphores: []*SemaphoreMetadata{
			{DomainID: "domain-1", SemaphoreName: "sem-1"},
		},
		NextPageToken: []byte("token"),
	}
	store.EXPECT().ListSemaphores(gomock.Any(), req).Return(want, nil).Times(1)

	m := newTestSemaphoreMetadataManager(store, clock.NewMockedTimeSource(), t)
	resp, err := m.ListSemaphores(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, want, resp)
}

func TestSemaphoreManagerGetNameAndClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSemaphoreMetadataStore(ctrl)

	store.EXPECT().GetName().Return("cassandra").Times(1)
	store.EXPECT().Close().Times(1)

	m := newTestSemaphoreMetadataManager(store, clock.NewMockedTimeSource(), t)
	assert.Equal(t, "cassandra", m.GetName())
	m.Close()
}
