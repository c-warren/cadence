package testdata

import (
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"
)

func NewDomainRow(ts time.Time) *nosqlplugin.DomainRow {
	return &nosqlplugin.DomainRow{
		Info: &persistence.DomainInfo{
			ID:          "test-domain-id",
			Name:        "test-domain-name",
			Status:      persistence.DomainStatusRegistered,
			Description: "test-domain-description",
			OwnerEmail:  "test-domain-owner-email",
			Data:        map[string]string{"k1": "v1"},
		},
		Config: &persistence.InternalDomainConfig{
			Retention:                7 * 24 * time.Hour,
			EmitMetric:               true,
			ArchivalBucket:           "test-archival-bucket",
			ArchivalStatus:           types.ArchivalStatusEnabled,
			HistoryArchivalStatus:    types.ArchivalStatusEnabled,
			HistoryArchivalURI:       "test-history-archival-uri",
			VisibilityArchivalStatus: types.ArchivalStatusEnabled,
			VisibilityArchivalURI:    "test-visibility-archival-uri",
			BadBinaries:              &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("bad-binaries")},
			IsolationGroups:          &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("isolation-group")},
			AsyncWorkflowsConfig:     &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("async-workflows-config")},
		},
		ReplicationConfig: &persistence.InternalDomainReplicationConfig{
			ActiveClusterName: "test-active-cluster-name",
			Clusters: []*persistence.ClusterReplicationConfig{
				{
					ClusterName: "test-cluster-name",
				},
			},
			ActiveClustersConfig: &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("active-clusters-config")},
		},
		IsGlobalDomain:      true,
		ConfigVersion:       3,
		FailoverVersion:     4,
		FailoverEndTime:     &ts,
		LastUpdatedTime:     ts,
		NotificationVersion: 5,
		CurrentTimeStamp:    time.Date(2025, 1, 6, 15, 0, 0, 0, time.UTC),
	}
}
