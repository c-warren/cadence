package testdata

import (
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
)

func NewShardRow(ts time.Time) *nosqlplugin.ShardRow {
	return &nosqlplugin.ShardRow{
		InternalShardInfo: &persistence.InternalShardInfo{
			ShardID:                       15,
			Owner:                         "owner",
			RangeID:                       1000,
			ReplicationAckLevel:           2000,
			TransferAckLevel:              3000,
			TimerAckLevel:                 ts.Add(-time.Hour),
			ClusterTransferAckLevel:       map[string]int64{"cluster2": 4000},
			ClusterTimerAckLevel:          map[string]time.Time{"cluster2": ts.Add(-2 * time.Hour)},
			DomainNotificationVersion:     3,
			ClusterReplicationLevel:       map[string]int64{"cluster2": 5000},
			ReplicationDLQAckLevel:        map[string]int64{"cluster2": 10},
			PendingFailoverMarkers:        &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("failovermarkers")},
			TransferProcessingQueueStates: &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("transferqueue")},
			TimerProcessingQueueStates:    &persistence.DataBlob{Encoding: "thriftrw", Data: []byte("timerqueue")},
			CurrentTimestamp:              ts,
		},
		Data:         []byte("sharddata"),
		DataEncoding: "thriftrw",
	}
}

func NewShardMap(ts time.Time) map[string]interface{} {
	return map[string]interface{}{
		"shard_id":              int(15),
		"range_id":              int64(1000),
		"owner":                 "owner",
		"stolen_since_renew":    0,
		"updated_at":            ts,
		"replication_ack_level": int64(2000),
		"transfer_ack_level":    int64(3000),
		"timer_ack_level":       ts.Add(-1 * time.Hour),
		"cluster_transfer_ack_level": map[string]int64{
			"cluster1": int64(3000),
		},
		"cluster_timer_ack_level": map[string]time.Time{
			"cluster1": ts.Add(-1 * time.Hour),
		},
		"transfer_processing_queue_states":          []byte("transferqueue"),
		"transfer_processing_queue_states_encoding": "thriftrw",
		"timer_processing_queue_states":             []byte("timerqueue"),
		"timer_processing_queue_states_encoding":    "thriftrw",
		"domain_notification_version":               int64(3),
		"cluster_replication_level":                 map[string]int64{"cluster2": 1500},
		"replication_dlq_ack_level":                 map[string]int64{"cluster2": 5},
		"pending_failover_markers":                  []byte("failovermarkers"),
		"pending_failover_markers_encoding":         "thriftrw",
	}
}
