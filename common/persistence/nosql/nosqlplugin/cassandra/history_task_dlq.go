// Copyright (c) 2025 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"context"
	"time"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"github.com/uber/cadence/common/types"
)

func (db *CDB) InsertHistoryDLQTask(ctx context.Context, shardID int, domainID, clusterAttributeScope, clusterAttributeName string, task *nosqlplugin.HistoryDLQTask) error {
	payload, encoding := persistence.FromDataBlob(task.TaskPayload)
	query := db.session.Query(templateInsertHistoryDLQTask,
		shardID,
		domainID,
		clusterAttributeScope,
		clusterAttributeName,
		task.TaskType,
		task.VisibilityTimestamp,
		task.TaskID,
		task.WorkflowID,
		task.RunID,
		payload,
		encoding,
		task.Version,
		task.CreatedAt,
	).WithContext(ctx)
	return query.Exec()
}

func (db *CDB) SelectHistoryDLQTasksOrderByKey(
	ctx context.Context,
	shardID int,
	domainID, clusterAttributeScope, clusterAttributeName string,
	taskType int,
	pageSize int,
	pageToken []byte,
	exclusiveMinVisibilityTS time.Time,
	exclusiveMinTaskID int64,
	inclusiveMaxVisibilityTS time.Time,
	inclusiveMaxTaskID int64,
) ([]*nosqlplugin.HistoryDLQTask, []byte, error) {
	query := db.session.Query(templateSelectHistoryDLQTasksOrderByKey,
		shardID,
		domainID,
		clusterAttributeScope,
		clusterAttributeName,
		taskType,
		exclusiveMinVisibilityTS,
		exclusiveMinTaskID,
		inclusiveMaxVisibilityTS,
		inclusiveMaxTaskID,
		pageSize,
	).PageSize(pageSize).PageState(pageToken).WithContext(ctx)

	iter := query.Iter()
	if iter == nil {
		return nil, nil, &types.InternalServiceError{
			Message: "SelectHistoryDLQTasksOrderByKey operation failed. Not able to create query iterator.",
		}
	}

	var tasks []*nosqlplugin.HistoryDLQTask
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		tasks = append(tasks, parseHistoryDLQTask(row))
		row = make(map[string]interface{})
	}
	nextPageToken := getNextPageToken(iter)
	err := iter.Close()
	return tasks, nextPageToken, err
}

func (db *CDB) RangeDeleteHistoryDLQTasksBefore(
	ctx context.Context,
	shardID int,
	domainID, clusterAttributeScope, clusterAttributeName string,
	taskType int,
	exclusiveMaxVisibilityTS time.Time,
) error {
	query := db.session.Query(templateRangeDeleteHistoryDLQTasksBefore,
		shardID,
		domainID,
		clusterAttributeScope,
		clusterAttributeName,
		taskType,
		exclusiveMaxVisibilityTS,
	).WithContext(ctx)
	return db.executeWithConsistencyAll(query)
}

func (db *CDB) RangeDeleteHistoryDLQTasksAtTS(
	ctx context.Context,
	shardID int,
	domainID, clusterAttributeScope, clusterAttributeName string,
	taskType int,
	visibilityTS time.Time,
	inclusiveMaxTaskID int64,
) error {
	query := db.session.Query(templateRangeDeleteHistoryDLQTasksAtTS,
		shardID,
		domainID,
		clusterAttributeScope,
		clusterAttributeName,
		taskType,
		visibilityTS,
		inclusiveMaxTaskID,
	).WithContext(ctx)
	return db.executeWithConsistencyAll(query)
}

// SelectHistoryDLQAckLevels returns ack level rows for the given shard.
// If domainID is non-empty, the query is restricted to that domain.
// If clusterAttributeScope and clusterAttributeName are also non-empty, it is
// further restricted to that cluster attribute.
// If all filter args are empty, all ack levels for the shard are returned.
func (db *CDB) SelectHistoryDLQAckLevels(
	ctx context.Context,
	shardID int,
	domainID, clusterAttributeScope, clusterAttributeName string,
) ([]*nosqlplugin.HistoryDLQAckLevelRow, error) {
	var iter gocql.Iter
	switch {
	case domainID != "" && clusterAttributeScope != "" && clusterAttributeName != "":
		iter = db.session.Query(templateSelectHistoryDLQAckLevelsByDomainAndClusterAttribute,
			shardID, domainID, clusterAttributeScope, clusterAttributeName,
		).WithContext(ctx).Iter()
	case domainID != "":
		iter = db.session.Query(templateSelectHistoryDLQAckLevelsByDomain,
			shardID, domainID,
		).WithContext(ctx).Iter()
	default:
		iter = db.session.Query(templateSelectHistoryDLQAckLevels,
			shardID,
		).WithContext(ctx).Iter()
	}

	if iter == nil {
		return nil, &types.InternalServiceError{
			Message: "SelectHistoryDLQAckLevels operation failed. Not able to create query iterator.",
		}
	}

	var rows []*nosqlplugin.HistoryDLQAckLevelRow
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		rows = append(rows, parseHistoryDLQAckLevelRow(shardID, row))
		row = make(map[string]interface{})
	}
	err := iter.Close()
	return rows, err
}

func (db *CDB) UpsertHistoryDLQAckLevel(ctx context.Context, r *nosqlplugin.HistoryDLQAckLevelRow) error {
	query := db.session.Query(templateUpsertHistoryDLQAckLevel,
		r.ShardID,
		r.DomainID,
		r.ClusterAttributeScope,
		r.ClusterAttributeName,
		r.TaskType,
		r.AckLevelVisibilityTS,
		r.AckLevelTaskID,
		r.LastUpdatedAt,
	).WithContext(ctx)
	return query.Exec()
}

func parseHistoryDLQTask(row map[string]interface{}) *nosqlplugin.HistoryDLQTask {
	payload, _ := row["task_payload"].([]byte)
	encoding, _ := row["encoding_type"].(string)
	return &nosqlplugin.HistoryDLQTask{
		TaskType:            row["task_type"].(int),
		VisibilityTimestamp: row["visibility_ts"].(time.Time),
		TaskID:              row["task_id"].(int64),
		DomainID:            row["domain_id"].(string),
		WorkflowID:          row["workflow_id"].(string),
		RunID:               row["run_id"].(string),
		TaskPayload:         persistence.NewDataBlob(payload, constants.EncodingType(encoding)),
		Version:             row["version"].(int64),
		CreatedAt:           row["created_at"].(time.Time),
	}
}

func parseHistoryDLQAckLevelRow(shardID int, row map[string]interface{}) *nosqlplugin.HistoryDLQAckLevelRow {
	return &nosqlplugin.HistoryDLQAckLevelRow{
		ShardID:               shardID,
		DomainID:              row["domain_id"].(string),
		ClusterAttributeScope: row["cluster_attribute_scope"].(string),
		ClusterAttributeName:  row["cluster_attribute_name"].(string),
		TaskType:              row["task_type"].(int),
		AckLevelVisibilityTS:  row["ack_level_visibility_ts"].(time.Time),
		AckLevelTaskID:        row["ack_level_task_id"].(int64),
		LastUpdatedAt:         row["last_updated_at"].(time.Time),
	}
}
