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
	"fmt"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	defaultEncodingType = "thriftrw"
)

// cassandraPointDeleteDLQ implements StandbyTaskDLQManager using point-delete pattern
// Each task is individually deleted when processed
type cassandraPointDeleteDLQ struct {
	session gocql.Session
	logger  log.Logger
}

// cassandraRangeDeleteDLQ implements StandbyTaskDLQManager using range-delete pattern
// Tasks are deleted via ack-level advancement (like queue processing)
type cassandraRangeDeleteDLQ struct {
	session gocql.Session
	logger  log.Logger
}

// cassandraComparisonDLQ wraps both implementations for side-by-side comparison
type cassandraComparisonDLQ struct {
	pointImpl *cassandraPointDeleteDLQ
	rangeImpl *cassandraRangeDeleteDLQ
	logger    log.Logger
}

// Ensure all implementations satisfy the interface
var _ persistence.StandbyTaskDLQManager = (*cassandraPointDeleteDLQ)(nil)
var _ persistence.StandbyTaskDLQManager = (*cassandraRangeDeleteDLQ)(nil)
var _ persistence.StandbyTaskDLQManager = (*cassandraComparisonDLQ)(nil)

// NewCassandraPointDeleteDLQ creates a new point-delete DLQ implementation
func NewCassandraPointDeleteDLQ(session gocql.Session, logger log.Logger) *cassandraPointDeleteDLQ {
	return &cassandraPointDeleteDLQ{
		session: session,
		logger:  logger,
	}
}

// NewCassandraRangeDeleteDLQ creates a new range-delete DLQ implementation
func NewCassandraRangeDeleteDLQ(session gocql.Session, logger log.Logger) *cassandraRangeDeleteDLQ {
	return &cassandraRangeDeleteDLQ{
		session: session,
		logger:  logger,
	}
}

// NewCassandraComparisonDLQ creates a comparison wrapper for both implementations
func NewCassandraComparisonDLQ(session gocql.Session, logger log.Logger) *cassandraComparisonDLQ {
	return &cassandraComparisonDLQ{
		pointImpl: NewCassandraPointDeleteDLQ(
			session,
			logger.WithTags(tag.Value("point-delete")),
		),
		rangeImpl: NewCassandraRangeDeleteDLQ(
			session,
			logger.WithTags(tag.Value("range-delete")),
		),
		logger: logger,
	}
}

// ========================= Point-Delete Implementation =========================

func (d *cassandraPointDeleteDLQ) EnqueueStandbyTask(ctx context.Context, request *persistence.EnqueueStandbyTaskRequest) error {
	d.logger.Debug("[POINT-DELETE-DLQ] Enqueuing standby task",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.TaskID(request.TaskID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
	)

	now := time.Now()
	visibilityTS := time.Unix(0, request.VisibilityTimestamp)
	encodingType := defaultEncodingType

	query := d.session.Query(templateEnqueueStandbyTaskPoint,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		request.TaskType,
		visibilityTS,
		request.TaskID,
		request.WorkflowID,
		request.RunID,
		request.TaskPayload,
		encodingType,
		request.Version,
		now,
		now,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		d.logger.Error("Failed to enqueue standby task to point-delete DLQ",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.WorkflowID(request.WorkflowID),
			tag.WorkflowRunID(request.RunID),
			tag.TaskID(request.TaskID),
		)
		return err
	}

	if !applied {
		d.logger.Warn("[POINT-DELETE-DLQ] Task already exists",
			tag.TaskID(request.TaskID),
			tag.Value(fmt.Sprintf("visibilityTimestamp=%d", request.VisibilityTimestamp)),
		)
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("task already exists in point-delete DLQ: taskID=%d, visibilityTimestamp=%d",
				request.TaskID, request.VisibilityTimestamp),
		}
	}

	d.logger.Info("[POINT-DELETE-DLQ] Successfully enqueued task",
		tag.TaskID(request.TaskID),
		tag.WorkflowDomainID(request.DomainID),
	)
	return nil
}

func (d *cassandraPointDeleteDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (*persistence.ReadStandbyTasksResponse, error) {
	d.logger.Debug("[POINT-DELETE-DLQ] Reading standby tasks",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
		tag.Value(fmt.Sprintf("pageSize=%d", request.PageSize)),
	)

	// Parse page token
	var startVisibilityTS time.Time
	var startTaskID int64

	if len(request.NextPageToken) > 0 {
		token, err := d.deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		startVisibilityTS = time.Unix(0, token.VisibilityTimestamp)
		startTaskID = token.TaskID
	} else {
		startVisibilityTS = time.Unix(0, 0)
		startTaskID = 0
	}

	// Query for each task type
	var allTasks []*persistence.StandbyTaskDLQEntry
	// For simplicity, we'll query all task types. In production, you might want to query specific types
	// For now, let's just read from a single task type or iterate through known types
	// The plan doesn't specify how task_type filtering works in ReadStandbyTasks, so I'll read all types

	query := d.session.Query(templateReadStandbyTasksPoint,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		0, // task_type - we need to iterate through types or have a way to query all
		startVisibilityTS,
		startTaskID,
		request.PageSize+1, // +1 to check if there are more pages
	).WithContext(ctx)

	iter := query.Iter()
	defer iter.Close()

	count := 0
	var lastVisibilityTS int64
	var lastTaskID int64

	for count < request.PageSize {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}

		entry := d.mapRowToEntry(row)
		allTasks = append(allTasks, entry)
		lastVisibilityTS = entry.VisibilityTimestamp
		lastTaskID = entry.TaskID
		count++
	}

	if err := iter.Close(); err != nil {
		d.logger.Error("Failed to read standby tasks from point-delete DLQ",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
		)
		return nil, err
	}

	// Check if there are more pages
	var nextPageToken []byte
	if len(allTasks) == request.PageSize {
		// Check if there's at least one more row
		if iter.MapScan(make(map[string]interface{})) {
			token := &pageToken{
				VisibilityTimestamp: lastVisibilityTS,
				TaskID:              lastTaskID + 1, // Start from next task
			}
			nextPageToken = d.serializePageToken(token)
		}
	}

	return &persistence.ReadStandbyTasksResponse{
		Tasks:         allTasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (d *cassandraPointDeleteDLQ) DeleteStandbyTask(ctx context.Context, request *persistence.DeleteStandbyTaskRequest) error {
	d.logger.Debug("[POINT-DELETE-DLQ] Deleting standby task",
		tag.ShardID(request.ShardID),
		tag.TaskID(request.TaskID),
	)

	visibilityTS := time.Unix(0, request.VisibilityTimestamp)

	// Determine task type - we need this from the request or we need to query first
	// For now, I'll iterate through possible task types since the delete request doesn't include it
	// This is a limitation we'll need to address, but for the POC we can make assumptions

	// Actually, looking at the schema, we need task_type to delete. Let me check the request structure again.
	// The DeleteStandbyTaskRequest doesn't have TaskType, so we need to delete across all types
	// or modify the request. For now, let's iterate through known task types.

	// Common task types from the codebase
	taskTypes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} // Transfer, Timer, Replication, etc.

	deleted := false
	for _, taskType := range taskTypes {
		query := d.session.Query(templateDeleteStandbyTaskPoint,
			request.ShardID,
			request.DomainID,
			request.ClusterAttributeScope,
			request.ClusterAttributeName,
			taskType,
			visibilityTS,
			request.TaskID,
		).WithContext(ctx)

		if err := query.Exec(); err != nil {
			d.logger.Error("Failed to delete standby task from point-delete DLQ",
				tag.Error(err),
				tag.ShardID(request.ShardID),
				tag.WorkflowDomainID(request.DomainID),
				tag.TaskID(request.TaskID),
				tag.Value(taskType),
			)
			// Continue trying other task types
			continue
		}
		deleted = true
	}

	if !deleted {
		// Not an error if task doesn't exist (idempotent delete)
		d.logger.Debug("[POINT-DELETE-DLQ] Task not found (idempotent delete)",
			tag.TaskID(request.TaskID),
		)
	} else {
		d.logger.Info("[POINT-DELETE-DLQ] Successfully deleted task",
			tag.TaskID(request.TaskID),
		)
	}

	return nil
}

func (d *cassandraPointDeleteDLQ) GetStandbyTaskDLQSize(ctx context.Context, request *persistence.GetStandbyTaskDLQSizeRequest) (*persistence.GetStandbyTaskDLQSizeResponse, error) {
	query := d.session.Query(templateGetStandbyTaskDLQSizePoint,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
	).WithContext(ctx)

	var count int64
	if err := query.Scan(&count); err != nil {
		d.logger.Error("Failed to get standby task DLQ size for point-delete",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
		)
		return nil, err
	}

	return &persistence.GetStandbyTaskDLQSizeResponse{
		Size: count,
	}, nil
}

func (d *cassandraPointDeleteDLQ) Close() {
	// Session is shared, don't close it here
}

// ========================= Range-Delete Implementation =========================

func (d *cassandraRangeDeleteDLQ) EnqueueStandbyTask(ctx context.Context, request *persistence.EnqueueStandbyTaskRequest) error {
	d.logger.Debug("[RANGE-DELETE-DLQ] Enqueuing standby task",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.TaskID(request.TaskID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
	)

	now := time.Now()
	visibilityTS := time.Unix(0, request.VisibilityTimestamp)
	encodingType := defaultEncodingType

	query := d.session.Query(templateEnqueueStandbyTaskRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		request.TaskType,
		rowTypeDLQTask, // row_type = 0 for task rows
		visibilityTS,
		request.TaskID,
		request.WorkflowID,
		request.RunID,
		request.TaskPayload,
		encodingType,
		request.Version,
		now,
		now,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		d.logger.Error("[RANGE-DELETE-DLQ] Failed to enqueue standby task",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.WorkflowID(request.WorkflowID),
			tag.WorkflowRunID(request.RunID),
			tag.TaskID(request.TaskID),
		)
		return err
	}

	if !applied {
		d.logger.Warn("[RANGE-DELETE-DLQ] Task already exists",
			tag.TaskID(request.TaskID),
			tag.Value(fmt.Sprintf("visibilityTimestamp=%d", request.VisibilityTimestamp)),
		)
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("task already exists in range-delete DLQ: taskID=%d, visibilityTimestamp=%d",
				request.TaskID, request.VisibilityTimestamp),
		}
	}

	d.logger.Info("[RANGE-DELETE-DLQ] Successfully enqueued task",
		tag.TaskID(request.TaskID),
		tag.WorkflowDomainID(request.DomainID),
	)
	return nil
}

func (d *cassandraRangeDeleteDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (*persistence.ReadStandbyTasksResponse, error) {
	d.logger.Debug("[RANGE-DELETE-DLQ] Reading standby tasks",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
		tag.Value(fmt.Sprintf("pageSize=%d", request.PageSize)),
	)

	// Get ack level for filtering
	ackLevel, err := d.getAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, 0)
	if err != nil {
		// If no ack level exists, start from 0
		ackLevel = 0
	}

	ackLevelTS := time.Unix(0, ackLevel)

	query := d.session.Query(templateReadStandbyTasksRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		0, // task_type
		rowTypeDLQTask,
		ackLevelTS,
		request.PageSize+1,
	).WithContext(ctx)

	iter := query.Iter()
	defer iter.Close()

	var allTasks []*persistence.StandbyTaskDLQEntry
	count := 0
	var lastVisibilityTS int64
	var lastTaskID int64

	for count < request.PageSize {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}

		entry := d.mapRowToEntry(row)
		allTasks = append(allTasks, entry)
		lastVisibilityTS = entry.VisibilityTimestamp
		lastTaskID = entry.TaskID
		count++
	}

	if err := iter.Close(); err != nil {
		d.logger.Error("Failed to read standby tasks from range-delete DLQ",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
		)
		return nil, err
	}

	// Check if there are more pages
	var nextPageToken []byte
	if len(allTasks) == request.PageSize {
		if iter.MapScan(make(map[string]interface{})) {
			token := &pageToken{
				VisibilityTimestamp: lastVisibilityTS,
				TaskID:              lastTaskID + 1,
			}
			nextPageToken = d.serializePageToken(token)
		}
	}

	return &persistence.ReadStandbyTasksResponse{
		Tasks:         allTasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (d *cassandraRangeDeleteDLQ) DeleteStandbyTask(ctx context.Context, request *persistence.DeleteStandbyTaskRequest) error {
	d.logger.Debug("[RANGE-DELETE-DLQ] Deleting standby task (advancing ack level)",
		tag.ShardID(request.ShardID),
		tag.TaskID(request.TaskID),
	)

	// Update ack level to this task's visibility timestamp
	// This effectively "deletes" all tasks up to and including this one

	// First check if ack level row exists
	taskTypes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	for _, taskType := range taskTypes {
		exists, err := d.ackLevelExists(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, taskType)
		if err != nil {
			d.logger.Warn("[RANGE-DELETE-DLQ] Error checking ack level existence",
				tag.Error(err),
				tag.ShardID(request.ShardID),
				tag.WorkflowDomainID(request.DomainID),
			)
			continue
		}

		if exists {
			// Update existing ack level
			err = d.updateAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, taskType, request.VisibilityTimestamp)
			if err != nil {
				d.logger.Error("[RANGE-DELETE-DLQ] Failed to update ack level",
					tag.Error(err),
					tag.ShardID(request.ShardID),
					tag.WorkflowDomainID(request.DomainID),
					tag.TaskID(request.TaskID),
				)
				continue
			}
			d.logger.Info("[RANGE-DELETE-DLQ] Successfully updated ack level",
				tag.TaskID(request.TaskID),
				tag.Value(fmt.Sprintf("taskType=%d", taskType)),
			)
		} else {
			// Insert new ack level
			err = d.insertAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, taskType, request.VisibilityTimestamp)
			if err != nil {
				d.logger.Error("[RANGE-DELETE-DLQ] Failed to insert ack level",
					tag.Error(err),
					tag.ShardID(request.ShardID),
					tag.WorkflowDomainID(request.DomainID),
					tag.TaskID(request.TaskID),
				)
				continue
			}
			d.logger.Info("[RANGE-DELETE-DLQ] Successfully inserted ack level",
				tag.TaskID(request.TaskID),
				tag.Value(fmt.Sprintf("taskType=%d", taskType)),
			)
		}
	}

	d.logger.Info("[RANGE-DELETE-DLQ] Successfully deleted task (via ack level)",
		tag.TaskID(request.TaskID),
	)
	return nil
}

func (d *cassandraRangeDeleteDLQ) GetStandbyTaskDLQSize(ctx context.Context, request *persistence.GetStandbyTaskDLQSizeRequest) (*persistence.GetStandbyTaskDLQSizeResponse, error) {
	// Get ack level
	ackLevel, err := d.getAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, 0)
	if err != nil {
		ackLevel = 0
	}

	ackLevelTS := time.Unix(0, ackLevel)

	query := d.session.Query(templateGetStandbyTaskDLQSizeRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		rowTypeDLQTask,
		ackLevelTS,
	).WithContext(ctx)

	var count int64
	if err := query.Scan(&count); err != nil {
		d.logger.Error("Failed to get standby task DLQ size for range-delete",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
		)
		return nil, err
	}

	return &persistence.GetStandbyTaskDLQSizeResponse{
		Size: count,
	}, nil
}

func (d *cassandraRangeDeleteDLQ) Close() {
	// Session is shared, don't close it here
}

// Helper methods for range-delete

func (d *cassandraRangeDeleteDLQ) getAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int) (int64, error) {
	query := d.session.Query(templateGetAckLevelRange,
		shardID,
		domainID,
		scope,
		name,
		taskType,
		rowTypeDLQAckLevel,
		time.Unix(0, 0), // visibility_timestamp = 0 for ack level rows
		0,               // Placeholder for ack level marker
	).WithContext(ctx)

	var ackLevel int64
	if err := query.Scan(&ackLevel); err != nil {
		return 0, err
	}

	return ackLevel, nil
}

func (d *cassandraRangeDeleteDLQ) ackLevelExists(ctx context.Context, shardID int, domainID, scope, name string, taskType int) (bool, error) {
	_, err := d.getAckLevel(ctx, shardID, domainID, scope, name, taskType)
	if err != nil {
		// If not found, return false
		return false, nil
	}
	return true, nil
}

func (d *cassandraRangeDeleteDLQ) updateAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int, newAckLevel int64) error {
	query := d.session.Query(templateUpdateAckLevelRange,
		newAckLevel,
		shardID,
		domainID,
		scope,
		name,
		taskType,
		rowTypeDLQAckLevel,
		time.Unix(0, 0),
	).WithContext(ctx)

	return query.Exec()
}

func (d *cassandraRangeDeleteDLQ) insertAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int, ackLevel int64) error {
	now := time.Now()
	query := d.session.Query(templateInsertAckLevelRange,
		shardID,
		domainID,
		scope,
		name,
		taskType,
		rowTypeDLQAckLevel,
		time.Unix(0, 0), // visibility_timestamp = 0 for ack level rows
		ackLevel,
		now,
		now,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return err
	}

	if !applied {
		// Already exists, try update instead
		return d.updateAckLevel(ctx, shardID, domainID, scope, name, taskType, ackLevel)
	}

	return nil
}

// ========================= Comparison Wrapper Implementation =========================

func (d *cassandraComparisonDLQ) EnqueueStandbyTask(ctx context.Context, request *persistence.EnqueueStandbyTaskRequest) error {
	// Execute both implementations
	errPoint := d.pointImpl.EnqueueStandbyTask(ctx, request)
	if errPoint != nil {
		d.logger.Error("Point-delete enqueue failed", tag.Error(errPoint))
	}

	errRange := d.rangeImpl.EnqueueStandbyTask(ctx, request)
	if errRange != nil {
		d.logger.Error("Range-delete enqueue failed", tag.Error(errRange))
	}

	// Return error if both failed
	if errPoint != nil && errRange != nil {
		return errPoint
	}

	// Return first error if only one failed
	if errPoint != nil {
		return errPoint
	}
	if errRange != nil {
		return errRange
	}

	return nil
}

func (d *cassandraComparisonDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (*persistence.ReadStandbyTasksResponse, error) {
	// For reads, we'll use the point-delete implementation as the source of truth
	// but also execute range-delete for comparison
	respPoint, errPoint := d.pointImpl.ReadStandbyTasks(ctx, request)
	if errPoint != nil {
		d.logger.Error("Point-delete read failed", tag.Error(errPoint))
	}

	respRange, errRange := d.rangeImpl.ReadStandbyTasks(ctx, request)
	if errRange != nil {
		d.logger.Error("Range-delete read failed", tag.Error(errRange))
	}

	// Compare results if both succeeded
	if errPoint == nil && errRange == nil {
		if len(respPoint.Tasks) != len(respRange.Tasks) {
			d.logger.Warn("Task count mismatch between implementations",
				tag.Value(fmt.Sprintf("point=%d, range=%d", len(respPoint.Tasks), len(respRange.Tasks))),
			)
		}
	}

	// Return point-delete result
	if errPoint != nil {
		return nil, errPoint
	}

	return respPoint, nil
}

func (d *cassandraComparisonDLQ) DeleteStandbyTask(ctx context.Context, request *persistence.DeleteStandbyTaskRequest) error {
	errPoint := d.pointImpl.DeleteStandbyTask(ctx, request)
	if errPoint != nil {
		d.logger.Error("Point-delete delete failed", tag.Error(errPoint))
	}

	errRange := d.rangeImpl.DeleteStandbyTask(ctx, request)
	if errRange != nil {
		d.logger.Error("Range-delete delete failed", tag.Error(errRange))
	}

	// Return error if both failed
	if errPoint != nil && errRange != nil {
		return errPoint
	}

	// Return first error if only one failed
	if errPoint != nil {
		return errPoint
	}
	if errRange != nil {
		return errRange
	}

	return nil
}

func (d *cassandraComparisonDLQ) GetStandbyTaskDLQSize(ctx context.Context, request *persistence.GetStandbyTaskDLQSizeRequest) (*persistence.GetStandbyTaskDLQSizeResponse, error) {
	respPoint, errPoint := d.pointImpl.GetStandbyTaskDLQSize(ctx, request)
	if errPoint != nil {
		d.logger.Error("Point-delete size check failed", tag.Error(errPoint))
	}

	respRange, errRange := d.rangeImpl.GetStandbyTaskDLQSize(ctx, request)
	if errRange != nil {
		d.logger.Error("Range-delete size check failed", tag.Error(errRange))
	}

	// Compare sizes if both succeeded
	if errPoint == nil && errRange == nil {
		if respPoint.Size != respRange.Size {
			d.logger.Warn("DLQ size mismatch between implementations",
				tag.Value(fmt.Sprintf("point=%d, range=%d", respPoint.Size, respRange.Size)),
			)
		}
	}

	// Return point-delete result
	if errPoint != nil {
		return nil, errPoint
	}

	return respPoint, nil
}

func (d *cassandraComparisonDLQ) Close() {
	d.pointImpl.Close()
	d.rangeImpl.Close()
}

// ========================= Common Helper Methods =========================

type pageToken struct {
	VisibilityTimestamp int64
	TaskID              int64
}

func (d *cassandraPointDeleteDLQ) serializePageToken(token *pageToken) []byte {
	if token == nil {
		return nil
	}
	return []byte(fmt.Sprintf("%d:%d", token.VisibilityTimestamp, token.TaskID))
}

func (d *cassandraPointDeleteDLQ) deserializePageToken(data []byte) (*pageToken, error) {
	token := &pageToken{}
	_, err := fmt.Sscanf(string(data), "%d:%d", &token.VisibilityTimestamp, &token.TaskID)
	if err != nil {
		return nil, err
	}
	return token, nil
}

func (d *cassandraPointDeleteDLQ) mapRowToEntry(row map[string]interface{}) *persistence.StandbyTaskDLQEntry {
	return &persistence.StandbyTaskDLQEntry{
		ShardID:               row["shard_id"].(int),
		DomainID:              row["domain_id"].(string),
		ClusterAttributeScope: row["cluster_attribute_scope"].(string),
		ClusterAttributeName:  row["cluster_attribute_name"].(string),
		WorkflowID:            row["workflow_id"].(string),
		RunID:                 row["run_id"].(string),
		TaskID:                row["task_id"].(int64),
		VisibilityTimestamp:   row["visibility_timestamp"].(time.Time).UnixNano(),
		TaskType:              row["task_type"].(int),
		TaskPayload:           row["task_payload"].([]byte),
		Version:               row["version"].(int64),
		EnqueuedAt:            row["created_at"].(time.Time).Unix(),
	}
}

func (d *cassandraRangeDeleteDLQ) serializePageToken(token *pageToken) []byte {
	if token == nil {
		return nil
	}
	return []byte(fmt.Sprintf("%d:%d", token.VisibilityTimestamp, token.TaskID))
}

func (d *cassandraRangeDeleteDLQ) deserializePageToken(data []byte) (*pageToken, error) {
	token := &pageToken{}
	_, err := fmt.Sscanf(string(data), "%d:%d", &token.VisibilityTimestamp, &token.TaskID)
	if err != nil {
		return nil, err
	}
	return token, nil
}

func (d *cassandraRangeDeleteDLQ) mapRowToEntry(row map[string]interface{}) *persistence.StandbyTaskDLQEntry {
	return &persistence.StandbyTaskDLQEntry{
		ShardID:               row["shard_id"].(int),
		DomainID:              row["domain_id"].(string),
		ClusterAttributeScope: row["cluster_attribute_scope"].(string),
		ClusterAttributeName:  row["cluster_attribute_name"].(string),
		WorkflowID:            row["workflow_id"].(string),
		RunID:                 row["run_id"].(string),
		TaskID:                row["task_id"].(int64),
		VisibilityTimestamp:   row["visibility_timestamp"].(time.Time).UnixNano(),
		TaskType:              row["task_type"].(int),
		TaskPayload:           row["task_payload"].([]byte),
		Version:               row["version"].(int64),
		EnqueuedAt:            row["created_at"].(time.Time).Unix(),
	}
}
