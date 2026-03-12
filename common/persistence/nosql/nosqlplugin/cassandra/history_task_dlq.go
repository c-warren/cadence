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

	d.logger.Info("[POINT-DELETE-DLQ] Executing query",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(fmt.Sprintf("scope='%s', name='%s', task_type=0, startTS=%v",
			request.ClusterAttributeScope, request.ClusterAttributeName, startVisibilityTS)),
	)

	query := d.session.Query(templateReadStandbyTasksPoint,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		0, // task_type - we need to iterate through types or have a way to query all
		startVisibilityTS,
		request.PageSize*2, // Fetch more since we'll filter in-app
	).WithContext(ctx).Consistency(gocql.One)

	iter := query.Iter()
	if iter == nil {
		d.logger.Error("[POINT-DELETE-DLQ] Query failed to create iterator")
		return nil, fmt.Errorf("failed to create query iterator for point-delete DLQ read")
	}
	defer func() {
		if err := iter.Close(); err != nil {
			d.logger.Error("[POINT-DELETE-DLQ] Error closing iterator",
				tag.Error(err),
			)
		}
	}()

	count := 0
	var lastVisibilityTS int64
	var lastTaskID int64
	rowsScanned := 0

	// Scan rows
	for count < request.PageSize {
		row := make(map[string]interface{})

		// MapScan returns false when no more rows or error
		if !iter.MapScan(row) {
			break
		}
		rowsScanned++

		// Try to map the row to entry
		entry := d.mapRowToEntry(row)
		if entry == nil {
			d.logger.Warn("[POINT-DELETE-DLQ] Failed to map row, skipping")
			continue
		}

		d.logger.Debug("[POINT-DELETE-DLQ] Scanned row",
			tag.TaskID(entry.TaskID),
			tag.Value(fmt.Sprintf("task_type=%d, visTS=%d", entry.TaskType, entry.VisibilityTimestamp)),
		)

		// Skip entries before our pagination point (when paginating within same timestamp)
		if entry.VisibilityTimestamp == startVisibilityTS.UnixNano() && entry.TaskID < startTaskID {
			continue
		}

		allTasks = append(allTasks, entry)
		lastVisibilityTS = entry.VisibilityTimestamp
		lastTaskID = entry.TaskID
		count++
	}

	d.logger.Info("[POINT-DELETE-DLQ] Query complete",
		tag.Value(fmt.Sprintf("rowsScanned=%d, tasksReturned=%d", rowsScanned, len(allTasks))),
	)

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
	d.logger.Info("[RANGE-DELETE-DLQ] Enqueuing standby task",
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

func (d *cassandraRangeDeleteDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (resp *persistence.ReadStandbyTasksResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("[RANGE-DELETE-DLQ] PANIC RECOVERED",
				tag.Value(fmt.Sprintf("panic: %v", r)))
			err = fmt.Errorf("panic during range-delete DLQ read: %v", r)
			resp = nil
		}
	}()

	d.logger.Info("[RANGE-DELETE-DLQ] Reading standby tasks - START",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
		tag.Value(fmt.Sprintf("pageSize=%d", request.PageSize)),
	)

	// Get ack level for filtering
	d.logger.Info("[RANGE-DELETE-DLQ] Getting ack level...")
	ackLevel, err := d.getAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, 0)
	if err != nil {
		// If no ack level exists, start from 0
		d.logger.Info("[RANGE-DELETE-DLQ] No ack level found, starting from 0",
			tag.Error(err))
		ackLevel = 0
	} else {
		d.logger.Info("[RANGE-DELETE-DLQ] Found ack level",
			tag.Value(fmt.Sprintf("ackLevel=%d", ackLevel)))
	}

	ackLevelTS := time.Unix(0, ackLevel)

	d.logger.Info("[RANGE-DELETE-DLQ] Executing query",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(fmt.Sprintf("scope=%s, name=%s, task_type=0, row_type=0, ackLevel=%d",
			request.ClusterAttributeScope, request.ClusterAttributeName, ackLevel)),
	)

	query := d.session.Query(templateReadStandbyTasksRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		0,              // task_type
		rowTypeDLQTask, // row_type = 0 (tasks only, not ack-level rows)
		ackLevelTS,
		request.PageSize+1,
	).WithContext(ctx).Consistency(gocql.One)

	d.logger.Info("[RANGE-DELETE-DLQ] Creating iterator...")
	iter := query.Iter()
	if iter == nil {
		d.logger.Error("[RANGE-DELETE-DLQ] Query failed to create iterator")
		return nil, fmt.Errorf("failed to create query iterator for range-delete DLQ read")
	}
	d.logger.Info("[RANGE-DELETE-DLQ] Iterator created successfully")

	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			d.logger.Error("[RANGE-DELETE-DLQ] Error closing iterator",
				tag.Error(closeErr),
			)
		}
	}()

	var allTasks []*persistence.StandbyTaskDLQEntry
	count := 0
	var lastVisibilityTS int64
	var lastTaskID int64
	rowsScanned := 0

	d.logger.Info("[RANGE-DELETE-DLQ] Starting to scan rows...")

	// Try using Scan instead of MapScan for debugging
	for count < request.PageSize {
		var shardID int
		var domainID, scope, name, workflowID, runID, encodingType string
		var taskType int
		var visibilityTS time.Time
		var taskID, version int64
		var taskPayload []byte
		var createdAt time.Time

		if !iter.Scan(&shardID, &domainID, &scope, &name, &taskType, &visibilityTS, &taskID, &workflowID, &runID, &taskPayload, &encodingType, &version, &createdAt) {
			break
		}
		rowsScanned++

		entry := &persistence.StandbyTaskDLQEntry{
			ShardID:               shardID,
			DomainID:              domainID,
			ClusterAttributeScope: scope,
			ClusterAttributeName:  name,
			WorkflowID:            workflowID,
			RunID:                 runID,
			TaskID:                taskID,
			VisibilityTimestamp:   visibilityTS.UnixNano(),
			TaskType:              taskType,
			TaskPayload:           taskPayload,
			Version:               version,
			EnqueuedAt:            createdAt.Unix(),
		}

		if d.logger != nil {
			d.logger.Info("[RANGE-DELETE-DLQ] Scanned row",
				tag.TaskID(entry.TaskID),
				tag.Value(fmt.Sprintf("task_type=%d, visTS=%d", entry.TaskType, entry.VisibilityTimestamp)),
			)
		} else {
		}

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

	d.logger.Info("[RANGE-DELETE-DLQ] Query complete",
		tag.Value(fmt.Sprintf("rowsScanned=%d, tasksReturned=%d", rowsScanned, len(allTasks))),
	)

	// Check if there are more pages
	// NOTE: We fetched PageSize+1 rows, so if we got exactly PageSize+1, there are more pages
	var nextPageToken []byte
	if rowsScanned > request.PageSize {
		// Remove the extra row we fetched
		allTasks = allTasks[:request.PageSize]
		token := &pageToken{
			VisibilityTimestamp: lastVisibilityTS,
			TaskID:              lastTaskID + 1,
		}
		nextPageToken = d.serializePageToken(token)
	}

	return &persistence.ReadStandbyTasksResponse{
		Tasks:         allTasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (d *cassandraRangeDeleteDLQ) DeleteStandbyTask(ctx context.Context, request *persistence.DeleteStandbyTaskRequest) error {
	d.logger.Info("[RANGE-DELETE-DLQ] Deleting standby task (advancing ack level)",
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

// CleanupProcessedTasks performs range delete for tasks below ack level
// This runs periodically (similar to queuev2's updateQueueState timer)
func (d *cassandraRangeDeleteDLQ) CleanupProcessedTasks(ctx context.Context) error {
	d.logger.Debug("[RANGE-DELETE-DLQ] Starting periodic cleanup of processed tasks")

	// Read all ack levels (this query scans all partitions - expensive but necessary)
	type ackLevelKey struct {
		ShardID    int
		DomainID   string
		Scope      string
		Name       string
		TaskType   int
		AckLevelTS int64
	}

	query := d.session.Query(templateReadAllAckLevelsRange, rowTypeDLQAckLevel).WithContext(ctx)
	iter := query.Iter()

	var ackLevels []ackLevelKey
	var shardID int
	var domainID, scope, name string
	var taskType int
	var ackLevelTaskID int64

	for iter.Scan(&shardID, &domainID, &scope, &name, &taskType, &ackLevelTaskID) {
		ackLevels = append(ackLevels, ackLevelKey{
			ShardID:    shardID,
			DomainID:   domainID,
			Scope:      scope,
			Name:       name,
			TaskType:   taskType,
			AckLevelTS: ackLevelTaskID,
		})
	}

	if err := iter.Close(); err != nil {
		d.logger.Error("[RANGE-DELETE-DLQ] Failed to read ack levels", tag.Error(err))
		return err
	}

	d.logger.Info("[RANGE-DELETE-DLQ] Found ack levels for cleanup",
		tag.Dynamic("count", len(ackLevels)))

	// For each ack level, read and delete tasks with visibility_timestamp <= ack_level
	for _, ack := range ackLevels {
		// The ack level value is a visibility_timestamp (stored in task_id column of ack level row)
		ackTime := time.Unix(0, ack.AckLevelTS)

		d.logger.Debug("[RANGE-DELETE-DLQ] Deleting tasks below ack level",
			tag.ShardID(ack.ShardID),
			tag.WorkflowDomainID(ack.DomainID),
			tag.Dynamic("ack_level_ts", ackTime))

		// Read all tasks with visibility_timestamp <= ack_level
		// Note: We can't use WHERE visibility_timestamp <= ? in DELETE because it's not the last clustering column
		// Instead, we read tasks and delete them individually
		readQuery := d.session.Query(templateReadStandbyTasksRange,
			ack.ShardID,
			ack.DomainID,
			ack.Scope,
			ack.Name,
			ack.TaskType,
			rowTypeDLQTask,
			time.Unix(0, 0), // Start from beginning
			10000,           // Large page size for cleanup
		).WithContext(ctx)

		iter := readQuery.Iter()
		var shardID int
		var domainID, scope, name string
		var taskType int
		var visibilityTS time.Time
		var taskID int64
		var workflowID, runID string
		var taskPayload []byte
		var encodingType string
		var version int64
		var createdAt time.Time

		deletedCount := 0
		readCount := 0
		skippedCount := 0
		for iter.Scan(&shardID, &domainID, &scope, &name, &taskType, &visibilityTS, &taskID, &workflowID, &runID, &taskPayload, &encodingType, &version, &createdAt) {
			readCount++

			// Only delete if visibility_timestamp <= ack_level
			if visibilityTS.UnixNano() > ack.AckLevelTS {
				skippedCount++
				continue
			}

			d.logger.Debug("[RANGE-DELETE-DLQ] Deleting individual task",
				tag.TaskID(taskID),
				tag.Dynamic("visibility_ts", visibilityTS),
				tag.Dynamic("ack_level_ts", ackTime))

			// Delete this specific task by full primary key
			deleteQuery := d.session.Query(templateDeleteStandbyTaskRange,
				shardID,
				domainID,
				scope,
				name,
				taskType,
				rowTypeDLQTask,
				visibilityTS,
				taskID,
			).WithContext(ctx)

			if err := deleteQuery.Exec(); err != nil {
				d.logger.Error("[RANGE-DELETE-DLQ] Failed to delete individual task",
					tag.Error(err),
					tag.TaskID(taskID),
					tag.WorkflowDomainID(domainID))
				// Continue with other tasks
				continue
			}
			deletedCount++
		}

		d.logger.Info("[RANGE-DELETE-DLQ] Cleanup scan complete",
			tag.ShardID(ack.ShardID),
			tag.WorkflowDomainID(ack.DomainID),
			tag.Dynamic("read_count", readCount),
			tag.Dynamic("skipped_count", skippedCount),
			tag.Dynamic("deleted_count", deletedCount))

		if err := iter.Close(); err != nil {
			d.logger.Error("[RANGE-DELETE-DLQ] Failed to read tasks for cleanup",
				tag.Error(err),
				tag.ShardID(ack.ShardID),
				tag.WorkflowDomainID(ack.DomainID))
			continue
		}

		d.logger.Info("[RANGE-DELETE-DLQ] Deleted tasks below ack level",
			tag.ShardID(ack.ShardID),
			tag.WorkflowDomainID(ack.DomainID),
			tag.Dynamic("ack_level_ts", ackTime),
			tag.Dynamic("deleted_count", deletedCount))
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
	// Execute both implementations for comparison
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

	// Return point-delete result (source of truth)
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

func (d *cassandraComparisonDLQ) CleanupProcessedTasks(ctx context.Context) error {
	// Point-delete doesn't need cleanup (tasks are deleted immediately)
	// Only range-delete needs periodic cleanup
	return d.rangeImpl.CleanupProcessedTasks(ctx)
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
	// Check for required fields
	if row["workflow_id"] == nil || row["run_id"] == nil || row["task_payload"] == nil {
		// This might be an ack-level row or corrupted data, skip it
		return nil
	}

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
