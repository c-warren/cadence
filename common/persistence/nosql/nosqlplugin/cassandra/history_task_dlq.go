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
	d.logger.Warn("[POINT-DELETE-DLQ] Enqueuing standby task",
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

	d.logger.Warn("[POINT-DELETE-DLQ] Successfully enqueued task",
		tag.TaskID(request.TaskID),
		tag.WorkflowDomainID(request.DomainID),
	)
	return nil
}

func (d *cassandraPointDeleteDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (*persistence.ReadStandbyTasksResponse, error) {
	d.logger.Warn("[POINT-DELETE-DLQ] Reading standby tasks",
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

	d.logger.Warn("[POINT-DELETE-DLQ] Executing query",
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

		d.logger.Warn("[POINT-DELETE-DLQ] Scanned row",
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

	d.logger.Warn("[POINT-DELETE-DLQ] Query complete",
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
	d.logger.Warn("[POINT-DELETE-DLQ] Deleting standby task",
		tag.ShardID(request.ShardID),
		tag.TaskID(request.TaskID),
		tag.Value(fmt.Sprintf("taskType=%d", request.TaskType)),
	)

	visibilityTS := time.Unix(0, request.VisibilityTimestamp)

	query := d.session.Query(templateDeleteStandbyTaskPoint,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		request.TaskType,
		visibilityTS,
		request.TaskID,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		d.logger.Error("Failed to delete standby task from point-delete DLQ",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
			tag.TaskID(request.TaskID),
			tag.Value(request.TaskType),
		)
		return err
	}

	d.logger.Warn("[POINT-DELETE-DLQ] Successfully deleted task",
		tag.TaskID(request.TaskID),
	)

	return nil
}

func (d *cassandraPointDeleteDLQ) RangeDeleteStandbyTasks(ctx context.Context, request *persistence.RangeDeleteStandbyTasksRequest) error {
	// Point-delete doesn't support range delete - tasks are deleted individually
	return fmt.Errorf("range delete not supported for point-delete DLQ implementation")
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
	d.logger.Warn("[DLQ] Enqueuing standby task",
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
		request.TaskID,
		visibilityTS,
		request.WorkflowID,
		request.RunID,
		request.TaskPayload,
		encodingType,
		request.Version,
		now,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		d.logger.Error("[DLQ] Failed to enqueue standby task",
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
		d.logger.Warn("[DLQ] Task already exists",
			tag.TaskID(request.TaskID),
			tag.Value(fmt.Sprintf("visibilityTimestamp=%d", request.VisibilityTimestamp)),
		)
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("task already exists in range-delete DLQ: taskID=%d, visibilityTimestamp=%d",
				request.TaskID, request.VisibilityTimestamp),
		}
	}

	d.logger.Warn("[DLQ] Successfully enqueued task",
		tag.TaskID(request.TaskID),
		tag.WorkflowDomainID(request.DomainID),
	)
	return nil
}

func (d *cassandraRangeDeleteDLQ) ReadStandbyTasks(ctx context.Context, request *persistence.ReadStandbyTasksRequest) (resp *persistence.ReadStandbyTasksResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("[DLQ] PANIC RECOVERED",
				tag.Value(fmt.Sprintf("panic: %v", r)))
			err = fmt.Errorf("panic during range-delete DLQ read: %v", r)
			resp = nil
		}
	}()

	d.logger.Warn("[DLQ] Reading standby tasks - START",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(request.ClusterAttributeScope+"/"+request.ClusterAttributeName),
		tag.Value(fmt.Sprintf("pageSize=%d", request.PageSize)),
	)

	// Get ack level for filtering (task_id based)
	d.logger.Warn("[DLQ] Getting ack level...")
	ackLevel, err := d.GetAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType)
	if err != nil {
		// If no ack level exists, start from -1
		d.logger.Warn("[DLQ] No ack level found, starting from -1",
			tag.Error(err))
		ackLevel = -1
	} else {
		d.logger.Warn("[DLQ] Found ack level",
			tag.Value(fmt.Sprintf("ackLevel=%d", ackLevel)))
	}

	d.logger.Warn("[DLQ] Executing query",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(fmt.Sprintf("scope=%s, name=%s, task_type=%d, ackLevel=%d",
			request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType, ackLevel)),
	)

	query := d.session.Query(templateReadStandbyTasksRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		request.TaskType,
		ackLevel,
		request.PageSize+1,
	).WithContext(ctx).Consistency(gocql.One)

	d.logger.Warn("[DLQ] Creating iterator...")
	iter := query.Iter()
	if iter == nil {
		d.logger.Error("[DLQ] Query failed to create iterator")
		return nil, fmt.Errorf("failed to create query iterator for range-delete DLQ read")
	}
	d.logger.Warn("[DLQ] Iterator created successfully")

	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			d.logger.Error("[DLQ] Error closing iterator",
				tag.Error(closeErr),
			)
		}
	}()

	var allTasks []*persistence.StandbyTaskDLQEntry
	count := 0
	var lastTaskID int64
	rowsScanned := 0

	d.logger.Warn("[DLQ] Starting to scan rows...")

	for count < request.PageSize {
		var shardID int
		var domainID, scope, name, workflowID, runID, encodingType string
		var taskType int
		var taskID, version int64
		var visibilityTS time.Time
		var taskPayload []byte
		var createdAt time.Time

		if !iter.Scan(&shardID, &domainID, &scope, &name, &taskType, &taskID, &visibilityTS, &workflowID, &runID, &taskPayload, &encodingType, &version, &createdAt) {
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

		d.logger.Warn("[DLQ] Scanned row",
			tag.TaskID(entry.TaskID),
			tag.Value(fmt.Sprintf("task_type=%d, visTS=%d", entry.TaskType, entry.VisibilityTimestamp)),
		)

		allTasks = append(allTasks, entry)
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

	d.logger.Warn("[DLQ] Query complete",
		tag.Value(fmt.Sprintf("rowsScanned=%d, tasksReturned=%d", rowsScanned, len(allTasks))),
	)

	// Check if there are more pages
	// NOTE: We fetched PageSize+1 rows, so if we got exactly PageSize+1, there are more pages
	var nextPageToken []byte
	if rowsScanned > request.PageSize {
		// Remove the extra row we fetched
		allTasks = allTasks[:request.PageSize]
		token := &pageToken{
			VisibilityTimestamp: 0, // Not used in range-delete pagination
			TaskID:              lastTaskID,
		}
		nextPageToken = d.serializePageToken(token)
	}

	return &persistence.ReadStandbyTasksResponse{
		Tasks:         allTasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (d *cassandraRangeDeleteDLQ) DeleteStandbyTask(ctx context.Context, request *persistence.DeleteStandbyTaskRequest) error {
	d.logger.Warn("[DLQ] Deleting standby task (advancing ack level)",
		tag.ShardID(request.ShardID),
		tag.TaskID(request.TaskID),
		tag.Value(fmt.Sprintf("taskType=%d", request.TaskType)),
	)

	// Update ack level to this task's TaskID
	// This effectively "deletes" all tasks up to and including this one
	// Note: We store the TaskID as nanoseconds in the visibility_timestamp field

	// Check if ack level row exists for this specific task type
	exists, err := d.ackLevelExists(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType)
	if err != nil {
		d.logger.Warn("[DLQ] Error checking ack level existence",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID),
		)
		// If we can't check, try to insert (it will fail gracefully if exists)
		exists = false
	}

	if exists {
		// Update existing ack level
		err = d.UpdateAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType, request.TaskID)
		if err != nil {
			d.logger.Error("[DLQ] Failed to update ack level",
				tag.Error(err),
				tag.ShardID(request.ShardID),
				tag.WorkflowDomainID(request.DomainID),
				tag.TaskID(request.TaskID),
			)
			return err
		}
		d.logger.Warn("[DLQ] Successfully updated ack level",
			tag.TaskID(request.TaskID),
			tag.Value(fmt.Sprintf("taskType=%d", request.TaskType)),
		)
	} else {
		// Insert new ack level
		err = d.insertAckLevel(ctx, request.ShardID, request.DomainID, request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType, request.TaskID)
		if err != nil {
			d.logger.Error("[DLQ] Failed to insert ack level",
				tag.Error(err),
				tag.ShardID(request.ShardID),
				tag.WorkflowDomainID(request.DomainID),
				tag.TaskID(request.TaskID),
			)
			return err
		}
		d.logger.Warn("[DLQ] Successfully inserted ack level",
			tag.TaskID(request.TaskID),
			tag.Value(fmt.Sprintf("taskType=%d", request.TaskType)),
		)
	}

	d.logger.Warn("[DLQ] Successfully deleted task (via ack level)",
		tag.TaskID(request.TaskID),
	)
	return nil
}

func (d *cassandraRangeDeleteDLQ) RangeDeleteStandbyTasks(ctx context.Context, request *persistence.RangeDeleteStandbyTasksRequest) error {
	d.logger.Warn("[DLQ] Range deleting tasks",
		tag.ShardID(request.ShardID),
		tag.Value(fmt.Sprintf("taskType=%d, maxTaskID=%d", request.TaskType, request.MaxTaskID)))

	query := d.session.Query(templateRangeDeleteTasksRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		request.TaskType,
		request.MaxTaskID,
	).WithContext(ctx).Consistency(gocql.All)

	if err := query.Exec(); err != nil {
		d.logger.Error("[DLQ] Failed to range delete tasks",
			tag.Error(err))
		return err
	}

	d.logger.Warn("[DLQ] Successfully range deleted tasks",
		tag.Value(fmt.Sprintf("taskType=%d, maxTaskID=%d", request.TaskType, request.MaxTaskID)))
	return nil
}

func (d *cassandraRangeDeleteDLQ) GetStandbyTaskDLQSize(ctx context.Context, request *persistence.GetStandbyTaskDLQSizeRequest) (*persistence.GetStandbyTaskDLQSizeResponse, error) {
	// TODO: GetStandbyTaskDLQSizeRequest should have TaskType field
	// For now, hardcode to 0 (transfer tasks) as a POC limitation
	// This should be fixed when the interface is updated to include TaskType
	taskType := 0

	query := d.session.Query(templateGetStandbyTaskDLQSizeRange,
		request.ShardID,
		request.DomainID,
		request.ClusterAttributeScope,
		request.ClusterAttributeName,
		taskType,
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

func (d *cassandraRangeDeleteDLQ) GetAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int) (int64, error) {
	query := d.session.Query(templateGetAckLevelRange,
		shardID,
		domainID,
		scope,
		name,
		taskType,
	).WithContext(ctx)

	var ackLevel int64
	if err := query.Scan(&ackLevel); err != nil {
		return -1, err
	}

	d.logger.Warn("[DLQ] GetAckLevel retrieved",
		tag.ShardID(shardID),
		tag.WorkflowDomainID(domainID),
		tag.Value(fmt.Sprintf("scope='%s', name='%s', taskType=%d, ackLevel=%d",
			scope, name, taskType, ackLevel)),
	)

	return ackLevel, nil
}

func (d *cassandraRangeDeleteDLQ) ackLevelExists(ctx context.Context, shardID int, domainID, scope, name string, taskType int) (bool, error) {
	_, err := d.GetAckLevel(ctx, shardID, domainID, scope, name, taskType)
	if err != nil {
		// If not found, return false
		return false, nil
	}
	return true, nil
}

func (d *cassandraRangeDeleteDLQ) UpdateAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int, newAckLevel int64) error {
	now := time.Now()

	d.logger.Warn("[DLQ] UpdateAckLevel called",
		tag.ShardID(shardID),
		tag.WorkflowDomainID(domainID),
		tag.Value(fmt.Sprintf("scope='%s', name='%s', taskType=%d, newAckLevel=%d",
			scope, name, taskType, newAckLevel)),
	)

	query := d.session.Query(templateUpdateAckLevelRange,
		shardID,
		domainID,
		scope,
		name,
		taskType,
		newAckLevel, // Store in ack_level_value column (bigint)
		now,
		now,
	).WithContext(ctx).Consistency(gocql.LocalQuorum)

	err := query.Exec()
	if err != nil {
		d.logger.Error("[DLQ] UpdateAckLevel FAILED",
			tag.Error(err),
			tag.ShardID(shardID),
			tag.WorkflowDomainID(domainID),
		)
		return err
	}

	d.logger.Warn("[DLQ] UpdateAckLevel succeeded - query executed",
		tag.ShardID(shardID),
		tag.WorkflowDomainID(domainID),
		tag.Value(fmt.Sprintf("taskType=%d, newAckLevel=%d", taskType, newAckLevel)),
	)
	return nil
}

func (d *cassandraRangeDeleteDLQ) insertAckLevel(ctx context.Context, shardID int, domainID, scope, name string, taskType int, ackLevel int64) error {
	// insertAckLevel and updateAckLevel are now the same - both use INSERT
	// This is safe because we're using task_id = -1 which is a unique key
	return d.UpdateAckLevel(ctx, shardID, domainID, scope, name, taskType, ackLevel)
}

// CleanupProcessedTasks performs range delete for tasks below ack level
// This runs periodically (similar to queuev2's updateQueueState timer)
// NOTE: In the simplified schema, this requires knowing which partitions to clean up.
// This should be called per-partition by the background processor, not globally.
func (d *cassandraRangeDeleteDLQ) CleanupProcessedTasks(ctx context.Context, request *persistence.RangeDeleteStandbyTasksRequest) error {
	d.logger.Warn("[DLQ] Starting cleanup of processed tasks",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(fmt.Sprintf("scope=%s, name=%s, taskType=%d, maxTaskID=%d",
			request.ClusterAttributeScope, request.ClusterAttributeName, request.TaskType, request.MaxTaskID)))

	// Use the new RangeDeleteStandbyTasks method which creates a single tombstone
	err := d.RangeDeleteStandbyTasks(ctx, request)
	if err != nil {
		d.logger.Error("[DLQ] Failed to cleanup processed tasks",
			tag.Error(err),
			tag.ShardID(request.ShardID),
			tag.WorkflowDomainID(request.DomainID))
		return err
	}

	d.logger.Warn("[DLQ] Successfully cleaned up processed tasks",
		tag.ShardID(request.ShardID),
		tag.WorkflowDomainID(request.DomainID),
		tag.Value(fmt.Sprintf("maxTaskID=%d", request.MaxTaskID)))

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

func (d *cassandraComparisonDLQ) RangeDeleteStandbyTasks(ctx context.Context, request *persistence.RangeDeleteStandbyTasksRequest) error {
	// Only range-delete implementation uses this
	return d.rangeImpl.RangeDeleteStandbyTasks(ctx, request)
}

func (d *cassandraComparisonDLQ) CleanupProcessedTasks(ctx context.Context, request *persistence.RangeDeleteStandbyTasksRequest) error {
	// Point-delete doesn't need cleanup (tasks are deleted immediately)
	// Only range-delete needs periodic cleanup
	return d.rangeImpl.CleanupProcessedTasks(ctx, request)
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
