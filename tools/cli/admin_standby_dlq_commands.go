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

package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/tools/common/commoncli"
)

// AdminReadStandbyDLQ reads standby task DLQ for a domain/cluster attribute
// This uses direct persistence layer access for POC
func AdminReadStandbyDLQ(c *cli.Context) error {
	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error creating context: ", err)
	}

	shardID := c.Int(FlagShardID)
	domainID := c.String(FlagDomainID)
	clusterScope := c.String(FlagClusterAttributeScope)
	clusterName := c.String(FlagClusterAttributeName)
	pageSize := c.Int(FlagPageSize)

	// Get persistence factory
	factory, err := getDeps(c).initPersistenceFactory(c)
	if err != nil {
		return commoncli.Problem("Failed to initialize persistence factory: ", err)
	}

	dlqManager, err := factory.NewStandbyTaskDLQManager()
	if err != nil {
		return commoncli.Problem("Failed to get DLQ manager: ", err)
	}

	// Debug: Print the actual type of DLQ manager
	fmt.Printf("DLQ Manager Type: %T\n", dlqManager)

	request := &persistence.ReadStandbyTasksRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: clusterScope,
		ClusterAttributeName:  clusterName,
		PageSize:              pageSize,
	}

	// Debug output
	fmt.Printf("Querying DLQ with:\n")
	fmt.Printf("  ShardID: %d\n", shardID)
	fmt.Printf("  DomainID: %s\n", domainID)
	fmt.Printf("  ClusterAttributeScope: %s\n", clusterScope)
	fmt.Printf("  ClusterAttributeName: %s\n", clusterName)
	fmt.Printf("  PageSize: %d\n\n", pageSize)

	response, err := dlqManager.ReadStandbyTasks(ctx, request)
	if err != nil {
		fmt.Printf("Error reading DLQ: %v\n", err)
		return commoncli.Problem("Failed to read standby task DLQ: ", err)
	}

	fmt.Printf("\nFound %d tasks in standby task DLQ\n", len(response.Tasks))
	for i, task := range response.Tasks {
		fmt.Printf("\nTask %d:\n", i+1)
		fmt.Printf("  ShardID: %d\n", task.ShardID)
		fmt.Printf("  DomainID: %s\n", task.DomainID)
		fmt.Printf("  WorkflowID: %s\n", task.WorkflowID)
		fmt.Printf("  RunID: %s\n", task.RunID)
		fmt.Printf("  TaskID: %d\n", task.TaskID)
		fmt.Printf("  TaskType: %d\n", task.TaskType)
		fmt.Printf("  VisibilityTimestamp: %d\n", task.VisibilityTimestamp)
		fmt.Printf("  Version: %d\n", task.Version)
	}

	if len(response.NextPageToken) > 0 {
		fmt.Println("\nMore tasks available. Use pagination to continue.")
	}

	return nil
}

// AdminCountStandbyDLQ counts tasks in the standby task DLQ
func AdminCountStandbyDLQ(c *cli.Context) error {
	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error creating context: ", err)
	}

	shardID := c.Int(FlagShardID)
	domainID := c.String(FlagDomainID)
	clusterScope := c.String(FlagClusterAttributeScope)
	clusterName := c.String(FlagClusterAttributeName)

	// Get persistence factory
	factory, err := getDeps(c).initPersistenceFactory(c)
	if err != nil {
		return commoncli.Problem("Failed to initialize persistence factory: ", err)
	}

	dlqManager, err := factory.NewStandbyTaskDLQManager()
	if err != nil {
		return commoncli.Problem("Failed to get DLQ manager: ", err)
	}

	request := &persistence.GetStandbyTaskDLQSizeRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: clusterScope,
		ClusterAttributeName:  clusterName,
	}

	response, err := dlqManager.GetStandbyTaskDLQSize(ctx, request)
	if err != nil {
		return commoncli.Problem("Failed to get standby task DLQ size: ", err)
	}

	fmt.Printf("Standby Task DLQ Size: %d tasks\n", response.Size)
	fmt.Printf("  ShardID: %d\n", shardID)
	fmt.Printf("  DomainID: %s\n", domainID)
	fmt.Printf("  Cluster Attribute: %s/%s\n", clusterScope, clusterName)

	return nil
}

// AdminDeleteStandbyDLQ deletes a specific task from the standby task DLQ
func AdminDeleteStandbyDLQ(c *cli.Context) error {
	ctx, cancel, err := newContext(c)
	defer cancel()
	if err != nil {
		return commoncli.Problem("Error creating context: ", err)
	}

	shardID := c.Int(FlagShardID)
	domainID := c.String(FlagDomainID)
	clusterScope := c.String(FlagClusterAttributeScope)
	clusterName := c.String(FlagClusterAttributeName)
	taskID := c.Int64(FlagTaskID)
	visibilityTimestamp := c.Int64(FlagTaskVisibilityTimestamp)

	if taskID == 0 {
		return commoncli.Problem("TaskID is required for delete operation", nil)
	}

	// Get persistence factory
	factory, err := getDeps(c).initPersistenceFactory(c)
	if err != nil {
		return commoncli.Problem("Failed to initialize persistence factory: ", err)
	}

	dlqManager, err := factory.NewStandbyTaskDLQManager()
	if err != nil {
		return commoncli.Problem("Failed to get DLQ manager: ", err)
	}

	request := &persistence.DeleteStandbyTaskRequest{
		ShardID:               shardID,
		DomainID:              domainID,
		ClusterAttributeScope: clusterScope,
		ClusterAttributeName:  clusterName,
		TaskID:                taskID,
		VisibilityTimestamp:   visibilityTimestamp,
	}

	err = dlqManager.DeleteStandbyTask(ctx, request)
	if err != nil {
		return commoncli.Problem("Failed to delete standby task from DLQ: ", err)
	}

	fmt.Printf("Successfully deleted task from standby DLQ\n")
	fmt.Printf("  ShardID: %d\n", shardID)
	fmt.Printf("  DomainID: %s\n", domainID)
	fmt.Printf("  Cluster Attribute: %s/%s\n", clusterScope, clusterName)
	fmt.Printf("  TaskID: %d\n", taskID)
	fmt.Printf("  VisibilityTimestamp: %d\n", visibilityTimestamp)

	return nil
}
