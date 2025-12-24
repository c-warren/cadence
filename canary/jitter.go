// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"context"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	// Default values - can be overridden via workflow input
	defaultConcurrentActivities = 20
	defaultTotalActivities      = 100
)

// JitterWorkflowInput contains parameters for the jitter workflow
type JitterWorkflowInput struct {
	ScheduledTimeNanos   int64
	TotalActivities      int // Total number of activities to execute (default: 100)
	ConcurrentActivities int // Number of activities to run concurrently (default: 20)
}

func init() {
	registerWorkflow(jitterWorkflow, wfTypeJitter)
	registerActivity(jitterActivity, activityTypeJitter)
}

// jitterWorkflow is a workflow that continuously executes activities with random cluster attributes
// This stresses the matching service and is particularly sensitive to failover issues
func jitterWorkflow(ctx workflow.Context, input JitterWorkflowInput) error {
	scheduledTimeNanos := getScheduledTimeFromInputIfNonZero(ctx, input.ScheduledTimeNanos)

	// Use provided values or defaults
	totalActivities := input.TotalActivities
	if totalActivities <= 0 {
		totalActivities = defaultTotalActivities
	}
	concurrentActivities := input.ConcurrentActivities
	if concurrentActivities <= 0 {
		concurrentActivities = defaultConcurrentActivities
	}
	if concurrentActivities > totalActivities {
		concurrentActivities = totalActivities
	}

	profile, err := beginWorkflow(ctx, wfTypeJitter, scheduledTimeNanos)
	if err != nil {
		return err
	}

	// Get cluster attribute from workflow input or randomly select
	locations := []string{"seattle", "london", "tokyo"}
	selectedLocation := locations[workflow.Now(ctx).UnixNano()%int64(len(locations))]

	workflow.GetLogger(ctx).Info("jitterWorkflow started",
		zap.String("location", selectedLocation),
		zap.Int("totalActivities", totalActivities),
		zap.Int("concurrentActivities", concurrentActivities))

	selector := workflow.NewSelector(ctx)
	errors := make([]error, totalActivities)

	doActivity := func(index int) {
		now := workflow.Now(ctx).UnixNano()
		activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())

		future := workflow.ExecuteActivity(activityCtx, activityTypeJitter, now, index)
		selector.AddFuture(future, func(f workflow.Future) {
			errors[index] = f.Get(activityCtx, nil)
		})
	}

	// Start initial batch of concurrent activities
	for index := 0; index < concurrentActivities; index++ {
		doActivity(index)
	}

	// Continue launching activities as previous ones complete
	for index := concurrentActivities; index < totalActivities; index++ {
		selector.Select(ctx)
		doActivity(index)
	}

	// Wait for final batch to complete
	for index := 0; index < concurrentActivities; index++ {
		selector.Select(ctx)
	}

	// Check for any errors
	for i, err := range errors {
		if err != nil {
			workflow.GetLogger(ctx).Error("jitterActivity failed",
				zap.Int("activityIndex", i),
				zap.Error(err))
			return profile.end(err)
		}
	}

	workflow.GetLogger(ctx).Info("jitterWorkflow completed successfully",
		zap.Int("totalActivities", totalActivities))

	return profile.end(nil)
}

// jitterActivity is a simple activity that simulates work
// The high volume of these activities stresses the matching service
func jitterActivity(ctx context.Context, scheduledTimeNanos int64, activityIndex int) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeJitter, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	// Add small random sleep to introduce jitter and make timing more unpredictable
	// This makes the workflow more sensitive to matching issues
	if activityIndex%10 == 0 {
		activity.GetLogger(ctx).Debug("jitterActivity executing", zap.Int("index", activityIndex))
	}

	return nil
}
