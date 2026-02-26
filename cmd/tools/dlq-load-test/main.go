package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/zap"
)

var (
	// Connection flags
	hostPort = flag.String("host", "localhost:7833", "Cadence frontend host:port")
	domain   = flag.String("domain", "dlq-test", "Cadence domain name")
	taskList = flag.String("tasklist", "dlq-test-tl", "Task list name")

	// Load test flags
	workflowCount = flag.Int("count", 10, "Number of workflows to start (1-10000)")
	batchSize     = flag.Int("batch", 5, "Workflows to start per batch")
	batchDelay    = flag.Duration("batch-delay", 1*time.Second, "Delay between batches")

	// Logging flags
	verbose = flag.Bool("verbose", false, "Enable verbose logging (activity start/end)")

	// Workflow config flags
	numActivities = flag.Int("activities", 3, "Number of activities per workflow")
	activityDelay = flag.Duration("activity-delay", 100*time.Millisecond, "Delay in each activity")

	// Mode flags
	workerOnly = flag.Bool("worker-only", false, "Only run worker, don't start workflows")
	clientOnly = flag.Bool("client-only", false, "Only start workflows, don't run worker")
)

type (
	// WorkflowInput defines the input to the test workflow
	WorkflowInput struct {
		WorkflowNum   int
		NumActivities int
		ActivityDelay time.Duration
	}

	// ActivityInput defines the input to each activity
	ActivityInput struct {
		WorkflowNum   int
		ActivityNum   int
		DelayDuration time.Duration
	}
)

var (
	logger              *zap.Logger
	workflowsStarted    atomic.Int64
	workflowsCompleted  atomic.Int64
	activitiesStarted   atomic.Int64
	activitiesCompleted atomic.Int64
)

func main() {
	flag.Parse()

	// Validate flags
	if *workflowCount < 1 || *workflowCount > 10000 {
		fmt.Fprintf(os.Stderr, "Error: workflow count must be between 1 and 10000\n")
		os.Exit(1)
	}
	if *batchSize < 1 {
		fmt.Fprintf(os.Stderr, "Error: batch size must be >= 1\n")
		os.Exit(1)
	}
	if *workerOnly && *clientOnly {
		fmt.Fprintf(os.Stderr, "Error: cannot specify both -worker-only and -client-only\n")
		os.Exit(1)
	}

	// Setup logger
	var err error
	if *verbose {
		logger, err = zap.NewDevelopment()
	} else {
		cfg := zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		logger, err = cfg.Build()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Create YARPC dispatcher
	transport := grpc.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "dlq-load-test",
		Outbounds: yarpc.Outbounds{
			"cadence-frontend": {Unary: transport.NewSingleOutbound(*hostPort)},
		},
	})

	if err := dispatcher.Start(); err != nil {
		logger.Fatal("Failed to start YARPC dispatcher", zap.Error(err))
	}
	defer dispatcher.Stop()

	// Create workflow service client
	service := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))

	// Create Cadence client
	ch := client.NewClient(service, *domain, &client.Options{
		Identity: "dlq-load-test",
	})

	logger.Info("DLQ Load Test Tool",
		zap.String("host", *hostPort),
		zap.String("domain", *domain),
		zap.String("tasklist", *taskList),
		zap.Int("workflow-count", *workflowCount),
		zap.Int("batch-size", *batchSize),
		zap.Int("activities-per-workflow", *numActivities))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Start worker (unless client-only mode)
	if !*clientOnly {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, service)
		}()
		// Give worker time to start
		time.Sleep(2 * time.Second)
	}

	// Start workflows (unless worker-only mode)
	if !*workerOnly {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startWorkflows(ctx, ch)
		}()
	}

	// Wait for signal or completion
	select {
	case <-sigCh:
		logger.Info("Received signal, shutting down...")
		cancel()
	case <-ctx.Done():
	}

	wg.Wait()

	// Print final stats
	logger.Info("Load test complete",
		zap.Int64("workflows-started", workflowsStarted.Load()),
		zap.Int64("workflows-completed", workflowsCompleted.Load()),
		zap.Int64("activities-started", activitiesStarted.Load()),
		zap.Int64("activities-completed", activitiesCompleted.Load()))
}

func runWorker(ctx context.Context, service workflowserviceclient.Interface) {
	logger.Info("Starting worker", zap.String("tasklist", *taskList))

	w := worker.New(service, *domain, *taskList, worker.Options{
		Logger: logger,
	})

	// Register workflow and activities
	w.RegisterWorkflowWithOptions(TestWorkflow, workflow.RegisterOptions{Name: "TestWorkflow"})
	w.RegisterActivityWithOptions(TestActivity, activity.RegisterOptions{Name: "TestActivity"})

	err := w.Start()
	if err != nil {
		logger.Fatal("Failed to start worker", zap.Error(err))
	}

	logger.Info("Worker started successfully")

	// Wait for context cancellation
	<-ctx.Done()

	logger.Info("Stopping worker...")
	w.Stop()
	logger.Info("Worker stopped")
}

func startWorkflows(ctx context.Context, ch client.Client) {
	logger.Info("Starting workflows",
		zap.Int("total", *workflowCount),
		zap.Int("batch-size", *batchSize))

	startTime := time.Now()

	for i := 0; i < *workflowCount; i++ {
		// Check if context cancelled
		select {
		case <-ctx.Done():
			logger.Info("Stopping workflow starts due to cancellation")
			return
		default:
		}

		// Start workflow
		workflowID := fmt.Sprintf("dlq-test-wf-%d-%d", time.Now().Unix(), i)
		input := WorkflowInput{
			WorkflowNum:   i,
			NumActivities: *numActivities,
			ActivityDelay: *activityDelay,
		}

		options := client.StartWorkflowOptions{
			ID:                              workflowID,
			TaskList:                        *taskList,
			ExecutionStartToCloseTimeout:    5 * time.Minute,
			DecisionTaskStartToCloseTimeout: 1 * time.Minute,
		}

		_, err := ch.StartWorkflow(ctx, options, "TestWorkflow", input)
		if err != nil {
			logger.Error("Failed to start workflow",
				zap.String("workflow-id", workflowID),
				zap.Error(err))
			continue
		}

		workflowsStarted.Add(1)
		logger.Info("Started workflow",
			zap.String("workflow-id", workflowID),
			zap.Int("num", i+1),
			zap.Int("total", *workflowCount))

		// Batch delay
		if (i+1)%*batchSize == 0 && i+1 < *workflowCount {
			logger.Info("Batch complete, sleeping",
				zap.Int("completed", i+1),
				zap.Duration("delay", *batchDelay))
			time.Sleep(*batchDelay)
		}
	}

	duration := time.Since(startTime)
	logger.Info("All workflows started",
		zap.Int64("count", workflowsStarted.Load()),
		zap.Duration("duration", duration))
}

// TestWorkflow is a simple workflow that executes multiple activities
func TestWorkflow(ctx workflow.Context, input WorkflowInput) error {
	logger := workflow.GetLogger(ctx)
	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID

	logger.Info("Workflow started",
		zap.String("workflow-id", workflowID),
		zap.Int("workflow-num", input.WorkflowNum),
		zap.Int("num-activities", input.NumActivities))

	// Activity options
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Minute,
		StartToCloseTimeout:    1 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Execute activities
	for i := 0; i < input.NumActivities; i++ {
		activityInput := ActivityInput{
			WorkflowNum:   input.WorkflowNum,
			ActivityNum:   i,
			DelayDuration: input.ActivityDelay,
		}

		var result string
		err := workflow.ExecuteActivity(ctx, "TestActivity", activityInput).Get(ctx, &result)
		if err != nil {
			logger.Error("Activity failed",
				zap.String("workflow-id", workflowID),
				zap.Int("activity-num", i),
				zap.Error(err))
			return err
		}

		if *verbose {
			logger.Info("Activity completed",
				zap.String("workflow-id", workflowID),
				zap.Int("activity-num", i),
				zap.String("result", result))
		}
	}

	workflowsCompleted.Add(1)
	logger.Info("Workflow completed",
		zap.String("workflow-id", workflowID),
		zap.Int("workflow-num", input.WorkflowNum),
		zap.Int64("total-completed", workflowsCompleted.Load()))

	return nil
}

// TestActivity is a simple activity that just sleeps
func TestActivity(ctx context.Context, input ActivityInput) (string, error) {
	activitiesStarted.Add(1)

	if *verbose {
		logger.Info("Activity started",
			zap.Int("workflow-num", input.WorkflowNum),
			zap.Int("activity-num", input.ActivityNum))
	}

	// Simulate work
	time.Sleep(input.DelayDuration)

	activitiesCompleted.Add(1)

	result := fmt.Sprintf("Activity %d of workflow %d completed", input.ActivityNum, input.WorkflowNum)
	return result, nil
}
