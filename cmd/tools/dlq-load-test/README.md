# DLQ Load Test Tool

Simple, focused tool for testing the Standby Task DLQ implementation.

## Build

```bash
go build -o dlq-load-test ./cmd/tools/dlq-load-test
```

## Usage

### Basic Usage (Worker + Client)

Run both worker and start workflows:

```bash
./dlq-load-test \
  --domain dlq-test \
  --count 10 \
  --batch 5
```

### Worker Only Mode

Run just the worker (start workflows separately):

```bash
./dlq-load-test \
  --domain dlq-test \
  --worker-only
```

### Client Only Mode

Start workflows without running a worker:

```bash
./dlq-load-test \
  --domain dlq-test \
  --count 100 \
  --batch 10 \
  --client-only
```

## Flags

### Connection Flags
- `--host` - Cadence frontend host:port (default: `localhost:7833`)
- `--domain` - Cadence domain name (default: `dlq-test`)
- `--tasklist` - Task list name (default: `dlq-test-tl`)

### Load Test Flags
- `--count` - Number of workflows to start, 1-10000 (default: `10`)
- `--batch` - Workflows to start per batch (default: `5`)
- `--batch-delay` - Delay between batches (default: `1s`)

### Workflow Configuration
- `--activities` - Number of activities per workflow (default: `3`)
- `--activity-delay` - Delay in each activity (default: `100ms`)

### Logging Flags
- `--verbose` - Enable verbose logging (activity start/end)

### Mode Flags
- `--worker-only` - Only run worker, don't start workflows
- `--client-only` - Only start workflows, don't run worker

## Examples

### DLQ Testing Scenario

**Terminal 1: Start worker on cluster0**
```bash
./dlq-load-test \
  --host localhost:7833 \
  --domain dlq-test \
  --tasklist dlq-test-tl \
  --worker-only
```

**Terminal 2: Start workflows**
```bash
./dlq-load-test \
  --host localhost:7833 \
  --domain dlq-test \
  --tasklist dlq-test-tl \
  --count 20 \
  --batch 5 \
  --batch-delay 2s \
  --client-only
```

**Watch cluster1 (standby) logs for:**
- Tasks being retried
- Tasks being enqueued to DLQ after 10s
- DLQ processing after failover

### High Volume Test

```bash
./dlq-load-test \
  --count 1000 \
  --batch 50 \
  --batch-delay 500ms \
  --activities 5
```

### Verbose Logging

```bash
./dlq-load-test \
  --count 10 \
  --verbose
```

## Output

### Normal Mode
```
INFO    Workflow started        {"workflow-id": "dlq-test-wf-...", "workflow-num": 0}
INFO    Workflow completed      {"workflow-id": "dlq-test-wf-...", "workflow-num": 0}
```

### Verbose Mode
```
INFO    Workflow started        {"workflow-id": "dlq-test-wf-...", "workflow-num": 0}
INFO    Activity started        {"workflow-num": 0, "activity-num": 0}
INFO    Activity completed      {"workflow-id": "dlq-test-wf-...", "activity-num": 0}
INFO    Activity started        {"workflow-num": 0, "activity-num": 1}
INFO    Activity completed      {"workflow-id": "dlq-test-wf-...", "activity-num": 1}
INFO    Workflow completed      {"workflow-id": "dlq-test-wf-...", "workflow-num": 0}
```

## Workflow Details

**TestWorkflow:**
- Executes N activities sequentially (configurable via `--activities`)
- Each activity sleeps for a configurable duration (`--activity-delay`)
- Logs start and end of workflow
- Optionally logs start/end of each activity (`--verbose`)

**TestActivity:**
- Simple activity that sleeps for configured duration
- Returns a string result
- Generates transfer tasks that will be replicated to standby clusters

## Use Cases

### 1. Basic DLQ Testing
Start small number of workflows, observe DLQ behavior on standby cluster:
```bash
./dlq-load-test --count 5 --batch 1 --batch-delay 15s
```

### 2. Failover Testing
Start workflows, wait for DLQ to fill, then failover:
```bash
# Start workflows (they'll hit DLQ on standby after 10s)
./dlq-load-test --count 20 --batch 5

# Wait 15 seconds for tasks to hit DLQ

# Failover domain (in another terminal)
./cadence domain update --domain dlq-test --active_clusters 'location.london:cluster1'

# Watch cluster1 logs for DLQ processing
```

### 3. Load Testing
Test DLQ behavior under high load:
```bash
./dlq-load-test --count 1000 --batch 100 --batch-delay 1s
```

### 4. Multi-Cluster Worker Testing
Start workers on different clusters:
```bash
# Cluster 0
./dlq-load-test --host localhost:7833 --worker-only

# Cluster 1
./dlq-load-test --host localhost:8833 --worker-only

# Start workflows from client
./dlq-load-test --host localhost:7833 --count 100 --client-only
```

## Stopping

Press `Ctrl+C` to gracefully stop the tool. It will:
- Stop starting new workflows
- Wait for worker to finish in-flight tasks
- Print final statistics
