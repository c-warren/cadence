# History-Backed Async Workflow Queue

This document is an operator guide for enabling and running the **history-backed,
Cassandra-backed async workflow queue** — a drop-in replacement for the Kafka
backing of the async workflow APIs (`StartWorkflowExecutionAsync` and
`SignalWithStartWorkflowExecutionAsync`).

## What it is

The async workflow APIs let a client fire-and-forget a start/signal-with-start
request; the request is durably queued and later replayed against the synchronous
`frontend.StartWorkflowExecution` path by a background consumer.

The generic `messaging.Producer` / `messaging.Consumer` contracts used by the
async workflow feature have two implementations:

- **`kafka`** — the original backing. Requires an external Kafka broker/topic.
- **`history`** — the backing documented here. Needs **no external broker**; it
  reuses Cadence's own history service and Cassandra.

### Architecture (history backing)

- **Producer (frontend):** each async request is routed by `workflowID → history
  shard` to the owning history host, which writes it to the
  `async_workflow_queue` Cassandra table and emits a replication task so the
  request durably fans out to all regions.
- **Consumer (worker service):** the worker pulls messages per-shard from
  history (`GetAsyncWorkflowMessages`) and calls the **synchronous**
  `frontend.StartWorkflowExecution`. This preserves all the normal start-path
  behavior: request validation, per-domain rate limiting, active-active routing,
  and `AlreadyStarted` de-duplication.
- **Per-region independent ack / GC:** each region tracks its own consumption
  cursor (ack level) in `async_workflow_queue_metadata`. A background GC daemon on
  each history shard range-deletes messages behind the committed ack level.
- **DLQ:** poison / un-processable messages are moved to
  `async_workflow_queue_dlq` so they don't block the queue.

### When to use it vs Kafka

Prefer the history backing when you want to avoid operating a Kafka cluster for
async workflows and you already run Cassandra-backed history. Cross-region
delivery rides Cadence's existing replication stream instead of a separate Kafka
mirroring setup. Keep Kafka if you have an existing Kafka-based deployment you do
not want to migrate.

## Prerequisites: schema

The three tables ship in the Cassandra schema:

- `async_workflow_queue` — queued async requests (start / signal-with-start).
- `async_workflow_queue_metadata` — the per-`(queue_name, shard_id)` consumption
  cursor (`ack_level`).
- `async_workflow_queue_dlq` — poison / corrupt requests (same shape as
  `async_workflow_queue`).

They are defined in `schema/cassandra/cadence/schema.cql` and shipped as the
versioned upgrade `schema/cassandra/cadence/versioned/v0.48/`
(`async_workflow_queue.cql`, `manifest.json`).

Apply them with:

```bash
make install-schema-cassandra
```

(or the standard versioned-schema upgrade tooling if you upgrade an existing
keyspace to schema version 0.48).

## Cluster config: predefine a history-backed queue

Define a predefined queue in the static server config under the
`asyncWorkflowQueues` key (Go field `AsyncWorkflowQueues` in
`common/config/config.go`, a `map[string]AsyncWorkflowQueueProvider`). Each
provider has a `type` and a provider-specific `config` block. For the history
backing, `type` is `"history"` and the config carries a `queueName`:

```yaml
# Minimal history-backed async workflow queue.
# The map key ("test-async-wf-queue") is the predefined queue name that a
# domain's AsyncWorkflowConfiguration.PredefinedQueueName must reference.
asyncWorkflowQueues:
  test-async-wf-queue:
    type: "history"
    config:
      queueName: "test-async-wf-queue"
```

Compare with the Kafka form, which needs broker/topic details:

```yaml
asyncWorkflowQueues:
  queue1:
    type: "kafka"
    config:
      connection:
        brokers:
          - "localhost:9092"
      topic: "async-wf-topic1"
```

Real examples already in the repo:

- `host/testdata/integration_async_wf_with_history_cluster.yaml` — single-cluster
  test config with predefined queue `test-async-wf-queue`, type `history`.
- `host/testdata/xdc_async_wf_clusters.yaml` — cross-region (active/standby)
  example using the same history-backed queue on both clusters.
- `config/development_async_wf_kafka_queue.yaml` — the Kafka equivalent server
  config (useful for contrast).

Note: the `host/testdata/*` files are integration `TestClusterConfig` files and
use the lowercase `asyncwfqueues` key of that test struct; the **server** config
key is `asyncWorkflowQueues` as shown above.

> **`numHistoryShards`:** shard derivation is global, so `numHistoryShards` must
> match the value used across the cluster (and be identical across regions in an
> XDC setup). Changing it is not supported after the fact.

## Per-domain enablement

Enabling a predefined queue for a domain is done by setting that domain's
`AsyncWorkflowConfiguration` (see `common/types/admin.go`):

| Field                 | Meaning                                                        |
| --------------------- | ------------------------------------------------------------- |
| `Enabled`             | Whether async workflows are enabled for the domain.           |
| `PredefinedQueueName` | Must match a key in the cluster `asyncWorkflowQueues` map.     |
| `QueueType`           | Provider type; use `"history"` for the history backing.       |
| `QueueConfig`         | Inline queue config (`DataBlob`) — omit when using a predefined queue. |

Set it with the admin CLI (`cadence admin async-wf-queue`, alias `aq`; see
`tools/cli/admin.go` → `newAdminAsyncQueueCommands`, and the handlers in
`tools/cli/admin_async_queue_commands.go`):

```bash
# Upsert the config (references the predefined queue by name).
cadence --domain my-domain admin async-wf-queue update \
  --json '{"Enabled":true,"PredefinedQueueName":"test-async-wf-queue","QueueType":"history"}'

# Read it back.
cadence --domain my-domain admin async-wf-queue get
```

`update` requires the `--json` flag (the JSON must deserialize into
`types.AsyncWorkflowConfiguration`). `PredefinedQueueName` must exactly match the
queue name predefined in the cluster config above.

## Worker consumption gate

Even with a per-domain config, the worker service only actually consumes when the
dynamic config flag is on:

- Key: **`worker.enableAsyncWorkflowConsumption`** (Go:
  `EnableAsyncWorkflowConsumption`). Default `false`.

(Note: this is the `worker.` namespace, not `system.`.) In the integration test
configs this is enabled via `workerconfig.enableasyncwfconsumer: true`.

## Tuning (dynamic config)

All keys and defaults below are from
`common/dynamicconfig/dynamicproperties/constants.go`.

### Worker consumer

| Key                                          | Default | Purpose                                                                 |
| -------------------------------------------- | ------- | ----------------------------------------------------------------------- |
| `worker.asyncWorkflowConsumerPageSize`       | `100`   | Messages fetched per `GetAsyncWorkflowMessages` RPC per owned shard.     |
| `worker.asyncWorkflowConsumerBufferSize`     | `1000`  | Size of the in-memory channel buffering polled messages.                |
| `worker.asyncWorkflowConsumerPollInterval`   | `1s`    | Wait after an empty page before polling a shard again.                  |
| `worker.asyncWorkflowConsumerCommitInterval` | `5s`    | How often each shard flushes its ack level to history.                  |
| `worker.asyncWorkflowConsumerErrorBackoff`   | `1s`    | Pause after a failed poll RPC before retrying.                          |
| `worker.asyncWorkflowConsumerRebalanceInterval` | `30s` | Safety-net period to re-evaluate shard ownership.                      |
| `worker.asyncWorkflowConsumerRPCTimeout`     | `10s`   | Bounds a single ack-level commit or DLQ-enqueue RPC.                    |

### History GC of consumed messages

| Key                                   | Default | Purpose                                                             |
| ------------------------------------- | ------- | ------------------------------------------------------------------- |
| `history.asyncWorkflowQueueGCEnabled` | `false` | Enables the per-shard background GC daemon.                         |
| `history.asyncWorkflowQueueGCInterval`| `5m`    | Interval between GC sweeps on each history shard (filter: `ShardID`).|
| `history.asyncWorkflowQueueGCQueueNames` | `[]` (empty) | List of queue names the GC daemon sweeps.                     |

GC range-deletes messages **behind the committed ack level** for each listed
queue. It is a no-op unless `asyncWorkflowQueueGCEnabled` is `true` **and**
`asyncWorkflowQueueGCQueueNames` lists the queue names to sweep. Without GC the
queue tables grow unbounded, so enable it and list your queue names in
steady-state operation.

## Metrics

Emitted metric names (from `common/metrics/defs.go`):

### Consumer (scope `AsyncWorkflowConsumer`)

| Metric name                                    | Type    | Meaning                                             |
| ---------------------------------------------- | ------- | --------------------------------------------------- |
| `async_workflow_consumer_message_consumed`     | Counter | Messages pulled and handed to the consumer.         |
| `async_workflow_consumer_message_ack`          | Counter | Successfully processed (acked) messages.            |
| `async_workflow_consumer_message_nack`         | Counter | Nacked (dead-lettered) messages.                    |
| `async_workflow_consumer_dlq_enqueue`          | Counter | Messages successfully enqueued to the DLQ on nack.  |
| `async_workflow_consumer_dlq_enqueue_failures` | Counter | **Alert.** Failed DLQ-enqueue attempts.             |
| `async_workflow_consumer_poll_failures`        | Counter | **Alert.** Failed `GetAsyncWorkflowMessages` RPCs.  |
| `async_workflow_consumer_commit_failures`      | Counter | **Alert.** Failed `UpdateAsyncWorkflowAckLevel` RPCs.|
| `async_workflow_consumer_owned_shard_count`    | Gauge   | Per-host: shards this consumer currently owns.      |
| `async_workflow_consumer_message_age`          | Timer   | Age of a message (`now - createdTime`) when emitted.|
| `async_workflow_consumer_count`               | Gauge   | Consumer count.                                     |

### GC (scope `AsyncWorkflowQueueGC`)

| Metric name                        | Type    | Meaning                                    |
| ---------------------------------- | ------- | ------------------------------------------ |
| `async_workflow_queue_gc_sweeps`   | Counter | GC range-delete sweeps performed.          |
| `async_workflow_queue_gc_failures` | Counter | **Alert.** GC failures.                    |

**Alert on:** `..._dlq_enqueue_failures`, `..._poll_failures`,
`..._commit_failures`, and `async_workflow_queue_gc_failures`. Watch
`async_workflow_consumer_message_age` for growing backlog/latency.

## DLQ operations

Poison messages land in `async_workflow_queue_dlq`.

Inspect and drain the DLQ with the `cadence admin async-wf-queue dlq` (alias
`aq dlq`) subcommands:

```bash
# Inspect DLQ contents without modifying them.
cadence admin async-wf-queue dlq read  --queue_name <q> --shard_id <n> --pagesize <k> [--last_message_id <id>] [--max_message_count <k>]
# Re-inject DLQ messages back into the main queue for reprocessing.
cadence admin async-wf-queue dlq merge --queue_name <q> --shards <n[,m-o]> [--last_message_id <inclusive-end-id>] [--pagesize <k>] [--max_message_count <k>]
# Delete DLQ messages at or below an inclusive message id.
cadence admin async-wf-queue dlq purge --queue_name <q> --shards <n[,m-o]> [--last_message_id <inclusive-end-id>]
```

- `--queue_name` (alias `qn`) is **required** and must match a `queueName`
  configured under `asyncWorkflowQueues`.
- Target shards with `--shard_id <n>` (single) or `--shards <ranges>` (e.g.
  `1,2` or `2,5-6`); with neither, shards are read from STDIN.
- For `read`, `--last_message_id` is the exclusive start cursor (defaults to the
  beginning) and the command paginates until the DLQ is exhausted or
  `--max_message_count` rows are printed. For `merge`/`purge`, `--last_message_id`
  is the **inclusive end** bound and defaults to "all". `merge` re-enqueues each
  message to the main queue **before** deleting it from the DLQ, so an
  interrupted merge cannot lose messages (any duplicate that already started is
  absorbed by `AlreadyStarted` de-dup). Pass `--max_message_count` to `merge` as
  a per-shard safety cap when a queue may still be receiving new poison messages
  during the drain.

Semantics: `read` inspects; `merge` re-injects messages back into the main queue
so they are retried through the frontend start path (any duplicates that already
started are absorbed by `AlreadyStarted` de-dup); `purge` deletes them.

## Cross-region behavior

- Async requests replicate via history's replication stream to all regions.
- Each region's worker consumes its **local** copy and pushes it to its local
  frontend.
- Active-active routing plus `AlreadyStarted` de-dup ensure a single execution
  even though multiple regions may attempt the start.
- A region outage **after** replication does not lose the request — another
  region still holds and processes its copy.

### Replication-task DLQ backstop

If the async **replication task** itself fails persistently, it lands in the
regular history replication DLQ (a Phase 6.2 backstop). Recover it with the
standard replication DLQ tooling:

```bash
cadence admin dlq merge --dlq_type history
```

(`cadence admin dlq` supports `read` / `merge` / `purge` / `count`; the
`--dlq_type` flag distinguishes `history` vs `domain`. See
`tools/cli/admin_dlq_commands.go`.)
</content>
</invoke>
