# Partitioning Strategy — Distributed Job Queue

How jobs are distributed across queues and workers, and how to scale horizontally.

---

## Current Strategy: Job Type Partitioning

The system uses dedicated queues per job type. This is the simplest form of partitioning and sufficient for the current scale.

### Queue Topology

```
import_queue   ─── [import handlers] ─── all workers listen
export_queue   ─── [export handlers] ─── all workers listen
report_queue   ─── [report handlers] ─── all workers listen
email_queue    ─── [email handlers]  ─── all workers listen
dead_letter_queue  (DLQ — manual processing)
```

### Worker Queue Assignment

Workers can be configured to listen on specific queues:

```bash
# Worker listening on all queues (default)
WORKER_QUEUES="import_queue,export_queue,report_queue,email_queue" ./worker

# Dedicated import worker (isolation for slow jobs)
WORKER_QUEUES="import_queue" ./worker

# High-priority worker pool
WORKER_QUEUES="email_queue,report_queue" ./worker
```

### Why Job Type Partitioning?

**Isolation of slow jobs:**
```
Problem: A slow bulk_import job (processing 100k rows) blocks goroutines
         that could be handling fast email_queue jobs.

Solution: Dedicated workers per queue type, or priority queue ordering.
```

**Visibility:**
```
Prometheus metric: queue_depth{queue="import_queue"}
Easy to see: "import queue has 500 pending jobs" → scale up import workers
```

**Independent scaling:**
```
If email_queue backs up → spin up more workers with WORKER_QUEUES="email_queue"
Without type partitioning, all queues compete for the same worker pool
```

### Trade-offs

| Advantage | Disadvantage |
|-----------|-------------|
| Simple to implement | Workers idle if their queue is empty |
| Easy to monitor per-queue | Potential underutilization |
| Slow jobs can't starve fast ones | Requires queue assignment configuration |
| Natural scaling unit | |

---

## Future Strategy: Hash Partitioning (1M+ jobs/day)

When job type partitioning is no longer sufficient (estimated: 1M+ jobs/day or 10k+ concurrent jobs), switch to hash partitioning for better distribution.

### Concept

Instead of routing by job type, route by a hash of a partition key (e.g., `workspace_id`):

```
workspace_id "abc123"
    │
    ▼
hash("abc123") % 4 = 2
    │
    ▼
import_queue_partition_2
```

```go
func getPartitionedQueue(queueName string, partitionKey string, numPartitions int) string {
    h := fnv.New32a()
    h.Write([]byte(partitionKey))
    partition := h.Sum32() % uint32(numPartitions)
    return fmt.Sprintf("%s_p%d", queueName, partition)
}

// Usage:
queue := getPartitionedQueue("import_queue", job.WorkspaceID, 4)
// → "import_queue_p0", "import_queue_p1", "import_queue_p2", or "import_queue_p3"
```

### Benefits at Scale

**Ordering within a partition key:**
```
All jobs for workspace "abc123" → always go to partition 2
→ Jobs for that workspace processed in enqueue order
→ No interleaving with other workspaces
→ Useful if business requires: "complete import before starting export for same workspace"
```

**Horizontal Redis scaling:**
```
4 partitions → 4 Redis instances (or Redis Cluster shards)
→ Distribute load across Redis nodes
→ Each Redis node handles 25% of traffic
```

**Worker specialization:**
```
Worker pool A → handles partitions 0 and 1
Worker pool B → handles partitions 2 and 3
→ Independent scaling per partition
```

### When to Switch to Hash Partitioning

| Metric | Threshold |
|--------|-----------|
| Jobs/day | > 500,000 |
| Queue depth spikes | > 10,000 regularly |
| Redis CPU | > 70% on single instance |
| Latency degradation | Enqueue p99 > 50ms |

---

## Priority Queues (Alternative Approach)

Instead of (or in addition to) partitioning, implement priority lanes:

```
high_priority_queue   → fast workers (10 goroutines dedicated)
normal_priority_queue → standard workers
low_priority_queue    → background workers (1-2 goroutines)
```

**Use case:**
- `email_queue` → high priority (user is waiting)
- `bulk_export` → normal priority
- `report_generation` → low priority (batch, nobody waiting)

**Implementation:**
```go
// Workers check high priority first, then normal, then low
result, _ := w.redis.BRPop(ctx, 5*time.Second,
    "high_priority_queue",
    "normal_priority_queue",
    "low_priority_queue",
).Result()
// BRPOP checks queues in order — first non-empty wins
```

---

## Consistent Hashing for Multi-Redis (Future)

When a single Redis instance can't handle the throughput:

```
Virtual nodes (ring):
  import_queue_p0  → Redis Node A
  import_queue_p1  → Redis Node B
  import_queue_p2  → Redis Node C
  import_queue_p3  → Redis Node A (wraps)

Adding a new node:
  import_queue_p4  → Redis Node D
  → Rebalance: Move ~25% of keys to new node
  → Old keys still valid on original nodes during migration
  → Producers start routing new jobs to new node immediately
```

**Implementation:** Use `github.com/serialx/hashring` Go library for consistent hashing.

---

## Rebalancing Strategy

When changing the number of partitions (the hard problem):

**Option 1: Double-write period (safest)**
```
1. Deploy new partition count (e.g., 4 → 8)
2. Workers listen on both old (4) and new (8) queues
3. New jobs go to new queue
4. Old jobs drain from old queues
5. After old queues empty: Remove old queue listeners
```

**Option 2: Drain-and-restart (simplest for low-traffic)**
```
1. Stop accepting new jobs (maintenance window)
2. Process all queued jobs until empty
3. Change partition count
4. Resume accepting jobs
```

**Option 3: Progressive rollout**
```
1. Deploy new partition count
2. Route 10% of new jobs to new partitions
3. Monitor for issues
4. Gradually increase to 100% over 1-2 days
5. Old partition queues drain naturally
```

---

## Current Implementation Decision

For this portfolio project, **Job Type Partitioning is implemented**. Hash partitioning is documented but not implemented — the architecture makes it straightforward to add later.

The key design choice that makes future partitioning easy:
- `getQueueForJobType(jobType)` is a single function → easy to swap for hash-based routing
- Workers are configured via environment variables → easy to redeploy with new queue names
- Queue names are stored in `job_executions` table → easy to audit which partition handled a job
