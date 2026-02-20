# Idempotency Design — Distributed Job Queue

Two-layer deduplication strategy ensuring jobs are processed exactly once even under network failures, retries, and worker crashes.

---

## The Problem

Without idempotency, these scenarios cause duplicate processing:

1. **Network timeout**: Producer calls POST /jobs, network times out, producer retries → Two identical jobs enqueued
2. **Worker crash after processing, before acknowledging**: Job processed, worker dies → Job re-queued, processed again
3. **Duplicate API calls**: Client bug sends the same job payload twice

---

## Layer 1: Redis Deduplication (Enqueue-Time)

Prevents the same logical job from being enqueued more than once.

### Mechanism

```go
func (q *Queue) Enqueue(ctx context.Context, job Job) (string, error) {
    dedupKey := fmt.Sprintf("dedup:%s:%s", job.Queue, job.IdempotencyKey)
    
    // Atomic: SET key value EX 86400 NX
    // NX = only set if Not eXists
    set, err := q.redis.SetNX(ctx, dedupKey, job.ID, 24*time.Hour).Result()
    
    if !set {
        // Key already exists → this job was already enqueued
        existingJobID, _ := q.redis.Get(ctx, dedupKey).Result()
        return existingJobID, nil // Return existing job ID (idempotent)
    }
    
    // New job — proceed with enqueue
    if err := q.persistJob(ctx, job); err != nil {
        q.redis.Del(ctx, dedupKey) // Rollback dedup key if DB write fails
        return "", err
    }
    
    q.redis.LPush(ctx, job.Queue, job.ToJSON())
    return job.ID, nil
}
```

### Idempotency Key Design

Producers must supply a meaningful `idempotency_key`:

```
Format: <operation-type>-<resource-identifiers>

Good examples:
  "bulk_import-ws_abc123-file_xyz789"
  "bulk_export-ws_abc123-tool_def456-2024-01-15"
  "report-ws_abc123-monthly-2024-01"

Bad examples:
  "job-1234"        (sequential, not idempotent across restarts)
  ""                (no deduplication)
  UUID every time   (no deduplication)
```

### TTL Choice: 24 Hours

The 24-hour TTL means:
- Duplicate submissions within 24 hours are deduplicated
- After 24 hours, the same idempotency key can be reused (intentional re-run)
- Keeps Redis memory bounded (no infinite accumulation)

---

## Layer 2: PostgreSQL State Check (Processing-Time)

Prevents a job from being processed more than once even if it appears in the queue multiple times (e.g., after a worker crash recovery re-enqueues it).

### Mechanism

```go
func (w *Worker) processJob(ctx context.Context, jobJSON string) {
    job := parseJob(jobJSON)
    
    // Acquire processing lock in PostgreSQL
    updated, err := w.db.Exec(ctx, `
        UPDATE job_executions
        SET status = 'processing',
            started_at = NOW(),
            worker_id = $1
        WHERE job_id = $2
          AND status = 'pending'     -- Only transition from pending
    `, w.id, job.ID)
    
    if updated.RowsAffected() == 0 {
        // Job is not in 'pending' state
        // Either: already processing (another worker), completed, or cancelled
        // Safe to discard — no double processing
        return
    }
    
    // Set visibility timeout (30s) — if worker dies, job becomes visible again
    w.redis.SetEx(ctx, fmt.Sprintf("vis:%s", job.ID), w.id, 30*time.Second)
    
    // Execute the job handler
    result, err := w.executeHandler(ctx, job)
    
    // Mark complete or failed
    if err == nil {
        w.completeJob(ctx, job, result)
    } else {
        w.failJob(ctx, job, err)
    }
}
```

### Why Both Layers?

| Scenario | Redis Layer | PostgreSQL Layer |
|----------|-------------|------------------|
| Duplicate API call within 24h | ✅ Catches it | ✅ Would catch it too |
| Network timeout → producer retries | ✅ Catches it | ✅ Would catch it too |
| Two workers claim same job | ❌ Can't prevent (queue gives to one) | ✅ UPDATE WHERE status='pending' is atomic |
| Worker crash → re-enqueue → re-process | ❌ Key may be expired | ✅ Job status is 'processing' not 'pending' |
| Redis failure → PostgreSQL fallback | ❌ Unavailable | ✅ Still prevents duplicate processing |

Redis Layer = fast dedup at enqueue time (prevents queue flooding)
PostgreSQL Layer = authoritative dedup at execution time (prevents double processing)

---

## Failure Scenarios Handled

### Scenario 1: Network timeout during enqueue

```
Producer → POST /jobs → Network drops → Producer retries

Without idempotency: 2 jobs enqueued
With idempotency:    API checks dedup key, returns same job_id
```

### Scenario 2: Worker dies after processing, before completing

```
Worker dequeues job → Processes → Dies before DB update

Without idempotency: Job re-enqueued (via visibility timeout), processed again
With idempotency:    Visibility timeout expires → Job re-enqueued →
                     Next worker: UPDATE WHERE status='pending' → 0 rows affected →
                     Discarded (job_execution.result may be missing, but no double processing)
```

**Note:** This is the at-least-once vs exactly-once trade-off. In this case:
- Job processing is idempotent (handlers are designed to be re-runnable)
- The completion webhook might not have been sent → recovery loop checks for completed jobs with no webhook sent

### Scenario 3: Two workers pop same job

Impossible by design — Redis LIST's BRPOP is atomic; only one consumer receives each item.

### Scenario 4: Job re-enqueued due to crash recovery

```
Recovery goroutine detects stuck job (status=processing, vis timeout expired)
→ Re-enqueues job to Redis
→ Worker dequeues
→ PostgreSQL UPDATE WHERE status='pending' → 0 rows affected (still 'processing' from crash)
→ Worker discards job
```

Wait — this means the job is lost. The recovery goroutine must also reset the status:

```go
// Recovery goroutine:
updated, _ := db.Exec(`
    UPDATE job_executions
    SET status = 'pending',
        started_at = NULL,
        worker_id = NULL
    WHERE status = 'processing'
      AND started_at < NOW() - INTERVAL '35 seconds'
      AND NOT EXISTS (
        SELECT 1 FROM redis_visibility WHERE job_id = job_executions.job_id
      )
`)
// Then re-enqueue to Redis
```

---

## Trade-offs

**Deduplication window**: 24 hours is a business choice. Too long = can't intentionally re-run jobs. Too short = duplicates slip through for slow systems.

**Idempotency key responsibility**: The producer must supply meaningful keys. If the producer sends random UUIDs every time, deduplication doesn't work. This is a contract between producer and queue.

**Exactly-once is impossible**: True exactly-once delivery in a distributed system requires 2-phase commit, which is expensive. We provide at-least-once with idempotent handlers — the practical equivalent.
