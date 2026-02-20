# Failure Handling — Distributed Job Queue

How the system handles retries, worker crashes, network failures, and the Dead Letter Queue.

---

## Retry Logic with Exponential Backoff

When a job fails, it is not immediately re-queued. It's scheduled for a future retry using exponential backoff to avoid overwhelming a struggling downstream service.

### Backoff Schedule

```
Attempt 1 (immediate)  → Wait 1s  → Retry at T+1s
Attempt 2              → Wait 2s  → Retry at T+3s
Attempt 3              → Wait 4s  → Retry at T+7s
Attempt 4              → Wait 8s  → Retry at T+15s
Attempt 5              → Wait 16s → Retry at T+31s
Attempt 6 (max=5)      → Wait 32s → Dead Letter Queue
```

**Formula:** `delay = min(2^(attempt-1), max_delay_seconds)` (default max: 300s)

### Implementation

```go
func (w *Worker) failJob(ctx context.Context, job Job, err error) {
    attempts := job.Attempts + 1
    
    if attempts >= job.MaxRetries {
        // Move to Dead Letter Queue
        w.moveToDLQ(ctx, job, err)
        return
    }
    
    // Calculate next retry time using exponential backoff
    delaySecs := math.Pow(2, float64(attempts-1))
    delaySecs = math.Min(delaySecs, 300) // Cap at 5 minutes
    nextRetryAt := time.Now().Add(time.Duration(delaySecs) * time.Second)
    
    // Update PostgreSQL
    w.db.Exec(ctx, `
        UPDATE job_executions
        SET status = 'pending',
            attempts = $1,
            last_error = $2,
            next_retry_at = $3
        WHERE job_id = $4
    `, attempts, err.Error(), nextRetryAt, job.ID)
    
    // Schedule in Redis Sorted Set (score = Unix timestamp to execute)
    updatedJob := job.WithAttempts(attempts)
    w.redis.ZAdd(ctx, "delayed_jobs", redis.Z{
        Score:  float64(nextRetryAt.Unix()),
        Member: updatedJob.ToJSON(),
    })
    
    metrics.jobsFailed.WithLabelValues(job.Type, "retry_scheduled").Inc()
}
```

### Retry Conditions

Jobs are retried on:
- Any unhandled error from the handler function
- Network errors (HTTP 5xx from downstream services)
- Timeout errors
- Panic/recovery (worker catches panics)

Jobs are **not** retried on:
- Validation errors (data problem, won't fix itself)
- Resource not found (404 from downstream)
- Explicit `ErrNoRetry` returned by handler

```go
// Handlers can signal "don't retry this"
var ErrNoRetry = errors.New("non-retryable error")

func bulkImportHandler(ctx context.Context, job Job) (any, error) {
    if isMalformedCSV(job.Payload.FileURL) {
        return nil, fmt.Errorf("%w: CSV file is corrupt", ErrNoRetry)
    }
    // ...
}

// In failJob:
if errors.Is(err, ErrNoRetry) {
    w.moveToDLQ(ctx, job, err) // Skip retries, go straight to DLQ
    return
}
```

---

## Visibility Timeout Mechanism

The visibility timeout prevents a job from being processed by multiple workers simultaneously and provides automatic recovery when a worker crashes.

### How It Works

```
Worker dequeues job via BRPOP
           │
           ▼
Worker sets: SETEX vis:<job_id> 30 <worker_id>
           │
           ▼
Worker processes job (expected: < 30 seconds)
           │
    ┌──────┴──────┐
    │             │
  Normal       Crash
  Completion    │
    │           │
    ▼           ▼
  DEL         30 seconds pass →
  vis:<job_id>  vis:<job_id> KEY EXPIRES
    │           │
    ▼           ▼
  Job done    Recovery goroutine detects
              stuck job → re-enqueues
```

### Heartbeat Extension for Long-Running Jobs

For jobs that legitimately take longer than 30 seconds (e.g., large CSV imports):

```go
func (w *Worker) processJobWithHeartbeat(ctx context.Context, job Job) {
    // Extend visibility timeout while job is running
    heartbeat := time.NewTicker(10 * time.Second)
    defer heartbeat.Stop()
    
    go func() {
        for {
            select {
            case <-heartbeat.C:
                w.redis.Expire(ctx, fmt.Sprintf("vis:%s", job.ID), 30*time.Second)
            case <-ctx.Done():
                return
            }
        }
    }()
    
    // Execute handler (potentially long-running)
    result, err := w.executeHandler(ctx, job)
    // ...
}
```

### Recovery Goroutine

Runs in the API server process (can also run as a separate process):

```go
func (r *Recovery) Run(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            r.recoverStuckJobs(ctx)
        }
    }
}

func (r *Recovery) recoverStuckJobs(ctx context.Context) {
    // Find jobs that have been "processing" for > 35 seconds
    // (30s visibility + 5s buffer for clock skew)
    rows, _ := r.db.Query(ctx, `
        SELECT job_id, queue, payload, attempts, max_retries
        FROM job_executions
        WHERE status = 'processing'
          AND started_at < NOW() - INTERVAL '35 seconds'
    `)
    
    for rows.Next() {
        var job JobExecution
        rows.Scan(&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.MaxRetries)
        
        // Double-check: is visibility key still in Redis?
        exists, _ := r.redis.Exists(ctx, fmt.Sprintf("vis:%s", job.ID)).Result()
        if exists > 0 {
            continue // Still alive, skip
        }
        
        // Worker is dead, recover the job
        if job.Attempts >= job.MaxRetries {
            r.moveToDLQ(ctx, job, "worker_crash_max_retries_exceeded")
        } else {
            r.requeue(ctx, job)
        }
    }
}
```

---

## Dead Letter Queue (DLQ)

Jobs that exhaust all retries are moved to the DLQ for manual inspection and optional re-processing.

### DLQ Storage

```go
func (w *Worker) moveToDLQ(ctx context.Context, job Job, err error) {
    // Write to PostgreSQL dead_letter table (persistent, queryable)
    w.db.Exec(ctx, `
        INSERT INTO dead_letter (job_id, queue, type, payload, last_error, attempts, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW() + INTERVAL '7 days')
    `, job.ID, job.Queue, job.Type, job.Payload, err.Error(), job.Attempts)
    
    // Update job_executions
    w.db.Exec(ctx, `
        UPDATE job_executions
        SET status = 'dead', last_error = $1, completed_at = NOW()
        WHERE job_id = $2
    `, err.Error(), job.ID)
    
    // Also push to Redis DLQ for visibility in monitoring
    w.redis.LPush(ctx, "dead_letter_queue", job.WithError(err.Error()).ToJSON())
    
    metrics.dlqDepth.WithLabelValues(job.Queue).Inc()
}
```

### DLQ API

```
GET  /dlq                        # List DLQ jobs (paginated)
GET  /dlq/:job_id               # Get specific failed job
POST /dlq/:job_id/retry         # Re-enqueue job (resets attempts to 0)
DELETE /dlq/:job_id             # Discard job permanently
POST /dlq/retry-all             # Retry all DLQ jobs of a specific type
```

### DLQ Automatic Cleanup

```sql
-- Daily cron: Remove DLQ entries older than 7 days
DELETE FROM dead_letter WHERE expires_at < NOW();
```

---

## Network Partition Handling

### Redis Unavailable

If Redis is down:
- API server: Returns 503 Service Unavailable (cannot enqueue)
- Workers: Pause processing, retry Redis connection with exponential backoff
- Recovery: PostgreSQL still tracks job state

```go
func (w *Worker) connectWithRetry(ctx context.Context) {
    backoff := time.Second
    for {
        if err := w.redis.Ping(ctx).Err(); err == nil {
            return
        }
        log.Printf("Redis unavailable, retrying in %v", backoff)
        time.Sleep(backoff)
        backoff = min(backoff*2, 30*time.Second)
    }
}
```

### PostgreSQL Unavailable

If PostgreSQL is down:
- Jobs can still be enqueued to Redis (degraded mode, no persistence)
- Workers cannot update job state → pause processing, retry connection
- Risk: If Redis loses data during PostgreSQL outage, job history is lost

**Mitigation:** Redis AOF (Append-Only File) persistence enabled for durability.

---

## Panic Recovery

Worker goroutines catch panics to prevent one bad job from killing the worker:

```go
func (w *Worker) executeHandler(ctx context.Context, job Job) (result any, err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
        }
    }()
    
    handler, ok := w.handlers[job.Type]
    if !ok {
        return nil, fmt.Errorf("%w: unknown job type %s", ErrNoRetry, job.Type)
    }
    
    return handler(ctx, job)
}
```

---

## Chaos Testing

The test suite includes a chaos testing framework:

```bash
# Kill random workers
./scripts/chaos.sh kill-workers 2   # Kill 2 of 3 workers

# Verify jobs complete (via recovery or surviving workers)
./scripts/chaos.sh verify-completion --timeout 120s

# Simulate Redis network partition (using tc/iptables)
./scripts/chaos.sh block-redis 30s

# Verify DLQ population on repeated failures
./scripts/chaos.sh exhaust-retries --job-type always_fail
```

### Chaos Test: Worker Kill

```
1. Enqueue 1000 jobs
2. Kill 2 of 3 workers (docker-compose kill worker-1 worker-2)
3. Wait 35 seconds (visibility timeout + buffer)
4. Verify: Recovery goroutine re-enqueued stuck jobs
5. Verify: All 1000 jobs eventually complete (worker-3 processes them)
6. Verify: No duplicate processing (idempotency check in PostgreSQL)
```
