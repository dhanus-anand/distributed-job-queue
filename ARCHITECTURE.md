# Architecture — Distributed Job Queue System

## System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Producer (any HTTP client)               │
│                    e.g., Project 1 (Internal Tool Platform) │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP POST /jobs
┌───────────────────────────▼─────────────────────────────────┐
│                    API Server (Gin)                         │
│                                                             │
│  POST /jobs        GET /jobs/:id     GET /queues            │
│  DELETE /jobs/:id  GET /metrics      GET /health            │
└──────────┬──────────────────────────────────────────────────┘
           │ LPUSH to queue
┌──────────▼──────────────────────────────────────────────────┐
│                    Redis                                    │
│                                                             │
│  ┌─────────────────┐   ┌──────────────────────────────────┐ │
│  │  Queue Lists    │   │  Deduplication Sets              │ │
│  │  import_queue   │   │  SET dedup:import_queue:<key>    │ │
│  │  export_queue   │   │  (TTL: 24h)                      │ │
│  │  report_queue   │   └──────────────────────────────────┘ │
│  │  email_queue    │                                        │
│  └─────────────────┘   ┌──────────────────────────────────┐ │
│                        │  Delayed Jobs (Sorted Set)       │ │
│  ┌─────────────────┐   │  ZADD delayed_jobs <timestamp>   │ │
│  │  Visibility     │   │  (for retry backoff scheduling)  │ │
│  │  Timeout Keys   │   └──────────────────────────────────┘ │
│  │  vis:<job_id>   │                                        │
│  │  (TTL: 30s)     │                                        │
│  └─────────────────┘                                        │
└──────────┬──────────────────────────────────────────────────┘
           │ BRPOP (blocking pop)
┌──────────▼──────────────────────────────────────────────────┐
│                    Worker Pool                              │
│                                                             │
│  Worker Process 1          Worker Process 2                 │
│  ┌────────────────────┐    ┌────────────────────┐           │
│  │ Goroutine Pool     │    │ Goroutine Pool     │           │
│  │ ─────────────────  │    │ ─────────────────  │           │
│  │ goroutine 1: job A │    │ goroutine 1: job D │           │
│  │ goroutine 2: job B │    │ goroutine 2: job E │           │
│  │ goroutine 3: job C │    │ ...                │           │
│  │ ...                │    └────────────────────┘           │
│  └────────────────────┘                                     │
└──────────┬──────────────────────────────────────────────────┘
           │ Writes job state
┌──────────▼──────────────────────────────────────────────────┐
│                    PostgreSQL                               │
│                                                             │
│  job_executions  (id, job_id, status, attempts,            │
│                   result, error, started_at, completed_at)  │
│  dead_letter     (id, job_id, type, payload, error,        │
│                   failed_at, expires_at)                    │
└──────────┬──────────────────────────────────────────────────┘
           │ Webhook callback
┌──────────▼──────────────────────────────────────────────────┐
│                    Producer Callback                        │
│                    POST {callback_url} with result          │
└─────────────────────────────────────────────────────────────┘
```

---

## Job Lifecycle

```
1. ENQUEUE
   Producer calls POST /jobs
        │
        ▼
   API checks Redis dedup set:
   EXISTS dedup:<queue>:<idempotency_key>
        │
        ├─ YES → Return existing job_id (idempotent)
        │
        └─ NO  → Proceed
              │
              ▼
        PostgreSQL: INSERT job_executions (status='pending')
              │
              ▼
        Redis: SETEX dedup:<queue>:<key> 86400 <job_id>
              │
              ▼
        Redis: LPUSH <queue_name> <job_json>
              │
              ▼
        Return 202 Accepted { job_id, status: "pending" }

2. DEQUEUE (Worker)
   Worker calls Redis: BRPOP <queue_name> 5 (5s timeout)
        │
        ▼
   Worker receives job JSON
        │
        ▼
   PostgreSQL: Check job_executions.status
        ├─ 'cancelled' → Discard, no processing
        └─ 'pending'   → Proceed
              │
              ▼
        Redis: SETEX vis:<job_id> 30 <worker_id>
        (visibility timeout: 30 seconds)
              │
              ▼
        PostgreSQL: UPDATE status='processing', started_at=NOW()
              │
              ▼
        Execute job handler

3. JOB SUCCESS
   Handler completes successfully
        │
        ▼
   Redis: DEL vis:<job_id>
        │
        ▼
   PostgreSQL: UPDATE status='completed', result=..., completed_at=NOW()
        │
        ▼
   Send webhook callback to {callback_url}
        │
        ▼
   Done

4. JOB FAILURE
   Handler returns error (or panics)
        │
        ▼
   Redis: DEL vis:<job_id>
        │
        ▼
   PostgreSQL: UPDATE attempts=attempts+1, last_error=error
        │
        ├─ attempts < max_retries → Schedule retry:
        │        Redis: ZADD delayed_jobs <future_timestamp> <job_json>
        │        PostgreSQL: UPDATE status='pending'
        │
        └─ attempts >= max_retries → Move to DLQ:
                 PostgreSQL: INSERT dead_letter (...)
                 PostgreSQL: UPDATE job_executions.status='dead'
                 Redis: LPUSH dead_letter_queue <job_json>

5. WORKER CRASH (Visibility Timeout Recovery)
   Worker dies while processing job
        │
        ▼
   30 seconds pass → Redis vis:<job_id> key expires
        │
        ▼
   Recovery goroutine (in API server or separate process):
   Scans PostgreSQL for jobs with status='processing' AND
   started_at < NOW() - 30s AND vis:<job_id> not in Redis
        │
        ▼
   For each stuck job:
   PostgreSQL: UPDATE status='pending', attempts=attempts+1
        │
        ▼
   Redis: LPUSH <queue_name> <job_json>
   (Job returns to queue for another worker to pick up)
```

---

## Storage Strategy

### Why Redis + PostgreSQL (Not Just Redis)

| Concern | Redis | PostgreSQL |
|---------|-------|------------|
| Queue operations (LPUSH/BRPOP) | ✅ Optimal | ❌ Too slow |
| Deduplication (SET with TTL) | ✅ Atomic, TTL | ✅ But slower |
| Job history / audit | ❌ No persistence guarantees | ✅ ACID |
| Complex queries (jobs by workspace) | ❌ Not designed for this | ✅ SQL |
| DLQ inspection | ❌ No structure | ✅ Queryable |
| Worker crash detection | ❌ Without persistence risk | ✅ Reliable |

Redis handles the high-speed queue operations. PostgreSQL handles durability, history, and complex queries.

### Redis Data Structures Used

```
Lists (queues):
  import_queue          → [job_json, job_json, ...]  (LPUSH/BRPOP)
  export_queue          → [...]
  report_queue          → [...]
  email_queue           → [...]
  dead_letter_queue     → [...]

Sets (deduplication):
  dedup:import_queue:<key>  → job_id  (TTL: 86400s)

Sorted Sets (delayed jobs / retry scheduling):
  delayed_jobs  → { job_json: timestamp }  (ZADD score=execute_at)

Strings (visibility timeout):
  vis:<job_id>  → worker_id  (TTL: 30s)
```

### PostgreSQL Schema

```sql
-- Job execution tracking
CREATE TABLE job_executions (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id        UUID NOT NULL UNIQUE,
  queue         VARCHAR(100) NOT NULL,
  type          VARCHAR(100) NOT NULL,
  payload       JSONB NOT NULL,
  status        VARCHAR(20) NOT NULL DEFAULT 'pending',
  -- pending | processing | completed | failed | dead | cancelled
  attempts      INTEGER NOT NULL DEFAULT 0,
  max_retries   INTEGER NOT NULL DEFAULT 5,
  result        JSONB,
  last_error    TEXT,
  callback_url  TEXT,
  idempotency_key VARCHAR(255),
  enqueued_at   TIMESTAMP NOT NULL DEFAULT NOW(),
  started_at    TIMESTAMP,
  completed_at  TIMESTAMP,
  next_retry_at TIMESTAMP
);

CREATE INDEX idx_job_executions_status ON job_executions(status);
CREATE INDEX idx_job_executions_queue ON job_executions(queue, status);
CREATE INDEX idx_job_executions_started ON job_executions(started_at)
  WHERE status = 'processing';

-- Dead letter queue metadata
CREATE TABLE dead_letter (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id     UUID NOT NULL REFERENCES job_executions(job_id),
  queue      VARCHAR(100) NOT NULL,
  type       VARCHAR(100) NOT NULL,
  payload    JSONB NOT NULL,
  last_error TEXT,
  attempts   INTEGER NOT NULL,
  failed_at  TIMESTAMP NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMP NOT NULL  -- AUTO cleanup after 7 days
);

CREATE INDEX idx_dead_letter_expires ON dead_letter(expires_at);
```

---

## Worker Architecture

### Worker Process

Each worker process is an independent binary (`./cmd/worker`):

```go
type Worker struct {
    id          string
    concurrency int          // Number of goroutines
    queues      []string     // Queues to listen on
    redis       *redis.Client
    db          *pgx.Pool
    handlers    map[string]JobHandler
    semaphore   chan struct{} // Controls max concurrent jobs
    stopCh      chan struct{}
    wg          sync.WaitGroup
}
```

### Goroutine Pool

```go
func (w *Worker) Start(ctx context.Context) {
    // Launch `concurrency` goroutines, each blocking on BRPOP
    for i := 0; i < w.concurrency; i++ {
        w.wg.Add(1)
        go w.runLoop(ctx)
    }
    w.wg.Wait()
}

func (w *Worker) runLoop(ctx context.Context) {
    defer w.wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        default:
            result, err := w.redis.BRPop(ctx, 5*time.Second, w.queues...).Result()
            if err != nil {
                continue // Timeout or disconnect — retry
            }
            w.processJob(ctx, result[1]) // result[0]=queue name, result[1]=job JSON
        }
    }
}
```

### Graceful Shutdown

```go
// On SIGTERM/SIGINT:
// 1. Stop accepting new jobs (cancel context)
// 2. Wait for in-flight jobs to complete (w.wg.Wait() with timeout)
// 3. Exit

func (w *Worker) Shutdown(ctx context.Context) error {
    w.cancel() // Stop new dequeues
    
    done := make(chan struct{})
    go func() {
        w.wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        return nil // Clean shutdown
    case <-ctx.Done():
        return fmt.Errorf("shutdown timeout: %d jobs still running", w.activeJobs())
    }
}
```

---

## Delayed Job Scheduler

A separate goroutine moves jobs from the delayed set to the active queue when their scheduled time arrives:

```go
func (s *Scheduler) Run(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Second)
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := float64(time.Now().Unix())
            // Get all jobs scheduled for <= now
            jobs, _ := s.redis.ZRangeByScoreWithScores(ctx, "delayed_jobs",
                &redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%f", now)}).Result()
            
            for _, job := range jobs {
                s.redis.ZRem(ctx, "delayed_jobs", job.Member)
                s.redis.LPush(ctx, getQueueForJob(job.Member), job.Member)
            }
        }
    }
}
```

---

## Observability

Prometheus metrics are registered and incremented throughout the codebase:

```go
var (
    jobsEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "jobs_enqueued_total",
        Help: "Total jobs enqueued",
    }, []string{"type", "queue"})

    jobsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "jobs_processed_total",
        Help: "Total jobs processed",
    }, []string{"type", "status"})

    jobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "job_processing_duration_seconds",
        Help:    "Job processing duration",
        Buckets: prometheus.DefBuckets,
    }, []string{"type"})

    queueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Name: "queue_depth",
        Help: "Current queue depth",
    }, []string{"queue"})
)
```
