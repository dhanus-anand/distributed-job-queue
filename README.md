# Distributed Job Queue System

A production-grade distributed background job processing system built in Go — demonstrating fault tolerance, retry logic, idempotency, and observability. Used by the [Internal Tool Platform](../project-1-internal-tool-platform) for background operations.

**"Handle background jobs reliably at scale with visibility, retries, and failure handling."**

> **Portfolio context:** This is Project 2 of 3. Origin story: while building the [Internal Tool Platform](../project-1-internal-tool-platform), bulk CSV imports and exports needed reliable async processing. Instead of bolting async logic onto a Node.js backend, the right architectural decision was a purpose-built job queue — written in Go to learn the language through a real problem. Go's goroutine model is a natural fit for a concurrent worker pool.

---

## What It Does

A standalone job queue system (similar to AWS SQS + a processing layer, or Sidekiq for Go) that:

- Accepts jobs via HTTP API
- Distributes them across multiple workers
- Retries failed jobs with exponential backoff
- Routes exhausted-retry jobs to a Dead Letter Queue
- Provides real-time visibility via Prometheus + Grafana
- Recovers automatically from worker crashes (visibility timeout)
- Guarantees at-least-once delivery via two-layer idempotency

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Go 1.21+ |
| Queue Storage | Redis 7 (Lists, Sorted Sets, Sets) |
| Metadata Storage | PostgreSQL 16 (job state, history, DLQ) |
| HTTP API | Gin framework |
| Monitoring | Prometheus + Grafana |
| Load Testing | Vegeta |
| Deployment | Docker Compose (local, multi-worker simulation) |

---

## Architecture

```
                    ┌─────────────────┐
  Producer          │  HTTP API       │
  (Project 1  ─────▶│  POST /jobs     │
   or any app)      │  GET  /jobs/:id │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Redis          │
                    │  ─────────────  │
                    │  queues (Lists) │
                    │  dedup (Sets)   │
                    │  delayed (ZSet) │
                    │  vis. timeout   │
                    └────────┬────────┘
                             │  BRPOP
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼────┐ ┌───────▼─────┐ ┌─────▼───────┐
     │  Worker 1   │ │  Worker 2   │ │  Worker 3   │
     │  10 gorout. │ │  10 gorout. │ │  10 gorout. │
     └────────┬────┘ └───────┬─────┘ └─────┬───────┘
              └──────────────┼──────────────┘
                             │
                    ┌────────▼────────┐
                    │  PostgreSQL     │
                    │  job_executions │
                    │  (state + hist) │
                    └─────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Webhook        │
                    │  Callback       │
                    │  (to Project 1) │
                    └─────────────────┘
```

---

## API Endpoints

```
POST   /jobs              Enqueue a new job
GET    /jobs/:id          Get job status and result
DELETE /jobs/:id          Cancel a pending job
GET    /queues            List all queues with depth and stats
GET    /queues/:name      Get specific queue stats
POST   /webhooks          Register a webhook for job completion
GET    /metrics           Prometheus metrics endpoint
GET    /health            Health check
```

### Enqueue a Job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "bulk_import",
    "idempotency_key": "import-ws-123-file-456",
    "payload": {
      "workspace_id": "uuid",
      "tool_id": "uuid",
      "file_url": "https://r2.example.com/file.csv"
    },
    "options": {
      "max_retries": 5,
      "priority": "normal"
    }
  }'
```

**Response:**
```json
{
  "job_id": "uuid",
  "status": "pending",
  "queue": "import_queue",
  "enqueued_at": "2024-01-15T10:00:00Z"
}
```

### Get Job Status

```bash
curl http://localhost:8080/jobs/uuid
```

```json
{
  "job_id": "uuid",
  "type": "bulk_import",
  "status": "completed",
  "attempts": 1,
  "result": { "imported": 4987, "errors": 3 },
  "enqueued_at": "2024-01-15T10:00:00Z",
  "started_at": "2024-01-15T10:00:05Z",
  "completed_at": "2024-01-15T10:01:30Z",
  "duration_ms": 85000
}
```

---

## Queue Types

| Queue | Job Types | Workers |
|-------|-----------|---------|
| `import_queue` | bulk_import | Shared pool |
| `export_queue` | bulk_export | Shared pool |
| `report_queue` | report_generation | Shared pool |
| `email_queue` | email_send | Shared pool |
| `dead_letter_queue` | Failed jobs (all types) | Manual inspection |

---

## Project Structure

```
project-2-distributed-job-queue/
├── cmd/
│   ├── server/main.go      # HTTP API server entrypoint
│   └── worker/main.go      # Worker process entrypoint
├── internal/
│   ├── queue/              # Redis queue operations (enqueue, dequeue)
│   ├── worker/             # Worker pool, job handlers, goroutine management
│   └── storage/            # PostgreSQL job state persistence
├── docker-compose.yml      # Redis + PostgreSQL + Prometheus + Grafana + 3 workers
├── prometheus.yml          # Metrics scrape config
└── grafana/dashboards/     # Pre-built Grafana dashboards
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose

```bash
# Clone
git clone <repo-url>
cd project-2-distributed-job-queue

# Start all services (Redis, PostgreSQL, server, 3 workers, Prometheus, Grafana)
docker-compose up --build

# Or start dependencies only and run locally:
docker-compose up redis postgres prometheus grafana -d

# Run server
go run ./cmd/server

# Run worker (in another terminal)
go run ./cmd/worker
```

**Service URLs:**
- API: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001 (admin/admin)

### Running Load Tests

```bash
# Install vegeta
brew install vegeta

# 1000 req/sec for 30 seconds
echo "POST http://localhost:8080/jobs
Content-Type: application/json
@test/fixtures/sample_job.json" | vegeta attack -duration=30s -rate=1000 | vegeta report
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Queue flow, worker coordination, storage strategy |
| [IDEMPOTENCY.md](IDEMPOTENCY.md) | Two-layer deduplication design |
| [PARTITIONING.md](PARTITIONING.md) | Job type vs hash partitioning |
| [FAILURE_HANDLING.md](FAILURE_HANDLING.md) | Retry logic, DLQ, worker crash recovery |
| [SCALING.md](SCALING.md) | 1k → 1M+ jobs/day scaling path |
| [INTEGRATION.md](INTEGRATION.md) | How Project 1 uses this queue |
| [OPUS_AGENT_INSTRUCTIONS.md](OPUS_AGENT_INSTRUCTIONS.md) | Build instructions |

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Enqueue throughput | 10,000 jobs/sec |
| Enqueue latency | p99 < 10ms |
| Processing throughput | 1,000 jobs/sec (10 workers) |
| Worker crash recovery | < 30 seconds |
| Total simulated jobs | 50,000 |

---

## Monitoring

Prometheus metrics exposed on each worker and the API server:

```
jobs_enqueued_total{type="bulk_import"}
jobs_processed_total{type="bulk_import", status="success"}
jobs_failed_total{type="bulk_import", reason="max_retries"}
queue_depth{queue="import_queue"}
job_processing_duration_seconds{type="bulk_import", quantile="0.95"}
worker_goroutines_active{worker_id="worker-1"}
dlq_depth{queue="dead_letter_queue"}
```

---

## Cost

| Service | Cost |
|---------|------|
| Everything | **$0** — runs locally with Docker Compose |
| AWS production equivalent | ~$50-100/month (see SCALING.md) |
