# Scaling Strategy — Distributed Job Queue

From local Docker Compose development to AWS production handling 1M+ jobs/day.

---

## Current: Local Docker Compose ($0/month)

```
docker-compose up
├── Redis 1x (single instance, 256MB limit)
├── PostgreSQL 1x
├── API Server 1x
├── Workers 3x (each with 10 goroutines = 30 total concurrent jobs)
├── Prometheus 1x
└── Grafana 1x

Capacity:
  Enqueue: ~500-1000 jobs/sec (Redis single-threaded write)
  Process: ~30 concurrent jobs
  Daily capacity: ~1-5k jobs/day
  Redis memory: ~500MB for 10k queued jobs
```

---

## Tier 2: Railway/Fly.io (~$20/month)

Target: 10,000 jobs/day

```
Railway / Fly.io
├── API Server          1 container, 512MB RAM
├── Worker Process      2 containers, 256MB each, 10 goroutines each
├── PostgreSQL          Railway plugin or Neon free tier
└── Redis               Upstash Pro ($10/month, 10M commands/month)

Changes from local:
  - Workers are standalone containers (no docker-compose)
  - Redis is managed (Upstash)
  - PostgreSQL is managed
  - Prometheus metrics exposed, scraped externally (Grafana Cloud free tier)
```

**Bottlenecks at this tier:**
- Upstash Redis: Latency ~5-10ms (vs <1ms local) → impacts enqueue throughput
- Single API server: no horizontal scaling
- Workers can't scale beyond 2 containers on free tier

---

## Tier 3: AWS Production (~$100-150/month)

Target: 100,000+ jobs/day

### Architecture

```
Internet
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Route 53 → ALB → ECS Fargate (API Server, 2-4 tasks)      │
│                   ECS Fargate (Workers, 5-10 tasks)         │
│                   ECS Service autoscaling                   │
└──────────┬──────────────────────────────────────────────────┘
           │
     ┌─────┴────────────────────────────────────┐
     │                │                         │
┌────▼──────┐   ┌──────▼──────┐        ┌───────▼──────┐
│ElastiCache│   │  RDS        │        │  CloudWatch   │
│ Redis     │   │  PostgreSQL │        │  + Grafana    │
│ Cluster   │   │  (primary + │        │  (monitoring) │
│ (3 nodes) │   │  1 replica) │        └──────────────┘
└───────────┘   └─────────────┘
```

### AWS Services

| Service | Configuration | Cost |
|---------|---------------|------|
| ECS Fargate (API) | 2 tasks, 0.5 vCPU, 1GB RAM | ~$20/month |
| ECS Fargate (Workers) | 5 tasks, 1 vCPU, 2GB RAM each | ~$60/month |
| ElastiCache Redis | 3-node cluster, cache.t3.micro | ~$30/month |
| RDS PostgreSQL | db.t3.micro, Multi-AZ | ~$30/month |
| ALB | - | ~$15/month |
| CloudWatch | Basic metrics | ~$5/month |
| **Total** | | **~$160/month** |

### Autoscaling Workers Based on Queue Depth

```
CloudWatch alarm: queue_depth > 1000
    │
    ▼
ECS autoscaling: Scale out worker tasks
    │
    ▼
CloudWatch alarm: queue_depth < 100 for 5 minutes
    │
    ▼
ECS autoscaling: Scale in worker tasks (min: 2)
```

Custom CloudWatch metric from Prometheus:
```bash
# Push queue_depth to CloudWatch every 60s
aws cloudwatch put-metric-data \
  --namespace JobQueue \
  --metric-name QueueDepth \
  --dimensions Queue=import_queue \
  --value $(redis-cli LLEN import_queue)
```

---

## Scaling to 1M+ Jobs/Day

### Bottleneck Analysis at 100k jobs/day

| Component | At 100k/day | Bottleneck? |
|-----------|------------|-------------|
| Redis enqueue (LPUSH) | 1 write/sec average | No — Redis handles 100k+ writes/sec |
| Redis dequeue (BRPOP) | 1 read/sec per goroutine | No |
| Redis dedup (SET NX) | 1 read/sec average | No |
| PostgreSQL inserts | ~1 row/sec average | No — can handle ~10k TPS |
| PostgreSQL updates | ~1 row/sec average | No |
| Workers | 30 goroutines = fine | No |

**Not a bottleneck until:** ~1M jobs/day (11 jobs/sec average, but bursts of 100+ jobs/sec)

### Scaling to 1M Jobs/Day

**Primary change: Multiple worker instances with more goroutines**

```
10 worker processes × 20 goroutines = 200 concurrent jobs
At 200 concurrent jobs × 60 seconds average = 720,000 jobs/hour
```

**Secondary change: Redis Cluster**

```
Single Redis: ~100k ops/sec
Redis Cluster (6 nodes): ~600k ops/sec
→ 1M jobs/day = 11 jobs/sec average = well within Redis single-node capacity
→ Redis Cluster is only needed at sustained 50k+ jobs/sec (4B+ jobs/day)
```

**Tertiary change: PostgreSQL read replica**

```
Read replica for:
  - GET /jobs/:id (status queries)
  - GET /queues (stats)
  - Recovery queries

Primary only for:
  - INSERT (enqueue)
  - UPDATE (status changes)
```

**Hash partitioning triggers at:**

```
1M+ jobs/day consistently
  → Redis single-instance CPU > 70%
  → Switch to hash partitioning: 4 queues × 4 Redis shards
  → 4x throughput capacity
```

### AWS Architecture at 1M Jobs/Day

```
ECS Workers: 20 tasks × 20 goroutines = 400 concurrent jobs
ECS API: 4 tasks (read replicated)
ElastiCache: Redis 6-node cluster
RDS: db.r6g.large + 2 read replicas
Estimated cost: ~$400-600/month
```

---

## Comparison: Custom Queue vs AWS SQS

| Feature | This System | AWS SQS |
|---------|------------|---------|
| Throughput | Up to 10k msg/sec | Unlimited (managed) |
| Visibility timeout | ✅ Configurable | ✅ Configurable |
| Dead letter queue | ✅ Implemented | ✅ Built-in |
| Retry logic | ✅ Custom (exponential) | ❌ Client-side only |
| Idempotency | ✅ Two-layer | ❌ At-least-once only |
| Observability | ✅ Custom Prometheus | Limited (CloudWatch) |
| Multi-queue | ✅ Job type partitioning | ✅ Multiple queues |
| Cost at 1M/day | ~$200/month | ~$0.40/month |
| Operational burden | High | Zero |
| Learning value | ✅ High | ❌ Zero |

**Conclusion for portfolio:** Building this from scratch demonstrates distributed systems depth. In production, you'd use SQS (or Temporal for orchestration). The point is understanding the internals.

---

## Monitoring at Scale

### Key Metrics to Watch

```
Queue depth trending up → Not enough workers, scale out
Job p95 duration increasing → Handler is slow, profile and optimize
DLQ depth increasing → Systematic errors, check handler logs
Worker count decreasing → Worker crashes, check crash logs + visibility timeout
Redis memory growing → Job payloads too large, or backlog accumulating
```

### Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| queue_depth | > 500 | > 5000 |
| job_processing_duration p95 | > 30s | > 120s |
| dlq_depth | > 50 | > 500 |
| worker_goroutines_active / max | > 80% | > 95% |
| jobs_failed_total (rate) | > 10/min | > 100/min |
