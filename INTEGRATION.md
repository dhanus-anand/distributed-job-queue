# Integration — How Project 1 Uses This Queue

Documentation for producers (specifically the Internal Tool Platform) integrating with this job queue.

---

## Integration Pattern

```
Project 1 (Producer)          Job Queue (This Project)
       │                              │
       │  POST /jobs                  │
       │  {type, payload, callback}   │
       │─────────────────────────────▶│
       │                              │
       │  {job_id, status: "pending"} │
       │◀─────────────────────────────│
       │                              │
       │  (async processing)          │
       │                              │
       │  POST {callback_url}         │
       │  {job_id, status, result}    │
       │◀─────────────────────────────│
       │                              │
```

---

## Job Payload Specifications

### bulk_import

Import records from a CSV/JSON file into a tool.

```json
POST /jobs
{
  "type": "bulk_import",
  "idempotency_key": "bulk_import-{workspace_id}-{file_id}-{timestamp}",
  "payload": {
    "workspace_id": "uuid",
    "tool_id": "uuid",
    "file_url": "https://bucket.r2.cloudflarestorage.com/imports/uuid.csv",
    "file_type": "csv",
    "tool_schema": {
      "fields": [
        {"name": "email", "type": "email", "required": true},
        {"name": "name", "type": "string"},
        {"name": "status", "type": "enum", "options": ["active", "inactive"]}
      ]
    },
    "options": {
      "skip_header_row": true,
      "on_duplicate": "skip",
      "batch_size": 500
    },
    "callback_url": "https://platform.railway.app/api/internal/webhooks/job-complete",
    "webhook_secret": "hmac-secret-for-signature-verification"
  },
  "options": {
    "max_retries": 3,
    "queue": "import_queue"
  }
}
```

**Callback on success:**
```json
{
  "job_id": "uuid",
  "type": "bulk_import",
  "status": "completed",
  "result": {
    "total_rows": 5000,
    "imported": 4987,
    "skipped_duplicates": 10,
    "validation_errors": 3,
    "error_details": [
      {"row": 42, "field": "email", "message": "Invalid email format: 'not-an-email'"},
      {"row": 156, "field": "status", "message": "Invalid enum: 'pending' not in [active, inactive]"},
      {"row": 890, "field": "email", "message": "Required field missing"}
    ]
  }
}
```

**Column mapping (CSV):** The worker reads the first row as headers. Headers are matched to tool schema field names (case-insensitive, underscore-normalized). Unrecognized headers are ignored.

---

### bulk_export

Export all (or filtered) tool records to a file, upload to storage, return download URL.

```json
POST /jobs
{
  "type": "bulk_export",
  "idempotency_key": "bulk_export-{workspace_id}-{tool_id}-{request_hash}",
  "payload": {
    "workspace_id": "uuid",
    "tool_id": "uuid",
    "format": "csv",
    "filters": {
      "status": "active",
      "created_at_after": "2024-01-01"
    },
    "columns": ["email", "name", "status", "created_at"],
    "upload": {
      "bucket": "itp-exports",
      "key_prefix": "workspaces/{workspace_id}/exports/",
      "presign_expiry_hours": 24
    },
    "callback_url": "https://platform.railway.app/api/internal/webhooks/job-complete"
  }
}
```

**Callback on success:**
```json
{
  "job_id": "uuid",
  "type": "bulk_export",
  "status": "completed",
  "result": {
    "record_count": 4987,
    "file_size_bytes": 2097152,
    "file_name": "customers-2024-01-15.csv",
    "download_url": "https://bucket.r2.cloudflarestorage.com/exports/uuid.csv?X-Signature=...",
    "download_expires_at": "2024-01-16T10:00:00Z"
  }
}
```

---

### bulk_delete

Delete many records from a tool asynchronously.

```json
POST /jobs
{
  "type": "bulk_delete",
  "idempotency_key": "bulk_delete-{workspace_id}-{sha256_of_record_ids}",
  "payload": {
    "workspace_id": "uuid",
    "tool_id": "uuid",
    "record_ids": ["uuid1", "uuid2", "...up to 10000"],
    "initiated_by_user_id": "uuid",
    "callback_url": "https://platform.railway.app/api/internal/webhooks/job-complete"
  }
}
```

**Callback on success:**
```json
{
  "job_id": "uuid",
  "type": "bulk_delete",
  "status": "completed",
  "result": {
    "requested": 1000,
    "deleted": 997,
    "not_found": 3,
    "audit_log_entry_id": "uuid"
  }
}
```

---

### report_generation

Generate a structured report (PDF/Excel) for a workspace.

```json
POST /jobs
{
  "type": "report_generation",
  "payload": {
    "workspace_id": "uuid",
    "report_type": "tool_summary",
    "params": {
      "tool_id": "uuid",
      "date_range": {"from": "2024-01-01", "to": "2024-12-31"},
      "group_by": "status"
    },
    "format": "pdf",
    "callback_url": "https://platform.railway.app/api/internal/webhooks/job-complete"
  }
}
```

---

## Webhook Callback Protocol

### Signature Verification

Every callback is signed with HMAC-SHA256 using the `webhook_secret` provided at enqueue time:

```
X-Webhook-Signature: sha256=<hmac_hex>
X-Webhook-Job-ID: <job_id>
X-Webhook-Timestamp: <unix_timestamp>
```

**Verification (Project 1 side):**
```typescript
const signature = req.headers['x-webhook-signature'];
const timestamp = req.headers['x-webhook-timestamp'];
const body = req.rawBody; // raw bytes, not parsed

// Prevent replay attacks: reject if timestamp > 5 minutes old
if (Math.abs(Date.now() / 1000 - parseInt(timestamp)) > 300) {
  throw new UnauthorizedException('Webhook timestamp too old');
}

const message = `${timestamp}.${body}`;
const expected = 'sha256=' + createHmac('sha256', WEBHOOK_SECRET)
  .update(message)
  .digest('hex');

if (!timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) {
  throw new UnauthorizedException('Invalid webhook signature');
}
```

### Callback Retry Policy

If the callback URL returns a non-2xx response, the queue system retries the webhook:
- Retry after: 5s, 30s, 5m, 30m (4 attempts total)
- After 4 failures: Log the failure, mark webhook as "undelivered" in PostgreSQL
- Project 1 polling fallback: If webhook not received, poll `GET /jobs/:id` every 60s

---

## Error Handling on Client Side (Project 1)

### Failed Job Handling

When a `status: "failed"` callback is received:

```typescript
switch (result.error.code) {
  case 'IMPORT_PARSE_ERROR':
    // File was malformed — show user-friendly error with details
    await notifyUser(workspaceId, `Import failed: ${result.error.message}`);
    break;
    
  case 'IMPORT_VALIDATION_ERRORS':
    // Some rows failed validation — show error report
    await storeImportErrors(jobId, result.error_details);
    await notifyUser(workspaceId, `Import completed with ${result.error_details.length} errors`);
    break;
    
  case 'STORAGE_UPLOAD_FAILED':
    // Export failed to upload — allow retry
    await markJobRetryable(jobId);
    break;
    
  default:
    await notifyUser(workspaceId, 'Operation failed. Please try again.');
}
```

### Job Polling Fallback

```typescript
// Background job in Project 1 — polls job queue for missed webhooks
async function reconcileJobs() {
  const staleJobs = await prisma.jobRecord.findMany({
    where: {
      status: 'PROCESSING',
      updatedAt: { lt: new Date(Date.now() - 5 * 60 * 1000) } // >5 min stale
    }
  });
  
  for (const job of staleJobs) {
    const remoteStatus = await jobQueueClient.getJob(job.externalJobId);
    if (remoteStatus.status !== 'processing') {
      await this.processWebhookCallback(remoteStatus);
    }
  }
}
```

---

## Standalone Operation (Without Project 1)

The job queue works with any HTTP producer. Example: enqueue a job from the command line:

```bash
# Enqueue a custom job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email_send",
    "idempotency_key": "welcome-email-user-123",
    "payload": {
      "to": "user@example.com",
      "template": "welcome",
      "data": {"name": "Alice"}
    },
    "options": {
      "max_retries": 5,
      "queue": "email_queue"
    }
  }'
```

To register a custom job handler, implement the `JobHandler` interface:

```go
type JobHandler func(ctx context.Context, job Job) (any, error)

// Register in worker main:
worker.RegisterHandler("email_send", handlers.NewEmailHandler(smtpClient))
worker.RegisterHandler("custom_task", handlers.NewCustomTaskHandler())
```
