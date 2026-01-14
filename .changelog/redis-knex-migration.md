# Redis + Knex migration notes

> **Breaking Change**: This release introduces a new storage layout for both Redis and Knex adapters. Existing data is incompatible and requires migration.

## New features

### Job retention

Jobs can now be kept in history after completion or failure using the `removeOnComplete` and `removeOnFail` options:

```typescript
// Global configuration
await QueueManager.init({
  // ...
  defaultJobOptions: {
    removeOnComplete: false, // Keep all completed jobs
    removeOnFail: { count: 100 }, // Keep last 100 failed jobs
  },
})

// Per-queue configuration
await QueueManager.init({
  // ...
  queues: {
    critical: {
      defaultJobOptions: {
        removeOnFail: { age: '7d', count: 1000 }, // Keep for 7 days, max 1000
      },
    },
  },
})

// Per-job configuration (via Job class)
class MyJob extends Job {
  static options = {
    removeOnComplete: { count: 50 },
  }
}
```

Retention options:
- `true` (default): Remove job immediately after completion/failure
- `false`: Keep job in history indefinitely
- `{ age?: Duration, count?: number }`: Keep with pruning by age and/or count

### Job status API

A new `getJob` method allows retrieving job status and data:

```typescript
const adapter = QueueManager.getAdapter()
const record = await adapter.getJob('job-id', 'queue-name')

if (record) {
  console.log(record.status) // 'pending' | 'active' | 'delayed' | 'completed' | 'failed'
  console.log(record.data) // Original job data
  console.log(record.finishedAt) // Timestamp (for completed/failed)
  console.log(record.error) // Error message (for failed)
}
```

## Redis adapter migration

### New storage layout

The Redis adapter now stores job payloads in a dedicated hash and tracks queue state in separate sets/hashes:

| Key | Type | Description |
|-----|------|-------------|
| `jobs::<queue>::data` | Hash | jobId -> job payload JSON |
| `jobs::<queue>::pending` | Sorted Set | jobId -> priority score |
| `jobs::<queue>::delayed` | Sorted Set | jobId -> executeAt timestamp |
| `jobs::<queue>::active` | Hash | jobId -> { workerId, acquiredAt } |
| `jobs::<queue>::completed` | Hash | jobId -> { finishedAt } |
| `jobs::<queue>::completed::index` | Sorted Set | jobId -> finishedAt (for pruning) |
| `jobs::<queue>::failed` | Hash | jobId -> { finishedAt, error } |
| `jobs::<queue>::failed::index` | Sorted Set | jobId -> finishedAt (for pruning) |

### Migration steps

**Option 1: Flush existing data (recommended for alpha users)**

```bash
redis-cli KEYS "jobs::*" | xargs redis-cli DEL
```

**Option 2: Wait for existing jobs to complete**

1. Stop pushing new jobs
2. Wait for all workers to drain existing queues
3. Deploy new version
4. Clean up old keys:

```bash
# Remove old format keys (adjust pattern to your prefix)
redis-cli KEYS "jobs::*" | grep -v "::data\|::pending\|::delayed\|::active\|::completed\|::failed" | xargs redis-cli DEL
```

## Knex adapter migration

### Schema changes

The Knex adapter now persists completed/failed job state. Existing tables need these changes:

| Column | Type | Description |
|--------|------|-------------|
| `finished_at` | BIGINT (nullable) | Completion/failure timestamp |
| `error` | TEXT (nullable) | Error message for failed jobs |
| `status` | ENUM | Add `completed` and `failed` values |

New index for pruning queries:
- `(queue, status, finished_at)`

### Migration SQL

**PostgreSQL:**

```sql
-- Add new columns
ALTER TABLE queue_jobs ADD COLUMN finished_at BIGINT;
ALTER TABLE queue_jobs ADD COLUMN error TEXT;

-- Add new enum values (PostgreSQL specific)
ALTER TYPE queue_jobs_status ADD VALUE 'completed';
ALTER TYPE queue_jobs_status ADD VALUE 'failed';

-- Add index for pruning
CREATE INDEX idx_queue_jobs_finished ON queue_jobs (queue, status, finished_at);
```

**MySQL:**

```sql
-- Add new columns
ALTER TABLE queue_jobs ADD COLUMN finished_at BIGINT UNSIGNED NULL;
ALTER TABLE queue_jobs ADD COLUMN error TEXT NULL;

-- Modify enum to include new values
ALTER TABLE queue_jobs MODIFY COLUMN status ENUM('pending', 'active', 'delayed', 'completed', 'failed') NOT NULL;

-- Add index for pruning
CREATE INDEX idx_queue_jobs_finished ON queue_jobs (queue, status, finished_at);
```

**SQLite:**

```sql
-- Add new columns (SQLite doesn't enforce enum, so status just works)
ALTER TABLE queue_jobs ADD COLUMN finished_at INTEGER;
ALTER TABLE queue_jobs ADD COLUMN error TEXT;

-- Add index for pruning
CREATE INDEX idx_queue_jobs_finished ON queue_jobs (queue, status, finished_at);
```

### Fresh install

For new installations, drop and recreate the table to get the new schema automatically:

```sql
DROP TABLE IF EXISTS queue_jobs;
-- Table will be recreated on first adapter use
```
