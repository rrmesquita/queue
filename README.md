# @boringnode/queue

<div align="center">

[![typescript-image]][typescript-url]
[![gh-workflow-image]][gh-workflow-url]
[![npm-image]][npm-url]
[![npm-download-image]][npm-download-url]
[![license-image]][license-url]

</div>

A simple and efficient queue system for Node.js applications. Built for simplicity and ease of use, `@boringnode/queue` allows you to dispatch background jobs and process them asynchronously with support for multiple queue adapters.

## Installation

```bash
npm install @boringnode/queue
```

## Features

- **Multiple Queue Adapters**: Support for Redis, Knex (PostgreSQL, MySQL, SQLite), and Sync
- **Type-Safe Jobs**: Define jobs as TypeScript classes with typed payloads
- **Delayed Jobs**: Schedule jobs to run after a specific delay
- **Multiple Queues**: Organize jobs into different queues for better organization
- **Worker Management**: Process jobs with configurable concurrency
- **Auto-Discovery**: Automatically discover and register jobs from specified locations
- **Priority Queues**: Process high-priority jobs first
- **Retry with Backoff**: Automatic retries with exponential, linear, or fixed backoff strategies
- **Job Timeout**: Automatically fail or retry jobs that exceed a time limit
- **Scheduled Jobs**: Cron-based or interval-based job scheduling with pause/resume support

## Quick Start

### 1. Define a Job

Create a job by extending the `Job` class:

```typescript
import { Job } from '@boringnode/queue'
import type { JobOptions } from '@boringnode/queue/types'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    console.log(`[Attempt ${this.context.attempt}] Sending email to: ${this.payload.to}`)
  }
}
```

> [!NOTE]
> The job name defaults to the class name (`SendEmailJob`). You can override it with `name: 'CustomName'` in options if needed.

> [!WARNING]
> If you minify your code in production, class names may be mangled. In that case, always specify `name` explicitly in your job options.

### 2. Configure the Queue Manager

```typescript
import { QueueManager } from '@boringnode/queue'
import { redis } from '@boringnode/queue/drivers/redis_adapter'
import { sync } from '@boringnode/queue/drivers/sync_adapter'
import { Redis } from 'ioredis'

const redisConnection = new Redis({
  host: 'localhost',
  port: 6379,
  keyPrefix: 'boringnode::queue::',
  db: 0,
})

const config = {
  default: 'redis',

  adapters: {
    sync: sync(),
    redis: redis(redisConnection),
  },

  worker: {
    concurrency: 5,
    idleDelay: '2s',
  },

  locations: ['./app/jobs/**/*.ts'],
}

await QueueManager.init(config)
```

### 3. Dispatch Jobs

```typescript
import SendEmailJob from './jobs/send_email_job.ts'

// Dispatch immediately
await SendEmailJob.dispatch({ to: 'user@example.com' })

// Dispatch with delay
await SendEmailJob.dispatch({ to: 'user@example.com' }).in('5m')
```

### 4. Start a Worker

Create a worker to process jobs:

```typescript
import { Worker } from '@boringnode/queue'

const worker = new Worker(config)
await worker.start(['default', 'email', 'reports'])
```

## Configuration

### Queue Manager Options

```typescript
interface QueueManagerConfig {
  // Default adapter to use
  default: string

  // Available queue adapters
  adapters: {
    [key: string]: QueueAdapter
  }

  // Worker configuration
  worker: {
    concurrency: number
    idleDelay: Duration
  }

  // Job discovery locations
  locations: string[]
}
```

### Job Options

Configure individual jobs with the `options` property:

```typescript
static options: JobOptions = {
  queue: 'email',       // Queue name (default: 'default')
  adapter: 'redis',     // Override default adapter
  priority: 1,          // Lower number = higher priority (default: 5)
  maxRetries: 3,        // Maximum retry attempts
  timeout: '30s',       // Job timeout duration
  failOnTimeout: true,  // Fail permanently on timeout (default: false, will retry)
}
```

## Adapters

### Redis Adapter

For production use with distributed systems:

```typescript
import { redis } from '@boringnode/queue/drivers/redis_adapter'
import { Redis } from 'ioredis'

const redisConnection = new Redis({
  host: 'localhost',
  port: 6379,
  keyPrefix: 'boringnode::queue::',
})

const adapter = redis(redisConnection)
```

### Sync Adapter

For testing and development:

```typescript
import { sync } from '@boringnode/queue/drivers/sync_adapter'

const adapter = sync()
```

### Knex Adapter

For SQL databases (PostgreSQL, MySQL, SQLite) using Knex:

```typescript
import { knex } from '@boringnode/queue/drivers/knex_adapter'

// With configuration (adapter manages connection lifecycle)
const adapter = knex({
  client: 'pg',
  connection: {
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'postgres',
    database: 'myapp',
  },
})

// Or with an existing Knex instance (you manage connection lifecycle)
import Knex from 'knex'

const connection = Knex({ client: 'pg', connection: '...' })
const adapter = knex(connection)
```

The adapter automatically creates the `queue_jobs` table on first use. You can customize the table name:

```typescript
const adapter = knex(config, 'custom_jobs_table')
```

## Worker Configuration

Workers process jobs from one or more queues:

```typescript
const worker = new Worker(config)

// Process specific queues
await worker.start(['default', 'email', 'reports'])

// Worker will:
// - Process jobs with configured concurrency
// - Poll queues at the configured interval
// - Execute jobs in the order they were queued
```

## Delayed Jobs

Schedule jobs to run in the future:

```typescript
// Various time formats
await SendEmailJob.dispatch(payload).in('30s') // 30 seconds
await SendEmailJob.dispatch(payload).in('5m') // 5 minutes
await SendEmailJob.dispatch(payload).in('2h') // 2 hours
await SendEmailJob.dispatch(payload).in('1d') // 1 day
```

## Priority

Jobs with lower priority numbers are processed first:

```typescript
export default class UrgentJob extends Job<Payload> {
  static options: JobOptions = {
    priority: 1, // Processed before default priority (5)
  }

  async execute(): Promise<void> {
    // ...
  }
}
```

## Retry and Backoff

Configure automatic retries with backoff strategies:

```typescript
import { exponentialBackoff, linearBackoff, fixedBackoff } from '@boringnode/queue'

export default class ReliableJob extends Job<Payload> {
  static options: JobOptions = {
    maxRetries: 5,
    retry: {
      backoff: () =>
        exponentialBackoff({
          baseDelay: '1s',
          maxDelay: '1m',
          multiplier: 2,
          jitter: true,
        }),
    },
  }

  async execute(): Promise<void> {
    // ...
  }
}
```

Available backoff strategies:

- `exponentialBackoff({ baseDelay, maxDelay, multiplier, jitter })` - Exponential increase
- `linearBackoff({ baseDelay, maxDelay, multiplier })` - Linear increase
- `fixedBackoff({ baseDelay, jitter })` - Fixed delay between retries

## Job Timeout

Set a maximum execution time for jobs:

```typescript
export default class LimitedJob extends Job<Payload> {
  static options: JobOptions = {
    timeout: '30s', // Maximum execution time
    failOnTimeout: false, // Retry on timeout (default)
  }

  async execute(): Promise<void> {
    // Long running operation...
  }
}
```

You can also set a global timeout in the worker configuration:

```typescript
const config = {
  worker: {
    timeout: '1m', // Default timeout for all jobs
  },
}
```

### Handling Timeout Gracefully

Jobs have access to an abort signal via `this.signal` to handle timeouts gracefully:

```typescript
export default class LongRunningJob extends Job<Payload> {
  static options: JobOptions = {
    timeout: '30s',
  }

  async execute(): Promise<void> {
    for (const item of this.payload.items) {
      // Check if the job has been aborted
      if (this.signal?.aborted) {
        throw new Error('Job timed out')
      }

      await this.processItem(item)
    }
  }

  private async processItem(item: any): Promise<void> {
    // Pass the signal to fetch or other async operations
    await fetch(item.url, { signal: this.signal })
  }
}
```

## Job Context

Every job has access to execution context via `this.context`. This provides metadata about the current job execution:

```typescript
import { Job } from '@boringnode/queue'

export default class MyJob extends Job<Payload> {
  async execute(): Promise<void> {
    console.log(`Job ID: ${this.context.jobId}`)
    console.log(`Attempt: ${this.context.attempt}`) // 1, 2, 3...
    console.log(`Queue: ${this.context.queue}`)
    console.log(`Priority: ${this.context.priority}`)
    console.log(`Acquired at: ${this.context.acquiredAt}`)

    if (this.context.attempt > 1) {
      console.log('This is a retry!')
    }
  }
}
```

### Context Properties

| Property       | Type   | Description                                     |
|----------------|--------|-------------------------------------------------|
| `jobId`        | string | Unique identifier for this job                  |
| `name`         | string | Job class name                                  |
| `attempt`      | number | Current attempt number (1-based)                |
| `queue`        | string | Queue name this job is being processed from     |
| `priority`     | number | Job priority (lower = higher priority)          |
| `acquiredAt`   | Date   | When this job was acquired by the worker        |
| `stalledCount` | number | Times this job was recovered from stalled state |

## Dependency Injection

Use the `jobFactory` option to integrate with IoC containers for dependency injection. The constructor is reserved for injecting dependencies - payload and context are provided separately by the worker.

```typescript
import { QueueManager } from '@boringnode/queue'

await QueueManager.init({
  default: 'redis',
  adapters: { redis: redis(connection) },
  jobFactory: async (JobClass) => {
    // Use your IoC container to instantiate jobs
    return app.container.make(JobClass)
  },
})
```

Example with injected dependencies:

```typescript
import { Job } from '@boringnode/queue'

interface SendEmailPayload {
  to: string
  subject: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  constructor(
    private mailer: MailerService, // Injected by IoC container
    private logger: Logger // Injected by IoC container
  ) {
    super()
  }

  async execute(): Promise<void> {
    this.logger.info(`[Attempt ${this.context.attempt}] Sending email to ${this.payload.to}`)
    await this.mailer.send(this.payload)
  }
}
```

Without a `jobFactory`, jobs are instantiated with `new JobClass()`.

## Scheduled Jobs

Schedule jobs to run on a recurring basis using cron expressions or fixed intervals. Schedules are persisted and survive worker restarts.

### Creating a Schedule

```typescript
import { Schedule } from '@boringnode/queue'

// Run every 10 seconds (uses job name as schedule ID by default)
const { scheduleId } = await MetricsJob.schedule({ endpoint: '/api/health' }).every('10s').run()

// Run on a cron schedule with custom ID
await CleanupJob.schedule({ days: 30 })
  .id('daily-cleanup') // Custom ID (optional, defaults to job name)
  .cron('0 * * * *') // Every hour at minute 0
  .timezone('Europe/Paris') // Optional timezone (default: UTC)
  .run()

// Schedule with constraints
await ReportJob.schedule({ type: 'weekly' })
  .id('weekly-report')
  .cron('0 9 * * MON') // Every Monday at 9am
  .from(new Date('2024-01-01')) // Start date
  .to(new Date('2024-12-31')) // End date
  .limit(52) // Maximum 52 runs
  .run()
```

### Managing Schedules

```typescript
import { Schedule } from '@boringnode/queue'

// Find a schedule by ID
const schedule = await Schedule.find('health-check')

if (schedule) {
  console.log(`Status: ${schedule.status}`) // 'active' or 'paused'
  console.log(`Run count: ${schedule.runCount}`)
  console.log(`Next run: ${schedule.nextRunAt}`)
  console.log(`Last run: ${schedule.lastRunAt}`)

  // Pause the schedule
  await schedule.pause()

  // Resume the schedule
  await schedule.resume()

  // Trigger an immediate run (outside of the normal schedule)
  await schedule.trigger()

  // Delete the schedule
  await schedule.delete()
}
```

### Listing Schedules

```typescript
import { Schedule } from '@boringnode/queue'

// List all schedules
const all = await Schedule.list()

// Filter by status
const active = await Schedule.list({ status: 'active' })
const paused = await Schedule.list({ status: 'paused' })
```

### Schedule Options

| Method               | Description                                     |
|----------------------|-------------------------------------------------|
| `.id(string)`        | Unique identifier (defaults to job name)        |
| `.every(duration)`   | Run at fixed intervals ('5s', '1m', '1h', '1d') |
| `.cron(expression)`  | Run on a cron schedule                          |
| `.timezone(tz)`      | Timezone for cron expressions (default: 'UTC')  |
| `.from(date)`        | Don't run before this date                      |
| `.to(date)`          | Don't run after this date                       |
| `.between(from, to)` | Shorthand for `.from().to()`                    |
| `.limit(n)`          | Maximum number of runs                          |

### How Scheduling Works

- Schedules are **persisted** in the database (via the adapter)
- The **Worker** polls for due schedules and dispatches jobs automatically
- Each schedule run creates a **new job** with a unique ID
- Multiple workers can run concurrently - only one will claim each due schedule
- Failed jobs do **not** affect the schedule (the next run will still occur)

## Job Discovery

The queue manager automatically discovers and registers jobs from the specified locations:

```typescript
const config = {
  locations: ['./app/jobs/**/*.ts', './modules/**/jobs/**/*.ts'],
}
```

Jobs must:

- Extend the `Job` class
- Implement the `execute` method
- Be exported as default

The job name is automatically derived from the class name, or can be explicitly set via `static options = { name: 'CustomName' }`.

## Logging

You can pass a logger to the queue manager for debugging or monitoring. The logger must be compatible with the [pino](https://github.com/pinojs/pino) interface.

```typescript
import { pino } from 'pino'

const config = {
  default: 'redis',
  adapters: {
    /* ... */
  },
  logger: pino(),
}

await QueueManager.init(config)
```

By default, a simple console logger is used that only outputs warnings and errors.

## Benchmarks

Performance comparison with BullMQ using realistic jobs (5ms simulated work per job):

| Jobs | Concurrency | @boringnode/queue | BullMQ | Diff        |
|------|-------------|-------------------|--------|-------------|
| 100  | 1           | 562ms             | 596ms  | 5.7% faster |
| 100  | 5           | 116ms             | 117ms  | ~same       |
| 100  | 10          | 62ms              | 62ms   | ~same       |
| 500  | 1           | 2728ms            | 2798ms | 2.5% faster |
| 500  | 5           | 565ms             | 565ms  | ~same       |
| 500  | 10          | 287ms             | 288ms  | ~same       |
| 1000 | 1           | 5450ms            | 5547ms | 1.7% faster |
| 1000 | 5           | 1096ms            | 1116ms | 1.8% faster |
| 1000 | 10          | 565ms             | 579ms  | 2.4% faster |
| 100K | 5           | 110.5s            | 112.3s | 1.5% faster |
| 100K | 10          | 56.2s             | 57.5s  | 2.1% faster |
| 100K | 20          | 29.1s             | 29.6s  | 1.7% faster |

Run benchmarks yourself:

```bash
# Realistic benchmark (5ms job duration)
npm run benchmark -- --realistic

# Pure dequeue overhead (no-op jobs)
npm run benchmark

# Custom job duration
npm run benchmark -- --duration=10
```

[gh-workflow-image]: https://img.shields.io/github/actions/workflow/status/boringnode/queue/checks.yml?branch=main&style=for-the-badge
[gh-workflow-url]: https://github.com/boringnode/queue/actions/workflows/checks.yml
[npm-image]: https://img.shields.io/npm/v/@boringnode/queue.svg?style=for-the-badge&logo=npm
[npm-url]: https://www.npmjs.com/package/@boringnode/queue
[npm-download-image]: https://img.shields.io/npm/dm/@boringnode/queue?style=for-the-badge
[npm-download-url]: https://www.npmjs.com/package/@boringnode/queue
[typescript-image]: https://img.shields.io/badge/Typescript-294E80.svg?style=for-the-badge&logo=typescript
[typescript-url]: https://www.typescriptlang.org
[license-image]: https://img.shields.io/npm/l/@boringnode/queue?color=blueviolet&style=for-the-badge
[license-url]: LICENSE.md
