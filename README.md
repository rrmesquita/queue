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
- **Repeating Jobs**: Schedule jobs to repeat at fixed intervals

## Quick Start

### 1. Define a Job

Create a job by extending the `Job` class:

```typescript
import { Job } from '@boringnode/queue'
import type { JobContext, JobOptions } from '@boringnode/queue/types'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static readonly jobName = 'SendEmailJob'

  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    console.log(`[Attempt ${this.context.attempt}] Sending email to: ${this.payload.to}`)
  }
}
```

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
await SendEmailJob.dispatch(payload).in('5m')  // 5 minutes
await SendEmailJob.dispatch(payload).in('2h')  // 2 hours
await SendEmailJob.dispatch(payload).in('1d')  // 1 day
```

## Priority

Jobs with lower priority numbers are processed first:

```typescript
export default class UrgentJob extends Job<Payload> {
  static readonly jobName = 'UrgentJob'

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
  static readonly jobName = 'ReliableJob'

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
  static readonly jobName = 'LimitedJob'

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

## Job Context

Every job has access to execution context via `this.context`. This provides metadata about the current job execution:

```typescript
import { Job } from '@boringnode/queue'
import type { JobContext } from '@boringnode/queue'

export default class MyJob extends Job<Payload> {
  constructor(payload: Payload, context: JobContext) {
    super(payload, context)
  }

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

| Property          | Type                | Description                                       |
|-------------------|---------------------|---------------------------------------------------|
| `jobId`           | string              | Unique identifier for this job                    |
| `name`            | string              | Job class name                                    |
| `attempt`         | number              | Current attempt number (1-based)                  |
| `queue`           | string              | Queue name this job is being processed from       |
| `priority`        | number              | Job priority (lower = higher priority)            |
| `acquiredAt`      | Date                | When this job was acquired by the worker          |
| `stalledCount`    | number              | Times this job was recovered from stalled state   |
| `isRepeating`     | boolean             | Whether this job is configured to repeat          |
| `repeatRemaining` | number \| undefined | Remaining repetitions (undefined = infinite)      |
| `repeatId`        | string \| undefined | Unique ID for the repeat chain (for cancellation) |

## Dependency Injection

Use the `jobFactory` option to integrate with IoC containers for dependency injection. This allows your jobs to receive injected services in their constructor.

```typescript
import { QueueManager } from '@boringnode/queue'

await QueueManager.init({
  default: 'redis',
  adapters: { redis: redis(connection) },
  jobFactory: async (JobClass, payload, context) => {
    // Use your IoC container to instantiate jobs
    return app.container.make(JobClass, [payload, context])
  },
})
```

Example with injected dependencies:

```typescript
import { Job } from '@boringnode/queue'
import type { JobContext } from '@boringnode/queue'

interface SendEmailPayload {
  to: string
  subject: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static readonly jobName = 'SendEmailJob'

  constructor(
    payload: SendEmailPayload,
    context: JobContext,
    private mailer: MailerService, // Injected by IoC container
    private logger: Logger // Injected by IoC container
  ) {
    super(payload, context)
  }

  async execute(): Promise<void> {
    this.logger.info(`[Attempt ${this.context.attempt}] Sending email to ${this.payload.to}`)
    await this.mailer.send(this.payload)
  }
}
```

Without a `jobFactory`, jobs are instantiated with `new JobClass(payload, context)`.

## Repeating Jobs

Schedule jobs to repeat automatically at fixed intervals:

```typescript
// Repeat every 5 seconds indefinitely
await SyncJob.dispatch({ source: 'api' }).every('5s')

// Repeat every hour, 10 times total
await CleanupJob.dispatch({ days: 30 }).every('1h').times(10)

// Combine with delay (start after 30 seconds, then repeat every minute)
await ReportJob.dispatch({ type: 'daily' }).in('30s').every('1m')
```

### Cancelling a Repeating Job

When dispatching a repeating job, you receive a `repeatId` that can be used to cancel the entire repeat chain from anywhere:

```typescript
import { QueueManager } from '@boringnode/queue'

// Dispatch returns jobId and repeatId
const { jobId, repeatId } = await SyncJob.dispatch({ source: 'api' }).every('5s')

console.log(`Started repeating job ${jobId} with repeat chain ${repeatId}`)

// Later, cancel the repeat chain from anywhere
if (repeatId) {
  await QueueManager.cancelRepeat(repeatId)
}
```

The `repeatId` is also available inside the job via `this.context.repeatId`.

### Stopping from Within the Job

A job can stop its own repetition by calling `this.stopRepeating()`:

```typescript
import { Job } from '@boringnode/queue'
import type { JobContext } from '@boringnode/queue/types'

export default class SyncJob extends Job<SyncPayload> {
  static readonly jobName = 'SyncJob'

  async execute(): Promise<void> {
    const result = await this.syncData()

    // Stop repeating when sync is complete
    if (result.isComplete) {
      this.stopRepeating()
    }
  }
}
```

### Repeat Context

Jobs have access to repeat information via `this.context`:

```typescript
async execute(): Promise<void> {
  if (this.context.isRepeating) {
    console.log(`Repeating job, ${this.context.repeatRemaining ?? 'infinite'} runs remaining`)
  }
}
```

| Property          | Type                | Description                                       |
|-------------------|---------------------|---------------------------------------------------|
| `isRepeating`     | boolean             | Whether this job is configured to repeat          |
| `repeatRemaining` | number \| undefined | Remaining repetitions (undefined = infinite)      |
| `repeatId`        | string \| undefined | Unique ID for the repeat chain (for cancellation) |

### How Repeating Works

- Each repeat creates a **new job** with a new ID
- The payload is **preserved** across repeats
- Failed jobs do **not** repeat (only successful completions trigger the next run)
- The repeat interval is the delay **between** job completions

## Job Discovery

The queue manager automatically discovers and registers jobs from the specified locations:

```typescript
const config = {
  locations: ['./app/jobs/**/*.ts', './modules/**/jobs/**/*.ts'],
}
```

Jobs must:

- Extend the `Job` class
- Have a static `jobName` property
- Implement the `execute` method
- Be exported as default

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
