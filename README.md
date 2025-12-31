# @boringnode/queue

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

## Quick Start

### 1. Define a Job

Create a job by extending the `Job` class:

```typescript
import { Job } from '@boringnode/queue'
import type { JobOptions } from '@boringnode/queue/types/main'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static readonly jobName = 'SendEmailJob'

  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    console.log(`Sending email to: ${this.payload.to}`)
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
    pollingInterval: '10ms',
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
    pollingInterval: string
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
await SendEmailJob.dispatch(payload).in('30s')  // 30 seconds
await SendEmailJob.dispatch(payload).in('5m')   // 5 minutes
await SendEmailJob.dispatch(payload).in('2h')   // 2 hours
await SendEmailJob.dispatch(payload).in('1d')   // 1 day
```

## Priority

Jobs with lower priority numbers are processed first:

```typescript
export default class UrgentJob extends Job<Payload> {
  static readonly jobName = 'UrgentJob'

  static options: JobOptions = {
    priority: 1,  // Processed before default priority (5)
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
      backoff: () => exponentialBackoff({
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
    timeout: '30s',       // Maximum execution time
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
    timeout: '1m',  // Default timeout for all jobs
  },
}
```

## Job Discovery

The queue manager automatically discovers and registers jobs from the specified locations:

```typescript
const config = {
  locations: [
    './app/jobs/**/*.ts',
    './modules/**/jobs/**/*.ts',
  ],
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

| Jobs | Concurrency | @boringnode/queue | BullMQ | Diff         |
|------|-------------|-------------------|--------|--------------|
| 100  | 1           | 562ms             | 596ms  | 5.7% faster  |
| 100  | 5           | 116ms             | 117ms  | ~same        |
| 100  | 10          | 62ms              | 62ms   | ~same        |
| 500  | 1           | 2728ms            | 2798ms | 2.5% faster  |
| 500  | 5           | 565ms             | 565ms  | ~same        |
| 500  | 10          | 287ms             | 288ms  | ~same        |
| 1000 | 1           | 5450ms            | 5547ms | 1.7% faster  |
| 1000 | 5           | 1096ms            | 1116ms | 1.8% faster  |
| 1000 | 10          | 565ms             | 579ms  | 2.4% faster  |
| 100K | 5           | 110.5s            | 112.3s | 1.5% faster  |
| 100K | 10          | 56.2s             | 57.5s  | 2.1% faster  |
| 100K | 20          | 29.1s             | 29.6s  | 1.7% faster  |

Run benchmarks yourself:

```bash
# Realistic benchmark (5ms job duration)
npm run benchmark -- --realistic

# Pure dequeue overhead (no-op jobs)
npm run benchmark

# Custom job duration
npm run benchmark -- --duration=10
```
