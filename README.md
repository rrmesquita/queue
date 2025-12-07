# @boringnode/queue

A simple and efficient queue system for Node.js applications. Built for simplicity and ease of use, `@boringnode/queue` allows you to dispatch background jobs and process them asynchronously with support for multiple queue adapters.

## Installation

```bash
npm install @boringnode/queue
```

## Features

- **Multiple Queue Adapters**: Support for Redis (production) and Sync (testing/development)
- **Type-Safe Jobs**: Define jobs as TypeScript classes with typed payloads
- **Delayed Jobs**: Schedule jobs to run after a specific delay
- **Multiple Queues**: Organize jobs into different queues for better organization
- **Worker Management**: Process jobs with configurable concurrency
- **Auto-Discovery**: Automatically discover and register jobs from specified locations

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

## Benchmarks

Performance comparison with BullMQ measuring pure dequeue overhead (jobs are no-ops). Results are averaged over 3 runs:

| Jobs | Concurrency | @boringnode/queue | BullMQ | Diff         |
|------|-------------|-------------------|--------|--------------|
| 100  | 1           | 15ms              | 23ms   | 34.8% faster |
| 100  | 5           | 24ms              | 18ms   | 33.3% slower |
| 100  | 10          | 16ms              | 17ms   | ~same        |
| 1000 | 1           | 171ms             | 135ms  | 26.7% slower |
| 1000 | 5           | 106ms             | 55ms   | 92.7% slower |
| 1000 | 10          | 88ms              | 57ms   | 54.4% slower |
| 5000 | 1           | 495ms             | 615ms  | 19.5% faster |
| 5000 | 5           | 342ms             | 253ms  | 35.2% slower |
| 5000 | 10          | 456ms             | 234ms  | 94.9% slower |

These numbers represent queue overhead only. With real job execution, the difference becomes negligible.

Run benchmarks yourself:

```bash
npm run benchmark
```
