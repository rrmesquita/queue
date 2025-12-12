import { setTimeout } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { Worker } from '#src/worker'
import { Job } from '#src/job'
import { Locator } from '#src/locator'
import { redis } from '#drivers/redis_adapter'
import { barrier, type BenchmarkOptions, type BenchmarkResult } from '../helpers.js'
import type { QueueManagerConfig } from '#types/main'

// Barrier callback and job duration - set before each benchmark run
let onJobComplete: (() => boolean) | null = null
let jobDuration: number = 0

class BenchmarkJob extends Job<{ i: number }> {
  async execute() {
    if (jobDuration > 0) {
      await setTimeout(jobDuration)
    }
    onJobComplete?.()
  }
}

async function clearQueue(host: string, port: number) {
  const cleanupConnection = new Redis({ host, port })
  const keys = await cleanupConnection.keys('boringnode::queue::*')
  if (keys.length > 0) {
    await cleanupConnection.del(...keys)
  }
  await cleanupConnection.quit()
}

export async function run(options: BenchmarkOptions): Promise<BenchmarkResult> {
  const host = process.env.REDIS_HOST || 'localhost'
  const port = Number.parseInt(process.env.REDIS_PORT || '6379', 10)

  const connection = new Redis({
    host,
    port,
    keyPrefix: 'boringnode::queue::',
  })

  await clearQueue(host, port)

  // Setup barrier for completion tracking and job duration
  const { done, next } = barrier(options.numRuns)
  onJobComplete = next
  jobDuration = options.jobDuration ?? 0

  const config: QueueManagerConfig = {
    default: 'redis',
    adapters: {
      redis: redis(connection),
    },
    locations: [''],
    worker: {
      concurrency: options.concurrency,
      pollingInterval: 1, // Very short polling for benchmarks
    },
  }

  Locator.register('BenchmarkJob', BenchmarkJob)

  const adapter = config.adapters.redis()
  const worker = new Worker(config)

  // Enqueue all jobs first
  for (let i = 0; i < options.numRuns; ++i) {
    await adapter.pushOn('default', {
      id: `job-${i}`,
      name: 'BenchmarkJob',
      payload: { i },
      attempts: 0,
    })
  }

  const startTime = Date.now()

  // Start worker in background
  void worker.start(['default'])

  // Wait for all jobs to complete
  await done

  const elapsed = Date.now() - startTime

  // Stop worker (also closes the connection) and cleanup
  await worker.stop()
  await clearQueue(host, port)

  Locator.clear()

  return {
    library: '@boringnode/queue',
    numRuns: options.numRuns,
    concurrency: options.concurrency,
    elapsed,
    jobsPerSecond: (options.numRuns / elapsed) * 1000,
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const numRuns = Number.parseInt(process.env.NUM_RUNS || '1000', 10)
  const concurrency = Number.parseInt(process.env.CONCURRENCY || '1', 10)

  run({ numRuns, concurrency })
    .then((result) => {
      if (process.stdout.isTTY) {
        console.log(
          `Ran ${result.numRuns} jobs through @boringnode/queue with concurrency ${result.concurrency} in ${result.elapsed}ms`
        )
      } else {
        console.log(result.elapsed)
      }
    })
    .catch((error) => {
      console.error('Benchmark failed:', error)
      process.exit(1)
    })
}
