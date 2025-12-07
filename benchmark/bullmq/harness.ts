import { Queue, Worker, type ConnectionOptions } from 'bullmq'
import { Redis } from 'ioredis'
import { barrier, type BenchmarkOptions, type BenchmarkResult } from '../helpers.ts'

async function clearQueue(connection: Redis) {
  const keys = await connection.keys('bull:benchmark:*')
  if (keys.length > 0) {
    await connection.del(...keys)
  }
}

export async function run(options: BenchmarkOptions): Promise<BenchmarkResult> {
  const connection = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
    maxRetriesPerRequest: null,
  })

  await clearQueue(connection)

  const queue = new Queue('benchmark', {
    connection: connection as ConnectionOptions,
    defaultJobOptions: {
      removeOnComplete: true,
      removeOnFail: true,
    },
  })

  // Enqueue all jobs first (before worker starts)
  for (let i = 0; i < options.numRuns; ++i) {
    await queue.add('job', { i })
  }

  const { done, next } = barrier(options.numRuns)

  const startTime = Date.now()

  // Create worker AFTER all jobs are enqueued (pure dequeue test)
  const worker = new Worker(
    'benchmark',
    async () => {
      // No-op - just measure queue overhead
      next()
    },
    {
      connection: connection as ConnectionOptions,
      concurrency: options.concurrency,
    }
  )

  // Wait for all jobs to complete
  await done

  const elapsed = Date.now() - startTime

  // Cleanup
  await worker.close()
  await queue.close()
  await clearQueue(connection)
  await connection.quit()

  return {
    library: 'BullMQ',
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
          `Ran ${result.numRuns} jobs through BullMQ with concurrency ${result.concurrency} in ${result.elapsed}ms`
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
