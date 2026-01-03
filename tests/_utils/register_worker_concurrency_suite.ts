import { test as JapaTest } from '@japa/runner'
import { setTimeout } from 'node:timers/promises'
import { Worker } from '../../src/worker.js'
import { Locator } from '../../src/locator.js'
import { Job } from '../../src/job.js'
import type { Adapter } from '../../src/contracts/adapter.js'
import type { QueueManagerConfig } from '../../src/types/main.js'

interface WorkerConcurrencyTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
}

export function registerWorkerConcurrencyTestSuite(options: WorkerConcurrencyTestSuiteOptions) {
  const { test } = options

  test('single job should be processed exactly once with concurrency > 1', async ({
    assert,
    cleanup,
  }) => {
    const jobExecutions: Map<string, number> = new Map()

    class TrackingJob extends Job<{ jobId: string }> {
      async execute() {
        const count = jobExecutions.get(this.payload.jobId) || 0
        jobExecutions.set(this.payload.jobId, count + 1)
        await setTimeout(50)
      }
    }

    const adapter = await options.createAdapter()
    Locator.register(TrackingJob.name, TrackingJob)

    cleanup(() => Locator.clear())

    const config: QueueManagerConfig = {
      default: 'test',
      adapters: { test: () => adapter },
      worker: { concurrency: 5 },
    }

    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    // Push a single job
    await adapter.pushOn('default', {
      id: 'single-job-1',
      name: 'TrackingJob',
      payload: { jobId: 'job-1' },
      attempts: 0,
    })

    // Process until idle
    let cycles = 0
    const maxCycles = 20
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++
      if (cycle?.type === 'idle') break
    }

    assert.equal(
      jobExecutions.get('job-1'),
      1,
      'Job should be executed exactly once with concurrency 5'
    )
  })

  test('multiple jobs should each be processed exactly once with high concurrency', async ({
    assert,
    cleanup,
  }) => {
    const jobExecutions: Map<string, number> = new Map()

    class TrackingJob extends Job<{ jobId: string }> {
      async execute() {
        const count = jobExecutions.get(this.payload.jobId) || 0
        jobExecutions.set(this.payload.jobId, count + 1)
        await setTimeout(30)
      }
    }

    const adapter = await options.createAdapter()
    Locator.register(TrackingJob.name, TrackingJob)

    cleanup(() => Locator.clear())

    const config: QueueManagerConfig = {
      default: 'test',
      adapters: { test: () => adapter },
      worker: { concurrency: 5 },
    }

    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    // Push 3 jobs (less than concurrency)
    for (let i = 1; i <= 3; i++) {
      await adapter.pushOn('default', {
        id: `job-${i}`,
        name: 'TrackingJob',
        payload: { jobId: `job-${i}` },
        attempts: 0,
      })
    }

    // Process until idle
    let cycles = 0
    const maxCycles = 30
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++
      if (cycle?.type === 'idle') break
    }

    assert.equal(jobExecutions.size, 3, 'All 3 jobs should have been executed')
    for (const [jobId, count] of jobExecutions) {
      assert.equal(count, 1, `${jobId} should be executed exactly once`)
    }
  })

  test('jobs should not be duplicated under concurrent popFrom stress', async ({
    assert,
    cleanup,
  }) => {
    const jobExecutions: Map<string, number> = new Map()
    const executionOrder: string[] = []

    class TrackingJob extends Job<{ jobId: string }> {
      async execute() {
        executionOrder.push(this.payload.jobId)
        const count = jobExecutions.get(this.payload.jobId) || 0
        jobExecutions.set(this.payload.jobId, count + 1)
        // Very short execution to maximize concurrency pressure
        await setTimeout(5)
      }
    }

    const adapter = await options.createAdapter()
    Locator.register(TrackingJob.name, TrackingJob)

    cleanup(() => Locator.clear())

    const config: QueueManagerConfig = {
      default: 'test',
      adapters: { test: () => adapter },
      worker: { concurrency: 10 },
    }

    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    // Push many jobs quickly
    const jobCount = 20
    for (let i = 1; i <= jobCount; i++) {
      await adapter.pushOn('default', {
        id: `stress-job-${i}`,
        name: 'TrackingJob',
        payload: { jobId: `stress-job-${i}` },
        attempts: 0,
      })
    }

    // Process until idle
    let cycles = 0
    const maxCycles = 100
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++
      if (cycle?.type === 'idle') break
    }

    // Verify no duplicates
    assert.equal(jobExecutions.size, jobCount, `All ${jobCount} jobs should have been executed`)
    for (const [jobId, count] of jobExecutions) {
      assert.equal(count, 1, `${jobId} should be executed exactly once`)
    }

    // Verify execution order has no duplicates
    const uniqueExecutions = new Set(executionOrder)
    assert.equal(
      executionOrder.length,
      uniqueExecutions.size,
      'No job should appear twice in execution order'
    )
  })
}
