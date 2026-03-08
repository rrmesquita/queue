import { test as JapaTest } from '@japa/runner'
import { Worker } from '../../src/worker.js'
import { Locator } from '../../src/locator.js'
import { Job } from '../../src/job.js'
import type { Adapter } from '../../src/contracts/adapter.js'
import type { QueueManagerConfig } from '../../src/types/main.js'

interface WorkerRetryTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
}

export function registerWorkerRetryTestSuite(options: WorkerRetryTestSuiteOptions) {
  const { test } = options

  test('should respect top-level maxRetries in job options', async ({ assert, cleanup }) => {
    class FailingJob extends Job {
      static options = {
        maxRetries: 1,
      }

      async execute() {
        throw new Error('Job failed as expected')
      }
    }

    const adapter = await options.createAdapter()

    Locator.register('FailingJob', FailingJob)
    cleanup(() => Locator.clear())

    const config: QueueManagerConfig = {
      default: 'test',
      adapters: { test: () => adapter },
    }

    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    await adapter.pushOn('default', {
      id: 'test-job-top-level-max-retries',
      name: 'FailingJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (queued for retry)
    const cycle = await worker.processCycle(['default']) // started

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'started')
    // @ts-ignore
    assert.equal(cycle.job.attempts, 1)
  })
}
