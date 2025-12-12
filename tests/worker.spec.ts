import { test } from '@japa/runner'
import { setTimeout } from 'node:timers/promises'
import { Worker } from '../src/worker.js'
import { memory } from './_mocks/memory_adapter.js'
import { ChaosAdapter } from './_mocks/chaos_adapter.js'
import type { QueueManagerConfig } from '../src/types/main.js'
import { Locator } from '../src/locator.js'
import { Job } from '../src/job.js'
import * as errors from '../src/exceptions.js'

const config = {
  default: 'memory',
  adapters: { memory: memory() },
  locations: ['./jobs/**/*'],
} satisfies QueueManagerConfig

test.group('Worker', () => {
  test('should create a worker with a unique worker ID', ({ assert, cleanup }) => {
    const worker1 = new Worker(config)
    const worker2 = new Worker(config)

    cleanup(async () => {
      await Promise.all([worker1.stop(), worker2.stop()])
    })

    assert.isString(worker1.id)
    assert.isString(worker2.id)
    assert.notEqual(worker1.id, worker2.id)
  })

  test('should yield idle when no jobs are available', async ({ assert, cleanup }) => {
    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'idle')
    // @ts-ignore
    assert.isNumber(cycle.suggestedDelay)
  })

  test('should yield error when an exception occurs', async ({ assert, cleanup }) => {
    const chaosAdapter = new ChaosAdapter()
    chaosAdapter.alwaysThrow()

    const localConfig = {
      default: 'chaos',
      adapters: { chaos: () => chaosAdapter },
      locations: ['./jobs/**/*'],
    }

    const worker = new Worker(localConfig)

    cleanup(async () => {
      await worker.stop()
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'error')
    // @ts-ignore
    assert.isNumber(cycle.suggestedDelay)
    // @ts-ignore
    assert.isNotNull(cycle.error)
  })

  test('should yield job when a job is available', async ({ assert, cleanup }) => {
    class TestJob extends Job {
      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-1',
      name: 'TestJob',
      payload: { to: 'romain.lanz@example.com' },
      attempts: 0,
      priority: 0,
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'started')
    // @ts-ignore
    assert.equal(cycle.queue, 'default')
    // @ts-ignore
    assert.equal(cycle.job.id, 'test-job-1')
    // @ts-ignore
    assert.equal(cycle.job.name, 'TestJob')
  })

  test('should execute job when a job is available', async ({ assert, cleanup }) => {
    assert.plan(6)

    const payload = { foo: 'bar' }

    class TestJob extends Job {
      async execute() {
        assert.isTrue(true)
        assert.equal(this.payload, payload)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-2',
      name: 'TestJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    const cycle1 = await worker.processCycle(['default'])
    assert.isNotNull(cycle1)
    // @ts-ignore
    assert.equal(cycle1.type, 'started')

    const cycle2 = await worker.processCycle(['default'])
    assert.isNotNull(cycle2)
    // @ts-ignore
    assert.equal(cycle2.type, 'completed')
  })

  test('should retry failed job', async ({ assert, cleanup }) => {
    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
      retry: {
        maxRetries: 3,
      },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (rollback)
    const cycle = await worker.processCycle(['default']) // started

    // @ts-ignore
    assert.equal(cycle.job.attempts, 1)
  })

  test('should retry failed job until maxRetries is reached', async ({ assert, cleanup }) => {
    assert.plan(2)

    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }

      async failed(error: Error): Promise<void> {
        assert.instanceOf(error, errors.E_JOB_MAX_ATTEMPTS_REACHED)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],

      retry: {
        maxRetries: 2,
      },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (rollback)
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed
    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // started

    // @ts-ignore
    assert.equal(cycle.job.attempts, 2)
  })

  test('should not retry failed job when maxRetries is not configured', async ({
    assert,
    cleanup,
  }) => {
    assert.plan(3)

    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }

      async failed(error: Error): Promise<void> {
        assert.instanceOf(error, Error)
        assert.equal(error.message, 'Job failed as expected')
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed

    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })

  test('should maintain concurrency when one job is slow', async ({ assert, cleanup }) => {
    const executionOrder: string[] = []
    const startTimes: Record<string, number> = {}

    class SlowJob extends Job {
      async execute() {
        startTimes[this.payload.id] = Date.now()
        await setTimeout(200)
        executionOrder.push(this.payload.id)
      }
    }

    class FastJob extends Job {
      async execute() {
        startTimes[this.payload.id] = Date.now()
        await setTimeout(10)
        executionOrder.push(this.payload.id)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
      worker: {
        concurrency: 2,
      },
    }

    Locator.register('SlowJob', SlowJob)
    Locator.register('FastJob', FastJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Push jobs: 1 slow job + 3 fast jobs
    await sharedAdapter.push({
      id: 'job-1',
      name: 'SlowJob',
      payload: { id: 'slow-1' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-2',
      name: 'FastJob',
      payload: { id: 'fast-1' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-3',
      name: 'FastJob',
      payload: { id: 'fast-2' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-4',
      name: 'FastJob',
      payload: { id: 'fast-3' },
      attempts: 0,
      priority: 0,
    })

    // Start the worker and let it process all jobs
    const startTime = Date.now()

    // Process until idle (all jobs done)
    let cycles = 0
    const maxCycles = 20
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'idle') {
        break
      }
    }

    const totalTime = Date.now() - startTime

    // All 4 jobs should have executed
    assert.equal(executionOrder.length, 4)

    // With proper concurrency, fast jobs should complete before slow job
    // fast-1 starts with slow-1, completes quickly, then fast-2 starts, etc.
    // So execution order should be: fast-1, fast-2, fast-3, slow-1
    // (fast jobs complete before the slow job)
    assert.equal(executionOrder[executionOrder.length - 1], 'slow-1')

    // Total time should be around 200ms (slow job time) + overhead
    // NOT 200ms + 3*10ms in sequence
    // If batch processing was used, it would take ~200ms per batch
    // With proper pool, all fast jobs run while slow job runs
    assert.isBelow(totalTime, 350, 'Total time should be close to slow job time, not cumulative')
  })

  test('should timeout job that exceeds timeout duration', async ({ assert, cleanup }) => {
    assert.plan(2)

    class SlowJob extends Job {
      static options = { timeout: 50 }

      async execute() {
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startTime = Date.now()

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    const elapsed = Date.now() - startTime

    assert.isBelow(elapsed, 150, 'Job should be killed before completing')
  })

  test('should retry timed out job when failOnTimeout is false', async ({ assert, cleanup }) => {
    let attempts = 0

    class SlowJob extends Job {
      static options = { timeout: 50, retry: { maxRetries: 2 } }

      async execute() {
        attempts++
        await setTimeout(200)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-retry-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    // First attempt
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    // Second attempt (retried)
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    assert.equal(attempts, 2)
  })

  test('should not retry timed out job when failOnTimeout is true', async ({ assert, cleanup }) => {
    assert.plan(2)

    let attempts = 0

    class SlowJob extends Job {
      static options = { timeout: 50, failOnTimeout: true, retry: { maxRetries: 3 } }

      async execute() {
        attempts++
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-fail-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout, failed)

    // Should not retry
    const cycle = await worker.processCycle(['default'])
    // @ts-ignore
    assert.equal(cycle.type, 'idle')
  })

  test('should use global worker timeout when job timeout is not set', async ({
    assert,
    cleanup,
  }) => {
    assert.plan(2)

    class SlowJob extends Job {
      async execute() {
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
      worker: {
        timeout: 50,
      },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'global-timeout-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startTime = Date.now()

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    const elapsed = Date.now() - startTime

    assert.isBelow(elapsed, 150)
  })

  test('should wait for running jobs to complete before stopping', async ({ assert, cleanup }) => {
    let jobCompleted = false

    class SlowJob extends Job {
      async execute() {
        await setTimeout(100)
        jobCompleted = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
    })

    await sharedAdapter.push({
      id: 'slow-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    // Start the job - it's now running in the pool
    await worker.processCycle(['default'])

    // Call stop while job is still running (job takes 100ms)
    await worker.stop()

    // Job should have completed before stop() returned
    assert.isTrue(jobCompleted, 'Job should have completed before worker stopped')
  })

  test('should handle job that fails permanently', async ({ assert, cleanup }) => {
    let failedCalled = false

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed')
      }

      async failed() {
        failedCalled = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'failing-job',
      name: 'FailingJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(failedCalled, 'Failed callback should be called')
  })

  test('should handle job class not found', async ({ assert, cleanup }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'unknown-job',
      name: 'UnknownJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed (job failed)

    // Job initialization failure is handled gracefully - job is marked as failed
    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })

  test('should handle job constructor that throws', async ({ assert, cleanup }) => {
    class BrokenJob extends Job {
      constructor(payload: any) {
        super(payload)
        throw new Error('Constructor failed')
      }

      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    Locator.register('BrokenJob', BrokenJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'broken-job',
      name: 'BrokenJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed (job failed)

    // Job initialization failure is handled gracefully - job is marked as failed
    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })
})
