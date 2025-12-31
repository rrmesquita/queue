import { test as JapaTest } from '@japa/runner'
import type { Adapter } from '../../src/contracts/adapter.js'

interface DriverTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
}

export function registerDriverTestSuite(options: DriverTestSuiteOptions) {
  const { test } = options

  test('popFrom should return null when queue is empty', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const job = await adapter.popFrom('test-queue')
    assert.isNull(job)
  })

  test('popFrom should return job with acquiredAt timestamp', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')

    assert.isNotNull(job)
    assert.equal(job!.id, 'job-1')
    assert.equal(job!.name, 'TestJob')
    assert.deepEqual(job!.payload, { foo: 'bar' })
    assert.isNumber(job!.acquiredAt)
    assert.approximately(job!.acquiredAt, Date.now(), 1000)
  })

  test('popFrom should remove job from pending queue', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)

    const job2 = await adapter.popFrom('test-queue')
    assert.isNull(job2)
  })

  test('completeJob should remove job from active tracking', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.completeJob(job!.id, 'test-queue')

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!.id, 'test-queue')

    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('retryJob should put job back in queue with incremented attempts', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.equal(job1!.attempts, 0)

    await adapter.retryJob(job1!.id, 'test-queue')

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)
    assert.equal(job2!.id, 'job-1')
    assert.equal(job2!.attempts, 1)
  })

  test('retryJob with future date should delay the job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    const futureDate = new Date(Date.now() + 60000) // 1 minute in future
    await adapter.retryJob(job!.id, 'test-queue', futureDate)

    // Job should not be immediately available
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('failJob should remove job from active tracking', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.failJob(job!.id, 'test-queue', new Error('Test error'))

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!.id, 'test-queue')

    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('multiple jobs should be processed in order', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-3',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    const job3 = await adapter.popFrom('test-queue')
    const job4 = await adapter.popFrom('test-queue')

    assert.equal(job1!.id, 'job-1')
    assert.equal(job2!.id, 'job-2')
    assert.equal(job3!.id, 'job-3')
    assert.isNull(job4)
  })

  test('recoverStalledJobs should return 0 when no stalled jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    // No jobs at all
    const recovered = await adapter.recoverStalledJobs('test-queue', 1000, 1)
    assert.equal(recovered, 0)
  })

  test('recoverStalledJobs should not recover jobs within threshold', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Acquire the job
    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    // Try to recover with a long threshold (job is not stalled yet)
    const recovered = await adapter.recoverStalledJobs('test-queue', 60000, 1)
    assert.equal(recovered, 0)

    // Job should still be active, not back in pending
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('recoverStalledJobs should recover stalled jobs back to pending', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    // Acquire the job
    await adapter.popFrom('test-queue')

    // Wait a bit and recover with a very short threshold
    await new Promise((resolve) => setTimeout(resolve, 50))
    const recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 1)

    // Job should be back in pending queue
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNotNull(nextJob)
    assert.equal(nextJob!.id, 'job-1')
    assert.deepEqual(nextJob!.payload, { foo: 'bar' })
  })

  test('recoverStalledJobs should increment stalledCount', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 0,
    })

    // First stall cycle
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    await adapter.recoverStalledJobs('test-queue', 10, 3)

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.equal(job1!.stalledCount, 1)

    // Second stall cycle
    await new Promise((resolve) => setTimeout(resolve, 50))
    await adapter.recoverStalledJobs('test-queue', 10, 3)

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)
    assert.equal(job2!.stalledCount, 2)
  })

  test('recoverStalledJobs should fail job permanently when maxStalledCount exceeded', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 0,
    })

    // First stall - should recover (stalledCount becomes 1)
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    let recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 1)

    // Second stall - should fail permanently (stalledCount would be 2, exceeds maxStalledCount=1)
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 0) // Not recovered, but failed

    // Job should be gone (failed permanently)
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('recoverStalledJobs should handle multiple stalled jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Acquire both jobs
    await adapter.popFrom('test-queue')
    await adapter.popFrom('test-queue')

    // Recover all stalled jobs
    await new Promise((resolve) => setTimeout(resolve, 50))
    const recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 2)

    // Both jobs should be back
    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    const job3 = await adapter.popFrom('test-queue')

    assert.isNotNull(job1)
    assert.isNotNull(job2)
    assert.isNull(job3)
  })
}
