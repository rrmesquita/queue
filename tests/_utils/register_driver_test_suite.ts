import { test as JapaTest } from '@japa/runner'
import type { Adapter } from '#contracts/adapter'

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
}
