import { test as JapaTest } from '@japa/runner'
import type { Adapter } from '../../src/contracts/adapter.js'

interface DriverTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
  /**
   * Whether this adapter supports concurrent access from multiple instances.
   * Memory adapter doesn't share state between instances, so concurrent tests are skipped.
   * @default true
   */
  supportsConcurrency?: boolean
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

  // Concurrent tests only run for adapters that support multi-instance concurrency
  // Memory adapter doesn't share state between instances
  if (options.supportsConcurrency !== false) {
    test('concurrent popFrom should not return the same job twice', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      adapter1.setWorkerId('worker-1')
      adapter2.setWorkerId('worker-2')

      // Push a single job
      await adapter1.pushOn('test-queue', {
        id: 'job-1',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })

      // Both workers try to pop simultaneously
      const [job1, job2] = await Promise.all([
        adapter1.popFrom('test-queue'),
        adapter2.popFrom('test-queue'),
      ])

      // Only one worker should get the job
      const acquiredJobs = [job1, job2].filter((job) => job !== null)
      assert.equal(acquiredJobs.length, 1, 'Only one worker should acquire the job')
    })

    test('concurrent popFrom with multiple jobs should distribute jobs', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      adapter1.setWorkerId('worker-1')
      adapter2.setWorkerId('worker-2')

      // Push multiple jobs
      await adapter1.pushOn('test-queue', {
        id: 'job-1',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })
      await adapter1.pushOn('test-queue', {
        id: 'job-2',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })

      // Both workers try to pop simultaneously
      const [job1, job2] = await Promise.all([
        adapter1.popFrom('test-queue'),
        adapter2.popFrom('test-queue'),
      ])

      // Both workers should get different jobs
      assert.isNotNull(job1)
      assert.isNotNull(job2)
      assert.notEqual(job1!.id, job2!.id, 'Workers should acquire different jobs')
    })
  }

  test('createSchedule should create a new schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const id = await adapter.createSchedule({
      name: 'TestJob',
      payload: { foo: 'bar' },
      everyMs: 5000,
      timezone: 'UTC',
    })

    assert.isString(id)

    const schedule = await adapter.getSchedule(id)
    assert.isNotNull(schedule)
    assert.equal(schedule!.name, 'TestJob')
    assert.deepEqual(schedule!.payload, { foo: 'bar' })
    assert.equal(schedule!.everyMs, 5000)
    assert.equal(schedule!.status, 'active')
  })

  test('createSchedule should use provided id', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const id = await adapter.createSchedule({
      id: 'my-custom-id',
      name: 'TestJob',
      payload: {},
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
    })

    assert.equal(id, 'my-custom-id')

    const schedule = await adapter.getSchedule('my-custom-id')
    assert.isNotNull(schedule)
    assert.equal(schedule!.cronExpression, '0 0 * * *')
  })

  test('createSchedule should upsert when id exists', async ({ assert }) => {
    const adapter = await options.createAdapter()

    // Create initial schedule
    await adapter.createSchedule({
      id: 'upsert-test',
      name: 'TestJob',
      payload: { version: 1 },
      everyMs: 5000,
      timezone: 'UTC',
    })

    // Upsert with new values
    await adapter.createSchedule({
      id: 'upsert-test',
      name: 'TestJob',
      payload: { version: 2 },
      everyMs: 10000,
      timezone: 'Europe/Paris',
    })

    const schedule = await adapter.getSchedule('upsert-test')
    assert.deepEqual(schedule!.payload, { version: 2 })
    assert.equal(schedule!.everyMs, 10000)
    assert.equal(schedule!.timezone, 'Europe/Paris')
  })

  test('getSchedule should return null for non-existent schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const schedule = await adapter.getSchedule('non-existent')
    assert.isNull(schedule)
  })

  test('listSchedules should return all schedules', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'list-test-1',
      name: 'Job1',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })
    await adapter.createSchedule({
      id: 'list-test-2',
      name: 'Job2',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    const schedules = await adapter.listSchedules()
    const ids = schedules.map((s) => s.id)

    assert.include(ids, 'list-test-1')
    assert.include(ids, 'list-test-2')
  })

  test('listSchedules should filter by status', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'filter-active',
      name: 'Job1',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })
    await adapter.createSchedule({
      id: 'filter-paused',
      name: 'Job2',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('filter-paused', { status: 'paused' })

    const activeSchedules = await adapter.listSchedules({ status: 'active' })
    const pausedSchedules = await adapter.listSchedules({ status: 'paused' })

    assert.isTrue(activeSchedules.some((s) => s.id === 'filter-active'))
    assert.isFalse(activeSchedules.some((s) => s.id === 'filter-paused'))
    assert.isTrue(pausedSchedules.some((s) => s.id === 'filter-paused'))
    assert.isFalse(pausedSchedules.some((s) => s.id === 'filter-active'))
  })

  test('updateSchedule should update status', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'update-status-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('update-status-test', { status: 'paused' })

    const schedule = await adapter.getSchedule('update-status-test')
    assert.equal(schedule!.status, 'paused')
  })

  test('updateSchedule should update run metadata', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'update-meta-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    const now = new Date()
    const nextRun = new Date(now.getTime() + 5000)

    await adapter.updateSchedule('update-meta-test', {
      runCount: 5,
      lastRunAt: now,
      nextRunAt: nextRun,
    })

    const schedule = await adapter.getSchedule('update-meta-test')
    assert.equal(schedule!.runCount, 5)
    assert.approximately(schedule!.lastRunAt!.getTime(), now.getTime(), 1000)
    assert.approximately(schedule!.nextRunAt!.getTime(), nextRun.getTime(), 1000)
  })

  test('deleteSchedule should remove schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'delete-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.deleteSchedule('delete-test')

    const schedule = await adapter.getSchedule('delete-test')
    assert.isNull(schedule)
  })

  test('claimDueSchedule should return null when no schedules are due', async ({ assert }) => {
    const adapter = await options.createAdapter()

    // Create schedule with nextRunAt in the future
    await adapter.createSchedule({
      id: 'future-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('future-schedule', {
      nextRunAt: new Date(Date.now() + 60000),
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  test('claimDueSchedule should claim a due schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'due-schedule',
      name: 'DueJob',
      payload: { key: 'value' },
      everyMs: 5000,
      timezone: 'UTC',
    })

    // Make it due
    await adapter.updateSchedule('due-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const claimed = await adapter.claimDueSchedule()

    assert.isNotNull(claimed)
    assert.equal(claimed!.id, 'due-schedule')
    assert.equal(claimed!.name, 'DueJob')
    assert.deepEqual(claimed!.payload, { key: 'value' })
  })

  test('claimDueSchedule should update nextRunAt after claiming', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'claim-update-test',
      name: 'TestJob',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    const pastDate = new Date(Date.now() - 1000)
    await adapter.updateSchedule('claim-update-test', { nextRunAt: pastDate })

    await adapter.claimDueSchedule()

    const after = await adapter.getSchedule('claim-update-test')
    assert.isNotNull(after!.nextRunAt)
    assert.isTrue(after!.nextRunAt!.getTime() > Date.now())
  })

  test('claimDueSchedule should increment runCount', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'runcount-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('runcount-test', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const before = await adapter.getSchedule('runcount-test')
    assert.equal(before!.runCount, 0)

    await adapter.claimDueSchedule()

    const after = await adapter.getSchedule('runcount-test')
    assert.equal(after!.runCount, 1)
  })

  test('claimDueSchedule should not claim paused schedules', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'paused-claim-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('paused-claim-test', {
      nextRunAt: new Date(Date.now() - 1000),
      status: 'paused',
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  test('claimDueSchedule should not claim when limit reached', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.createSchedule({
      id: 'limit-claim-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
      limit: 5,
    })

    await adapter.updateSchedule('limit-claim-test', {
      nextRunAt: new Date(Date.now() - 1000),
      runCount: 5,
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  // Concurrent schedule tests
  if (options.supportsConcurrency !== false) {
    test('concurrent claimDueSchedule should not claim same schedule twice', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      // Create a single due schedule
      await adapter1.createSchedule({
        id: 'concurrent-claim-test',
        name: 'TestJob',
        payload: {},
        everyMs: 60000,
        timezone: 'UTC',
      })

      await adapter1.updateSchedule('concurrent-claim-test', {
        nextRunAt: new Date(Date.now() - 1000),
      })

      // Both adapters try to claim simultaneously
      const [claimed1, claimed2] = await Promise.all([
        adapter1.claimDueSchedule(),
        adapter2.claimDueSchedule(),
      ])

      // Only one should succeed
      const claimedSchedules = [claimed1, claimed2].filter((s) => s !== null)
      assert.equal(claimedSchedules.length, 1, 'Only one adapter should claim the schedule')
    })

    test('high-concurrency claimDueSchedule stress test', async ({ assert }) => {
      const adapters = await Promise.all(Array.from({ length: 10 }, () => options.createAdapter()))

      // Create a single due schedule
      await adapters[0].createSchedule({
        id: 'stress-test-schedule',
        name: 'StressJob',
        payload: { test: true },
        everyMs: 60000,
        timezone: 'UTC',
      })

      await adapters[0].updateSchedule('stress-test-schedule', {
        nextRunAt: new Date(Date.now() - 1000),
      })

      // All 10 adapters try to claim simultaneously
      const results = await Promise.all(adapters.map((adapter) => adapter.claimDueSchedule()))

      // Exactly one should succeed
      const claimedSchedules = results.filter((s) => s !== null)
      assert.equal(claimedSchedules.length, 1, 'Exactly one adapter should claim the schedule')

      // The claimed schedule should have the correct data
      const claimed = claimedSchedules[0]!
      assert.equal(claimed.id, 'stress-test-schedule')
      assert.equal(claimed.name, 'StressJob')
      assert.deepEqual(claimed.payload, { test: true })
    })
  }
}
