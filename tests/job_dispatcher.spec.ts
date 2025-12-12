import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { memory } from './_mocks/memory_adapter.js'
import { QueueManager } from '#src/queue_manager'
import { JobDispatcher } from '#src/job_dispatcher'

test.group('JobDispatcher', () => {
  test('should dispatch job correctly', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('SendEmailJob', { to: 'romain.lanz@pm.me' })

    const jobId = await dispatcher.run()

    assert.isString(jobId)
    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'SendEmailJob')
    assert.deepEqual(job!.payload, { to: 'romain.lanz@pm.me' })
  })

  test('should dispatch job to specified queue', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('GenerateReportJob', { reportId: 123 })

    const jobId = await dispatcher.toQueue('emails').run()

    assert.isString(jobId)

    const job = await sharedAdapter.popFrom('emails')

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'GenerateReportJob')
    assert.deepEqual(job!.payload, { reportId: 123 })
  })

  test('should dispatch job using specific adapter', async ({ assert }) => {
    const memoryAdapter = memory()()
    const anotherMemoryAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => memoryAdapter, anotherMemory: () => anotherMemoryAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('CleanupJob', { days: 30 })

    const jobId = await dispatcher.with('anotherMemory').run()

    assert.isString(jobId)

    const job = await anotherMemoryAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'CleanupJob')
    assert.deepEqual(job!.payload, { days: 30 })

    const emptyJob = await memoryAdapter.pop()
    assert.isNull(emptyJob)
  })

  test('should dispatch job using adapter instance', async ({ assert }) => {
    const memoryAdapter = memory()()
    const anotherMemoryAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => memoryAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('CleanupJob', { days: 30 })

    const jobId = await dispatcher.with(() => anotherMemoryAdapter).run()

    assert.isString(jobId)

    const job = await anotherMemoryAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'CleanupJob')
    assert.deepEqual(job!.payload, { days: 30 })

    const emptyJob = await memoryAdapter.pop()
    assert.isNull(emptyJob)
  })

  test('should dispatch job with priority', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('HighPriorityJob', { task: 'urgent' })

    const jobId = await dispatcher.priority(10).run()

    assert.isString(jobId)

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'HighPriorityJob')
    assert.deepEqual(job!.payload, { task: 'urgent' })
    assert.equal(job!.priority, 10)
  })

  test('should dispatch job with delay', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('DelayedJob', { info: 'wait for it' })

    const jobId = await dispatcher.in('1s').run()
    assert.isString(jobId)

    const job = await sharedAdapter.pop()
    assert.isNull(job)

    await setTimeout(2000)

    const delayedJob = await sharedAdapter.pop()
    assert.isNotNull(delayedJob)
    assert.equal(delayedJob!.id, jobId)
    assert.equal(delayedJob!.name, 'DelayedJob')
    assert.deepEqual(delayedJob!.payload, { info: 'wait for it' })
  }).timeout(5000)

  test('should support promise then() for auto-dispatching', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('AutoDispatchJob', { auto: true })

    const jobId = await dispatcher.priority(5).toQueue('auto-queue')
    assert.isString(jobId)

    const job = await sharedAdapter.popFrom('auto-queue')
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'AutoDispatchJob')
    assert.deepEqual(job!.payload, { auto: true })
    assert.equal(job!.priority, 5)
  })
})
