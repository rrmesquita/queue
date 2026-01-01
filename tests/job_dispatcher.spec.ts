import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { memory } from './_mocks/memory_adapter.js'
import { QueueManager } from '../src/queue_manager.js'
import { JobDispatcher } from '../src/job_dispatcher.js'

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

    const { jobId } = await dispatcher.run()

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

    const { jobId } = await dispatcher.toQueue('emails').run()

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

    const { jobId } = await dispatcher.with('anotherMemory').run()

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

    const { jobId } = await dispatcher.with(() => anotherMemoryAdapter).run()

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

    const { jobId } = await dispatcher.priority(10).run()

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

    const { jobId } = await dispatcher.in('1s').run()
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

    const { jobId } = await dispatcher.priority(5).toQueue('auto-queue')
    assert.isString(jobId)

    const job = await sharedAdapter.popFrom('auto-queue')
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'AutoDispatchJob')
    assert.deepEqual(job!.payload, { auto: true })
    assert.equal(job!.priority, 5)
  })

  test('should dispatch job with repeat config using every()', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('RepeatJob', { sync: true })

    const { jobId, repeatId } = await dispatcher.every('5s').run()

    assert.isString(jobId)
    assert.isString(repeatId)

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.repeat!.interval, 5000)
    assert.isUndefined(job!.repeat!.remaining)
    assert.equal(job!.repeat!.groupId, repeatId)
  })

  test('should dispatch job with limited repeats using times()', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('RepeatJob', { sync: true })

    const { jobId, repeatId } = await dispatcher.every('1h').times(5).run()

    assert.isString(jobId)
    assert.isString(repeatId)

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    // times(5) means 5 total runs, so remaining = 4 after first
    assert.equal(job!.repeat!.interval, 3600000)
    assert.equal(job!.repeat!.remaining, 4)
    assert.equal(job!.repeat!.groupId, repeatId)
  })

  test('should throw error when times() is less than 1', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('RepeatJob', { sync: true })

    assert.throws(() => dispatcher.times(0), 'times() must be at least 1')
  })

  test('should not include repeat config when every() is not called', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('NormalJob', { data: 'test' })

    await dispatcher.run()

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.isUndefined(job!.repeat)
  })

  test('should combine every() with delay using in()', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('DelayedRepeatJob', { data: 'test' })

    await dispatcher.every('1m').in('30s').run()

    // Job should be delayed, so not available immediately
    const job = await sharedAdapter.pop()
    assert.isNull(job)
  })
})

test.group('JobDispatcher | DispatchResult', () => {
  test('run() should return DispatchResult with jobId', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('TestJob', { data: 'test' })

    const result = await dispatcher.run()

    // Should return an object with jobId, not just a string
    assert.isObject(result)
    assert.property(result, 'jobId')
    assert.isString(result.jobId)
    assert.isUndefined(result.repeatId)
  })

  test('run() should return repeatId when every() is used', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('RepeatJob', { data: 'test' })

    const result = await dispatcher.every('5s').run()

    assert.isObject(result)
    assert.property(result, 'jobId')
    assert.property(result, 'repeatId')
    assert.isString(result.jobId)
    assert.isString(result.repeatId)
    // repeatId should be different from jobId
    assert.notEqual(result.jobId, result.repeatId)
  })

  test('repeatId should be consistent across repeat chain (stored in job data)', async ({
    assert,
  }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('RepeatJob', { data: 'test' })

    const result = await dispatcher.every('5s').run()

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.property(job!.repeat!, 'groupId')
    assert.equal(job!.repeat!.groupId, result.repeatId)
  })

  test('then() should also return DispatchResult', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      locations: ['./jobs/**/*'],
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('TestJob', { data: 'test' })

    // Using await on dispatcher directly (via thenable)
    const result = await dispatcher.every('1s')

    assert.isObject(result)
    assert.property(result, 'jobId')
    assert.property(result, 'repeatId')
  })
})
