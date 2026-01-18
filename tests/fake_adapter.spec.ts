import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { fake } from '../src/drivers/fake_adapter.js'

test.group('FakeAdapter', () => {
  test('should record pushes and support assertions', async ({ assert }) => {
    const adapter = fake()()

    adapter.assertNothingPushed()

    await adapter.pushOn('default', {
      id: 'job-1',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      attempts: 0,
    })

    await adapter.pushLaterOn(
      'emails',
      {
        id: 'job-2',
        name: 'SendEmailJob',
        payload: { to: 'admin@example.com' },
        attempts: 0,
      },
      500
    )

    adapter.assertPushed('SendEmailJob')
    adapter.assertPushed('SendEmailJob', {
      queue: 'default',
      payload: { to: 'user@example.com' },
    })
    adapter.assertPushed('SendEmailJob', { queue: 'emails', delay: 500 })
    adapter.assertPushedCount(2)
    adapter.assertPushedCount(1, { queue: 'emails' })

    assert.throws(() => adapter.assertNotPushed('SendEmailJob', { queue: 'default' }))
    adapter.assertNotPushed('MissingJob')

    adapter.clearPushedJobs()
    adapter.assertNothingPushed()

    await adapter.destroy()
  })

  test('should support matcher functions and getters', async ({ assert }) => {
    const adapter = fake()()

    await adapter.pushOn('default', {
      id: 'job-1',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      attempts: 0,
    })

    await adapter.pushLaterOn(
      'emails',
      {
        id: 'job-2',
        name: 'SendEmailJob',
        payload: { to: 'admin@example.com' },
        attempts: 0,
      },
      250
    )

    assert.equal(adapter.getPushedJobs().length, 2)
    assert.equal(adapter.getPushedJobsOn('emails').length, 1)

    const matcherRecord = adapter.findPushed(
      (job) => job.name === 'SendEmailJob' && job.payload?.to === 'user@example.com',
      { queue: 'default' }
    )
    assert.isDefined(matcherRecord)

    const payloadRecord = adapter.findPushed('SendEmailJob', {
      payload: (payload) => payload?.to === 'admin@example.com',
    })
    assert.isDefined(payloadRecord)

    const delayRecord = adapter.findPushed('SendEmailJob', {
      delay: (delay) => (delay ?? 0) >= 250,
    })
    assert.isDefined(delayRecord)

    await adapter.destroy()
  })

  test('should recover stalled jobs only for targeted queue', async ({ assert }) => {
    const adapter = fake()()

    await adapter.pushOn('queue-a', {
      id: 'job-a',
      name: 'JobA',
      payload: null,
      attempts: 0,
    })

    await adapter.pushOn('queue-b', {
      id: 'job-b',
      name: 'JobB',
      payload: null,
      attempts: 0,
    })

    const jobA = await adapter.popFrom('queue-a')
    const jobB = await adapter.popFrom('queue-b')

    assert.isNotNull(jobA)
    assert.isNotNull(jobB)

    await setTimeout(2)

    const recoveredA = await adapter.recoverStalledJobs('queue-a', 0, 1)
    assert.equal(recoveredA, 1)

    const recoveredJobA = await adapter.popFrom('queue-a')
    assert.equal(recoveredJobA?.id, 'job-a')

    const noneInB = await adapter.popFrom('queue-b')
    assert.isNull(noneInB)

    await setTimeout(2)

    const recoveredB = await adapter.recoverStalledJobs('queue-b', 0, 1)
    assert.equal(recoveredB, 1)

    const recoveredJobB = await adapter.popFrom('queue-b')
    assert.equal(recoveredJobB?.id, 'job-b')

    await adapter.destroy()
  })

  test('should ignore active jobs from other queues in getJob', async ({ assert }) => {
    const adapter = fake()()

    await adapter.pushOn('queue-a', {
      id: 'job-a',
      name: 'JobA',
      payload: null,
      attempts: 0,
    })

    const jobA = await adapter.popFrom('queue-a')
    assert.isNotNull(jobA)

    const wrongQueue = await adapter.getJob('job-a', 'queue-b')
    assert.isNull(wrongQueue)

    const correctQueue = await adapter.getJob('job-a', 'queue-a')
    assert.isNotNull(correctQueue)
    assert.equal(correctQueue?.status, 'active')

    await adapter.destroy()
  })
})
