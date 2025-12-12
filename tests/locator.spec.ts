import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import * as errors from '../src/exceptions.js'
import SendEmailJob from '../examples/jobs/send_email_job.js'

class TestJob extends Job<{ message: string }> {
  static jobName = 'TestJob'

  execute(): Promise<void> {
    return Promise.resolve()
  }

  rescue(_error: Error): Promise<void> {
    return Promise.resolve()
  }
}

class AnotherTestJob extends Job<{ value: number }> {
  static jobName = 'AnotherTestJob'

  execute(): Promise<void> {
    return Promise.resolve()
  }

  rescue(_error: Error): Promise<void> {
    return Promise.resolve()
  }
}

test.group('Locator', (group) => {
  group.each.setup(() => {
    Locator.clear()
  })

  test('should register a job class', ({ assert }) => {
    Locator.register('TestJob', TestJob)

    const job = Locator.get('TestJob')
    assert.equal(job, TestJob)
  })

  test('should register a job class from glob pattern', async ({ assert }) => {
    await Locator.registerFromGlob(['./examples/jobs/*.ts'])

    assert.equal(Locator.get('SendEmailJob'), SendEmailJob)
  })

  test('should return undefined for non-existent job', ({ assert }) => {
    const job = Locator.get('NonExistentJob')
    assert.isUndefined(job)
  })

  test('should clear all registered jobs', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    Locator.register('AnotherTestJob', AnotherTestJob)

    assert.equal(Locator.get('TestJob'), TestJob)
    assert.equal(Locator.get('AnotherTestJob'), AnotherTestJob)

    Locator.clear()

    assert.isUndefined(Locator.get('TestJob'))
    assert.isUndefined(Locator.get('AnotherTestJob'))
  })

  test('should overwrite existing job registration', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    assert.equal(Locator.get('TestJob'), TestJob)

    Locator.register('TestJob', AnotherTestJob)
    assert.equal(Locator.get('TestJob'), AnotherTestJob)
  })

  test('should register multiple jobs and retrieve them correctly', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    Locator.register('AnotherTestJob', AnotherTestJob)

    assert.equal(Locator.get('TestJob'), TestJob)
    assert.equal(Locator.get('AnotherTestJob'), AnotherTestJob)
    assert.isUndefined(Locator.get('Job3'))
  })

  test('should getOrThrow should return the job class if it exists', ({ assert }) => {
    Locator.register('TestJob', TestJob)

    const job = Locator.getOrThrow('TestJob')

    assert.equal(job, TestJob)
  })

  test('should getOrThrow should throw for non-existent job', ({ assert }) => {
    try {
      Locator.getOrThrow('NonExistentJob')
    } catch (error) {
      assert.instanceOf(error, errors.E_JOB_NOT_FOUND)
      assert.equal(error.message, 'Requested job "NonExistentJob" is not registered')
    }
  })
})
