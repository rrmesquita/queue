import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import { QueueManager } from '../src/queue_manager.js'
import { sync } from '../src/drivers/sync_adapter.js'
import * as errors from '../src/exceptions.js'

test.group('SyncAdapter', (group) => {
  group.each.teardown(async () => {
    Locator.clear()
    await QueueManager.destroy()
  })

  test('should retry sync jobs and call failed() without bubbling execute errors', async ({
    assert,
  }) => {
    let executeAttempts = 0
    let failedCalls = 0
    let failedError: Error | undefined
    const attempts: number[] = []
    const contextJobIds: string[] = []

    class RetryingSyncJob extends Job<Record<string, never>> {
      static options = {
        maxRetries: 3,
      }

      async execute() {
        executeAttempts++
        attempts.push(this.context.attempt)
        contextJobIds.push(this.context.jobId)

        throw new Error('boom')
      }

      async failed(error: Error) {
        failedCalls++
        failedError = error
        contextJobIds.push(this.context.jobId)
      }
    }

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    Locator.register('RetryingSyncJob', RetryingSyncJob)

    const { jobId } = await RetryingSyncJob.dispatch({}).run()

    assert.equal(executeAttempts, 4)
    assert.deepEqual(attempts, [1, 2, 3, 4])
    assert.equal(failedCalls, 1)
    assert.instanceOf(failedError, errors.E_JOB_MAX_ATTEMPTS_REACHED)
    assert.deepEqual(contextJobIds, Array(contextJobIds.length).fill(jobId))
  })
})
