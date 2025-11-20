import { test } from '@japa/runner'
import * as errors from '#src/exceptions'
import { QueueManager } from '#src/queue_manager'
import { sync } from '#drivers/sync_adapter'
import { exponentialBackoff } from '#strategies/backoff_strategy'

test.group('QueueManager', () => {
  test('should validate adapter presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: {},
        locations: ['./jobs/**/*'],
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: At least one adapter must be configured'
      )
    }
  })

  test('should validate default adapter presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: '',
        adapters: { sync: sync() },
        locations: ['./jobs/**/*'],
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(error.message, 'Configuration error. Reason: Default adapter must be specified')
    }
  })

  test('should validate locations presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: { sync: sync() },
        locations: [],
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(error.message, 'Configuration error. Reason: Job locations must be specified')
    }
  })

  test('should validate that adapter is a function', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: { sync: 'not-a-function' as any },
        locations: ['./jobs/**/*'],
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: Adapter "sync" must be a factory function'
      )
    }
  })

  test('should validate default adapter existence in adapters', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'missing',
        adapters: { sync: sync() },
        locations: ['./jobs/**/*'],
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: Default adapter "missing" not found in adapters configuration'
      )
    }
  })

  test('should merge retry configurations correctly (global)', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./jobs/**/*'],
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
    })

    let config = QueueManager.getMergedRetryConfig('default')
    assert.equal(config.maxRetries, 5)
  })

  test('should merge retry configurations correctly (queue)', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./jobs/**/*'],
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = QueueManager.getMergedRetryConfig('email')
    assert.equal(config.maxRetries, 3)
  })

  test('should merge retry configurations correctly (job)', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./jobs/**/*'],
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = QueueManager.getMergedRetryConfig('email', { maxRetries: 2 })
    assert.equal(config.maxRetries, 2)
  })
})
