import { test } from '@japa/runner'
import * as errors from '../src/exceptions.js'
import { QueueManager } from '../src/queue_manager.js'
import { sync } from '../src/drivers/sync_adapter.js'
import { exponentialBackoff } from '../src/strategies/backoff_strategy.js'
import { MemoryLogger } from './_mocks/memory_logger.js'

test.group('QueueManager', () => {
  test('should validate adapter presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: {},
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
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(error.message, 'Configuration error. Reason: Default adapter must be specified')
    }
  })

  test('should validate that adapter is a function', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: { sync: 'not-a-function' as any },
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
      locations: ['./examples/jobs/**/*'],
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
    })

    let config = QueueManager.getMergedRetryConfig('default')
    assert.equal(config.maxRetries, 5)
  })

  test('should merge retry configurations correctly (queue)', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./examples/jobs/**/*'],
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
      locations: ['./examples/jobs/**/*'],
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = QueueManager.getMergedRetryConfig('email', { maxRetries: 2 })
    assert.equal(config.maxRetries, 2)
  })

  test('should throw E_QUEUE_NOT_INITIALIZED when use() called before init()', async ({
    assert,
  }) => {
    assert.plan(2)

    await QueueManager.destroy()

    try {
      QueueManager.use()
    } catch (error) {
      assert.instanceOf(error, errors.E_QUEUE_NOT_INITIALIZED)
      assert.equal(
        error.message,
        'QueueManager is not initialized. Call QueueManager.init() before using it.'
      )
    }
  })

  test('should throw E_ADAPTER_INIT_ERROR when adapter factory throws', async ({ assert }) => {
    assert.plan(2)

    await QueueManager.init({
      default: 'broken',
      adapters: {
        broken: () => {
          throw new Error('Connection failed')
        },
      },
    })

    try {
      QueueManager.use()
    } catch (error) {
      assert.instanceOf(error, errors.E_ADAPTER_INIT_ERROR)
      assert.equal(
        error.message,
        'Failed to initialize adapter "broken". Reason: Connection failed'
      )
    }
  })

  test('should log warning when locations match no jobs', async ({ assert }) => {
    const logger = new MemoryLogger()

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./non-existent-path/**/*.ts'],
      logger,
    })

    assert.equal(logger.logs.length, 1)
    assert.equal(logger.logs[0].level, 'warn')
    assert.include(logger.logs[0].message, 'No jobs found for locations')
  })

  test('should fake adapters and restore them', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    const original = QueueManager.use()
    const fakeAdapter = QueueManager.fake()

    assert.strictEqual(QueueManager.use(), fakeAdapter)

    QueueManager.restore()

    assert.strictEqual(QueueManager.use(), original)

    await QueueManager.destroy()
  })
})
