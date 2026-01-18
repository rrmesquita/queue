import * as errors from './exceptions.js'
import debug from './debug.js'
import { Locator } from './locator.js'
import { consoleLogger, type Logger } from './logger.js'
import { FakeAdapter } from './drivers/fake_adapter.js'
import type { Adapter } from './contracts/adapter.js'
import type {
  AdapterFactory,
  JobFactory,
  JobOptions,
  QueueConfig,
  QueueManagerConfig,
  RetryConfig,
} from './types/main.js'

type QueueManagerFakeState = {
  defaultAdapter: string
  adapters: Record<string, AdapterFactory>
  adapterInstances: Map<string, Adapter>
  globalRetryConfig?: RetryConfig
  globalJobOptions?: JobOptions
  queueConfigs: Map<string, QueueConfig>
  logger: Logger
  jobFactory?: JobFactory
  fakeAdapter: FakeAdapter
}

/**
 * Central configuration and adapter management for the queue system.
 *
 * The QueueManager is responsible for:
 * - Initializing adapters and job registration
 * - Providing adapter instances to workers and dispatchers
 * - Managing retry configuration across global, queue, and job levels
 *
 * @example
 * ```typescript
 * import { QueueManager, redis } from '@boringnode/queue'
 *
 * await QueueManager.init({
 *   default: 'redis',
 *   adapters: {
 *     redis: redis({ host: 'localhost' }),
 *   },
 *   locations: ['./jobs/**\/*.js'],
 *   retry: {
 *     maxRetries: 3,
 *     backoff: exponentialBackoff(),
 *   },
 * })
 *
 * // Get the default adapter
 * const adapter = QueueManager.use()
 *
 * // Clean up when done
 * await QueueManager.destroy()
 * ```
 */
class QueueManagerSingleton {
  #initialized = false
  #defaultAdapter!: string
  #adapters: Record<string, AdapterFactory> = {}
  #adapterInstances: Map<string, Adapter> = new Map()
  #globalRetryConfig?: RetryConfig
  #globalJobOptions?: JobOptions
  #queueConfigs: Map<string, QueueConfig> = new Map()
  #logger: Logger = consoleLogger
  #jobFactory?: JobFactory
  #fakeState?: QueueManagerFakeState

  /**
   * Initialize the queue system with the given configuration.
   *
   * This must be called before using the queue system. It:
   * - Validates the configuration
   * - Registers adapters
   * - Auto-discovers and registers job classes from `locations`
   *
   * @param config - The queue configuration
   * @returns This instance for chaining
   * @throws {E_CONFIGURATION_ERROR} If the configuration is invalid
   *
   * @example
   * ```typescript
   * await QueueManager.init({
   *   default: 'redis',
   *   adapters: {
   *     redis: redis(),
   *     postgres: knex(pgConfig),
   *   },
   *   locations: ['./jobs/**\/*.js'],
   * })
   * ```
   */
  async init(config: QueueManagerConfig) {
    debug('initializing queue manager with config: %O', config)

    this.#validateConfig(config)

    this.#adapterInstances.clear()

    this.#defaultAdapter = config.default
    this.#adapters = config.adapters
    this.#globalRetryConfig = config.retry
    this.#globalJobOptions = config.defaultJobOptions
    this.#logger = config.logger ?? consoleLogger
    this.#jobFactory = config.jobFactory

    if (config.queues) {
      for (const [queue, queueConfig] of Object.entries(config.queues)) {
        this.#queueConfigs.set(queue, queueConfig as QueueConfig)
      }
    }

    if (config.locations && config.locations.length > 0) {
      const registered = await Locator.registerFromGlob(config.locations)

      if (registered === 0) {
        this.#logger.warn(
          `No jobs found for locations: ${config.locations.join(', ')}. ` +
            'Verify your glob patterns match your job files.'
        )
      }
    }

    this.#initialized = true

    return this
  }

  /**
   * Get an adapter instance by name.
   *
   * Adapter instances are cached and reused. If no name is provided,
   * the default adapter is returned.
   *
   * @param adapter - Adapter name (optional, defaults to the default adapter)
   * @returns The adapter instance
   * @throws {E_QUEUE_NOT_INITIALIZED} If `init()` hasn't been called
   * @throws {E_CONFIGURATION_ERROR} If the adapter is not registered
   * @throws {E_ADAPTER_INIT_ERROR} If the adapter factory throws
   *
   * @example
   * ```typescript
   * // Get default adapter
   * const adapter = QueueManager.use()
   *
   * // Get specific adapter
   * const redisAdapter = QueueManager.use('redis')
   * ```
   */
  use(adapter?: string): Adapter {
    if (!this.#initialized) {
      throw new errors.E_QUEUE_NOT_INITIALIZED()
    }

    if (!adapter) {
      adapter = this.#defaultAdapter
    }

    // Return cached instance if exists
    const cached = this.#adapterInstances.get(adapter)
    if (cached) {
      return cached
    }

    const adapterFactory = this.#adapters[adapter]

    if (!adapterFactory) {
      throw new errors.E_CONFIGURATION_ERROR([`Adapter "${adapter}" is not registered`])
    }

    debug('using adapter "%s"', adapter)

    try {
      const instance = adapterFactory()
      this.#adapterInstances.set(adapter, instance)
      return instance
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      throw new errors.E_ADAPTER_INIT_ERROR([adapter, message])
    }
  }

  /**
   * Replace all adapters with a fake adapter for testing.
   *
   * The fake adapter records pushed jobs and exposes assertion helpers.
   * Call `restore()` to return to the previous configuration.
   *
   * @returns The fake adapter instance for assertions
   * @throws {E_QUEUE_NOT_INITIALIZED} If `init()` hasn't been called
   *
   * @example
   * ```typescript
   * const fake = QueueManager.fake()
   *
   * await SendEmailJob.dispatch({ to: 'user@example.com' })
   *
   * fake.assertPushed(SendEmailJob)
   * QueueManager.restore()
   * ```
   */
  fake(): FakeAdapter {
    if (!this.#initialized) {
      throw new errors.E_QUEUE_NOT_INITIALIZED()
    }

    if (this.#fakeState) {
      return this.#fakeState.fakeAdapter
    }

    const fakeAdapter = new FakeAdapter()

    this.#fakeState = {
      defaultAdapter: this.#defaultAdapter,
      adapters: this.#adapters,
      adapterInstances: this.#adapterInstances,
      globalRetryConfig: this.#globalRetryConfig,
      globalJobOptions: this.#globalJobOptions,
      queueConfigs: this.#queueConfigs,
      logger: this.#logger,
      jobFactory: this.#jobFactory,
      fakeAdapter,
    }

    const fakeFactory = () => fakeAdapter
    const nextAdapters: Record<string, AdapterFactory> = {}

    for (const name of Object.keys(this.#fakeState.adapters)) {
      nextAdapters[name] = fakeFactory
    }

    this.#adapters = nextAdapters
    this.#adapterInstances = new Map()

    return fakeAdapter
  }

  /**
   * Restore adapters after calling `fake()`.
   */
  restore(): void {
    if (!this.#fakeState) {
      return
    }

    void this.#fakeState.fakeAdapter.destroy()

    for (const adapter of this.#adapterInstances.values()) {
      void adapter.destroy()
    }

    const state = this.#fakeState
    this.#fakeState = undefined

    this.#defaultAdapter = state.defaultAdapter
    this.#adapters = state.adapters
    this.#adapterInstances = state.adapterInstances
    this.#globalRetryConfig = state.globalRetryConfig
    this.#globalJobOptions = state.globalJobOptions
    this.#queueConfigs = state.queueConfigs
    this.#logger = state.logger
    this.#jobFactory = state.jobFactory
  }

  /**
   * Get the merged retry configuration for a job.
   *
   * Configuration is merged with priority: job > queue > global.
   * This allows specific jobs or queues to override global defaults.
   *
   * @param queue - The queue name
   * @param jobRetryConfig - Optional job-level retry config
   * @returns The merged retry configuration
   *
   * @example
   * ```typescript
   * // Global: maxRetries=3, Queue: maxRetries=5, Job: maxRetries=1
   * // Result: maxRetries=1 (job wins)
   * const config = QueueManager.getMergedRetryConfig('emails', { maxRetries: 1 })
   * ```
   */
  getMergedRetryConfig(queue: string, jobRetryConfig?: RetryConfig): RetryConfig {
    const queueConfig = this.#queueConfigs.get(queue)
    const queueRetryConfig = queueConfig?.retry || {}

    let maxRetries =
      jobRetryConfig?.maxRetries ||
      queueRetryConfig.maxRetries ||
      this.#globalRetryConfig?.maxRetries ||
      0

    let backoff =
      jobRetryConfig?.backoff || queueRetryConfig.backoff || this.#globalRetryConfig?.backoff

    return { maxRetries, backoff }
  }

  /**
   * Get the configured job factory for custom instantiation.
   *
   * @returns The job factory function, or undefined if not configured
   */
  getJobFactory(): JobFactory | undefined {
    return this.#jobFactory
  }

  /**
   * Get the merged job options for a job (priority: job > queue > global).
   */
  getMergedJobOptions(queue: string, jobOptions?: JobOptions): JobOptions {
    const queueConfig = this.#queueConfigs.get(queue)
    const queueJobOptions = queueConfig?.defaultJobOptions

    return {
      removeOnComplete:
        jobOptions?.removeOnComplete ??
        queueJobOptions?.removeOnComplete ??
        this.#globalJobOptions?.removeOnComplete,
      removeOnFail:
        jobOptions?.removeOnFail ??
        queueJobOptions?.removeOnFail ??
        this.#globalJobOptions?.removeOnFail,
    }
  }

  #validateConfig(config: QueueManagerConfig): void {
    if (!config.adapters || Object.keys(config.adapters).length === 0) {
      throw new errors.E_CONFIGURATION_ERROR(['At least one adapter must be configured'])
    }

    if (!config.default) {
      throw new errors.E_CONFIGURATION_ERROR(['Default adapter must be specified'])
    }

    if (!config.adapters[config.default]) {
      throw new errors.E_CONFIGURATION_ERROR([
        `Default adapter "${config.default}" not found in adapters configuration`,
      ])
    }

    for (const [name, factory] of Object.entries(config.adapters)) {
      if (typeof factory !== 'function') {
        throw new errors.E_CONFIGURATION_ERROR([`Adapter "${name}" must be a factory function`])
      }
    }
  }

  /**
   * Clean up all adapter instances and reset state.
   *
   * Call this when shutting down the application or when
   * you need to reinitialize with a new configuration.
   *
   * @example
   * ```typescript
   * // On application shutdown
   * await QueueManager.destroy()
   * ```
   */
  async destroy() {
    for (const [name, adapter] of this.#adapterInstances) {
      debug('destroying adapter "%s"', name)
      await adapter.destroy()
    }

    if (this.#fakeState) {
      await this.#fakeState.fakeAdapter.destroy()

      for (const [name, adapter] of this.#fakeState.adapterInstances) {
        debug('destroying adapter "%s"', name)
        await adapter.destroy()
      }
    }

    this.#adapterInstances.clear()
    this.#initialized = false
    this.#fakeState = undefined
  }
}

/** Global queue manager singleton */
export const QueueManager = new QueueManagerSingleton()
