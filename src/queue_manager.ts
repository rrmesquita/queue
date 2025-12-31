import * as errors from './exceptions.js'
import debug from './debug.js'
import { Locator } from './locator.js'
import { consoleLogger, type Logger } from './logger.js'
import type { Adapter } from './contracts/adapter.js'
import type { AdapterFactory, QueueConfig, QueueManagerConfig, RetryConfig } from './types/main.js'

class QueueManagerSingleton {
  #defaultAdapter!: string
  #adapters: Record<string, AdapterFactory> = {}
  #adapterInstances: Map<string, Adapter> = new Map()
  #globalRetryConfig?: RetryConfig
  #queueConfigs: Map<string, QueueConfig> = new Map()
  #logger: Logger = consoleLogger

  async init(config: QueueManagerConfig) {
    debug('initializing queue manager with config: %O', config)

    this.#validateConfig(config)

    this.#adapterInstances.clear()

    this.#defaultAdapter = config.default
    this.#adapters = config.adapters
    this.#globalRetryConfig = config.retry
    this.#logger = config.logger ?? consoleLogger

    if (config.queues) {
      for (const [queue, queueConfig] of Object.entries(config.queues)) {
        this.#queueConfigs.set(queue, queueConfig as QueueConfig)
      }
    }

    if (config.locations && config.locations.length > 0) {
      await Locator.registerFromGlob(config.locations)
    }

    return this
  }

  use(adapter?: string): Adapter {
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
      // TODO: Improve error handling
      throw new Error()
      // throw new errors.E_ADAPTER_ERROR(`Failed to initialize adapter "${adapter}"`, error as Error)
    }
  }

  /**
   * Priority: job > queue > global
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

  async destroy() {
    for (const [name, adapter] of this.#adapterInstances) {
      debug('destroying adapter "%s"', name)
      await adapter.destroy()
    }
    this.#adapterInstances.clear()
  }
}

export const QueueManager = new QueueManagerSingleton()
