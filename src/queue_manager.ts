import * as errors from '#src/exceptions'
import debug from '#src/debug'
import { Locator } from '#src/locator'
import type { Adapter } from '#contracts/adapter'
import type { AdapterFactory, QueueConfig, QueueManagerConfig, RetryConfig } from '#types/main'

class QueueManagerSingleton {
  #defaultAdapter!: string
  #adapters: Record<string, AdapterFactory> = {}
  #globalRetryConfig?: RetryConfig
  #queueConfigs: Map<string, QueueConfig> = new Map()

  async init(config: QueueManagerConfig) {
    debug('initializing queue manager with config: %O', config)

    this.#validateConfig(config)

    this.#defaultAdapter = config.default
    this.#adapters = config.adapters
    this.#globalRetryConfig = config.retry

    if (config.queues) {
      for (const [queue, queueConfig] of Object.entries(config.queues)) {
        this.#queueConfigs.set(queue, queueConfig as QueueConfig)
      }
    }

    await Locator.registerFromGlob(config.locations)

    return this
  }

  use(adapter?: string): Adapter {
    if (!adapter) {
      adapter = this.#defaultAdapter
    }

    const adapterInstance = this.#adapters[adapter]

    if (!adapterInstance) {
      throw new errors.E_CONFIGURATION_ERROR([`Adapter "${adapter}" is not registered`])
    }

    debug('using adapter "%s"', adapter)

    try {
      return adapterInstance()
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

    if (!config.locations || config.locations.length === 0) {
      throw new errors.E_CONFIGURATION_ERROR(['Job locations must be specified'])
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
    for (const adapterName in this.#adapters) {
      const adapter = this.#adapters[adapterName]()
      await adapter.destroy()
    }
  }
}

export const QueueManager = new QueueManagerSingleton()
