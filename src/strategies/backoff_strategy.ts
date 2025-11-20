import type { BackoffConfig, Duration } from '#types/main'
import * as errors from '#src/exceptions'
import { parse } from '#src/utils'
import { RuntimeException } from '@poppinss/utils'
import { assertUnreachable } from '@poppinss/utils/assert'

export class BackoffStrategy {
  readonly #config: BackoffConfig

  constructor(config: BackoffConfig) {
    this.#config = config
    this.#validateConfig()
  }

  calculateDelay(attempt: number): number {
    if (attempt < 1) {
      throw new RuntimeException('Attempt number must be >= 1')
    }

    const baseDelayMs = parse(this.#config.baseDelay)
    const maxDelayMs = this.#config.maxDelay ? parse(this.#config.maxDelay) : Infinity
    const multiplier = this.#config.multiplier ?? 2

    let delay: number

    switch (this.#config.strategy) {
      case 'exponential':
        delay = baseDelayMs * Math.pow(multiplier, attempt - 1)
        break
      case 'linear':
        delay = baseDelayMs * attempt
        break
      case 'fixed':
        delay = baseDelayMs
        break
      default:
        assertUnreachable(this.#config.strategy)
    }

    // Apply max delay limit
    delay = Math.min(delay, maxDelayMs)

    if (this.#config.jitter) {
      delay = this.#applyJitter(delay)
    }

    return Math.floor(delay)
  }

  getNextRetryAt(attempt: number): Date {
    const delay = this.calculateDelay(attempt)
    return new Date(Date.now() + delay)
  }

  getConfig(): Readonly<BackoffConfig> {
    return Object.freeze({ ...this.#config })
  }

  #validateConfig() {
    const baseDelayMs = parse(this.#config.baseDelay)

    if (baseDelayMs <= 0) {
      throw new errors.E_INVALID_BASE_DELAY([
        'Base delay must be a positive integer greater than zero',
      ])
    }

    if (this.#config.maxDelay) {
      const maxDelayMs = parse(this.#config.maxDelay)

      if (maxDelayMs <= 0) {
        throw new errors.E_INVALID_MAX_DELAY([
          'Max delay must be a positive integer greater than zero',
        ])
      }

      if (maxDelayMs <= baseDelayMs) {
        throw new errors.E_INVALID_MAX_DELAY(['Max delay should be greater than base delay'])
      }
    }

    if (this.#config.multiplier !== undefined) {
      if (this.#config.multiplier <= 0) {
        throw new errors.E_INVALID_MULTIPLIER([
          'Multiplier must be a positive number greater than zero',
        ])
      }

      if (this.#config.strategy === 'exponential' && this.#config.multiplier < 1) {
        throw new errors.E_INVALID_MULTIPLIER(['Exponential strategy multiplier should be >= 1'])
      }
    }
  }

  #applyJitter(delay: number): number {
    const jitterRange = delay * 0.25
    const jitter = (Math.random() - 0.5) * 2 * jitterRange

    return Math.max(0, delay + jitter)
  }
}

export function exponentialBackoff(config?: Partial<Omit<BackoffConfig, 'strategy'>>) {
  return () =>
    new BackoffStrategy({
      strategy: 'exponential',
      baseDelay: '1s',
      maxDelay: '5m',
      multiplier: 2,
      jitter: true,
      ...config,
    })
}

export function linearBackoff(config?: Partial<Omit<BackoffConfig, 'strategy'>>) {
  return () =>
    new BackoffStrategy({
      strategy: 'linear',
      baseDelay: '5s',
      maxDelay: '2m',
      ...config,
    })
}

export function fixedBackoff(delay: Duration = '10s') {
  return () =>
    new BackoffStrategy({
      strategy: 'fixed',
      baseDelay: delay,
    })
}

export function customBackoff(config: BackoffConfig) {
  return () => new BackoffStrategy(config)
}
