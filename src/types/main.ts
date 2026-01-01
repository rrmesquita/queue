import type { BackoffStrategy as BackoffStrategyClass } from '../strategies/backoff_strategy.js'
import type { Adapter } from '../contracts/adapter.js'
import type { Logger } from '../logger.js'
import { Job } from '../job.js'

export type { Logger }

export type Duration = number | string

export interface JobData {
  id: string
  name: string
  payload: any
  attempts: number
  priority?: number
  nextRetryAt?: Date
  stalledCount?: number
}

export interface JobOptions {
  queue?: string
  adapter?: string | (() => Adapter)
  maxRetries?: number
  priority?: number
  retry?: RetryConfig
  timeout?: Duration
  failOnTimeout?: boolean
}

export type JobClass<T extends Job = Job> = (new (payload: any) => T) & { options?: JobOptions }

/**
 * Factory function for custom job instantiation.
 *
 * Use this to integrate with IoC containers for dependency injection.
 * The factory receives the job class and payload, and must return
 * a job instance (or a Promise that resolves to one).
 *
 * @param JobClass - The job class to instantiate
 * @param payload - The payload data for the job
 * @returns The job instance, or a Promise resolving to the instance
 *
 * @example
 * ```typescript
 * // With AdonisJS IoC container
 * const worker = new Worker({
 *   worker: {
 *     jobFactory: async (JobClass, payload) => {
 *       return app.container.make(JobClass, [payload])
 *     }
 *   }
 * })
 * ```
 */
export type JobFactory = (JobClass: JobClass, payload: any) => Job | Promise<Job>

export interface RetryConfig {
  maxRetries?: number
  backoff?: () => BackoffStrategyClass
}

export type BackoffStrategy = 'exponential' | 'linear' | 'fixed'

export interface BackoffConfig {
  strategy: BackoffStrategy
  baseDelay: Duration
  maxDelay?: Duration
  multiplier?: number
  jitter?: boolean
}

export interface QueueConfig {
  adapter?: string
  retry?: any
}

export interface WorkerConfig {
  /**
   * Maximum number of jobs to process concurrently.
   * @default 1
   */
  concurrency?: number

  /**
   * Delay between queue polls when idle (no jobs available).
   * @default '2s'
   */
  idleDelay?: Duration

  /**
   * Maximum duration a job can run before being timed out.
   * Can be overridden per job via JobOptions.timeout.
   * @default undefined (no timeout)
   */
  timeout?: Duration

  /**
   * Duration after which an active job is considered stalled.
   * A stalled job is one that was acquired but the worker stopped
   * responding (e.g., due to a crash).
   * @default '30s'
   */
  stalledThreshold?: Duration

  /**
   * How often to check for stalled jobs.
   * @default '30s'
   */
  stalledInterval?: Duration

  /**
   * Maximum number of times a job can be recovered from stalled state
   * before being marked as failed permanently.
   * @default 1
   */
  maxStalledCount?: number

  /**
   * Whether to automatically stop the worker on SIGINT/SIGTERM signals.
   * When enabled, the worker will wait for running jobs to complete
   * before stopping.
   * @default true
   */
  gracefulShutdown?: boolean

  /**
   * Callback invoked when a shutdown signal is received.
   * Called before the worker starts stopping.
   */
  onShutdownSignal?: () => void | Promise<void>

  /**
   * Custom factory function for job instantiation.
   *
   * Use this to integrate with IoC containers for dependency injection.
   * When provided, this factory is called instead of `new JobClass(payload)`.
   *
   * @example
   * ```typescript
   * const worker = new Worker({
   *   worker: {
   *     jobFactory: async (JobClass, payload) => {
   *       // Inject dependencies via IoC container
   *       return app.container.make(JobClass, [payload])
   *     }
   *   }
   * })
   * ```
   */
  jobFactory?: JobFactory
}

export type WorkerCycle =
  | { type: 'started'; queue: string; job: any }
  | { type: 'completed'; queue: string; job: any }
  | { type: 'idle'; suggestedDelay: Duration }
  | { type: 'error'; error: Error; suggestedDelay: Duration }

export type AdapterFactory<T extends Adapter = Adapter> = () => T

export interface QueueManagerConfig {
  default: string
  adapters: Record<string, AdapterFactory>
  retry?: RetryConfig
  queues?: Record<string, QueueConfig>
  worker?: WorkerConfig
  locations?: string[]
  logger?: Logger
}
