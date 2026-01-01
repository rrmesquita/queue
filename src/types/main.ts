import type { BackoffStrategy as BackoffStrategyClass } from '../strategies/backoff_strategy.js'
import type { Adapter } from '../contracts/adapter.js'
import type { Logger } from '../logger.js'
import { Job } from '../job.js'

export type { Logger }

export type Duration = number | string

/**
 * Result returned when dispatching a job.
 *
 * @example
 * ```typescript
 * const { jobId, repeatId } = await SyncJob.dispatch(payload).every('5s')
 *
 * // Later, cancel the repeat chain
 * if (repeatId) {
 *   await QueueManager.cancelRepeat(repeatId)
 * }
 * ```
 */
export interface DispatchResult {
  /** Unique identifier for this specific job instance */
  jobId: string

  /**
   * Unique identifier for the repeat chain.
   * Only present when the job was dispatched with `.every()`.
   * Use this to cancel the repeat chain via `QueueManager.cancelRepeat()`.
   */
  repeatId?: string
}

/**
 * Configuration for repeating jobs.
 *
 * When a job completes successfully and has a repeat config,
 * it will be automatically re-dispatched after the specified interval.
 */
export interface RepeatConfig {
  /** Interval in milliseconds between job executions */
  interval: number

  /**
   * Number of repetitions remaining.
   * - undefined = infinite repetitions
   * - 0 = no more repetitions (last run)
   * - n = n repetitions remaining
   */
  remaining?: number

  /**
   * Unique identifier for the repeat chain.
   * All jobs in the same repeat chain share this ID.
   * Used for cancelling the entire repeat chain.
   */
  groupId?: string
}

export interface JobData {
  id: string
  name: string
  payload: any
  attempts: number
  priority?: number
  nextRetryAt?: Date
  stalledCount?: number

  /** Configuration for repeating this job after completion */
  repeat?: RepeatConfig
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

/**
 * Context information available to a job during execution.
 *
 * Provides metadata about the current job execution, including
 * retry information, queue details, and timing.
 *
 * @example
 * ```typescript
 * class MyJob extends Job<Payload> {
 *   async execute() {
 *     console.log(`Attempt ${this.context.attempt} of job ${this.context.jobId}`)
 *     console.log(`Running on queue: ${this.context.queue}`)
 *   }
 * }
 * ```
 */
export interface JobContext {
  /** Unique identifier for this job */
  jobId: string

  /** Job class name */
  name: string

  /** Current attempt number (1-based: first attempt = 1) */
  attempt: number

  /** Queue name this job is being processed from */
  queue: string

  /** Job priority (lower number = higher priority) */
  priority: number

  /** When this job was acquired by the worker for processing */
  acquiredAt: Date

  /** Number of times this job has been recovered from stalled state */
  stalledCount: number

  /**
   * Whether this job is configured to repeat.
   * True if the job was dispatched with `.every()`.
   */
  isRepeating: boolean

  /**
   * Number of repetitions remaining after this execution.
   * - undefined = infinite repetitions
   * - 0 = this is the last execution
   * - n = n more executions after this one
   */
  repeatRemaining?: number

  /**
   * Unique identifier for the repeat chain.
   * Only present for repeating jobs (when `.every()` was used).
   * All jobs in the same repeat chain share this ID.
   */
  repeatId?: string
}

export type JobClass<T extends Job = Job> = (new (payload: any, context: JobContext) => T) & {
  options?: JobOptions
}

/**
 * Factory function for custom job instantiation.
 *
 * Use this to integrate with IoC containers for dependency injection.
 * The factory receives the job class, payload, and context, and must return
 * a job instance (or a Promise that resolves to one).
 *
 * @param JobClass - The job class to instantiate
 * @param payload - The payload data for the job
 * @param context - The job execution context (jobId, attempt, queue, etc.)
 * @returns The job instance, or a Promise resolving to the instance
 *
 * @example
 * ```typescript
 * // With AdonisJS IoC container
 * const worker = new Worker({
 *   worker: {
 *     jobFactory: async (JobClass, payload, context) => {
 *       return app.container.make(JobClass, [payload, context])
 *     }
 *   }
 * })
 * ```
 */
export type JobFactory = (
  JobClass: JobClass,
  payload: any,
  context: JobContext
) => Job | Promise<Job>

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

  /**
   * Custom factory function for job instantiation.
   *
   * Use this to integrate with IoC containers for dependency injection.
   * When provided, this factory is called instead of `new JobClass(payload, context)`.
   *
   * @example
   * ```typescript
   * await QueueManager.init({
   *   default: 'redis',
   *   adapters: { redis: redis() },
   *   jobFactory: async (JobClass, payload, context) => {
   *     return app.container.make(JobClass, [payload, context])
   *   }
   * })
   * ```
   */
  jobFactory?: JobFactory
}
