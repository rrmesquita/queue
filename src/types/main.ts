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
   * How often to poll for new jobs when the queue is empty.
   * @default '2s'
   */
  pollingInterval?: Duration

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
