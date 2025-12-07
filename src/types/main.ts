import type { BackoffStrategy as BackoffStrategyClass } from '#strategies/backoff_strategy'
import type { Adapter } from '#contracts/adapter'
import { Job } from '#src/job'

export type Duration = number | string

export interface JobData {
  id: string
  name: string
  payload: any
  attempts: number
  priority?: number
  nextRetryAt?: Date
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
  concurrency?: number
  pollingInterval?: Duration
  leaseTimeout?: Duration
  renewalInterval?: Duration
  timeout?: Duration
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
  locations: string[]
}
