import debug from './debug.js'
import { randomUUID } from 'node:crypto'
import { QueueManager } from './queue_manager.js'
import type { Adapter } from './contracts/adapter.js'
import type { DispatchResult, Duration, RepeatConfig } from './types/main.js'
import { parse } from './utils.js'

/**
 * Fluent builder for dispatching jobs to the queue.
 *
 * Provides a chainable API for configuring job options before dispatch.
 * Usually created via `Job.dispatch()` rather than directly.
 *
 * ```
 * Job.dispatch(payload)
 *     .toQueue('emails')     // optional: target queue
 *     .priority(1)           // optional: 1-10, lower = higher priority
 *     .in('5m')              // optional: delay before processing
 *     .with('redis')         // optional: specific adapter
 *     .run()                 // dispatch the job
 * ```
 *
 * @typeParam T - The payload type for this job
 *
 * @example
 * ```typescript
 * // Simple dispatch (auto-runs via thenable)
 * await SendEmailJob.dispatch({ to: 'user@example.com', subject: 'Hello' })
 *
 * // With options
 * const jobId = await SendEmailJob.dispatch({ to: 'user@example.com' })
 *   .toQueue('high-priority')
 *   .priority(1)
 *   .run()
 *
 * // Delayed job
 * await ReminderJob.dispatch({ userId: 123 }).in('24h')
 * ```
 */
export class JobDispatcher<T> {
  readonly #name: string
  readonly #payload: T
  #queue: string = 'default'
  #adapter?: string | (() => Adapter)
  #delay?: Duration
  #priority?: number
  #repeatInterval?: number
  #repeatCount?: number

  /**
   * Create a new job dispatcher.
   *
   * @param name - The job class name (used to locate the class at runtime)
   * @param payload - The data to pass to the job
   */
  constructor(name: string, payload: T) {
    this.#name = name
    this.#payload = payload
  }

  /**
   * Set the target queue for this job.
   *
   * @param queue - Queue name (default: 'default')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await SendEmailJob.dispatch(payload).toQueue('emails')
   * ```
   */
  toQueue(queue: string): this {
    this.#queue = queue

    return this
  }

  /**
   * Delay the job execution.
   *
   * The job will be stored in a delayed state and moved to pending
   * after the delay expires.
   *
   * @param delay - Delay as milliseconds or duration string ('5s', '1h', '7d')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Send reminder in 24 hours
   * await ReminderJob.dispatch(payload).in('24h')
   *
   * // Process in 5 minutes
   * await CleanupJob.dispatch(payload).in('5m')
   * ```
   */
  in(delay: Duration): this {
    this.#delay = delay

    return this
  }

  /**
   * Set the job priority.
   *
   * Lower numbers = higher priority. Jobs with lower priority values
   * are processed before jobs with higher values.
   *
   * @param priority - Priority level (1-10, default: 5)
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // High priority job
   * await UrgentJob.dispatch(payload).priority(1)
   *
   * // Low priority job
   * await BackgroundJob.dispatch(payload).priority(10)
   * ```
   */
  priority(priority: number): this {
    this.#priority = priority

    return this
  }

  /**
   * Use a specific adapter for this job.
   *
   * @param adapter - Adapter name or factory function
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Use named adapter
   * await Job.dispatch(payload).with('redis')
   *
   * // Use custom adapter instance
   * await Job.dispatch(payload).with(() => new CustomAdapter())
   * ```
   */
  with(adapter: string | (() => Adapter)) {
    this.#adapter = adapter

    return this
  }

  /**
   * Make this job repeat at a fixed interval after each completion.
   *
   * The job will be re-dispatched after each successful execution.
   * Use `.times()` to limit the number of repetitions.
   *
   * @param interval - Interval as milliseconds or duration string ('5s', '1h', '7d')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Repeat every 5 seconds indefinitely
   * await SyncJob.dispatch(payload).every('5s')
   *
   * // Repeat every hour, 10 times total
   * await SyncJob.dispatch(payload).every('1h').times(10)
   * ```
   */
  every(interval: Duration): this {
    this.#repeatInterval = parse(interval)

    return this
  }

  /**
   * Limit the number of times this job will repeat.
   *
   * Must be used with `.every()`. The total number of executions
   * will be equal to the count specified (including the first run).
   *
   * @param count - Total number of times to execute the job
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Run exactly 5 times, every 10 seconds
   * await CleanupJob.dispatch(payload).every('10s').times(5)
   * ```
   */
  times(count: number): this {
    if (count < 1) {
      throw new Error('times() must be at least 1')
    }

    this.#repeatCount = count

    return this
  }

  /**
   * Dispatch the job to the queue.
   *
   * @returns A DispatchResult containing the jobId and optionally a repeatId
   *
   * @example
   * ```typescript
   * const { jobId, repeatId } = await SendEmailJob.dispatch(payload).every('5s').run()
   * console.log(`Dispatched job: ${jobId}`)
   *
   * // Cancel the repeat chain later
   * if (repeatId) {
   *   await QueueManager.cancelRepeat(repeatId)
   * }
   * ```
   */
  async run(): Promise<DispatchResult> {
    const id = randomUUID()

    debug('dispatching job %s with id %s using payload %s', this.#name, id, this.#payload)

    const adapter = this.#getAdapterInstance()

    const repeat = this.#buildRepeatConfig()

    const payload = {
      id,
      name: this.#name,
      payload: this.#payload,
      attempts: 0,
      priority: this.#priority,
      repeat,
    }

    if (this.#delay) {
      const parsedDelay = parse(this.#delay)

      await adapter.pushLaterOn(this.#queue, payload, parsedDelay)
    } else {
      await adapter.pushOn(this.#queue, payload)
    }

    return {
      jobId: id,
      repeatId: repeat?.groupId,
    }
  }

  #buildRepeatConfig(): RepeatConfig | undefined {
    if (!this.#repeatInterval) {
      return undefined
    }

    return {
      interval: this.#repeatInterval,
      // If times(n) was called, remaining = n - 1 (first run counts as one)
      // If not called, remaining is undefined (infinite)
      remaining: this.#repeatCount !== undefined ? this.#repeatCount - 1 : undefined,
      // Generate unique groupId for the repeat chain
      groupId: randomUUID(),
    }
  }

  /**
   * Thenable implementation for auto-dispatch when awaited.
   *
   * Allows `await Job.dispatch(payload)` without explicit `.run()`.
   *
   * @param onFulfilled - Success callback
   * @param onRejected - Error callback
   * @returns Promise resolving to the DispatchResult
   */
  then(
    onFulfilled?: (value: DispatchResult) => any,
    onRejected?: (reason: any) => any
  ): Promise<any> {
    return this.run().then(onFulfilled, onRejected)
  }

  #getAdapterInstance(): Adapter {
    if (!this.#adapter) {
      return QueueManager.use()
    }

    if (typeof this.#adapter === 'string') {
      return QueueManager.use(this.#adapter)
    }

    return this.#adapter()
  }
}
