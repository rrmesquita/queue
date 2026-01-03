import { JobDispatcher } from './job_dispatcher.js'
import { ScheduleBuilder } from './schedule_builder.js'
import type { JobContext, JobOptions } from './types/main.js'

/**
 * Abstract base class for all queue jobs.
 *
 * Extend this class to create your own jobs. Each job must implement
 * the `execute()` method which contains the job's business logic.
 *
 * The constructor is reserved for dependency injection. Payload and context
 * are provided separately via the `$hydrate()` method (called by the worker).
 *
 * @typeParam Payload - The type of data this job receives
 *
 * @example
 * ```typescript
 * import { Job } from '@boringnode/queue'
 *
 * interface SendEmailPayload {
 *   to: string
 *   subject: string
 *   body: string
 * }
 *
 * export default class SendEmailJob extends Job<SendEmailPayload> {
 *   static options = {
 *     queue: 'emails',
 *     maxRetries: 3,
 *   }
 *
 *   // Constructor is for dependency injection only
 *   constructor(private mailer: MailerService) {
 *     super()
 *   }
 *
 *   async execute() {
 *     console.log(`Attempt ${this.context.attempt} for job ${this.context.jobId}`)
 *     await this.mailer.send(this.payload.to, this.payload.subject, this.payload.body)
 *   }
 *
 *   async failed(error: Error) {
 *     console.error(`Failed to send email to ${this.payload.to}:`, error)
 *   }
 * }
 * ```
 */
export abstract class Job<Payload = any> {
  #payload!: Payload
  #context!: JobContext
  #signal?: AbortSignal

  /**
   * Static options for this job class.
   *
   * Override this property in subclasses to configure job behavior
   * such as queue name, retry policy, timeout, and more.
   *
   * @example
   * ```typescript
   * class SendEmailJob extends Job<SendEmailPayload> {
   *   static options = {
   *     queue: 'emails',
   *     maxRetries: 3,
   *     timeout: '30s',
   *   }
   * }
   * ```
   */
  static options: JobOptions = {}

  /**
   * The payload data passed to this job instance.
   *
   * Contains the data provided when the job was dispatched.
   * Available after the job has been hydrated by the worker.
   *
   * @example
   * ```typescript
   * async execute() {
   *   const { to, subject, body } = this.payload
   *   await sendEmail(to, subject, body)
   * }
   * ```
   */
  get payload(): Payload {
    return this.#payload
  }

  /**
   * Context information for the current job execution.
   *
   * Provides metadata such as job ID, current attempt number,
   * queue name, priority, and timing information.
   *
   * @example
   * ```typescript
   * async execute() {
   *   if (this.context.attempt > 1) {
   *     console.log(`Retry attempt ${this.context.attempt}`)
   *   }
   *   console.log(`Processing job ${this.context.jobId} on queue ${this.context.queue}`)
   * }
   * ```
   */
  get context(): JobContext {
    return this.#context
  }

  /**
   * The abort signal for timeout handling.
   *
   * Check `signal.aborted` in long-running operations to handle timeouts gracefully.
   *
   * @example
   * ```typescript
   * async execute() {
   *   for (const item of this.payload.items) {
   *     if (this.signal?.aborted) {
   *       throw new Error('Job timed out')
   *     }
   *     await processItem(item)
   *   }
   * }
   * ```
   */
  get signal(): AbortSignal | undefined {
    return this.#signal
  }

  /**
   * Hydrate the job with payload, context, and optional abort signal.
   *
   * This method is called by the worker after instantiation to provide
   * the job's runtime data. It should not be called directly by user code.
   *
   * @param payload - The data to be processed by this job
   * @param context - The job execution context
   * @param signal - Optional abort signal for timeout handling
   *
   * @internal
   */
  $hydrate(payload: Payload, context: JobContext, signal?: AbortSignal): void {
    this.#payload = payload
    this.#context = Object.freeze(context)
    this.#signal = signal
  }

  /**
   * Dispatch this job to the queue.
   *
   * Returns a JobDispatcher for fluent configuration before dispatching.
   * The job is not actually dispatched until `.run()` is called or the
   * dispatcher is awaited.
   *
   * @param payload - The data to pass to the job
   * @returns A JobDispatcher for fluent configuration
   *
   * @example
   * ```typescript
   * // Simple dispatch
   * await SendEmailJob.dispatch({ to: 'user@example.com', subject: 'Hello' })
   *
   * // With options
   * await SendEmailJob.dispatch({ to: 'user@example.com' })
   *   .toQueue('high-priority')
   *   .priority(1)
   *   .in('5m')
   *   .run()
   * ```
   */
  static dispatch<T extends Job>(
    this: new (...args: any[]) => T,
    payload: T extends Job<infer P> ? P : never
  ): JobDispatcher<T extends Job<infer P> ? P : never> {
    const dispatcher = new JobDispatcher<T extends Job<infer P> ? P : never>(
      (this as any).jobName,
      payload
    )

    if ((this as any).options.queue) {
      dispatcher.toQueue((this as any).options.queue)
    }

    if ((this as any).options.adapter) {
      dispatcher.with((this as any).options.adapter)
    }

    if ((this as any).options.priority !== undefined) {
      dispatcher.priority((this as any).options.priority)
    }

    return dispatcher
  }

  /**
   * Create a schedule for this job.
   *
   * Returns a ScheduleBuilder for fluent configuration before creating the schedule.
   * The schedule is not actually created until `.run()` is called or the
   * builder is awaited.
   *
   * @param payload - The data to pass to the job on each run
   * @returns A ScheduleBuilder for fluent configuration
   *
   * @example
   * ```typescript
   * // Cron schedule
   * await CleanupJob.schedule({ days: 30 })
   *   .id('cleanup-daily')
   *   .cron('0 0 * * *')
   *   .timezone('Europe/Paris')
   *   .run()
   *
   * // Interval schedule
   * await SyncJob.schedule({ source: 'api' })
   *   .every('5m')
   *   .run()
   * ```
   */
  static schedule<T extends Job>(
    this: new (...args: any[]) => T,
    payload: T extends Job<infer P> ? P : never
  ): ScheduleBuilder {
    return new ScheduleBuilder((this as any).jobName, payload)
  }

  /**
   * Execute the job's business logic.
   *
   * This method is called by the worker when processing the job.
   * Implement your job's logic here.
   *
   * For timeout handling, use `this.signal` which is available after hydration.
   *
   * @throws Any error thrown will trigger retry logic (if configured)
   *
   * @example
   * ```typescript
   * async execute() {
   *   for (const item of this.payload.items) {
   *     if (this.signal?.aborted) {
   *       throw new Error('Job timed out')
   *     }
   *     await processItem(item)
   *   }
   * }
   * ```
   */
  abstract execute(): Promise<void>

  /**
   * Called when the job has permanently failed (after all retries exhausted).
   *
   * Use this hook for cleanup, logging, or notifications.
   * This is optional - implement only if you need failure handling.
   *
   * @param error - The error that caused the final failure
   *
   * @example
   * ```typescript
   * async failed(error: Error) {
   *   await notifyAdmin(`Job failed: ${error.message}`)
   *   await cleanup(this.payload)
   * }
   * ```
   */
  failed?(error: Error): Promise<void>
}
