import { JobDispatcher } from './job_dispatcher.js'
import type { JobContext, JobOptions } from './types/main.js'

/**
 * Abstract base class for all queue jobs.
 *
 * Extend this class to create your own jobs. Each job must implement
 * the `execute()` method which contains the job's business logic.
 *
 * @typeParam Payload - The type of data this job receives
 *
 * @example
 * ```typescript
 * import { Job } from '@boringnode/queue'
 * import type { JobContext } from '@boringnode/queue'
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
 *   constructor(payload: SendEmailPayload, context: JobContext) {
 *     super(payload, context)
 *   }
 *
 *   async execute() {
 *     console.log(`Attempt ${this.context.attempt} for job ${this.context.jobId}`)
 *     await sendEmail(this.payload.to, this.payload.subject, this.payload.body)
 *   }
 *
 *   async failed(error: Error) {
 *     console.error(`Failed to send email to ${this.payload.to}:`, error)
 *   }
 * }
 * ```
 */
export abstract class Job<Payload = any> {
  readonly #payload: Payload
  readonly #context: JobContext

  /** Static options for this job class (queue, retries, timeout, etc.) */
  static options: JobOptions = {}

  /** The payload data passed to this job instance */
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
   * Create a new job instance.
   *
   * @param payload - The data to be processed by this job
   * @param context - The job execution context (provided by the worker)
   */
  constructor(payload: Payload, context: JobContext) {
    this.#payload = payload
    this.#context = Object.freeze(context)
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
    this: new (payload: any, context: JobContext) => T,
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
   * Execute the job's business logic.
   *
   * This method is called by the worker when processing the job.
   * Implement your job's logic here.
   *
   * @param signal - Optional AbortSignal for timeout handling.
   *                 Check `signal.aborted` for long-running operations.
   * @throws Any error thrown will trigger retry logic (if configured)
   *
   * @example
   * ```typescript
   * async execute(signal?: AbortSignal) {
   *   for (const item of this.payload.items) {
   *     if (signal?.aborted) {
   *       throw new Error('Job timed out')
   *     }
   *     await processItem(item)
   *   }
   * }
   * ```
   */
  abstract execute(signal?: AbortSignal): Promise<void>

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
