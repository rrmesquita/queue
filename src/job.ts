import { JobDispatcher } from './job_dispatcher.js'
import type { JobOptions } from './types/main.js'

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
 *   async execute() {
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

  /** Static options for this job class (queue, retries, timeout, etc.) */
  static options: JobOptions = {}

  /** The payload data passed to this job instance */
  get payload(): Payload {
    return this.#payload
  }

  /**
   * Create a new job instance.
   *
   * @param payload - The data to be processed by this job
   */
  constructor(payload: Payload) {
    this.#payload = payload
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
    this: new (payload: any) => T,
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
