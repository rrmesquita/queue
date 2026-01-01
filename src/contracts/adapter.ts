import type { JobData } from '../types/main.js'

/**
 * A job that has been acquired by a worker for processing.
 * Extends JobData with the timestamp when the job was acquired.
 */
export interface AcquiredJob extends JobData {
  /** Timestamp (in ms) when the job was acquired by the worker */
  acquiredAt: number
}

/**
 * Adapter interface for queue storage backends.
 *
 * Implementations handle job persistence, atomic operations, and
 * concurrency control. Built-in adapters: Redis, Knex (PostgreSQL/SQLite).
 *
 * @example
 * ```typescript
 * import { redis } from '@boringnode/queue'
 *
 * const config = {
 *   default: 'redis',
 *   adapters: {
 *     redis: redis({ host: 'localhost', port: 6379 })
 *   }
 * }
 * ```
 */
export interface Adapter {
  /**
   * Set the worker ID for this adapter instance.
   * Required before calling pop methods when consuming jobs.
   *
   * @param workerId - Unique identifier for the worker
   */
  setWorkerId(workerId: string): void

  /**
   * Pop the next available job from the default queue.
   * Atomically moves the job from pending to active state.
   *
   * @returns The acquired job, or null if queue is empty
   */
  pop(): Promise<AcquiredJob | null>

  /**
   * Pop the next available job from a specific queue.
   * Atomically moves the job from pending to active state.
   *
   * @param queue - The queue name to pop from
   * @returns The acquired job, or null if queue is empty
   */
  popFrom(queue: string): Promise<AcquiredJob | null>

  /**
   * Recover stalled jobs that have been active for too long.
   * A stalled job is one where the worker stopped responding (e.g., crash).
   *
   * Jobs within maxStalledCount are moved back to pending.
   * Jobs exceeding maxStalledCount are failed permanently.
   *
   * @param queue - The queue to check for stalled jobs
   * @param stalledThreshold - Duration in ms after which a job is considered stalled
   * @param maxStalledCount - Maximum times a job can be recovered before failing
   * @returns Number of jobs that were recovered (not including permanently failed ones)
   */
  recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number>

  /**
   * Mark a job as completed and remove it from the queue.
   *
   * @param jobId - The job ID to complete
   * @param queue - The queue the job belongs to
   */
  completeJob(jobId: string, queue: string): Promise<void>

  /**
   * Mark a job as failed permanently and remove it from the queue.
   *
   * @param jobId - The job ID to fail
   * @param queue - The queue the job belongs to
   * @param error - Optional error that caused the failure
   */
  failJob(jobId: string, queue: string, error?: Error): Promise<void>

  /**
   * Retry a job by moving it back to pending with incremented attempts.
   *
   * @param jobId - The job ID to retry
   * @param queue - The queue the job belongs to
   * @param retryAt - Optional future date to delay the retry
   */
  retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void>

  /**
   * Push a job to the default queue for immediate processing.
   *
   * @param jobData - The job data to push
   */
  push(jobData: JobData): Promise<void>

  /**
   * Push a job to a specific queue for immediate processing.
   *
   * @param queue - The queue name to push to
   * @param jobData - The job data to push
   */
  pushOn(queue: string, jobData: JobData): Promise<void>

  /**
   * Push a job to the default queue with a delay.
   *
   * @param jobData - The job data to push
   * @param delay - Delay in milliseconds before the job becomes available
   */
  pushLater(jobData: JobData, delay: number): Promise<void>

  /**
   * Push a job to a specific queue with a delay.
   *
   * @param queue - The queue name to push to
   * @param jobData - The job data to push
   * @param delay - Delay in milliseconds before the job becomes available
   */
  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void>

  /**
   * Get the number of pending jobs in the default queue.
   *
   * @returns The number of pending jobs
   */
  size(): Promise<number>

  /**
   * Get the number of pending jobs in a specific queue.
   *
   * @param queue - The queue name to check
   * @returns The number of pending jobs
   */
  sizeOf(queue: string): Promise<number>

  /**
   * Clean up resources (close connections, etc.).
   * Called when the worker stops or the adapter is no longer needed.
   */
  destroy(): Promise<void>

  /**
   * Cancel a repeating job chain.
   *
   * After calling this, `isRepeatCancelled` will return true for this groupId,
   * and the worker will not re-dispatch jobs with this groupId.
   *
   * @param groupId - The repeat chain identifier (from RepeatConfig.groupId)
   */
  cancelRepeat(groupId: string): Promise<void>

  /**
   * Check if a repeat chain has been cancelled.
   *
   * @param groupId - The repeat chain identifier to check
   * @returns True if the repeat chain has been cancelled
   */
  isRepeatCancelled(groupId: string): Promise<boolean>
}
