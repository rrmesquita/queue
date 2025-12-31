import type { JobData } from '../types/main.js'

export interface AcquiredJob extends JobData {
  acquiredAt: number
}

export interface Adapter {
  /**
   * Set the worker ID for this adapter instance.
   * Required before calling pop methods when consuming jobs.
   */
  setWorkerId(workerId: string): void

  /**
   * Pop the next available job from the default queue.
   * The driver handles locking internally.
   */
  pop(): Promise<AcquiredJob | null>

  /**
   * Pop the next available job from a specific queue.
   * The driver handles locking internally.
   */
  popFrom(queue: string): Promise<AcquiredJob | null>

  /**
   * Recover stalled jobs that have been active for too long.
   * Jobs that exceed maxStalledCount will be failed permanently.
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
   * Mark a job as completed and remove it from active set.
   */
  completeJob(jobId: string, queue: string): Promise<void>

  /**
   * Mark a job as failed permanently.
   */
  failJob(jobId: string, queue: string, error?: Error): Promise<void>

  /**
   * Retry a job - move back to pending queue with incremented attempts.
   */
  retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void>

  push(jobData: JobData): Promise<void>
  pushOn(queue: string, jobData: JobData): Promise<void>
  pushLater(jobData: JobData, delay: number): Promise<void>
  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void>

  size(): Promise<number>
  sizeOf(queue: string): Promise<number>
  destroy(): Promise<void>
}
