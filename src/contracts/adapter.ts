import type { JobData } from '#types/main'

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
   * Blocking pop that waits for a job to be available.
   * Supported by Redis adapter.
   */
  popAndWait?(queue: string, timeout: number): Promise<AcquiredJob | null>

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
