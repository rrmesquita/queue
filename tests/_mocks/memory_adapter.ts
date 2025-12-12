import type { Adapter, AcquiredJob } from '../../src/contracts/adapter.js'
import type { JobData } from '../../src/types/main.js'

interface ActiveJob {
  job: JobData
  acquiredAt: number
}

export function memory() {
  return () => new MemoryAdapter()
}

export class MemoryAdapter implements Adapter {
  #queues: Map<string, JobData[]> = new Map()
  #activeJobs: Map<string, ActiveJob> = new Map()
  #pendingTimeouts: Set<NodeJS.Timeout> = new Set()

  setWorkerId(_workerId: string): void {}

  async size(): Promise<number> {
    return this.sizeOf('default')
  }

  async sizeOf(queue: string): Promise<number> {
    const jobs = this.#queues.get(queue) || []

    return jobs.length
  }

  async push(jobData: JobData): Promise<void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<void> {
    if (!this.#queues.has(queue)) {
      this.#queues.set(queue, [])
    }

    this.#queues.get(queue)!.push(jobData)
  }

  async pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void> {
    const timeout = setTimeout(() => {
      this.#pendingTimeouts.delete(timeout)
      void this.pushOn(queue, jobData)
    }, delay)

    this.#pendingTimeouts.add(timeout)

    return Promise.resolve()
  }

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const jobs = this.#queues.get(queue)

    if (!jobs || jobs.length === 0) {
      return null
    }

    const job = jobs.shift()
    if (!job) {
      return null
    }

    const acquiredAt = Date.now()
    this.#activeJobs.set(job.id, { job, acquiredAt })

    return { ...job, acquiredAt }
  }

  async completeJob(jobId: string, _queue: string): Promise<void> {
    this.#activeJobs.delete(jobId)
  }

  async failJob(jobId: string, _queue: string, _error?: Error): Promise<void> {
    this.#activeJobs.delete(jobId)
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const active = this.#activeJobs.get(jobId)
    if (!active) return

    this.#activeJobs.delete(jobId)

    const updatedJob = {
      ...active.job,
      attempts: (active.job.attempts || 0) + 1,
    }

    if (retryAt) {
      const delay = retryAt.getTime() - Date.now()

      if (delay > 0) {
        await this.pushLaterOn(queue, updatedJob, delay)
        return
      }
    }

    await this.pushOn(queue, updatedJob)
  }

  destroy(): Promise<void> {
    for (const timeout of this.#pendingTimeouts) {
      clearTimeout(timeout)
    }

    this.#pendingTimeouts.clear()

    return Promise.resolve()
  }
}
