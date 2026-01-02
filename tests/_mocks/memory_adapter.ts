import type { Adapter, AcquiredJob } from '../../src/contracts/adapter.js'
import type {
  JobData,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../../src/types/main.js'
import { randomUUID } from 'node:crypto'

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
  #schedules: Map<string, ScheduleData> = new Map()

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

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number> {
    const now = Date.now()
    let recovered = 0

    for (const [jobId, active] of this.#activeJobs.entries()) {
      const isStalled = now - active.acquiredAt > stalledThreshold

      if (!isStalled) {
        continue
      }

      const currentStalledCount = active.job.stalledCount ?? 0

      // Check if job has exceeded max stalled count
      if (currentStalledCount >= maxStalledCount) {
        // Fail permanently - just remove from active
        this.#activeJobs.delete(jobId)
        continue
      }

      // Recover the job - put back in queue with incremented stalledCount
      this.#activeJobs.delete(jobId)

      const updatedJob = {
        ...active.job,
        stalledCount: currentStalledCount + 1,
      }

      await this.pushOn(queue, updatedJob)
      recovered++
    }

    return recovered
  }

  destroy(): Promise<void> {
    for (const timeout of this.#pendingTimeouts) {
      clearTimeout(timeout)
    }

    this.#pendingTimeouts.clear()

    return Promise.resolve()
  }

  async createSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const now = new Date()

    const schedule: ScheduleData = {
      id,
      jobName: config.jobName,
      payload: config.payload,
      cronExpression: config.cronExpression ?? null,
      everyMs: config.everyMs ?? null,
      timezone: config.timezone,
      from: config.from ?? null,
      to: config.to ?? null,
      limit: config.limit ?? null,
      runCount: 0,
      nextRunAt: null, // Will be calculated by the caller
      lastRunAt: null,
      status: 'active',
      createdAt: now,
    }

    this.#schedules.set(id, schedule)
    return id
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    return this.#schedules.get(id) ?? null
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    const schedules = Array.from(this.#schedules.values())

    if (options?.status) {
      return schedules.filter((s) => s.status === options.status)
    }

    return schedules
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    const schedule = this.#schedules.get(id)
    if (!schedule) return

    if (updates.status !== undefined) schedule.status = updates.status
    if (updates.nextRunAt !== undefined) schedule.nextRunAt = updates.nextRunAt
    if (updates.lastRunAt !== undefined) schedule.lastRunAt = updates.lastRunAt
    if (updates.runCount !== undefined) schedule.runCount = updates.runCount
  }

  async deleteSchedule(id: string): Promise<void> {
    this.#schedules.delete(id)
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = new Date()

    // Find first due schedule
    const schedule = Array.from(this.#schedules.values()).find((s) => {
      if (s.status !== 'active') return false
      if (s.nextRunAt === null || s.nextRunAt > now) return false
      if (s.limit !== null && s.runCount >= s.limit) return false
      if (s.to !== null && now > s.to) return false
      return true
    })

    if (!schedule) return null

    // Calculate next run
    let nextRunAt: Date | null = null
    if (schedule.everyMs) {
      nextRunAt = new Date(now.getTime() + schedule.everyMs)
    } else if (schedule.cronExpression) {
      // For memory adapter in tests, just add 24h as approximation
      // Real adapters will use cron-parser
      nextRunAt = new Date(now.getTime() + 24 * 60 * 60 * 1000)
    }

    // Check if limit will be reached after this run
    const newRunCount = schedule.runCount + 1
    if (schedule.limit !== null && newRunCount >= schedule.limit) {
      nextRunAt = null // No more runs
    }

    // Check if end date will be passed
    if (nextRunAt && schedule.to !== null && nextRunAt > schedule.to) {
      nextRunAt = null // Past end date
    }

    // Clone schedule data before updating (return old state)
    const claimedSchedule: ScheduleData = { ...schedule }

    // Update schedule atomically
    schedule.nextRunAt = nextRunAt
    schedule.lastRunAt = now
    schedule.runCount = newRunCount

    return claimedSchedule
  }
}
