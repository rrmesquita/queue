import { MemoryLeaseManager } from './memory_lease_manager.ts'
import type { Adapter } from '#contracts/adapter'
import type { LeaseManager } from '#contracts/lease_manager'
import type { JobData, LeaseConfig } from '#types/main'

export function memory() {
  return () => new MemoryAdapter()
}

export class MemoryAdapter implements Adapter {
  #queues: Map<string, JobData[]> = new Map()

  createLeaseManager(config: LeaseConfig): LeaseManager {
    return new MemoryLeaseManager(config)
  }

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
    setTimeout(() => {
      void this.pushOn(queue, jobData)
    }, delay)

    return Promise.resolve()
  }

  async pop(): Promise<JobData | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<JobData | null> {
    const jobs = this.#queues.get(queue)

    if (!jobs || jobs.length === 0) {
      return null
    }

    return jobs.shift() || null
  }

  destroy(): Promise<void> {
    return Promise.resolve()
  }
}
