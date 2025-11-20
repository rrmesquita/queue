import { Locator } from '#src/locator'
import type { Adapter } from '#contracts/adapter'
import type { JobData, LeaseConfig } from '#types/main'
import type { LeaseManager } from '#contracts/lease_manager'

export function sync() {
  return () => new SyncAdapter()
}

export class SyncAdapter implements Adapter {
  createLeaseManager(_config: LeaseConfig): LeaseManager {
    throw new Error('Method not implemented.')
  }

  push(jobData: JobData): Promise<void> {
    return this.pushOn('default', jobData)
  }

  pushOn(_queue: string, jobData: JobData): Promise<void> {
    return this.#execute(jobData.name, jobData.payload)
  }

  pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  pushLaterOn(_queue: string, jobData: JobData, delay: number): Promise<void> {
    setTimeout(() => {
      void this.#execute(jobData.name, jobData.payload)
    }, delay)

    return Promise.resolve()
  }

  size(): Promise<number> {
    return this.sizeOf('default')
  }

  sizeOf(_queue: string): Promise<number> {
    return Promise.resolve(0)
  }

  pop(): Promise<JobData | null> {
    return this.popFrom('default')
  }

  popFrom(_queue: string): Promise<JobData | null> {
    throw new Error('Method not implemented.')
  }

  destroy(): Promise<void> {
    return Promise.resolve()
  }

  async #execute(jobName: string, payload: any): Promise<any> {
    const JobClass = Locator.get(jobName)

    if (!JobClass) {
      throw new Error(`Job class ${jobName} not found.`)
    }

    const jobInstance = new JobClass(payload)
    await jobInstance.execute()
  }
}
