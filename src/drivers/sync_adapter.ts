import { Locator } from '#src/locator'
import type { Adapter, AcquiredJob } from '#contracts/adapter'
import type { JobData } from '#types/main'

export function sync() {
  return () => new SyncAdapter()
}

/**
 * Sync adapter executes jobs immediately when pushed.
 * Pop/complete/fail/retry are not supported as jobs are executed synchronously.
 */
export class SyncAdapter implements Adapter {
  setWorkerId(_workerId: string): void {}

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

  pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  popFrom(_queue: string): Promise<AcquiredJob | null> {
    throw new Error('SyncAdapter does not support pop - jobs are executed immediately on push')
  }

  completeJob(_jobId: string, _queue: string): Promise<void> {
    return Promise.resolve()
  }

  failJob(_jobId: string, _queue: string, _error?: Error): Promise<void> {
    return Promise.resolve()
  }

  retryJob(_jobId: string, _queue: string, _retryAt?: Date): Promise<void> {
    return Promise.resolve()
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
