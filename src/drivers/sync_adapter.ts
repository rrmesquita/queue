import { Locator } from '../locator.js'
import { QueueManager } from '../queue_manager.js'
import type { Adapter, AcquiredJob } from '../contracts/adapter.js'
import type { JobContext, JobData } from '../types/main.js'
import { DEFAULT_PRIORITY } from '../constants.js'

/**
 * Create a sync adapter factory.
 */
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

  pushOn(queue: string, jobData: JobData): Promise<void> {
    return this.#execute(jobData.name, jobData.payload, queue)
  }

  pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void> {
    setTimeout(() => {
      void this.#execute(jobData.name, jobData.payload, queue)
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

  recoverStalledJobs(
    _queue: string,
    _stalledThreshold: number,
    _maxStalledCount: number
  ): Promise<number> {
    // SyncAdapter has no stalled jobs - jobs are executed immediately
    return Promise.resolve(0)
  }

  destroy(): Promise<void> {
    return Promise.resolve()
  }

  cancelRepeat(_groupId: string): Promise<void> {
    // SyncAdapter doesn't support repeating jobs
    return Promise.resolve()
  }

  isRepeatCancelled(_groupId: string): Promise<boolean> {
    // SyncAdapter doesn't support repeating jobs
    return Promise.resolve(false)
  }

  async #execute(jobName: string, payload: any, queue: string = 'default'): Promise<any> {
    const JobClass = Locator.get(jobName)

    if (!JobClass) {
      throw new Error(`Job class ${jobName} not found.`)
    }

    const context: JobContext = Object.freeze({
      jobId: `sync-${Date.now()}`,
      name: jobName,
      attempt: 1,
      queue,
      priority: DEFAULT_PRIORITY,
      acquiredAt: new Date(),
      stalledCount: 0,
      isRepeating: false,
      repeatRemaining: undefined,
    })

    const jobFactory = QueueManager.getJobFactory()
    const jobInstance = jobFactory
      ? await jobFactory(JobClass, payload, context)
      : new JobClass(payload, context)

    await jobInstance.execute()
  }
}
