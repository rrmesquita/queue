import { Locator } from '../locator.js'
import { QueueManager } from '../queue_manager.js'
import type { Adapter, AcquiredJob } from '../contracts/adapter.js'
import type {
  JobContext,
  JobData,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
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

  pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    for (const job of jobs) {
      await this.pushOn(queue, job)
    }
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

  completeJob(_jobId: string, _queue: string, _removeOnComplete?: JobRetention): Promise<void> {
    return Promise.resolve()
  }

  failJob(
    _jobId: string,
    _queue: string,
    _error?: Error,
    _removeOnFail?: JobRetention
  ): Promise<void> {
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

  getJob(_jobId: string, _queue: string): Promise<null> {
    return Promise.resolve(null)
  }

  destroy(): Promise<void> {
    return Promise.resolve()
  }

  createSchedule(_config: ScheduleConfig): Promise<string> {
    // No-op: schedules don't make sense for sync adapter
    // Return a fake ID so code doesn't break in dev
    return Promise.resolve(`sync-schedule-${Date.now()}`)
  }

  getSchedule(_id: string): Promise<ScheduleData | null> {
    return Promise.resolve(null)
  }

  listSchedules(_options?: ScheduleListOptions): Promise<ScheduleData[]> {
    return Promise.resolve([])
  }

  updateSchedule(
    _id: string,
    _updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    return Promise.resolve()
  }

  deleteSchedule(_id: string): Promise<void> {
    return Promise.resolve()
  }

  claimDueSchedule(): Promise<ScheduleData | null> {
    // SyncAdapter doesn't support scheduling
    return Promise.resolve(null)
  }

  async #execute(jobName: string, payload: any, queue: string = 'default'): Promise<any> {
    const JobClass = Locator.get(jobName)

    if (!JobClass) {
      throw new Error(`Job class ${jobName} not found.`)
    }

    const context: JobContext = {
      jobId: `sync-${Date.now()}`,
      name: jobName,
      attempt: 1,
      queue,
      priority: DEFAULT_PRIORITY,
      acquiredAt: new Date(),
      stalledCount: 0,
    }

    const jobFactory = QueueManager.getJobFactory()
    const jobInstance = jobFactory ? await jobFactory(JobClass) : new JobClass()

    jobInstance.$hydrate(payload, context)
    await jobInstance.execute()
  }
}
