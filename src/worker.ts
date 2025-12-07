import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import debug from '#src/debug'
import { parse } from '#src/utils'
import * as errors from '#src/exceptions'
import { QueueManager } from '#src/queue_manager'
import { JobPool } from '#src/job_pool'
import type { Adapter } from '#contracts/adapter'
import type { LeaseManager } from '#contracts/lease_manager'
import type { AcquiredJob, JobData, QueueManagerConfig, WorkerCycle } from '#types/main'
import { Locator } from '#src/locator'
import type { JobOptions } from '#types/main'
import type { Job } from '#src/job'

export class Worker {
  readonly #id: string
  readonly #config: QueueManagerConfig
  #adapter!: Adapter
  #leaseManager!: LeaseManager
  #running = false
  #initialized = false
  #generator?: AsyncGenerator<WorkerCycle, void, unknown>
  #pool?: JobPool

  get id() {
    return this.#id
  }

  constructor(config: QueueManagerConfig) {
    this.#config = config
    this.#id = randomUUID()

    debug('created worker with id %s and config %O', this.#id, config)
  }

  async init() {
    if (this.#initialized) {
      return
    }

    debug('initializing worker %s', this.#id)

    await QueueManager.init(this.#config)

    this.#adapter = QueueManager.use()
    this.#leaseManager = this.#adapter.createLeaseManager({
      workerId: this.#id,
      leaseTimeout: parse(this.#config.worker?.leaseTimeout || '5m'),
      renewalInterval: parse(this.#config.worker?.renewalInterval || '5m'),
    })

    this.#initialized = true

    debug('worker %s initialized', this.#id)
  }

  async start(queues: string[] = ['default']): Promise<void> {
    await this.init()

    if (this.#running) {
      debug('worker %s is already running', this.#id)
      return
    }

    this.#running = true

    debug('starting worker %s on queues: %O', this.#id, queues)

    await this.#setupGracefulShutdown()

    for await (const cycle of this.process(queues)) {
      if (['started', 'completed'].includes(cycle.type)) {
        continue
      }

      if (['idle', 'error'].includes(cycle.type)) {
        // @ts-expect-error - we know suggestedDelay exists for these types
        const delay = parse(cycle.suggestedDelay)

        if (cycle.type === 'error') {
          debug('worker %s encountered an error: %O', this.#id, cycle.error)
        } else {
          debug('worker %s is idle, waiting for %dms', this.#id, delay)
        }

        await setTimeout(delay)
      }
    }
  }

  async stop() {
    debug('stopping worker %s', this.#id)

    this.#running = false

    if (this.#pool) {
      debug('worker %s: waiting for %d running jobs to complete', this.#id, this.#pool.size)
      await this.#pool.drain()
    }

    if (this.#leaseManager) {
      await this.#leaseManager.destroy()
    }

    if (this.#adapter) {
      await this.#adapter.destroy()
    }
  }

  async processCycle(queues: string[]): Promise<WorkerCycle | null> {
    await this.init()

    this.#running = true

    if (!this.#generator) {
      this.#generator = this.process(queues)
    }

    const result = await this.#generator.next()

    if (result.done) {
      this.#generator = undefined
      return null
    }

    return result.value
  }

  async *process(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    const pollingInterval = parse(this.#config.worker?.pollingInterval || '2s')
    this.#pool = new JobPool()

    while (this.#running) {
      try {
        yield* this.#fillPool(queues)

        if (this.#pool.isEmpty()) {
          yield { type: 'idle', suggestedDelay: pollingInterval }
          continue
        }

        const completed = await this.#pool.waitForNextCompletion()
        yield { type: 'completed', queue: completed.queue, job: completed.job }
      } catch (error) {
        yield { type: 'error', error: error as Error, suggestedDelay: parse('5s') }
      }
    }
  }

  async *#fillPool(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    const concurrency = this.#config.worker?.concurrency || 1

    while (this.#pool!.hasCapacity(concurrency)) {
      const result = await this.#acquireNextJob(queues)
      if (!result) break

      const { job, queue } = result
      const promise = this.#execute(job, queue)
      this.#pool!.add(job, queue, promise)

      yield { type: 'started', queue, job }
    }
  }

  async #execute(job: AcquiredJob, queue: string): Promise<void> {
    const startTime = performance.now()

    debug('worker %s: executing job %s (%s)', this.#id, job.id, job.name)

    const { instance, options, timeout } = await this.#initJob(job)

    try {
      await this.#executeWithTimeout(instance, timeout)
      await job._lease.commit()

      const duration = (performance.now() - startTime).toFixed(2)
      debug('worker %s: successfully executed job %s in %dms', this.#id, job.id, duration)
    } catch (e) {
      const isTimeout = e instanceof errors.E_JOB_TIMEOUT

      if (isTimeout && options.failOnTimeout) {
        debug('worker %s: job %s timed out and failOnTimeout is set', this.#id, job.id)
        await job._lease.commit()
        await instance.failed?.(e as Error)
        return
      }

      const mergedConfig = QueueManager.getMergedRetryConfig(queue, options.retry)

      if (typeof mergedConfig.maxRetries === 'undefined' || mergedConfig.maxRetries <= 0) {
        debug('worker %s: job %s has no retries configured, marking as failed', this.#id, job.id)
        await job._lease.commit()
        await instance.failed?.(e as Error)
        return
      }

      if (job.attempts >= mergedConfig.maxRetries!) {
        debug(
          'worker %s: job %s has exceeded max retries (%d), marking as failed',
          this.#id,
          job.id,
          mergedConfig.maxRetries
        )
        await job._lease.commit()
        const exception = new errors.E_JOB_MAX_ATTEMPTS_REACHED([job.name])
        await instance.failed?.(exception)

        return
      }

      if (mergedConfig.backoff) {
        const strategy = mergedConfig.backoff()
        const nextRetryAt = strategy.getNextRetryAt(job.attempts + 1)

        debug('worker %s: job %s will retry at %s', this.#id, job.id, nextRetryAt.toISOString())

        await this.#rollbackJobWithBackoff(job, queue, nextRetryAt)

        return
      }

      await this.#rollbackJob(job, queue)
    }
  }

  async #initJob(
    job: AcquiredJob
  ): Promise<{ instance: Job; options: JobOptions; timeout: number | undefined }> {
    try {
      const JobClass = Locator.getOrThrow(job.name)
      const instance = new JobClass(job.payload)
      const options = JobClass.options || {}
      const timeout = this.#getJobTimeout(options)

      return { instance, options, timeout }
    } catch (error) {
      debug('worker %s: failed to initialize job %s (%s)', this.#id, job.id, job.name)
      await job._lease.commit()
      throw error
    }
  }

  #getJobTimeout(options: JobOptions): number | undefined {
    if (options.timeout !== undefined) {
      return parse(options.timeout)
    }

    if (this.#config.worker?.timeout !== undefined) {
      return parse(this.#config.worker.timeout)
    }

    return undefined
  }

  async #executeWithTimeout(instance: Job, timeout?: number): Promise<void> {
    if (!timeout) {
      return instance.execute()
    }

    const timeoutPromise = setTimeout(timeout).then(() => {
      throw new errors.E_JOB_TIMEOUT([instance.constructor.name, timeout])
    })

    await Promise.race([instance.execute(), timeoutPromise])
  }

  async #acquireNextJob(queues: string[]): Promise<{ job: AcquiredJob; queue: string } | null> {
    for (const queue of queues) {
      const job = await this.#adapter.popFrom(queue)

      if (!job) {
        continue
      }

      debug('worker %s: attempting to acquire lease for job %s', this.#id, job.id)

      try {
        const acquired = await this.#leaseManager.acquire(job.id)

        if (!acquired) {
          debug('worker %s: failed to acquire lease for job %s', this.#id, job.id)
          await this.#adapter.pushOn(queue, job)
          continue
        }

        debug('worker %s: acquired lease for job %s', this.#id, job.id)

        return {
          job: {
            ...job,
            _lease: {
              commit: () => this.#commitJob(job.id),
              rollback: () => this.#rollbackJob(job, queue),
            },
          },
          queue,
        }
      } catch (error) {
        console.log(error)
        throw error
      }
    }

    return null
  }

  #commitJob(jobId: string) {
    debug('worker %s: committing job %s', this.#id, jobId)
    return this.#leaseManager.release(jobId)
  }

  async #rollbackJob(job: JobData, queue: string) {
    debug('worker %s: rolling back job %s', this.#id, job.id)

    const updatedJob = {
      ...job,
      attempts: (job.attempts || 0) + 1,
    }

    await Promise.all([this.#leaseManager.release(job.id), this.#adapter.pushOn(queue, updatedJob)])
  }

  async #rollbackJobWithBackoff(job: JobData, queue: string, nextRetryAt: Date) {
    debug('worker %s: rolling back job %s with backoff', this.#id, job.id)

    const updatedJob = {
      ...job,
      attempts: (job.attempts || 0) + 1,
      nextRetryAt,
    }

    await this.#leaseManager.release(job.id)

    const delay = nextRetryAt.getTime() - Date.now()

    if (delay > 0) {
      await this.#adapter.pushLaterOn(queue, updatedJob, delay)
    } else {
      await this.#adapter.pushOn(queue, updatedJob)
    }
  }

  async #setupGracefulShutdown() {
    const shutdown = async () => {
      debug('received shutdown signal, stopping worker...')
      await this.stop()
      process.exit(0)
    }

    process.on('SIGINT', shutdown)
    process.on('SIGTERM', shutdown)
  }
}
