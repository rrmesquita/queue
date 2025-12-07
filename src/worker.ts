import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import debug from '#src/debug'
import { parse } from '#src/utils'
import * as errors from '#src/exceptions'
import { QueueManager } from '#src/queue_manager'
import { JobPool } from '#src/job_pool'
import type { Adapter, AcquiredJob } from '#contracts/adapter'
import type { QueueManagerConfig, WorkerCycle } from '#types/main'
import { Locator } from '#src/locator'
import type { JobOptions } from '#types/main'
import type { Job } from '#src/job'

export class Worker {
  readonly #id: string
  readonly #config: QueueManagerConfig
  #adapter!: Adapter
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
    this.#adapter.setWorkerId(this.#id)

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
    const slotsAvailable = concurrency - this.#pool!.size

    if (slotsAvailable <= 0) return

    const popPromises = Array.from({ length: slotsAvailable }, () => this.#acquireNextJob(queues))

    const results = await Promise.all(popPromises)

    for (const result of results) {
      if (!result) continue

      const { job, queue } = result
      const promise = this.#execute(job, queue)
      this.#pool!.add(job, queue, promise)

      yield { type: 'started', queue, job }
    }
  }

  async #execute(job: AcquiredJob, queue: string): Promise<void> {
    const startTime = performance.now()

    debug('worker %s: executing job %s (%s)', this.#id, job.id, job.name)

    const { instance, options, timeout } = await this.#initJob(job, queue)

    try {
      await this.#executeWithTimeout(instance, timeout)
      await this.#adapter.completeJob(job.id, queue)

      const duration = (performance.now() - startTime).toFixed(2)
      debug('worker %s: successfully executed job %s in %dms', this.#id, job.id, duration)
    } catch (e) {
      const isTimeout = e instanceof errors.E_JOB_TIMEOUT

      if (isTimeout && options.failOnTimeout) {
        debug('worker %s: job %s timed out and failOnTimeout is set', this.#id, job.id)
        await this.#adapter.failJob(job.id, queue, e as Error)
        await instance.failed?.(e as Error)
        return
      }

      const mergedConfig = QueueManager.getMergedRetryConfig(queue, options.retry)

      if (typeof mergedConfig.maxRetries === 'undefined' || mergedConfig.maxRetries <= 0) {
        debug('worker %s: job %s has no retries configured, marking as failed', this.#id, job.id)
        await this.#adapter.failJob(job.id, queue, e as Error)
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
        await this.#adapter.failJob(job.id, queue, e as Error)
        const exception = new errors.E_JOB_MAX_ATTEMPTS_REACHED([job.name])
        await instance.failed?.(exception)

        return
      }

      if (mergedConfig.backoff) {
        const strategy = mergedConfig.backoff()
        const nextRetryAt = strategy.getNextRetryAt(job.attempts + 1)

        debug('worker %s: job %s will retry at %s', this.#id, job.id, nextRetryAt.toISOString())

        await this.#adapter.retryJob(job.id, queue, nextRetryAt)
        return
      }

      await this.#adapter.retryJob(job.id, queue)
    }
  }

  async #initJob(
    job: AcquiredJob,
    queue: string
  ): Promise<{ instance: Job; options: JobOptions; timeout: number | undefined }> {
    try {
      const JobClass = Locator.getOrThrow(job.name)
      const instance = new JobClass(job.payload)
      const options = JobClass.options || {}
      const timeout = this.#getJobTimeout(options)

      return { instance, options, timeout }
    } catch (error) {
      debug('worker %s: failed to initialize job %s (%s)', this.#id, job.id, job.name)
      await this.#adapter.failJob(job.id, queue, error as Error)
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

    const signal = AbortSignal.timeout(timeout)

    const abortPromise = new Promise<never>((_, reject) => {
      signal.addEventListener('abort', () => {
        reject(new errors.E_JOB_TIMEOUT([instance.constructor.name, timeout]))
      })
    })

    await Promise.race([instance.execute(signal), abortPromise])
  }

  async #acquireNextJob(queues: string[]): Promise<{ job: AcquiredJob; queue: string } | null> {
    for (const queue of queues) {
      const job = await this.#adapter.popFrom(queue)

      if (!job) {
        continue
      }

      debug('worker %s: acquired job %s', this.#id, job.id)
      return { job, queue }
    }

    return null
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
