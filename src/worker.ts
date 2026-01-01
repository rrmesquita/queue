import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import debug from './debug.js'
import { parse } from './utils.js'
import * as errors from './exceptions.js'
import { QueueManager } from './queue_manager.js'
import { JobPool } from './job_pool.js'
import type { Adapter, AcquiredJob } from './contracts/adapter.js'
import type { JobContext, JobOptions, QueueManagerConfig, WorkerCycle } from './types/main.js'
import { Locator } from './locator.js'
import { DEFAULT_PRIORITY } from './constants.js'
import type { Job } from './job.js'
import {
  DEFAULT_IDLE_DELAY,
  DEFAULT_STALLED_INTERVAL,
  DEFAULT_STALLED_THRESHOLD,
  DEFAULT_ERROR_RETRY_DELAY,
} from './constants.js'

/**
 * Job processing worker.
 *
 * The Worker continuously polls queues for jobs and executes them
 * with configurable concurrency. It handles:
 * - Concurrent job execution via JobPool
 * - Automatic retries with backoff strategies
 * - Stalled job detection and recovery
 * - Graceful shutdown on SIGINT/SIGTERM
 *
 * @example
 * ```typescript
 * import { Worker, redis } from '@boringnode/queue'
 *
 * const worker = new Worker({
 *   default: 'redis',
 *   adapters: { redis: redis() },
 *   locations: ['./jobs/**\/*.js'],
 *   worker: {
 *     concurrency: 5,
 *     idleDelay: '1s',
 *   },
 * })
 *
 * // Start processing jobs
 * await worker.start(['default', 'emails'])
 *
 * // Or for testing, process one cycle at a time
 * const cycle = await worker.processCycle(['default'])
 * ```
 */
export class Worker {
  readonly #id: string
  readonly #config: QueueManagerConfig
  readonly #idleDelay: number
  readonly #stalledInterval: number
  readonly #stalledThreshold: number
  readonly #maxStalledCount: number
  readonly #concurrency: number
  readonly #gracefulShutdown: boolean
  readonly #onShutdownSignal?: () => void | Promise<void>

  #adapter!: Adapter
  #running = false
  #initialized = false
  #generator?: AsyncGenerator<WorkerCycle, void, unknown>
  #pool?: JobPool
  #lastStalledCheck = 0
  #shutdownHandler?: () => Promise<void>

  /** Unique identifier for this worker instance */
  get id() {
    return this.#id
  }

  /**
   * Create a new worker instance.
   *
   * @param config - Queue configuration including adapter and worker settings
   */
  constructor(config: QueueManagerConfig) {
    this.#config = config
    this.#id = randomUUID()

    // Parse worker config once at construction
    this.#idleDelay = parse(config.worker?.idleDelay ?? DEFAULT_IDLE_DELAY)
    this.#stalledInterval = parse(config.worker?.stalledInterval ?? DEFAULT_STALLED_INTERVAL)
    this.#stalledThreshold = parse(config.worker?.stalledThreshold ?? DEFAULT_STALLED_THRESHOLD)
    this.#maxStalledCount = config.worker?.maxStalledCount ?? 1
    this.#concurrency = config.worker?.concurrency ?? 1
    this.#gracefulShutdown = config.worker?.gracefulShutdown ?? true
    this.#onShutdownSignal = config.worker?.onShutdownSignal

    debug('created worker with id %s and config %O', this.#id, config)
  }

  /**
   * Initialize the worker (called automatically by `start()`).
   *
   * Sets up the QueueManager and adapter connection.
   */
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

  /**
   * Start processing jobs from the specified queues.
   *
   * This method blocks until the worker is stopped (via `stop()` or signal).
   * Jobs are processed concurrently up to the configured concurrency limit.
   *
   * @param queues - Queue names to process (default: ['default'])
   *
   * @example
   * ```typescript
   * // Process single queue
   * await worker.start()
   *
   * // Process multiple queues (priority order)
   * await worker.start(['high-priority', 'default', 'low-priority'])
   * ```
   */
  async start(queues: string[] = ['default']): Promise<void> {
    await this.init()

    if (this.#running) {
      debug('worker %s is already running', this.#id)
      return
    }

    this.#running = true

    debug('starting worker %s on queues: %O', this.#id, queues)

    this.#setupGracefulShutdown()

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

  /**
   * Stop the worker gracefully.
   *
   * Waits for all running jobs to complete before shutting down.
   * Called automatically on SIGINT/SIGTERM if gracefulShutdown is enabled.
   */
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

    this.#removeShutdownHandlers()
  }

  /**
   * Process a single cycle and return the result.
   *
   * Useful for testing or when you need fine-grained control.
   * Each cycle may start new jobs, complete a job, or return idle.
   *
   * @param queues - Queue names to process
   * @returns The cycle result, or null if the worker was stopped
   *
   * @example
   * ```typescript
   * const worker = new Worker(config)
   *
   * // Process cycles manually
   * let cycle = await worker.processCycle(['default'])
   * while (cycle) {
   *   console.log('Cycle:', cycle.type)
   *   cycle = await worker.processCycle(['default'])
   * }
   * ```
   */
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

  /**
   * Generator that yields worker cycle events.
   *
   * Low-level API for processing jobs. Yields events for:
   * - `started`: A new job began execution
   * - `completed`: A job finished (success or failure)
   * - `idle`: No jobs available, suggest waiting
   * - `error`: An error occurred during processing
   *
   * @param queues - Queue names to process
   * @yields WorkerCycle events
   *
   * @example
   * ```typescript
   * for await (const cycle of worker.process(['default'])) {
   *   switch (cycle.type) {
   *     case 'started':
   *       console.log(`Started job ${cycle.job.id}`)
   *       break
   *     case 'completed':
   *       console.log(`Completed job ${cycle.job.id}`)
   *       break
   *     case 'idle':
   *       await sleep(cycle.suggestedDelay)
   *       break
   *   }
   * }
   * ```
   */
  async *process(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    this.#pool = new JobPool()

    while (this.#running) {
      try {
        // Check for stalled jobs periodically
        await this.#checkStalledJobs(queues)

        yield* this.#fillPool(queues)

        if (this.#pool.isEmpty()) {
          yield { type: 'idle', suggestedDelay: this.#idleDelay }
          continue
        }

        const completed = await this.#pool.waitForNextCompletion()
        yield { type: 'completed', queue: completed.queue, job: completed.job }
      } catch (error) {
        yield {
          type: 'error',
          error: error as Error,
          suggestedDelay: parse(DEFAULT_ERROR_RETRY_DELAY),
        }
      }
    }
  }

  async *#fillPool(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    const slotsAvailable = this.#concurrency - this.#pool!.size

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

      const context: JobContext = Object.freeze({
        jobId: job.id,
        name: job.name,
        attempt: job.attempts + 1,
        queue,
        priority: job.priority ?? DEFAULT_PRIORITY,
        acquiredAt: new Date(job.acquiredAt),
        stalledCount: job.stalledCount ?? 0,
      })

      const jobFactory = QueueManager.getJobFactory()
      const instance = jobFactory
        ? await jobFactory(JobClass, job.payload, context)
        : new JobClass(job.payload, context)
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

  async #checkStalledJobs(queues: string[]): Promise<void> {
    const now = Date.now()

    // Only check if enough time has passed since last check
    if (now - this.#lastStalledCheck < this.#stalledInterval) {
      return
    }

    this.#lastStalledCheck = now

    for (const queue of queues) {
      const recovered = await this.#adapter.recoverStalledJobs(
        queue,
        this.#stalledThreshold,
        this.#maxStalledCount
      )

      if (recovered > 0) {
        debug('worker %s: recovered %d stalled jobs from queue %s', this.#id, recovered, queue)
      }
    }
  }

  #setupGracefulShutdown() {
    if (!this.#gracefulShutdown) {
      return
    }

    this.#shutdownHandler = async () => {
      debug('received shutdown signal, stopping worker...')

      if (this.#onShutdownSignal) {
        await this.#onShutdownSignal()
      }

      await this.stop()
    }

    process.on('SIGINT', this.#shutdownHandler)
    process.on('SIGTERM', this.#shutdownHandler)
  }

  #removeShutdownHandlers() {
    if (this.#shutdownHandler) {
      process.off('SIGINT', this.#shutdownHandler)
      process.off('SIGTERM', this.#shutdownHandler)
      this.#shutdownHandler = undefined
    }
  }
}
