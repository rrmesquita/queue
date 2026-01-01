import KnexPkg from 'knex'
import type { Knex } from 'knex'
import type { Adapter, AcquiredJob } from '../contracts/adapter.js'
import type { JobData } from '../types/main.js'
import { DEFAULT_PRIORITY } from '../constants.js'
import { calculateScore } from '../utils.js'

export interface KnexAdapterOptions {
  connection: Knex
  tableName?: string
  schedulesTableName?: string
  ownsConnection?: boolean
}

type KnexConfig = Knex | Knex.Config

/**
 * Create a new Knex adapter factory.
 * Accepts either a Knex instance or a Knex configuration object.
 *
 * When passing a config object, the adapter will create and manage
 * the connection lifecycle (closing it on destroy).
 *
 * When passing a Knex instance, the caller is responsible for
 * managing the connection lifecycle.
 */
export function knex(config: KnexConfig, tableName?: string) {
  return () => {
    const isKnexInstance = typeof config === 'function'
    const connection = isKnexInstance ? config : KnexPkg(config)
    return new KnexAdapter({ connection, tableName, ownsConnection: !isKnexInstance })
  }
}

/**
 * Knex adapter for the queue system.
 * Stores jobs in a SQL database using Knex.
 */
export class KnexAdapter implements Adapter {
  readonly #connection: Knex
  readonly #jobsTable: string
  readonly #schedulesTable: string
  readonly #ownsConnection: boolean
  #workerId: string = ''
  #initialized: boolean = false

  constructor(config: KnexAdapterOptions) {
    this.#connection = config.connection
    this.#jobsTable = config.tableName ?? 'queue_jobs'
    this.#schedulesTable = config.schedulesTableName ?? 'queue_schedules'
    this.#ownsConnection = config.ownsConnection ?? false
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  /**
   * Ensure all required tables exist.
   * Creates them if not exists, handles race conditions.
   */
  async #ensureTables(): Promise<void> {
    if (this.#initialized) return

    await Promise.all([this.#createJobsTable(), this.#createSchedulesTable()])

    this.#initialized = true
  }

  async #createJobsTable(): Promise<void> {
    try {
      await this.#connection.schema.createTable(this.#jobsTable, (table) => {
        table.string('id', 255).notNullable()
        table.string('queue', 255).notNullable()
        table.enu('status', ['pending', 'active', 'delayed']).notNullable()
        table.text('data').notNullable()
        table.bigint('score').unsigned().nullable()
        table.string('worker_id', 255).nullable()
        table.bigint('acquired_at').unsigned().nullable()
        table.bigint('execute_at').unsigned().nullable()
        table.primary(['id', 'queue'])
        table.index(['queue', 'status', 'score'])
        table.index(['queue', 'status', 'execute_at'])
      })
    } catch {
      /**
       * If table creation fails, verify the table actually exists.
       * This handles race conditions where multiple instances try to create
       * the table simultaneously.
       */
      const hasTable = await this.#connection.schema.hasTable(this.#jobsTable)
      if (!hasTable) {
        throw new Error(`Failed to create table "${this.#jobsTable}"`)
      }
    }
  }

  async #createSchedulesTable(): Promise<void> {
    try {
      await this.#connection.schema.createTable(this.#schedulesTable, (table) => {
        table.string('id', 255).primary()
        table.string('status', 50).notNullable().defaultTo('cancelled')
        table.timestamp('cancelled_at').nullable()
      })
    } catch {
      /**
       * If table creation fails, verify the table actually exists.
       * This handles race conditions where multiple instances try to create
       * the table simultaneously.
       */
      const hasTable = await this.#connection.schema.hasTable(this.#schedulesTable)
      if (!hasTable) {
        throw new Error(`Failed to create table "${this.#schedulesTable}"`)
      }
    }
  }

  async destroy(): Promise<void> {
    if (this.#ownsConnection) {
      await this.#connection.destroy()
    }
  }

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    await this.#ensureTables()

    const now = Date.now()

    // First, move ready delayed jobs to pending
    await this.#processDelayedJobs(queue, now)

    // Use a transaction to atomically pop a job
    return this.#connection.transaction(async (trx) => {
      // Build the query for highest priority job (lowest score)
      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'pending')
        .orderBy('score', 'asc')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const job = await query.first()

      if (!job) {
        return null
      }

      // Update job to active status
      await trx(this.#jobsTable).where('id', job.id).where('queue', queue).update({
        status: 'active',
        worker_id: this.#workerId,
        acquired_at: now,
      })

      const jobData: JobData = JSON.parse(job.data)

      return {
        ...jobData,
        acquiredAt: now,
      }
    })
  }

  /**
   * Check if the database supports FOR UPDATE SKIP LOCKED.
   * PostgreSQL 9.5+, MySQL 8.0+, and MariaDB 10.6+ support it.
   * SQLite does not, but it's single-writer so it doesn't need it.
   */
  #supportsSkipLocked(): boolean {
    const client = this.#connection.client.config.client
    return client === 'pg' || client === 'mysql' || client === 'mysql2' || client === 'mariadb'
  }

  async #processDelayedJobs(queue: string, now: number): Promise<void> {
    // Use a transaction with row locking to prevent race conditions
    await this.#connection.transaction(async (trx) => {
      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'delayed')
        .where('execute_at', '<=', now)
        .select('id', 'data')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const delayedJobs = await query

      if (delayedJobs.length === 0) return

      // Move them to pending
      for (const job of delayedJobs) {
        const jobData: JobData = JSON.parse(job.data)
        const priority = jobData.priority ?? DEFAULT_PRIORITY
        const score = calculateScore(priority, now)

        await trx(this.#jobsTable).where('id', job.id).where('queue', queue).update({
          status: 'pending',
          score,
          execute_at: null,
        })
      }
    })
  }

  async completeJob(jobId: string, queue: string): Promise<void> {
    await this.#ensureTables()

    await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).delete()
  }

  async failJob(jobId: string, queue: string, _error?: Error): Promise<void> {
    await this.#ensureTables()

    await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).delete()
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    await this.#ensureTables()

    const now = Date.now()

    // Get the active job
    const activeJob = await this.#connection(this.#jobsTable)
      .where('id', jobId)
      .where('queue', queue)
      .where('status', 'active')
      .first()

    if (!activeJob) return

    const jobData: JobData = JSON.parse(activeJob.data)
    jobData.attempts = (jobData.attempts || 0) + 1

    const updatedData = JSON.stringify(jobData)

    if (retryAt && retryAt.getTime() > now) {
      // Move to delayed
      await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).update({
        status: 'delayed',
        data: updatedData,
        worker_id: null,
        acquired_at: null,
        score: null,
        execute_at: retryAt.getTime(),
      })
    } else {
      // Move back to pending
      const priority = jobData.priority ?? DEFAULT_PRIORITY
      const score = calculateScore(priority, now)

      await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).update({
        status: 'pending',
        data: updatedData,
        worker_id: null,
        acquired_at: null,
        score,
        execute_at: null,
      })
    }
  }

  async push(jobData: JobData): Promise<void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<void> {
    await this.#ensureTables()

    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const timestamp = Date.now()
    const score = calculateScore(priority, timestamp)

    await this.#connection(this.#jobsTable).insert({
      id: jobData.id,
      queue,
      status: 'pending',
      data: JSON.stringify(jobData),
      score,
    })
  }

  async pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void> {
    await this.#ensureTables()

    const executeAt = Date.now() + delay

    await this.#connection(this.#jobsTable).insert({
      id: jobData.id,
      queue,
      status: 'delayed',
      data: JSON.stringify(jobData),
      execute_at: executeAt,
    })
  }

  async size(): Promise<number> {
    return this.sizeOf('default')
  }

  async sizeOf(queue: string): Promise<number> {
    await this.#ensureTables()

    const result = await this.#connection(this.#jobsTable)
      .where('queue', queue)
      .where('status', 'pending')
      .count('* as count')
      .first()

    return Number(result?.count ?? 0)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number> {
    await this.#ensureTables()

    const now = Date.now()
    const stalledCutoff = now - stalledThreshold

    // Use a transaction with row locking to prevent race conditions
    return this.#connection.transaction(async (trx) => {
      let recovered = 0

      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'active')
        .where('acquired_at', '<', stalledCutoff)
        .select('id', 'data')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const stalledJobs = await query

      for (const row of stalledJobs) {
        const jobData: JobData = JSON.parse(row.data)
        const currentStalledCount = jobData.stalledCount ?? 0

        if (currentStalledCount >= maxStalledCount) {
          // Fail permanently - remove the job
          await trx(this.#jobsTable).where('id', row.id).where('queue', queue).delete()
        } else {
          // Recover: increment stalledCount and put back in pending
          jobData.stalledCount = currentStalledCount + 1
          const priority = jobData.priority ?? DEFAULT_PRIORITY
          const score = calculateScore(priority, now)

          await trx(this.#jobsTable)
            .where('id', row.id)
            .where('queue', queue)
            .update({
              status: 'pending',
              data: JSON.stringify(jobData),
              worker_id: null,
              acquired_at: null,
              score,
            })

          recovered++
        }
      }

      return recovered
    })
  }

  async cancelRepeat(groupId: string): Promise<void> {
    await this.#ensureTables()

    // Use upsert-like behavior: insert or ignore if exists
    try {
      await this.#connection(this.#schedulesTable).insert({
        id: groupId,
        status: 'cancelled',
        cancelled_at: this.#connection.fn.now(),
      })
    } catch {
      // Ignore duplicate key error (already cancelled)
    }
  }

  async isRepeatCancelled(groupId: string): Promise<boolean> {
    await this.#ensureTables()

    const result = await this.#connection(this.#schedulesTable)
      .where('id', groupId)
      .where('status', 'cancelled')
      .first()

    return result !== undefined
  }
}
