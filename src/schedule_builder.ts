import type { Duration, ScheduleConfig, ScheduleResult } from './types/main.js'
import { QueueManager } from './queue_manager.js'
import { parse } from './utils.js'
import { CronExpressionParser } from 'cron-parser'
import * as errors from './exceptions.js'

/**
 * Fluent builder for creating job schedules.
 *
 * @example
 * ```typescript
 * // Create with cron
 * const { scheduleId } = await new ScheduleBuilder('CleanupJob', { days: 30 })
 *   .id('cleanup-daily')
 *   .cron('0 0 * * *')
 *   .timezone('Europe/Paris')
 *   .run()
 *
 * // Create with interval
 * const { scheduleId } = await new ScheduleBuilder('SyncJob', {})
 *   .every('5m')
 *   .run()
 * ```
 */
export class ScheduleBuilder implements PromiseLike<ScheduleResult> {
  #jobName: string
  #payload: any
  #id?: string
  #cronExpression?: string
  #everyMs?: number
  #timezone: string = 'UTC'
  #from?: Date
  #to?: Date
  #limit?: number

  constructor(jobName: string, payload: any) {
    this.#jobName = jobName
    this.#payload = payload
  }

  /**
   * Set a custom schedule ID.
   * If not specified, defaults to the job name.
   * If a schedule with this ID exists, it will be updated (upsert).
   */
  id(scheduleId: string): this {
    this.#id = scheduleId
    return this
  }

  /**
   * Set a cron expression for the schedule.
   * Mutually exclusive with `every()`.
   */
  cron(expression: string): this {
    this.#cronExpression = expression
    return this
  }

  /**
   * Set a repeating interval for the schedule.
   * Mutually exclusive with `cron()`.
   */
  every(interval: Duration): this {
    this.#everyMs = parse(interval)
    return this
  }

  /**
   * Set the timezone for cron evaluation.
   * @default 'UTC'
   */
  timezone(tz: string): this {
    this.#timezone = tz
    return this
  }

  /**
   * Set the start boundary for the schedule.
   * No jobs will be dispatched before this date.
   */
  from(date: Date): this {
    this.#from = date
    return this
  }

  /**
   * Set the end boundary for the schedule.
   * No jobs will be dispatched after this date.
   */
  to(date: Date): this {
    this.#to = date
    return this
  }

  /**
   * Set both start and end boundaries for the schedule.
   * Shorthand for `.from(start).to(end)`.
   */
  between(from: Date, to: Date): this {
    return this.from(from).to(to)
  }

  /**
   * Set the maximum number of runs for this schedule.
   */
  limit(maxRuns: number): this {
    this.#limit = maxRuns
    return this
  }

  /**
   * Create the schedule and return the schedule ID.
   */
  async run(): Promise<ScheduleResult> {
    // Validation
    if (!this.#cronExpression && !this.#everyMs) {
      throw new errors.E_INVALID_SCHEDULE_CONFIG([
        'Schedule must have either a cron expression or an interval',
      ])
    }

    if (this.#cronExpression && this.#everyMs) {
      throw new errors.E_INVALID_SCHEDULE_CONFIG([
        'Schedule cannot have both a cron expression and an interval',
      ])
    }

    // Validate cron expression
    if (this.#cronExpression) {
      try {
        CronExpressionParser.parse(this.#cronExpression, { tz: this.#timezone })
      } catch (error) {
        throw new errors.E_INVALID_CRON_EXPRESSION([this.#cronExpression, (error as Error).message])
      }
    }

    const config: ScheduleConfig = {
      id: this.#id ?? this.#jobName,
      jobName: this.#jobName,
      payload: this.#payload,
      cronExpression: this.#cronExpression,
      everyMs: this.#everyMs,
      timezone: this.#timezone,
      from: this.#from,
      to: this.#to,
      limit: this.#limit,
    }

    const adapter = QueueManager.use()
    const scheduleId = await adapter.createSchedule(config)

    // Calculate and set nextRunAt
    const nextRunAt = this.#calculateNextRunAt()
    await adapter.updateSchedule(scheduleId, { nextRunAt })

    return { scheduleId }
  }

  /**
   * Calculate the next run time based on cron or interval.
   */
  #calculateNextRunAt(): Date {
    const now = new Date()
    let nextRun: Date

    if (this.#cronExpression) {
      const cron = CronExpressionParser.parse(this.#cronExpression, {
        currentDate: now,
        tz: this.#timezone,
      })
      nextRun = cron.next().toDate()
    } else {
      // Interval-based: next run is now + interval
      nextRun = new Date(now.getTime() + this.#everyMs!)
    }

    // Respect from boundary
    if (this.#from && nextRun < this.#from) {
      if (this.#cronExpression) {
        // Recalculate from the start boundary
        const cron = CronExpressionParser.parse(this.#cronExpression, {
          currentDate: this.#from,
          tz: this.#timezone,
        })
        nextRun = cron.next().toDate()
      } else {
        nextRun = this.#from
      }
    }

    return nextRun
  }

  /**
   * Implement PromiseLike to allow `await builder.every('5m')` syntax.
   */
  then<TResult1 = ScheduleResult, TResult2 = never>(
    onfulfilled?: ((value: ScheduleResult) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.run().then(onfulfilled, onrejected)
  }
}
