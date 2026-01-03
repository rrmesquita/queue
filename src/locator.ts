import { Job } from './job.js'
import * as errors from './exceptions.js'
import type { JobClass } from './types/main.js'
import debug from './debug.js'
import { glob } from 'node:fs/promises'
import { resolve } from 'node:path'

/**
 * Job class registry.
 *
 * The Locator maintains a mapping of job names to their classes,
 * allowing the Worker to instantiate jobs by name when processing.
 *
 * Jobs are typically registered automatically via `QueueManager.init()`
 * using the `locations` config option, but can also be registered manually.
 *
 * @example
 * ```typescript
 * import { Locator } from '@boringnode/queue'
 * import SendEmailJob from './jobs/send_email_job.js'
 *
 * // Manual registration
 * Locator.register('SendEmailJob', SendEmailJob)
 *
 * // Auto-registration via glob (used by QueueManager.init)
 * await Locator.registerFromGlob(['./jobs/**\/*.js'])
 *
 * // Retrieve a job class
 * const JobClass = Locator.getOrThrow('SendEmailJob')
 * ```
 */
class LocatorSingleton {
  #registry = new Map<string, JobClass>()

  /**
   * Register a job class with a given name.
   *
   * @param name - The job name (usually the class name)
   * @param JobClass - The job class constructor
   *
   * @example
   * ```typescript
   * Locator.register('SendEmailJob', SendEmailJob)
   * ```
   */
  register<T extends Job>(name: string, JobClass: JobClass<T>) {
    debug('registering job: %s', name)

    this.#registry.set(name, JobClass)
  }

  /**
   * Auto-register job classes from files matching glob patterns.
   *
   * Each file should have a default export that is a Job class.
   * The class name is used as the registration name.
   *
   * @param patterns - Glob patterns to match job files
   * @returns Number of jobs successfully registered
   *
   * @example
   * ```typescript
   * const count = await Locator.registerFromGlob([
   *   './jobs/**\/*.js',
   *   './app/jobs/**\/*.ts'
   * ])
   * console.log(`Registered ${count} jobs`)
   * ```
   */
  async registerFromGlob(patterns: string[]): Promise<number> {
    let registered = 0

    for (const pattern of patterns) {
      debug('registering jobs from glob pattern: %s', pattern)
      for await (const file of glob(pattern)) {
        debug('found job file: %s', file)

        try {
          const absolutePath = resolve(file)
          const module = await import(`file://${absolutePath}`)
          const JobClass = module.default as JobClass

          if (JobClass && typeof JobClass === 'function') {
            const jobName = JobClass.options?.name || JobClass.name
            this.register(jobName, JobClass)
            registered++
          }
        } catch (error) {
          console.warn(`Failed to load job from ${file}:`, error)
        }
      }
    }

    return registered
  }

  /**
   * Get a job class by name.
   *
   * @param name - The job name to look up
   * @returns The job class, or undefined if not found
   *
   * @example
   * ```typescript
   * const JobClass = Locator.get('SendEmailJob')
   * if (JobClass) {
   *   const instance = new JobClass(payload)
   * }
   * ```
   */
  get<T extends Job = Job>(name: string): JobClass<T> | undefined {
    return this.#registry.get(name) as JobClass<T> | undefined
  }

  /**
   * Get a job class by name, throwing if not found.
   *
   * @param name - The job name to look up
   * @returns The job class
   * @throws {E_JOB_NOT_FOUND} If the job is not registered
   *
   * @example
   * ```typescript
   * const JobClass = Locator.getOrThrow('SendEmailJob')
   * const instance = new JobClass(payload)
   * ```
   */
  getOrThrow<T extends Job = Job>(name: string): JobClass<T> {
    const JobClass = this.get<T>(name)

    if (!JobClass) {
      throw new errors.E_JOB_NOT_FOUND([name])
    }

    return JobClass
  }

  /**
   * Remove all registered jobs.
   *
   * Primarily useful for testing.
   */
  clear(): void {
    this.#registry.clear()
  }
}

/** Global job class registry singleton */
export const Locator = new LocatorSingleton()
