import { Job } from '#src/job'
import * as errors from '#src/exceptions'
import type { JobClass } from '#types/main'
import debug from '#src/debug'
import { glob } from 'node:fs/promises'
import { resolve } from 'node:path'

class LocatorSingleton {
  #registry = new Map<string, JobClass>()

  register<T extends Job>(name: string, JobClass: JobClass<T>) {
    debug('registering job: %s', name)

    this.#registry.set(name, JobClass)
  }

  async registerFromGlob(patterns: string[]) {
    for (const pattern of patterns) {
      debug('registering jobs from glob pattern: %s', pattern)
      for await (const file of glob(pattern)) {
        debug('found job file: %s', file)

        try {
          const absolutePath = resolve(file)
          const module = await import(`file://${absolutePath}`)
          const JobClass = module.default as JobClass

          if (JobClass && typeof JobClass === 'function' && JobClass.name) {
            this.register(JobClass.name, JobClass)
          }
        } catch (error) {
          console.warn(`Failed to load job from ${file}:`, error)
        }
      }
    }
  }

  get<T extends Job = Job>(name: string): JobClass<T> | undefined {
    return this.#registry.get(name) as JobClass<T> | undefined
  }

  getOrThrow<T extends Job = Job>(name: string): JobClass<T> {
    const JobClass = this.get<T>(name)

    if (!JobClass) {
      throw new errors.E_JOB_NOT_FOUND([name])
    }

    return JobClass
  }

  clear(): void {
    this.#registry.clear()
  }
}

export const Locator = new LocatorSingleton()
