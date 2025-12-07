import type { AcquiredJob } from '#types/main'

interface PoolEntry {
  promise: Promise<void>
  job: AcquiredJob
  queue: string
}

export class JobPool {
  #activeJobs = new Map<string, PoolEntry>()

  get size() {
    return this.#activeJobs.size
  }

  isEmpty() {
    return this.#activeJobs.size === 0
  }

  hasCapacity(concurrency: number) {
    return this.#activeJobs.size < concurrency
  }

  add(job: AcquiredJob, queue: string, promise: Promise<void>) {
    this.#activeJobs.set(job.id, { promise, job, queue })
  }

  async waitForNextCompletion(): Promise<PoolEntry> {
    const completedJobId = await Promise.race(
      [...this.#activeJobs.entries()].map(async ([id, { promise }]) => {
        try {
          await promise
        } catch {
          // Errors are handled in Worker#execute
        }
        return id
      })
    )

    const completed = this.#activeJobs.get(completedJobId)!
    this.#activeJobs.delete(completedJobId)

    return completed
  }

  async drain(): Promise<void> {
    const promises = [...this.#activeJobs.values()].map(async ({ promise }) => {
      try {
        await promise
      } catch {
        // Errors are handled in Worker#execute
      }
    })

    await Promise.all(promises)
    this.#activeJobs.clear()
  }
}
