/**
 * Creates a deferred promise that can be resolved externally
 */
export function deferred<T = void>(): {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (error: Error) => void
} {
  let resolve!: (value: T) => void
  let reject!: (error: Error) => void

  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

/**
 * A promise-based barrier that resolves when `n` calls to `next()` are made
 */
export function barrier(n: number = 1) {
  const { promise, resolve } = deferred<void>()

  return {
    done: promise,
    next() {
      --n
      if (n < 0) return false
      if (n === 0) resolve()
      return true
    },
  }
}

export interface BenchmarkOptions {
  numRuns: number
  concurrency: number
  jobDuration?: number // Simulated work duration in ms
}

export interface BenchmarkResult {
  library: string
  numRuns: number
  concurrency: number
  elapsed: number
  jobsPerSecond: number
}

export function formatResult(result: BenchmarkResult): string {
  return `${result.library}: ${result.numRuns} jobs with concurrency ${result.concurrency} in ${result.elapsed}ms (${result.jobsPerSecond.toFixed(2)} jobs/sec)`
}
