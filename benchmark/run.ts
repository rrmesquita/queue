import { formatResult, type BenchmarkOptions, type BenchmarkResult } from './helpers.js'
import { run as runBoringnode } from './boringnode/harness.js'
import { run as runBullMQ } from './bullmq/harness.js'

interface BenchmarkConfig {
  numRuns: number[]
  concurrency: number[]
  jobDuration?: number // Simulated work duration in ms
}

const defaultConfig: BenchmarkConfig = {
  numRuns: [100, 1000, 5000],
  concurrency: [1, 5, 10],
}

async function runBenchmarks(config: BenchmarkConfig = defaultConfig) {
  const results: BenchmarkResult[] = []

  console.log('='.repeat(60))
  console.log('Queue Benchmark Suite')
  if (config.jobDuration) {
    console.log(`Job duration: ${config.jobDuration}ms`)
  }
  console.log('='.repeat(60))
  console.log()

  for (const numRuns of config.numRuns) {
    for (const concurrency of config.concurrency) {
      const options: BenchmarkOptions = { numRuns, concurrency, jobDuration: config.jobDuration }

      console.log(`\nBenchmark: ${numRuns} jobs, concurrency ${concurrency}`)
      console.log('-'.repeat(50))

      // Run @boringnode/queue benchmark
      try {
        console.log('Running @boringnode/queue...')
        const boringnodeResult = await runBoringnode(options)
        results.push(boringnodeResult)
        console.log(`  ${formatResult(boringnodeResult)}`)
      } catch (error) {
        console.error(`  @boringnode/queue failed:`, error)
      }

      // Run BullMQ benchmark
      try {
        console.log('Running BullMQ...')
        const bullmqResult = await runBullMQ(options)
        results.push(bullmqResult)
        console.log(`  ${formatResult(bullmqResult)}`)
      } catch (error) {
        console.error(`  BullMQ failed:`, error)
      }

      // Small delay between runs to let Redis settle
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }

  // Print summary
  console.log()
  console.log('='.repeat(60))
  console.log('Summary')
  console.log('='.repeat(60))
  console.log()

  // Group by numRuns and concurrency
  const grouped = new Map<string, BenchmarkResult[]>()
  for (const result of results) {
    const key = `${result.numRuns}-${result.concurrency}`
    if (!grouped.has(key)) {
      grouped.set(key, [])
    }
    grouped.get(key)!.push(result)
  }

  console.log('Jobs\tConc.\t@boringnode/queue\t\tBullMQ\t\t\tDiff')
  console.log('-'.repeat(90))

  for (const [key, group] of grouped) {
    const [numRuns, concurrency] = key.split('-')
    const boringnode = group.find((r) => r.library === '@boringnode/queue')
    const bullmq = group.find((r) => r.library === 'BullMQ')

    const boringnodeStr = boringnode
      ? `${boringnode.elapsed}ms (${boringnode.jobsPerSecond.toFixed(0)} j/s)`
      : '-'
    const bullmqStr = bullmq ? `${bullmq.elapsed}ms (${bullmq.jobsPerSecond.toFixed(0)} j/s)` : '-'

    let diff = ''
    if (boringnode && bullmq) {
      const percentage = ((bullmq.elapsed - boringnode.elapsed) / bullmq.elapsed) * 100
      if (percentage > 0) {
        diff = `${percentage.toFixed(1)}% faster`
      } else {
        diff = `${Math.abs(percentage).toFixed(1)}% slower`
      }
    }

    console.log(
      `${numRuns}\t${concurrency}\t${boringnodeStr.padEnd(24)}\t${bullmqStr.padEnd(24)}\t${diff}`
    )
  }
}

// Parse CLI arguments
const args = process.argv.slice(2)
let config = defaultConfig

if (args.includes('--quick')) {
  config = {
    numRuns: [100],
    concurrency: [1],
  }
} else if (args.includes('--full')) {
  config = {
    numRuns: [100, 500, 1000, 2500, 5000, 10000],
    concurrency: [1, 5, 10, 25, 50],
  }
} else if (args.includes('--realistic')) {
  // Simulate real jobs with 5ms work duration
  config = {
    numRuns: [100, 500, 1000],
    concurrency: [1, 5, 10],
    jobDuration: 5,
  }
}

// Allow --duration=N to set job duration
const durationArg = args.find((arg) => arg.startsWith('--duration='))
if (durationArg) {
  config.jobDuration = Number.parseInt(durationArg.split('=')[1], 10)
}

runBenchmarks(config)
  .then(() => {
    console.log('\nBenchmark completed!')
    process.exit(0)
  })
  .catch((error) => {
    console.error('Benchmark suite failed:', error)
    process.exit(1)
  })
