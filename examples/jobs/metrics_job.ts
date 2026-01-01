import { Job } from '../../src/job.js'
import type { JobOptions } from '../../src/types/index.js'

interface MetricsJobPayload {
  endpoint: string
}

/**
 * Example job that collects metrics at regular intervals.
 * Demonstrates repeating jobs with external cancellation.
 */
export default class MetricsJob extends Job<MetricsJobPayload> {
  static readonly jobName = 'MetricsJob'

  static options: JobOptions = {
    queue: 'metrics',
  }

  async execute(): Promise<void> {
    const repeatInfo = this.context.isRepeating
      ? ` (repeat ${this.context.repeatRemaining ?? '∞'} remaining, repeatId: ${this.context.repeatId})`
      : ''

    console.log(
      `[Job ${this.context.jobId}] Collecting metrics from ${this.payload.endpoint}${repeatInfo}`
    )

    // Simulate metrics collection
    const metrics = {
      timestamp: new Date().toISOString(),
      cpu: Math.random() * 100,
      memory: Math.random() * 100,
    }

    console.log(`[Job ${this.context.jobId}] Metrics:`, metrics)
  }
}
