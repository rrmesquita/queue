import { Job } from '../../src/job.js'
import type { JobOptions } from '../../src/types/index.js'

interface MetricsJobPayload {
  endpoint: string
}

/**
 * Example job that collects metrics.
 * For scheduled/repeating execution, use the Schedule API:
 *
 * ```typescript
 * await MetricsJob.schedule({ endpoint: '/api/health' })
 *   .id('health-check')
 *   .every('10s')
 *   .run()
 * ```
 */
export default class MetricsJob extends Job<MetricsJobPayload> {
  static options: JobOptions = {
    queue: 'metrics',
    removeOnComplete: true,
    removeOnFail: { count: 100 },
  }

  async execute(): Promise<void> {
    console.log(`[Job ${this.context.jobId}] Collecting metrics from ${this.payload.endpoint}`)

    // Simulate metrics collection
    const metrics = {
      timestamp: new Date().toISOString(),
      cpu: Math.random() * 100,
      memory: Math.random() * 100,
    }

    console.log(`[Job ${this.context.jobId}] Metrics:`, metrics)
  }
}
