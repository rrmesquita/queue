import { setTimeout } from 'node:timers/promises'
import { Job } from '../../src/job.js'
import type { JobOptions } from '../../src/types/index.js'

interface SyncJobPayload {
  source: string
}

export default class SyncJob extends Job<SyncJobPayload> {
  static readonly jobName = 'SyncJob'

  static options: JobOptions = {
    adapter: 'sync',
  }

  async execute(): Promise<void> {
    await setTimeout(1000)
    console.log(`[Job ${this.context.jobId}] Syncing data from source: ${this.payload.source}`)
  }
}
