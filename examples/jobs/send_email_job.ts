import { setTimeout } from 'node:timers/promises'
import { Job } from '../../src/job.js'
import type { JobOptions } from '../../src/types/main.js'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static readonly jobName = 'SendEmailJob'

  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    await setTimeout(1000)
    console.log(`Sending email to: ${this.payload.to}`)
  }
}
