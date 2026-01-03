import { Job } from '../../src/job.js'
import type { JobOptions } from '../../src/types/index.js'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    console.log(`[Attempt ${this.context.attempt}] Sending email to: ${this.payload.to}`)
  }
}
