import { config } from './config.js'
import { QueueManager } from '../src/queue_manager.js'
import SendEmailJob from './jobs/send_email_job.js'
import SyncJob from './jobs/sync_job.js'
import MetricsJob from './jobs/metrics_job.js'

await QueueManager.init(config)

await SendEmailJob.dispatch({ to: 'julien@ripouteau.com' }).in('1s')

await SyncJob.dispatch({ source: 'remote_api' }).in('2s')

for (let i = 0; i < 10; i++) {
  await SendEmailJob.dispatch({ to: 'romain.lanz@pm.me' + i })
}

// Example: Dispatch a repeating job and get the repeatId for later cancellation
const { jobId, repeatId } = await MetricsJob.dispatch({ endpoint: '/api/health' }).every('10s')

console.log(`Started metrics collection job ${jobId}`)
console.log(`To cancel this repeating job, use: await QueueManager.cancelRepeat('${repeatId}')`)

// Example: Cancel a repeating job after some condition
// await QueueManager.cancelRepeat(repeatId)

await QueueManager.destroy()
