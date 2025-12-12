import { config } from './config.js'
import { QueueManager } from '#src/queue_manager'
import SendEmailJob from './jobs/send_email_job.js'
import SyncJob from './jobs/sync_job.js'

await QueueManager.init(config)

await SendEmailJob.dispatch({ to: 'julien@ripouteau.com' }).in('1s')

await SyncJob.dispatch({ source: 'remote_api' }).in('2s')

for (let i = 0; i < 10; i++) {
  await SendEmailJob.dispatch({ to: 'romain.lanz@pm.me' + i })
}

await QueueManager.destroy()
