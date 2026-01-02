import { config } from './config.js'
import { QueueManager } from '../src/queue_manager.js'
import { Schedule } from '../src/schedule.js'
import SendEmailJob from './jobs/send_email_job.js'
import SyncJob from './jobs/sync_job.js'
import MetricsJob from './jobs/metrics_job.js'

await QueueManager.init(config)

await SendEmailJob.dispatch({ to: 'julien@ripouteau.com' }).in('1s')

await SyncJob.dispatch({ source: 'remote_api' }).in('2s')

for (let i = 0; i < 10; i++) {
  await SendEmailJob.dispatch({ to: 'romain.lanz@pm.me' + i })
}

// Example: Schedule a recurring metrics job every 10 seconds
// By default, the schedule ID is the job name ('MetricsJob')
const { scheduleId } = await MetricsJob.schedule({ endpoint: '/api/health' }).every('10s').run()

console.log(`Created schedule: ${scheduleId}`) // 'MetricsJob'

// Manage the schedule using its ID
const schedule = await Schedule.find('MetricsJob')
if (schedule) {
  console.log(`Schedule status: ${schedule.status}, run count: ${schedule.runCount}`)

  // Pause, resume, or delete
  // await schedule.pause()
  // await schedule.resume()
  // await schedule.delete()
}

// List all active schedules
const activeSchedules = await Schedule.list({ status: 'active' })
console.log(`Active schedules: ${activeSchedules.length}`)

await QueueManager.destroy()
