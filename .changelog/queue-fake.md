# Queue testing improvements

## New feature

### QueueManager.fake / QueueManager.restore

You can replace all adapters with the fake adapter for test assertions, then restore the original configuration.

```typescript
import { QueueManager } from '@boringnode/queue'
import { redis } from '@boringnode/queue/drivers/redis_adapter'

await QueueManager.init({
  default: 'redis',
  adapters: {
    redis: redis({ host: 'localhost' }),
  },
})

const fake = QueueManager.fake()

await SendEmailJob.dispatch({ to: 'user@example.com' })

fake.assertPushed(SendEmailJob, {
  queue: 'default',
  payload: { to: 'user@example.com' },
})

QueueManager.restore()
```
