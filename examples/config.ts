import type { QueueManagerConfig } from '#types/main'
import { Redis } from 'ioredis'
import { redis } from '#drivers/redis_adapter'
import { sync } from '#drivers/sync_adapter'

export const redisConnection = new Redis({
  host: 'localhost',
  port: 6379,
  keyPrefix: 'boringnode::queue::',
  db: 0,
})

export const config: QueueManagerConfig = {
  default: 'redis',

  adapters: {
    sync: sync(),
    redis: redis(redisConnection),
  },

  worker: {
    concurrency: 5,
    pollingInterval: '10ms',
  },

  locations: ['./examples/jobs/**/*.ts'],
}
