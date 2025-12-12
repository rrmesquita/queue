import { redis } from '../src/drivers/redis_adapter.js'
import { sync } from '../src/drivers/sync_adapter.js'
import { knex } from '../src/drivers/knex_adapter.js'
import type { QueueManagerConfig } from '../src/types/main.js'

export const config: QueueManagerConfig = {
  default: 'knex',

  adapters: {
    sync: sync(),
    redis: redis({
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
    }),
    knex: knex({
      client: 'pg',
      connection: {
        host: process.env.PG_HOST || 'localhost',
        port: Number.parseInt(process.env.PG_PORT || '5432', 10),
        user: process.env.PG_USER || 'postgres',
        password: process.env.PG_PASSWORD || 'postgres',
        database: process.env.PG_DATABASE || 'queue_test',
      },
    }),
  },

  worker: {
    concurrency: 5,
    pollingInterval: '10ms',
  },

  locations: ['./examples/jobs/**/*.ts'],
}
