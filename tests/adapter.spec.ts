import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { RedisAdapter } from '#drivers/redis_adapter'
import { KnexAdapter } from '#drivers/knex_adapter'
import { registerDriverTestSuite } from './_utils/register_driver_test_suite.js'

const KEY_PREFIX = 'boringnode::queue::test::'

test.group('Adapter | Memory', (group) => {
  let adapter: MemoryAdapter

  group.each.teardown(async () => {
    await adapter?.destroy()
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new MemoryAdapter()
      return adapter
    },
  })
})

test.group('Adapter | Redis', (group) => {
  let connection: Redis

  group.each.setup(() => {
    connection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 0,
    })

    return async () => {
      const keys = await connection.keys(`${KEY_PREFIX}*`)
      if (keys.length > 0) {
        const keysWithoutPrefix = keys.map((k) => k.replace(KEY_PREFIX, ''))
        await connection.del(...keysWithoutPrefix)
      }
      await connection.quit()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => new RedisAdapter(connection),
  })
})

test.group('Adapter | Knex (SQLite)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter

  group.each.setup(async () => {
    connection = Knex({
      client: 'better-sqlite3',
      connection: {
        filename: ':memory:',
      },
      useNullAsDefault: true,
    })

    return async () => {
      await adapter?.destroy()
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection })
      return adapter
    },
  })
})

test.group('Adapter | Knex (PostgreSQL)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter
  const tableName = 'queue_jobs_test'

  group.each.setup(async () => {
    connection = Knex({
      client: 'pg',
      connection: {
        host: process.env.PG_HOST || 'localhost',
        port: Number.parseInt(process.env.PG_PORT || '5432', 10),
        user: process.env.PG_USER || 'postgres',
        password: process.env.PG_PASSWORD || 'postgres',
        database: process.env.PG_DATABASE || 'queue_test',
      },
    })

    // Clean up table before each test
    await connection.schema.dropTableIfExists(tableName)

    return async () => {
      await adapter?.destroy()
      await connection.schema.dropTableIfExists(tableName)
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection, tableName })
      return adapter
    },
  })
})
