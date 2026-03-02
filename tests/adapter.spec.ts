import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { RedisAdapter } from '../src/drivers/redis_adapter.js'
import { KnexAdapter } from '../src/drivers/knex_adapter.js'
import { QueueSchemaService } from '../src/services/queue_schema.js'
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
    supportsConcurrency: false,
  })
})

test.group('Adapter | Redis', (group) => {
  let connection: Redis

  group.each.setup(async () => {
    connection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15, // Use db 15 for tests so we can safely flush it
    })

    // Flush before test
    await connection.flushdb()

    return async () => {
      await connection.quit()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => new RedisAdapter(connection),
  })

  test('listSchedules should use bounded network round-trips as schedule count grows', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)

    for (let i = 0; i < 50; i++) {
      await adapter.upsertSchedule({
        id: `perf-schedule-${i}`,
        name: 'PerfJob',
        payload: { i },
        everyMs: 60_000,
        timezone: 'UTC',
      })
    }

    const stream = (connection as any).stream as { write: (...args: any[]) => any }
    const originalWrite = stream.write.bind(stream)
    let writes = 0

    stream.write = ((...args: any[]) => {
      writes++
      return originalWrite(...args)
    }) as typeof stream.write

    try {
      const schedules = await adapter.listSchedules()
      assert.lengthOf(schedules, 50)
      assert.isAtMost(
        writes,
        4,
        `Expected bounded write count with pipelining, got ${writes} writes for 50 schedules`
      )
    } finally {
      stream.write = originalWrite
    }
  })

  test('deleteSchedule should not leave ghost index under write-failure chaos', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'chaos-delete-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'ChaosJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const stream = (connection as any).stream as { write: (...args: any[]) => any }
    const originalWrite = stream.write.bind(stream)
    let writes = 0

    stream.write = ((...args: any[]) => {
      writes++
      if (writes === 2) {
        throw new Error('chaos: second network write blocked')
      }
      return originalWrite(...args)
    }) as typeof stream.write

    try {
      await adapter.deleteSchedule(id)
    } finally {
      stream.write = originalWrite
    }

    const scheduleExists = await connection.exists(`schedules::${id}`)
    const indexContains = await connection.sismember('schedules::index', id)

    assert.equal(scheduleExists, 0)
    assert.equal(indexContains, 0)
    assert.equal(
      writes,
      1,
      'deleteSchedule should be emitted in a single write window to avoid partial state'
    )
  })
})

test.group('Adapter | Knex (SQLite)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter

  group.each.setup(async () => {
    // Each test gets a fresh in-memory database, so no cleanup needed
    connection = Knex({
      client: 'better-sqlite3',
      connection: {
        filename: ':memory:',
      },
      useNullAsDefault: true,
    })

    // Create tables via QueueSchemaService
    const schemaService = new QueueSchemaService(connection)
    await schemaService.createJobsTable()
    await schemaService.createSchedulesTable()

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
  let schemaService: QueueSchemaService
  const tableName = 'queue_jobs_test'
  const schedulesTableName = 'queue_schedules_test'

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

    schemaService = new QueueSchemaService(connection)

    // Clean up tables before each test
    await schemaService.dropJobsTable(tableName)
    await schemaService.dropSchedulesTable(schedulesTableName)

    // Create tables
    await schemaService.createJobsTable(tableName)
    await schemaService.createSchedulesTable(schedulesTableName)

    return async () => {
      await adapter?.destroy()
      await schemaService.dropJobsTable(tableName)
      await schemaService.dropSchedulesTable(schedulesTableName)
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection, tableName, schedulesTableName })
      return adapter
    },
  })
})
