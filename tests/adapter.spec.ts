import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.ts'
import { RedisAdapter } from '#drivers/redis_adapter'
import { registerDriverTestSuite } from './_utils/register_driver_test_suite.ts'

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
