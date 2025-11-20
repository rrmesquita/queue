import { test } from '@japa/runner'
import { MemoryLeaseManager } from './_mocks/memory_lease_manager.ts'
import { registerLeaseTestSuite } from './_utils/register_lease_test_suite.ts'
import { VerrouLeaseManager } from '#lease_managers/verrou'
import { Redis } from 'ioredis'

test.group('LeaseManager | Memory', () => {
  registerLeaseTestSuite({
    test,
    createManager: (options) =>
      new MemoryLeaseManager({
        workerId: 'worker-1',
        leaseTimeout: '5s',
        renewalInterval: '2s',
        ...options,
      }),
  })
})

test.group('LeaseManager | Verrou Redis', (group) => {
  let redisConnection: Redis

  group.setup(() => {
    redisConnection = new Redis({
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
    })

    return () => {
      redisConnection.disconnect()
    }
  })

  registerLeaseTestSuite({
    test,
    createManager: (options) =>
      new VerrouLeaseManager(
        {
          workerId: 'worker-1',
          leaseTimeout: '5s',
          renewalInterval: '2s',
          ...options,
        },
        redisConnection
      ),
  })
})
