import { setTimeout } from 'node:timers/promises'
import { test as JapaTest } from '@japa/runner'
import type { LeaseConfig } from '#types/main'
import type { LeaseManager } from '#contracts/lease_manager'

interface LeaseTestSuiteOptions {
  test: typeof JapaTest
  createManager: (options?: Partial<LeaseConfig>) => LeaseManager
}

export function registerLeaseTestSuite(options: LeaseTestSuiteOptions) {
  const { test } = options

  test('should acquire a lease for a job', async ({ assert, cleanup }) => {
    const leaseManager = options.createManager()

    cleanup(async () => {
      await leaseManager.destroy()
    })

    const acquired = await leaseManager.acquire('job-1')

    assert.isTrue(acquired)
  })

  test('should not acquire a lease if already held', async ({ assert, cleanup }) => {
    const leaseManager1 = options.createManager()
    const leaseManager2 = options.createManager({ workerId: 'worker-2' })

    cleanup(async () => {
      await Promise.all([leaseManager1.destroy(), leaseManager2.destroy()])
    })

    const acquired1 = await leaseManager1.acquire('job-1')
    const acquired2 = await leaseManager2.acquire('job-1')

    assert.isTrue(acquired1)
    assert.isFalse(acquired2)
  })

  test('should automatically renew lease', async ({ assert, cleanup }) => {
    const leaseManager1 = options.createManager({ leaseTimeout: '200ms', renewalInterval: '100ms' })
    const leaseManager2 = options.createManager({ workerId: 'worker-2' })

    cleanup(async () => {
      await Promise.all([leaseManager1.destroy(), leaseManager2.destroy()])
    })

    const acquired1 = await leaseManager1.acquire('job-1')
    assert.isTrue(acquired1)

    await setTimeout(300)

    const acquired2 = await leaseManager2.acquire('job-1')
    assert.isFalse(acquired2)
  })

  test('should release lease when asked', async ({ assert, cleanup }) => {
    const leaseManager1 = options.createManager()
    const leaseManager2 = options.createManager({ workerId: 'worker-2' })

    cleanup(async () => {
      await Promise.all([leaseManager1.destroy(), leaseManager2.destroy()])
    })

    const acquired1 = await leaseManager1.acquire('job-1')
    assert.isTrue(acquired1)

    await leaseManager1.release('job-1')

    const acquired2 = await leaseManager2.acquire('job-1')
    assert.isTrue(acquired2)
  })

  test('should do nothing when renewing a non-existing lease', async ({ assert, cleanup }) => {
    const leaseManager = options.createManager()

    cleanup(async () => {
      await leaseManager.destroy()
    })

    const renewed = await leaseManager.renew('non-existing-job')

    assert.isFalse(renewed)
  })

  test('should do nothing when releasing a non-existing lease', async ({ cleanup }) => {
    const leaseManager = options.createManager()

    cleanup(async () => {
      await leaseManager.destroy()
    })

    await leaseManager.release('non-existing-job')
  })
}
