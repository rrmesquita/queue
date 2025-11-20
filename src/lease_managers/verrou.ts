import type { LeaseManager } from '#contracts/lease_manager'
import type { LeaseConfig } from '#types/main'
import { type Lock, Verrou } from '@verrou/core'
import { parse } from '#src/utils'
import { redisStore } from '@verrou/core/drivers/redis'
import type { Redis } from 'ioredis'
import debug from '#src/debug'

type SupportedConnection = Redis

function createVerrouStore(connection: SupportedConnection) {
  if ('rpush' in connection && 'zadd' in connection) {
    return redisStore({ connection: connection as Redis })
  }

  throw new Error('Unsupported connection type for VerrouLeaseManager')
}

export class VerrouLeaseManager implements LeaseManager {
  readonly #leaseTimeout: number
  readonly #renewalInterval: number
  readonly #verrou: Verrou<any>
  #activeLocks = new Map<string, Lock>()
  #activeRenewals = new Map<string, NodeJS.Timeout>()

  constructor(config: LeaseConfig, connection: SupportedConnection) {
    this.#leaseTimeout = parse(config.leaseTimeout)
    this.#renewalInterval = parse(config.renewalInterval)

    this.#verrou = new Verrou({
      default: 'main',
      stores: {
        main: {
          driver: createVerrouStore(connection),
        },
      },
    })
  }

  async acquire(jobId: string): Promise<boolean> {
    const lock = this.#verrou.createLock(jobId, this.#leaseTimeout)

    const acquired = await lock.acquire({
      retry: {
        attempts: 1,
      },
    })

    if (acquired) {
      this.#activeLocks.set(jobId, lock)
      this.#startRenewal(jobId)
    }

    return acquired
  }

  async renew(jobId: string): Promise<boolean> {
    const lock = this.#activeLocks.get(jobId)

    if (!lock) {
      return false
    }

    debug('renewing lease for job %s', jobId)

    await lock.extend(this.#leaseTimeout)

    return true
  }

  release(jobId: string): Promise<void> {
    const lock = this.#activeLocks.get(jobId)

    if (!lock) {
      return Promise.resolve()
    }

    this.#stopRenewal(jobId)
    this.#activeLocks.delete(jobId)

    return lock.release()
  }

  async destroy(): Promise<void> {
    debug('destroying lease manager, releasing all active locks')

    for (const [jobId, interval] of this.#activeRenewals.entries()) {
      if (interval) {
        clearInterval(interval)
      }

      const lock = this.#activeLocks.get(jobId)

      if (lock) {
        lock.release().catch(() => {})
      }
    }

    this.#activeRenewals.clear()
    this.#activeLocks.clear()
  }

  #startRenewal(jobId: string) {
    debug(
      'starting renewal for job %s with interval %d seconds',
      jobId,
      this.#renewalInterval / 1000
    )

    const interval = setInterval(async () => {
      const renewed = await this.renew(jobId)

      if (!renewed) {
        this.#stopRenewal(jobId)
      }
    }, this.#renewalInterval)

    this.#activeRenewals.set(jobId, interval)
  }

  #stopRenewal(jobId: string) {
    debug('stopping renewal for job %s', jobId)

    const interval = this.#activeRenewals.get(jobId)

    if (interval) {
      clearInterval(interval)
    }

    this.#activeRenewals.delete(jobId)
  }
}
