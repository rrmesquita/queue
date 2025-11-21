import type { LeaseManager } from '#contracts/lease_manager'
import type { LeaseConfig } from '#types/main'
import { parse } from '#src/utils'

export class MemoryLeaseManager implements LeaseManager {
  readonly #workerId: string
  readonly #leaseTimeout: number
  #activeRenewals = new Map<string, NodeJS.Timeout>()

  static leases = new Map<string, { workerId: string; expiresAt: Date }>()

  constructor(config: LeaseConfig) {
    this.#workerId = config.workerId
    this.#leaseTimeout = parse(config.leaseTimeout)
  }

  acquire(jobId: string): Promise<boolean> {
    const existing = MemoryLeaseManager.leases.get(jobId)

    if (existing && existing.expiresAt > new Date()) {
      return Promise.resolve(false)
    }

    MemoryLeaseManager.leases.set(jobId, {
      workerId: this.#workerId,
      expiresAt: new Date(Date.now() + this.#leaseTimeout),
    })

    this.#startRenewal(jobId)

    return Promise.resolve(true)
  }

  renew(jobId: string): Promise<boolean> {
    const lease = MemoryLeaseManager.leases.get(jobId)

    if (!lease || lease.workerId !== this.#workerId) {
      return Promise.resolve(false)
    }

    lease.expiresAt = new Date(Date.now() + this.#leaseTimeout)

    return Promise.resolve(true)
  }

  release(jobId: string): Promise<void> {
    const lease = MemoryLeaseManager.leases.get(jobId)

    if (lease && lease.workerId === this.#workerId) {
      MemoryLeaseManager.leases.delete(jobId)
    }

    this.#stopRenewal(jobId)

    return Promise.resolve()
  }

  destroy(): Promise<void> {
    for (const interval of this.#activeRenewals.values()) {
      clearInterval(interval)
    }

    this.#activeRenewals.clear()
    MemoryLeaseManager.leases.clear()

    return Promise.resolve()
  }

  #startRenewal(jobId: string) {
    const interval = setInterval(async () => {
      const renewed = await this.renew(jobId)

      if (!renewed) {
        this.#stopRenewal(jobId)
      }
    })

    this.#activeRenewals.set(jobId, interval)
  }

  #stopRenewal(jobId: string) {
    const interval = this.#activeRenewals.get(jobId)

    if (interval) {
      clearInterval(interval)
    }

    this.#activeRenewals.delete(jobId)
  }
}
