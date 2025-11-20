import type { LeaseManager } from '#contracts/lease_manager'
import type { JobData, LeaseConfig } from '#types/main'

export interface Adapter {
  createLeaseManager(config: LeaseConfig): LeaseManager

  size(): Promise<number>
  sizeOf(queue: string): Promise<number>

  push(jobData: JobData): Promise<void>
  pushOn(queue: string, jobData: JobData): Promise<void>

  pushLater(jobData: JobData, delay: number): Promise<void>
  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void>

  pop(): Promise<JobData | null>
  popFrom(queue: string): Promise<JobData | null>

  destroy(): Promise<void>
}
