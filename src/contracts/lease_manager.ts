export interface LeaseManager {
  acquire(jobId: string): Promise<boolean>
  renew(jobId: string): Promise<boolean>
  release(jobId: string): Promise<void>
  destroy(): Promise<void>
}
