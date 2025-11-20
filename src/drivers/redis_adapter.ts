import { VerrouLeaseManager } from '#lease_managers/verrou'
import { Redis, type RedisOptions } from 'ioredis'
import type { Adapter } from '#contracts/adapter'
import type { JobData, LeaseConfig } from '#types/main'
import type { LeaseManager } from '#contracts/lease_manager'

const redisKey = 'jobs'
type RedisConfig = Redis | RedisOptions

// Lua script for atomic delayed job processing
const PROCESS_DELAYED_JOBS_SCRIPT = `
  local delayed_key = KEYS[1]
  local queue_key = KEYS[2]
  local now = ARGV[1]

  -- Get ready jobs (score <= now)
  local ready_jobs = redis.call('ZRANGEBYSCORE', delayed_key, 0, now)

  if #ready_jobs > 0 then
    -- Move jobs to priority queue and remove from delayed queue atomically
    for i = 1, #ready_jobs do
      local job_data = ready_jobs[i]
      local job = cjson.decode(job_data)
      local priority = job.priority or 5
      redis.call('ZADD', queue_key, priority, job_data)
      redis.call('ZREM', delayed_key, job_data)
    end

    return #ready_jobs
  end

  return 0
`

export function redis(config?: RedisConfig) {
  return () => {
    if (config instanceof Redis) {
      return new RedisAdapter(config)
    }

    // Create new Redis instance from options
    const options: RedisOptions = {
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
      ...config,
    }

    const connection = new Redis(options)
    return new RedisAdapter(connection)
  }
}

export class RedisAdapter implements Adapter {
  readonly #connection: Redis

  constructor(connection: Redis) {
    this.#connection = connection
  }

  createLeaseManager(config: LeaseConfig): LeaseManager {
    return new VerrouLeaseManager(config, this.#connection)
  }

  async destroy(): Promise<void> {
    await this.#connection.quit()
  }

  pop(): Promise<JobData | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<JobData | null> {
    // First, move any ready delayed jobs to the regular queue
    await this.#processDelayedJobs(queue)

    // Pop from priority queue (sorted set) - highest priority (lowest score) first
    const queueContent = await this.#connection.zpopmin(`${redisKey}::${queue}`)

    if (queueContent && queueContent.length > 0) {
      return JSON.parse(queueContent[0])
    }

    return null
  }

  push(jobData: JobData): Promise<void> {
    return this.pushOn('default', jobData)
  }

  pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void> {
    const executeAt = Date.now() + delay
    const delayedKey = `${redisKey}::delayed::${queue}`

    await this.#connection.zadd(delayedKey, executeAt, JSON.stringify(jobData))
  }

  async pushOn(queue: string, jobData: JobData): Promise<void> {
    const priority = jobData.priority ?? 5

    // Use priority as primary score, add timestamp for FIFO order within same priority
    // Date.now() precision is sufficient but perfect FIFO within the same millisecond is not guaranteed
    const timestamp = Date.now()
    const score = priority * 1e13 + timestamp

    await this.#connection.zadd(`${redisKey}::${queue}`, score, JSON.stringify(jobData))
  }

  size(): Promise<number> {
    return this.sizeOf('default')
  }

  sizeOf(queue: string): Promise<number> {
    return this.#connection.zcard(`${redisKey}::${queue}`)
  }

  async #processDelayedJobs(queue: string): Promise<number> {
    const now = Date.now()
    const delayedKey = `${redisKey}::delayed::${queue}`
    const queueKey = `${redisKey}::${queue}`

    // Use Lua script for atomic operation - much faster than pipeline
    return (await this.#connection.eval(
      PROCESS_DELAYED_JOBS_SCRIPT,
      2, // number of keys
      delayedKey,
      queueKey,
      now.toString()
    )) as number
  }
}
