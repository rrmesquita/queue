import { Redis, type RedisOptions } from 'ioredis'
import type { Adapter, AcquiredJob } from '#contracts/adapter'
import type { JobData } from '#types/main'

const redisKey = 'jobs'
type RedisConfig = Redis | RedisOptions

/**
 * Lua script for atomic job acquisition.
 * 1. Check and process delayed jobs
 * 2. Pop from pending queue
 * 3. Add to active hash with worker info
 * 4. Return job data
 */
const ACQUIRE_JOB_SCRIPT = `
  local pending_key = KEYS[1]
  local active_key = KEYS[2]
  local delayed_key = KEYS[3]
  local worker_id = ARGV[1]
  local now = ARGV[2]

  -- First, process delayed jobs
  local ready_jobs = redis.call('ZRANGEBYSCORE', delayed_key, 0, now)
  if #ready_jobs > 0 then
    for i = 1, #ready_jobs do
      local job_data = ready_jobs[i]
      local job = cjson.decode(job_data)
      local priority = job.priority or 5
      local timestamp = tonumber(now)
      local score = priority * 10000000000000 + timestamp
      redis.call('ZADD', pending_key, score, job_data)
      redis.call('ZREM', delayed_key, job_data)
    end
  end

  -- Pop highest priority job (lowest score)
  local result = redis.call('ZPOPMIN', pending_key)
  if not result or #result == 0 then
    return nil
  end

  local job_data = result[1]
  local job = cjson.decode(job_data)

  -- Store in active hash: jobId -> {workerId, acquiredAt, data}
  local active_data = cjson.encode({
    workerId = worker_id,
    acquiredAt = tonumber(now),
    data = job
  })
  redis.call('HSET', active_key, job.id, active_data)

  -- Return job with acquiredAt
  job.acquiredAt = tonumber(now)
  return cjson.encode(job)
`

/**
 * Lua script for completing a job.
 * Removes the job from active hash.
 */
const COMPLETE_JOB_SCRIPT = `
  local active_key = KEYS[1]
  local job_id = ARGV[1]

  redis.call('HDEL', active_key, job_id)
  return 1
`

/**
 * Lua script for failing a job permanently.
 * Removes from active hash.
 */
const FAIL_JOB_SCRIPT = `
  local active_key = KEYS[1]
  local job_id = ARGV[1]

  redis.call('HDEL', active_key, job_id)
  return 1
`

/**
 * Lua script for retrying a job.
 * 1. Get job from active hash
 * 2. Remove from active hash
 * 3. Increment attempts
 * 4. Add back to pending (or delayed if retryAt is set)
 */
const RETRY_JOB_SCRIPT = `
  local active_key = KEYS[1]
  local pending_key = KEYS[2]
  local delayed_key = KEYS[3]
  local job_id = ARGV[1]
  local retry_at = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])

  -- Get job from active hash
  local active_data = redis.call('HGET', active_key, job_id)
  if not active_data then
    return 0
  end

  local active = cjson.decode(active_data)
  local job = active.data

  -- Remove from active
  redis.call('HDEL', active_key, job_id)

  -- Increment attempts
  job.attempts = (job.attempts or 0) + 1

  local job_data = cjson.encode(job)

  -- Add back to pending or delayed
  if retry_at and retry_at > now then
    redis.call('ZADD', delayed_key, retry_at, job_data)
  else
    local priority = job.priority or 5
    local score = priority * 10000000000000 + now
    redis.call('ZADD', pending_key, score, job_data)
  end

  return 1
`

export function redis(config?: RedisConfig) {
  return () => {
    if (config instanceof Redis) {
      return new RedisAdapter(config)
    }

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
  #workerId: string = ''

  constructor(connection: Redis) {
    this.#connection = connection
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    await this.#connection.quit()
  }

  pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const now = Date.now()
    const pendingKey = `${redisKey}::${queue}`
    const activeKey = `${redisKey}::${queue}::active`
    const delayedKey = `${redisKey}::delayed::${queue}`

    const result = await this.#connection.eval(
      ACQUIRE_JOB_SCRIPT,
      3,
      pendingKey,
      activeKey,
      delayedKey,
      this.#workerId,
      now.toString()
    )

    if (!result) {
      return null
    }

    return JSON.parse(result as string)
  }

  async popAndWait(queue: string, timeout: number): Promise<AcquiredJob | null> {
    // First try immediate pop
    const immediate = await this.popFrom(queue)
    if (immediate) {
      return immediate
    }

    // Wait for new job using BZPOPMIN on pending queue
    const pendingKey = `${redisKey}::${queue}`
    const activeKey = `${redisKey}::${queue}::active`
    const now = Date.now()

    // BZPOPMIN returns [key, member, score] or null
    const result = await this.#connection.bzpopmin(pendingKey, timeout / 1000)

    if (!result) {
      return null
    }

    const [, jobData] = result
    const job = JSON.parse(jobData)

    // Store in active hash
    const activeData = JSON.stringify({
      workerId: this.#workerId,
      acquiredAt: now,
      data: job,
    })
    await this.#connection.hset(activeKey, job.id, activeData)

    return {
      ...job,
      acquiredAt: now,
    }
  }

  async completeJob(jobId: string, queue: string): Promise<void> {
    const activeKey = `${redisKey}::${queue}::active`

    await this.#connection.eval(COMPLETE_JOB_SCRIPT, 1, activeKey, jobId)
  }

  async failJob(jobId: string, queue: string, _error?: Error): Promise<void> {
    const activeKey = `${redisKey}::${queue}::active`

    await this.#connection.eval(FAIL_JOB_SCRIPT, 1, activeKey, jobId)
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const now = Date.now()
    const activeKey = `${redisKey}::${queue}::active`
    const pendingKey = `${redisKey}::${queue}`
    const delayedKey = `${redisKey}::delayed::${queue}`

    await this.#connection.eval(
      RETRY_JOB_SCRIPT,
      3,
      activeKey,
      pendingKey,
      delayedKey,
      jobId,
      retryAt ? retryAt.getTime().toString() : '0',
      now.toString()
    )
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
}
