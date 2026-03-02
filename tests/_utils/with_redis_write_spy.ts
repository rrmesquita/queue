import type { Redis } from 'ioredis'

interface RedisStream {
  write: (...args: unknown[]) => unknown
}

interface RedisWithStream {
  stream: RedisStream
}

interface WithRedisWriteSpyOptions<T> {
  connection: Redis
  run: () => Promise<T>
  onWrite?: (writeCount: number) => void
}

export async function withRedisWriteSpy<T>({
  connection,
  run,
  onWrite,
}: WithRedisWriteSpyOptions<T>): Promise<{ result: T; writes: number }> {
  const stream = (connection as unknown as RedisWithStream).stream
  const originalWrite = stream.write.bind(stream)
  let writes = 0

  stream.write = ((...args: unknown[]) => {
    writes++
    onWrite?.(writes)
    return originalWrite(...args)
  }) as typeof stream.write

  try {
    const result = await run()
    return { result, writes }
  } finally {
    stream.write = originalWrite
  }
}
