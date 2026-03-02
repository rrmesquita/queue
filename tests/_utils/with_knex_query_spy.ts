import type { Knex } from 'knex'

interface WithKnexQuerySpyOptions<T> {
  connection: Knex
  run: () => Promise<T>
}

export async function withKnexQuerySpy<T>({
  connection,
  run,
}: WithKnexQuerySpyOptions<T>): Promise<{ result: T; queries: string[] }> {
  const queries: string[] = []
  const onQuery = (query: { sql: string }) => {
    queries.push(query.sql.toLowerCase())
  }

  connection.on('query', onQuery)
  try {
    const result = await run()
    return { result, queries }
  } finally {
    connection.off('query', onQuery)
  }
}
