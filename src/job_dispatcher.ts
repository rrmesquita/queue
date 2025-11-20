import debug from '#src/debug'
import { randomUUID } from 'node:crypto'
import { QueueManager } from '#src/queue_manager'
import type { Adapter } from '#contracts/adapter'
import type { Duration } from '#types/main'
import { parse } from '#src/utils'

export class JobDispatcher<T> {
  readonly #name: string
  readonly #payload: T
  #queue: string = 'default'
  #adapter?: string | (() => Adapter)
  #delay?: Duration
  #priority?: number

  constructor(name: string, payload: T) {
    this.#name = name
    this.#payload = payload
  }

  toQueue(queue: string): this {
    this.#queue = queue

    return this
  }
  in(delay: Duration): this {
    this.#delay = delay

    return this
  }

  priority(priority: number): this {
    this.#priority = priority

    return this
  }

  with(adapter: string | (() => Adapter)) {
    this.#adapter = adapter

    return this
  }

  async run() {
    const id = randomUUID()

    debug('dispatching job %s with id %s using payload %s', this.#name, id, this.#payload)

    const adapter = this.#getAdapterInstance()

    const payload = {
      id,
      name: this.#name,
      payload: this.#payload,
      attempts: 0,
      priority: this.#priority,
    }

    if (this.#delay) {
      const parsedDelay = parse(this.#delay)

      await adapter.pushLaterOn(this.#queue, payload, parsedDelay)
    } else {
      await adapter.pushOn(this.#queue, payload)
    }

    return id
  }

  then(onFulfilled?: (value: string) => any, onRejected?: (reason: any) => any): Promise<any> {
    return this.run().then(onFulfilled, onRejected)
  }

  #getAdapterInstance(): Adapter {
    if (!this.#adapter) {
      return QueueManager.use()
    }

    if (typeof this.#adapter === 'string') {
      return QueueManager.use(this.#adapter)
    }

    return this.#adapter()
  }
}
