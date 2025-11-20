import { JobDispatcher } from '#src/job_dispatcher'
import type { JobOptions } from '#types/main'

export abstract class Job<Payload = any> {
  readonly #payload: Payload

  static options: JobOptions = {}

  get payload(): Payload {
    return this.#payload
  }

  constructor(payload: Payload) {
    this.#payload = payload
  }

  static dispatch<T extends Job>(
    this: new (payload: any) => T,
    payload: T extends Job<infer P> ? P : never
  ): JobDispatcher<T extends Job<infer P> ? P : never> {
    const dispatcher = new JobDispatcher<T extends Job<infer P> ? P : never>(
      (this as any).jobName,
      payload
    )

    if ((this as any).options.queue) {
      dispatcher.toQueue((this as any).options.queue)
    }

    if ((this as any).options.adapter) {
      dispatcher.with((this as any).options.adapter)
    }

    if ((this as any).options.priority !== undefined) {
      dispatcher.priority((this as any).options.priority)
    }

    return dispatcher
  }

  abstract execute(): Promise<void>

  failed?(error: Error): Promise<void>
}
