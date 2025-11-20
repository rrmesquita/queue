import { MemoryAdapter } from './memory_adapter.ts'
import type { JobData } from '#types/main'

export class ChaosAdapter extends MemoryAdapter {
  #throwProbability = 0

  alwaysThrow() {
    this.#throwProbability = 1
  }

  neverThrow() {
    this.#throwProbability = 0
  }

  async popFrom(queue: string): Promise<JobData | null> {
    if (Math.random() < this.#throwProbability) {
      throw new Error('Simulated error')
    }

    return super.popFrom(queue)
  }
}
