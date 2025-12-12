import { MemoryAdapter } from './memory_adapter.js'
import type { AcquiredJob } from '#contracts/adapter'

export class ChaosAdapter extends MemoryAdapter {
  #throwProbability = 0

  alwaysThrow() {
    this.#throwProbability = 1
  }

  neverThrow() {
    this.#throwProbability = 0
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    if (Math.random() < this.#throwProbability) {
      throw new Error('Simulated error')
    }

    return super.popFrom(queue)
  }
}
