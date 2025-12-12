import { parse as parseDuration } from '@lukeed/ms'
import type { Duration } from './types/main.js'
import * as errors from './exceptions.js'

export function parse(duration: Duration): number {
  if (typeof duration === 'number') {
    return duration
  }

  const milliseconds = parseDuration(duration)

  if (typeof milliseconds === 'undefined') {
    throw new errors.E_INVALID_DURATION_EXPRESSION([duration])
  }

  return milliseconds
}
