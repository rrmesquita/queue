import { parse as parseDuration } from '@lukeed/ms'
import type { Duration } from '#types/main'
import * as errors from '#src/exceptions'

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
