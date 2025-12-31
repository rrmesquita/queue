import { parse as parseDuration } from '@lukeed/ms'
import type { Duration } from './types/main.js'
import * as errors from './exceptions.js'
import { PRIORITY_SCORE_MULTIPLIER } from './constants.js'

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

/**
 * Calculate the score for job ordering in the queue.
 * Lower scores are processed first.
 *
 * @param priority - Job priority (1-10, lower = higher priority)
 * @param timestamp - Timestamp in milliseconds
 * @returns Score for queue ordering
 */
export function calculateScore(priority: number, timestamp: number): number {
  return priority * PRIORITY_SCORE_MULTIPLIER + timestamp
}
