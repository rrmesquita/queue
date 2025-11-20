import { createError } from '@poppinss/utils'

export const E_INVALID_DURATION_EXPRESSION = createError(
  'Invalid duration expression: "%s"',
  'E_INVALID_DURATION_EXPRESSION',
  500
)

export const E_INVALID_BASE_DELAY = createError<[reason: string]>(
  'Invalid base delay. Reason: %s',
  'E_INVALID_BASE_DELAY',
  500
)

export const E_INVALID_MAX_DELAY = createError<[reason: string]>(
  'Invalid max delay. Reason: %s',
  'E_INVALID_MAX_DELAY',
  500
)

export const E_INVALID_MULTIPLIER = createError<[reason: string]>(
  'Invalid multiplier. Reason: %s',
  'E_INVALID_MULTIPLIER',
  500
)

export const E_CONFIGURATION_ERROR = createError<[reason: string]>(
  'Configuration error. Reason: %s',
  'E_CONFIGURATION_ERROR',
  500
)

export const E_JOB_NOT_FOUND = createError<[jobName: string]>(
  'Requested job "%s" is not registered',
  'E_JOB_NOT_FOUND'
)

export const E_JOB_MAX_ATTEMPTS_REACHED = createError<[jobName: string]>(
  'The job "%s" has reached the maximum number of retry attempts',
  'E_JOB_MAX_ATTEMPTS_REACHED'
)
