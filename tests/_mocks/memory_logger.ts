import type { Logger, LogObject, ErrorObject } from '../../src/logger.js'

export interface LogEntry {
  level: 'trace' | 'debug' | 'info' | 'warn' | 'error'
  message: string
  obj?: LogObject
}

export class MemoryLogger implements Logger {
  logs: LogEntry[] = []

  trace(msg: string): void
  trace(obj: LogObject, msg: string): void
  trace(msgOrObj: string | LogObject, msg?: string): void {
    this.#log('trace', msgOrObj, msg)
  }

  debug(msg: string): void
  debug(obj: LogObject, msg: string): void
  debug(msgOrObj: string | LogObject, msg?: string): void {
    this.#log('debug', msgOrObj, msg)
  }

  info(msg: string): void
  info(obj: LogObject, msg: string): void
  info(msgOrObj: string | LogObject, msg?: string): void {
    this.#log('info', msgOrObj, msg)
  }

  warn(msg: string): void
  warn(obj: LogObject, msg: string): void
  warn(msgOrObj: string | LogObject, msg?: string): void {
    this.#log('warn', msgOrObj, msg)
  }

  error(msg: string): void
  error(obj: ErrorObject, msg: string): void
  error(msgOrObj: string | ErrorObject, msg?: string): void {
    this.#log('error', msgOrObj, msg)
  }

  child(_obj: LogObject): Logger {
    return this
  }

  clear(): void {
    this.logs = []
  }

  #log(level: LogEntry['level'], msgOrObj: string | LogObject, msg?: string): void {
    if (typeof msgOrObj === 'object') {
      this.logs.push({ level, message: msg!, obj: msgOrObj })
    } else {
      this.logs.push({ level, message: msgOrObj })
    }
  }
}
