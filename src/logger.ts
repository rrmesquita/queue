export interface LogObject {
  [key: string]: unknown
}

export interface ErrorObject extends LogObject {
  err?: Error
}

export interface Logger {
  trace(msg: string): void
  trace(obj: LogObject, msg: string): void

  debug(msg: string): void
  debug(obj: LogObject, msg: string): void

  info(msg: string): void
  info(obj: LogObject, msg: string): void

  warn(msg: string): void
  warn(obj: LogObject, msg: string): void

  error(msg: string): void
  error(obj: ErrorObject, msg: string): void

  child(obj: LogObject): Logger
}

/**
 * A simple logger that writes to console.
 */
class ConsoleLogger implements Logger {
  #prefix: string

  constructor(prefix: string = 'queue') {
    this.#prefix = prefix
  }

  #format(level: string, msgOrObj: string | LogObject, msg?: string): [string, LogObject?] {
    const prefix = `[${this.#prefix}] ${level}:`

    if (typeof msgOrObj === 'object') {
      return [`${prefix} ${msg}`, msgOrObj]
    }

    return [`${prefix} ${msgOrObj}`]
  }

  trace(msg: string): void
  trace(obj: LogObject, msg: string): void
  trace(msgOrObj: string | LogObject, msg?: string): void {
    const [message, obj] = this.#format('TRACE', msgOrObj, msg)

    if (obj) {
      return console.log(message, obj)
    }

    console.log(message)
  }

  debug(msg: string): void
  debug(obj: LogObject, msg: string): void
  debug(msgOrObj: string | LogObject, msg?: string): void {
    const [message, obj] = this.#format('DEBUG', msgOrObj, msg)

    if (obj) {
      return console.log(message, obj)
    }

    console.log(message)
  }

  info(msg: string): void
  info(obj: LogObject, msg: string): void
  info(msgOrObj: string | LogObject, msg?: string): void {
    const [message, obj] = this.#format('INFO', msgOrObj, msg)

    if (obj) {
      return console.log(message, obj)
    }

    console.log(message)
  }

  warn(msg: string): void
  warn(obj: LogObject, msg: string): void
  warn(msgOrObj: string | LogObject, msg?: string): void {
    const [message, obj] = this.#format('WARN', msgOrObj, msg)

    if (obj) {
      return console.warn(message, obj)
    }

    console.warn(message)
  }

  error(msg: string): void
  error(obj: ErrorObject, msg: string): void
  error(msgOrObj: string | ErrorObject, msg?: string): void {
    const [message, obj] = this.#format('ERROR', msgOrObj, msg)

    if (obj) {
      return console.error(message, obj)
    }

    console.error(message)
  }

  child(obj: LogObject): Logger {
    const childPrefix = obj.pkg ? String(obj.pkg) : this.#prefix
    return new ConsoleLogger(childPrefix)
  }
}

export const consoleLogger = new ConsoleLogger()
