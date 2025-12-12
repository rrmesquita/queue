import { Worker } from '#src/worker'
import { config } from './config.js'

const worker = new Worker(config)
await worker.start(['default', 'email', 'reports'])
