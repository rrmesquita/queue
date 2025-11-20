import { Worker } from '#src/worker'
import { config } from './config.ts'

const worker = new Worker(config)
await worker.start(['default', 'email', 'reports'])
