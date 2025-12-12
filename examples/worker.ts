import { Worker } from '../src/worker.js'
import { config } from './config.js'

const worker = new Worker(config)
await worker.start(['default', 'email', 'reports'])
