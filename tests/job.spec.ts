import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { JobDispatcher } from '../src/job_dispatcher.js'
import { JobBatchDispatcher } from '../src/job_batch_dispatcher.js'
import { ScheduleBuilder } from '../src/schedule_builder.js'

class PaymentService {}

class ProcessPaymentJob extends Job<{ paymentId: string }> {
  constructor(protected paymentService: PaymentService) {
    super()
  }

  async execute() {
    void this.paymentService
  }
}

test.group('Job static methods', () => {
  test('should support subclasses with typed constructor injection', ({ assert, expectTypeOf }) => {
    const dispatcher = ProcessPaymentJob.dispatch({ paymentId: 'pay_123' })
    const batchDispatcher = ProcessPaymentJob.dispatchMany([{ paymentId: 'pay_123' }])
    const schedule = ProcessPaymentJob.schedule({ paymentId: 'pay_123' })

    assert.instanceOf(dispatcher, JobDispatcher)
    assert.instanceOf(batchDispatcher, JobBatchDispatcher)
    assert.instanceOf(schedule, ScheduleBuilder)

    expectTypeOf(dispatcher).toEqualTypeOf<JobDispatcher<{ paymentId: string }>>()
    expectTypeOf(batchDispatcher).toEqualTypeOf<JobBatchDispatcher<{ paymentId: string }>>()
    expectTypeOf(schedule).toEqualTypeOf<ScheduleBuilder<{ paymentId: string }>>()
  })
})
