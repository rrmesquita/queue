# Bulk job dispatch

## New feature

### dispatchMany

Jobs can now be dispatched in batches using `Job.dispatchMany()`. This is more efficient than calling `dispatch()` multiple times as it uses optimized batch operations (Redis MULTI/EXEC transaction, SQL batch insert).

```typescript
// Dispatch multiple jobs at once
const { jobIds } = await SendEmailJob.dispatchMany([
  { to: 'user1@example.com', subject: 'Newsletter' },
  { to: 'user2@example.com', subject: 'Newsletter' },
  { to: 'user3@example.com', subject: 'Newsletter' },
])
  .group('newsletter-jan-2025')
  .toQueue('emails')
  .priority(3)
  .run()

console.log(`Dispatched ${jobIds.length} jobs`)
```

### Use cases

- **Newsletters**: Send thousands of emails in a single batch operation
- **Bulk exports**: Create export jobs for multiple users
- **Data migrations**: Queue many transformation jobs at once
- **Notifications**: Dispatch notifications to many recipients

### API

The `JobBatchDispatcher` supports the same fluent API as `JobDispatcher`:

```typescript
await SendEmailJob.dispatchMany(payloads)
  .toQueue('emails')      // Target queue
  .group('batch-123')     // Group all jobs together
  .priority(1)            // Set priority for all jobs
  .with('redis')          // Use specific adapter
  .run()
```

### Adapter API

For low-level access, adapters now support `pushMany()` and `pushManyOn()`:

```typescript
await adapter.pushManyOn('emails', [
  { id: 'uuid1', name: 'SendEmailJob', payload: {...}, attempts: 0 },
  { id: 'uuid2', name: 'SendEmailJob', payload: {...}, attempts: 0 },
])
```
