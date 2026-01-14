# Job grouping

## New feature

### Group ID

Jobs can now be assigned to a group using the `groupId` option. This allows organizing related jobs together for easier monitoring and filtering in UIs.

```typescript
// Assign jobs to a group
await SendEmailJob.dispatch({ to: 'user@example.com' })
  .group('newsletter-jan-2025')
  .run()

// Combine with other options
await ExportJob.dispatch({ userId: 1 })
  .group('batch-export-123')
  .toQueue('exports')
  .priority(2)
  .run()
```

### Use cases

- **Batch operations**: Group all jobs from a newsletter send, bulk export, or data migration
- **Monitoring**: Filter and view related jobs together in queue UIs
- **Debugging**: Easily find all jobs related to a specific operation

### API

The `groupId` is stored with the job data and can be accessed via:

```typescript
const record = await adapter.getJob('job-id', 'queue-name')
console.log(record.data.groupId) // 'newsletter-jan-2025'
```
