# QueueSchemaService for Knex Adapter

## Breaking Change

The Knex adapter no longer automatically creates database tables on first use. You must now create the tables explicitly using the new `QueueSchemaService` or your own migration system.

## New Feature: QueueSchemaService

A new `QueueSchemaService` class is now exported from the main package, providing methods to create and drop the queue tables in a controlled manner.

### Methods

- `createJobsTable(tableName?, extend?)` - Creates the jobs table with the default schema
- `createSchedulesTable(tableName?, extend?)` - Creates the schedules table with the default schema
- `dropJobsTable(tableName?)` - Drops the jobs table if it exists
- `dropSchedulesTable(tableName?)` - Drops the schedules table if it exists

### Usage

```typescript
import { QueueSchemaService } from '@boringnode/queue'
import Knex from 'knex'

const connection = Knex({ client: 'pg', connection: '...' })
const schemaService = new QueueSchemaService(connection)

// Create tables with default names
await schemaService.createJobsTable()
await schemaService.createSchedulesTable()

// Or with custom table names
await schemaService.createJobsTable('my_jobs')
await schemaService.createSchedulesTable('my_schedules')

// Extend with custom columns
await schemaService.createJobsTable('queue_jobs', (table) => {
  table.string('tenant_id', 255).nullable()
})
```

### AdonisJS Migration Example

```typescript
import { BaseSchema } from '@adonisjs/lucid/schema'
import { QueueSchemaService } from '@boringnode/queue'

export default class extends BaseSchema {
  async up() {
    const schemaService = new QueueSchemaService(this.db.connection().getWriteClient())

    await schemaService.createJobsTable()
    await schemaService.createSchedulesTable()
  }

  async down() {
    const schemaService = new QueueSchemaService(this.db.connection().getWriteClient())

    await schemaService.dropSchedulesTable()
    await schemaService.dropJobsTable()
  }
}
```

## Migration Guide

If you were relying on automatic table creation, you need to:

1. Create a migration that uses `QueueSchemaService` to create the tables
2. Run the migration before starting your application

This change gives you full control over when and how the tables are created, and allows you to extend the schema with custom columns.
