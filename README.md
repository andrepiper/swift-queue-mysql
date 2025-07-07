# MySQL Boss

Queueing jobs in MySQL from Node.js

[![npm version](https://badge.fury.io/js/mysql-boss.svg)](https://badge.fury.io/js/mysql-boss)

```js
async function readme() {
  const MysqlBoss = require('./src/index');
  const boss = new MysqlBoss({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'mysql_boss'
  });

  boss.on('error', console.error)

  await boss.start()

  const queue = 'readme-queue'

  await boss.createQueue(queue)

  const id = await boss.send(queue, { arg1: 'read me' })

  console.log(`created job ${id} in queue ${queue}`)

  await boss.work(queue, async (jobs) => {
    for (const job of jobs) {
      console.log(`received job ${job.id} with data ${JSON.stringify(job.data)}`)
    }
  })
}

readme()
  .catch(err => {
    console.log(err)
    process.exit(1)
  })
```

mysql-boss is a job queue built in Node.js on top of MySQL to provide background processing and reliable asynchronous execution to Node.js applications.

mysql-boss uses MySQL's `FOR UPDATE` with optimistic locking to provide job processing safety and prevent job duplication. While it doesn't have PostgreSQL's `SKIP LOCKED` feature, it implements similar functionality using MySQL's locking mechanisms.

This will cater to teams already familiar with MySQL and want to limit how many systems are required to monitor and support in their architecture.

## Summary
* Reliable job delivery with MySQL locking
* Create jobs within your existing database transaction
* Backpressure-compatible polling workers
* Cron scheduling with timezone support
* Queue storage policies for rate limiting, debouncing, and concurrency
* Priority queues, dead letter queues, job deferral, automatic retries with exponential backoff
* Pub/sub API for fan-out queue relationships
* Built-in job monitoring and maintenance
* Automatic job archival and cleanup
* Configurable retention policies

## Installation

```bash
npm install mysql-boss
```

## Requirements

* MySQL 5.7+ or MySQL 8.0+
* Node.js 16+

## Configuration

mysql-boss accepts the following configuration options:

```js
const boss = new MysqlBoss({
  // MySQL connection options
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: 'password',
  database: 'mysql_boss',
  
  // Or use connection string
  // connectionString: 'mysql://user:password@host:port/database',
  
  // Connection pool options
  max: 10,
  connectionTimeoutMillis: 30000,
  idleTimeoutMillis: 30000,
  
  // mysql-boss specific options
  schema: 'mysql_boss',
  archiveInterval: 86400, // 24 hours
  deleteAfter: 86400, // 24 hours
  maintenanceIntervalSeconds: 300, // 5 minutes
  monitorStateIntervalSeconds: 60, // 1 minute
  pollingIntervalSeconds: 2
})
```

## API

### Queue Operations

#### `createQueue(name, options)`
Creates a new queue with the specified options.

```js
await boss.createQueue('my-queue', {
  policy: 'standard',
  retryLimit: 3,
  retryDelay: 60,
  retryBackoff: true,
  expireInSeconds: 900,
  retentionMinutes: 1440,
  deadLetter: 'failed-jobs'
})
```

#### `deleteQueue(name)`
Deletes a queue and all its jobs.

#### `getQueues()`
Returns all queues.

#### `getQueue(name)`
Returns queue information.

#### `getQueueSize(name)`
Returns the number of jobs in a queue.

#### `purgeQueue(name)`
Deletes all jobs in a queue.

### Job Operations

#### `send(name, data, options)`
Sends a job to a queue.

```js
const jobId = await boss.send('email-queue', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Welcome to our service!'
}, {
  priority: 10,
  startAfter: new Date(Date.now() + 60000), // Start after 1 minute
  retryLimit: 3,
  retryDelay: 30,
  retryBackoff: true,
  expireInSeconds: 300,
  singletonKey: 'unique-key' // Prevents duplicate jobs
})
```

#### `work(name, callback, options)`
Processes jobs from a queue.

```js
await boss.work('email-queue', async (jobs) => {
  for (const job of jobs) {
    await sendEmail(job.data)
  }
}, {
  batchSize: 5,
  pollingInterval: 2000,
  teamSize: 2,
  teamConcurrency: 1
})
```

#### `fetch(name, options)`
Manually fetch jobs from a queue.

```js
const jobs = await boss.fetch('my-queue', {
  batchSize: 10,
  includeMetadata: true
})
```

#### `complete(id, data)`
Marks job(s) as completed.

#### `fail(id, data)`
Marks job(s) as failed.

#### `cancel(id)`
Cancels job(s).

#### `resume(id)`
Resumes job(s).

#### `retry(id)`
Retries job(s).

### Scheduling

#### `schedule(name, cron, data, options)`
Schedules a recurring job.

```js
await boss.schedule('daily-cleanup', '0 2 * * *', {
  action: 'cleanup'
}, {
  tz: 'America/New_York'
})
```

#### `unschedule(name)`
Removes a scheduled job.

#### `getSchedules()`
Returns all scheduled jobs.

### Pub/Sub

#### `publish(event, data, options)`
Publishes data to all queues subscribed to an event.

#### `subscribe(event, name)`
Subscribes a queue to an event.

#### `unsubscribe(event, name)`
Unsubscribes a queue from an event.

### Monitoring

#### `getWipData(options)`
Returns work-in-progress data.

#### `countStates()`
Returns job counts by state.

### Maintenance

#### `maintain()`
Runs maintenance tasks (expire, archive, drop).

#### `archive()`
Archives completed jobs.

#### `drop()`
Drops old archived jobs.

#### `expire()`
Expires timed-out jobs.

#### `clearStorage()`
Clears all job data.

## Queue Policies

- **standard**: Default policy with no special behavior
- **short**: Optimized for short-running jobs
- **singleton**: Only one job with the same singleton key can be active
- **stately**: State-based job processing

## Job States

- **created**: Job is created and waiting to be processed
- **retry**: Job failed and is scheduled for retry
- **active**: Job is currently being processed
- **completed**: Job completed successfully
- **cancelled**: Job was cancelled
- **failed**: Job failed and won't be retried

## Error Handling

```js
boss.on('error', (error) => {
  console.error('mysql-boss error:', error)
})
```

## Testing

mysql-boss includes several test utilities and configuration options for testing:

```js
const boss = new MysqlBoss({
  // Test configuration
  __test__throw_worker: false,
  __test__delay_monitor: 0,
  __test__throw_monitor: false,
  __test__delay_maintenance: 0,
  __test__throw_maint: false
})
```

## Differences from pg-boss

This MySQL implementation differs from pg-boss in several ways:

1. **Locking**: Uses MySQL's `FOR UPDATE` instead of PostgreSQL's `SKIP LOCKED`
2. **Data Types**: Uses MySQL's `JSON` instead of PostgreSQL's `JSONB`
3. **UUIDs**: Uses MySQL's `UUID()` function instead of PostgreSQL's `gen_random_uuid()`
4. **Intervals**: Uses MySQL's `INTERVAL` syntax instead of PostgreSQL's interval types
5. **Partitioning**: Uses MySQL's partitioning features adapted for MySQL syntax
6. **Procedures**: Uses MySQL's stored procedure syntax instead of PL/pgSQL

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT

## Changelog

### Version 1.0.0
- Initial release
- MySQL-compatible job queue implementation
- Support for all major pg-boss features adapted for MySQL
- Comprehensive test suite
- Full documentation
