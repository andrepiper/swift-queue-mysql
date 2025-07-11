const SwiftQueueMySQL = require('../src/index.js')

async function advancedExample() {
  const swiftQueue = new SwiftQueueMySQL({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'swift_queue'
  })

  swiftQueue.on('error', console.error)
  swiftQueue.on('wip', (data) => console.log('Work in progress:', data))
  swiftQueue.on('maintenance', (data) => console.log('Maintenance completed:', data))

  console.log('Starting swift-queue-mysql...')
  await swiftQueue.start()

  // Create multiple queues with different policies
  const queues = [
    {
      name: 'high-priority',
      options: {
        policy: 'standard',
        retryLimit: 5,
        retryDelay: 10,
        retryBackoff: true,
        expireInSeconds: 300
      }
    },
    {
      name: 'singleton-tasks',
      options: {
        policy: 'singleton',
        retryLimit: 3,
        retryDelay: 30,
        expireInSeconds: 900
      }
    },
    {
      name: 'failed-jobs',
      options: {
        policy: 'standard',
        retryLimit: 0,
        retentionMinutes: 10080 // 1 week
      }
    }
  ]

  console.log('Creating queues...')
  for (const queue of queues) {
    await swiftQueue.createQueue(queue.name, queue.options)
    console.log(`Created queue: ${queue.name}`)
  }

  // Set up dead letter queue
  await swiftQueue.createQueue('email-queue', {
    retryLimit: 3,
    retryDelay: 60,
    retryBackoff: true,
    deadLetter: 'failed-jobs'
  })

  console.log('Setting up scheduled job...')
  await swiftQueue.schedule('daily-cleanup', '0 2 * * *', {
    action: 'cleanup',
    retention: 30
  }, {
    tz: 'UTC'
  })

  console.log('Setting up pub/sub...')
  await swiftQueue.subscribe('user-registered', 'email-queue')
  await swiftQueue.subscribe('user-registered', 'high-priority')

  console.log('Sending various jobs...')
  
  // Regular job
  await swiftQueue.send('high-priority', {
    type: 'process-payment',
    amount: 99.99,
    currency: 'USD'
  }, {
    priority: 10
  })

  // Delayed job
  await swiftQueue.send('email-queue', {
    to: 'user@example.com',
    template: 'welcome',
    data: { name: 'John' }
  }, {
    startAfter: new Date(Date.now() + 60000) // Start after 1 minute
  })

  // Singleton job
  await swiftQueue.send('singleton-tasks', {
    type: 'sync-database',
    table: 'users'
  }, {
    singletonKey: 'sync-users',
    singletonSeconds: 3600 // Only one per hour
  })

  // Publish event
  await swiftQueue.publish('user-registered', {
    userId: 123,
    email: 'newuser@example.com',
    timestamp: new Date().toISOString()
  })

  console.log('Setting up workers...')

  // High priority worker
  await swiftQueue.work('high-priority', async (jobs) => {
    for (const job of jobs) {
      console.log(`Processing high-priority job: ${job.id}`)
      
      try {
        // Simulate work based on job type
        switch (job.data.type) {
          case 'process-payment':
            console.log(`Processing payment: $${job.data.amount} ${job.data.currency}`)
            await new Promise(resolve => setTimeout(resolve, 2000))
            break
          
          default:
            console.log('Unknown job type:', job.data.type)
        }
      } catch (error) {
        console.error('Job failed:', error)
        throw error // This will mark the job as failed
      }
    }
  }, {
    batchSize: 5,
    pollingInterval: 1000,
    teamSize: 3
  })

  // Email worker
  await swiftQueue.work('email-queue', async (jobs) => {
    for (const job of jobs) {
      console.log(`Sending email: ${job.id}`)
      
      try {
        // Simulate email sending
        console.log(`Email to: ${job.data.to}`)
        await new Promise(resolve => setTimeout(resolve, 1500))
        console.log(`Email sent successfully: ${job.id}`)
      } catch (error) {
        console.error('Email failed:', error)
        throw error
      }
    }
  }, {
    batchSize: 10,
    pollingInterval: 5000
  })

  // Singleton worker
  await swiftQueue.work('singleton-tasks', async (jobs) => {
    for (const job of jobs) {
      console.log(`Processing singleton job: ${job.id}`)
      
      try {
        // Simulate long-running task
        console.log(`Syncing ${job.data.table} table...`)
        await new Promise(resolve => setTimeout(resolve, 5000))
        console.log(`Sync completed: ${job.id}`)
      } catch (error) {
        console.error('Sync failed:', error)
        throw error
      }
    }
  }, {
    batchSize: 1,
    pollingInterval: 10000
  })

  // Failed jobs worker
  await swiftQueue.work('failed-jobs', async (jobs) => {
    for (const job of jobs) {
      console.log(`Processing failed job: ${job.id}`)
      // Log failed job details or send to external monitoring
      console.log('Failed job data:', job.data)
      console.log('Failed job output:', job.output)
    }
  }, {
    batchSize: 5,
    pollingInterval: 30000
  })

  console.log('All workers started. System is running...')

  // Monitor system every 30 seconds
  setInterval(async () => {
    try {
      const states = await swiftQueue.countStates()
      console.log('System status:', states)
      
      const wipData = await swiftQueue.getWipData()
      console.log('Workers status:', wipData.length > 0 ? wipData : 'No active workers')
    } catch (error) {
      console.error('Monitoring error:', error)
    }
  }, 30000)

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...')
    await swiftQueue.stop({ timeout: 30000 })
    console.log('Shutdown complete')
    process.exit(0)
  })
}

advancedExample().catch(console.error)
