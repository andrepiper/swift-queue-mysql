const SwiftQueueMySQL = require('../src/index')
const Dashboard = require('./dashboard')

async function runMonitoringExample() {
  // Create SwiftQueueMySQL instance
  const boss = new SwiftQueueMySQL({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'swift_queue'
  })

  // Start MySQL Boss
  await boss.start()

  // Create and start dashboard
  const dashboard = new Dashboard(boss, {
    port: 3000,
    refreshInterval: 5000
  })

  await dashboard.start()

  // Create some test queues and jobs for demonstration
  await setupTestData(boss)

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\nShutting down...')
    await dashboard.stop()
    await boss.stop()
    process.exit(0)
  })
}

async function setupTestData(boss) {
  try {
    // Create test queues
    await boss.createQueue('email-queue', {
      policy: 'standard',
      retryLimit: 3,
      retryDelay: 30,
      expireInSeconds: 300
    })

    await boss.createQueue('image-processing', {
      policy: 'singleton',
      retryLimit: 2,
      retryDelay: 60,
      expireInSeconds: 600
    })

    await boss.createQueue('notifications', {
      policy: 'short',
      retryLimit: 1,
      retryDelay: 10,
      expireInSeconds: 60
    })

    // Send some test jobs
    await boss.send('email-queue', {
      to: 'user@example.com',
      subject: 'Welcome!',
      body: 'Welcome to our service!'
    })

    await boss.send('image-processing', {
      imageUrl: 'https://example.com/image.jpg',
      operations: ['resize', 'compress']
    })

    await boss.send('notifications', {
      userId: 123,
      message: 'Your order has been processed',
      type: 'push'
    })

    // Set up some workers
    await boss.work('email-queue', async (jobs) => {
      for (const job of jobs) {
        console.log(`Sending email to ${job.data.to}`)
        // Simulate email sending
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
    }, { batchSize: 5 })

    await boss.work('image-processing', async (jobs) => {
      for (const job of jobs) {
        console.log(`Processing image: ${job.data.imageUrl}`)
        // Simulate image processing
        await new Promise(resolve => setTimeout(resolve, 3000))
      }
    })

    await boss.work('notifications', async (jobs) => {
      for (const job of jobs) {
        console.log(`Sending notification to user ${job.data.userId}`)
        // Simulate notification sending
        await new Promise(resolve => setTimeout(resolve, 500))
      }
    })

    // Schedule a recurring job
    await boss.schedule('email-queue', '0 9 * * *', {
      to: 'admin@example.com',
      subject: 'Daily Report',
      body: 'Your daily system report is ready.'
    })

    console.log('Test data setup completed')
  } catch (error) {
    console.error('Failed to setup test data:', error)
  }
}

runMonitoringExample().catch(console.error)
