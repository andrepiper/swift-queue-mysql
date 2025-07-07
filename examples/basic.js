const SwiftQueueMySQL = require('../src/index')

async function basicExample() {
  const boss = new SwiftQueueMySQL({
    host: 'localhost',
    user: 'root',
    password: 'password',
    database: 'swift_queue'
  })

  boss.on('error', console.error)

  console.log('Starting swift-queue-mysql...')
  await boss.start()

  const queueName = 'basic-example'
  
  console.log('Creating queue:', queueName)
  await boss.createQueue(queueName, {
    retryLimit: 2,
    retryDelay: 5,
    expireInSeconds: 60
  })

  console.log('Sending job...')
  const jobId = await boss.send(queueName, {
    message: 'Hello from swift-queue-mysql!',
    timestamp: new Date().toISOString()
  })

  console.log('Job sent with ID:', jobId)

  console.log('Starting worker...')
  await boss.work(queueName, async (jobs) => {
    for (const job of jobs) {
      console.log('Processing job:', job.id)
      console.log('Job data:', job.data)
      
      // Simulate some work
      await new Promise(resolve => setTimeout(resolve, 1000))
      
      console.log('Job completed:', job.id)
    }
  }, {
    batchSize: 1,
    pollingInterval: 2000
  })

  console.log('Worker started. Waiting for jobs...')
  
  // Keep the process running
  process.on('SIGINT', async () => {
    console.log('Shutting down...')
    await boss.stop()
    process.exit(0)
  })
}

basicExample().catch(console.error)
