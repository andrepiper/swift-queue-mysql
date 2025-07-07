#!/usr/bin/env node

const { program } = require('commander')
const SwiftQueueMySQL = require('@swiftworks/swift-queue-mysql')
const pkg = require('../package.json')

program
  .version(pkg.version)
  .description('Swift Queue MySQL - Job Queue CLI')

// Database connection options
program
  .option('-h, --host <host>', 'MySQL host', 'localhost')
  .option('-P, --port <port>', 'MySQL port', '3306')
  .option('-u, --user <user>', 'MySQL user', 'root')
  .option('-p, --password <password>', 'MySQL password', '')
  .option('-d, --database <database>', 'MySQL database', 'swift_queue')
  .option('-s, --schema <schema>', 'Swift Queue MySQL schema', 'swift_queue')
  .option('-c, --connection-string <connectionString>', 'MySQL connection string')

// Create command
program
  .command('create')
  .description('Create Swift Queue MySQL database schema')
  .action(async (options) => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      console.log('✓ Swift Queue MySQL schema created successfully')
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Failed to create schema:', error.message)
      process.exit(1)
    }
  })

// Status command
program
  .command('status')
  .description('Show Swift Queue MySQL status')
  .action(async () => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      const states = await swiftQueue.countStates()
      const queues = await swiftQueue.getQueues()
      
      console.log('\n📊 Swift Queue MySQL Status')
      console.log('==================')
      console.log(`Total Jobs: ${states.all || 0}`)
      console.log(`Active: ${states.active || 0}`)
      console.log(`Completed: ${states.completed || 0}`)
      console.log(`Failed: ${states.failed || 0}`)
      console.log(`Queues: ${queues.length}`)
      
      if (queues.length > 0) {
        console.log('\n📋 Queues:')
        queues.forEach(queue => {
          const queueStates = states.queues[queue.name] || {}
          console.log(`  ${queue.name}:`)
          console.log(`    Policy: ${queue.policy || 'standard'}`)
          console.log(`    Jobs: ${Object.values(queueStates).reduce((a, b) => a + b, 0)}`)
          console.log(`    Active: ${queueStates.active || 0}`)
          console.log(`    Completed: ${queueStates.completed || 0}`)
          console.log(`    Failed: ${queueStates.failed || 0}`)
        })
      }
      
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Failed to get status:', error.message)
      process.exit(1)
    }
  })

// Send command
program
  .command('send <queue> [data]')
  .description('Send a job to a queue')
  .option('-p, --priority <priority>', 'Job priority', '0')
  .option('-d, --delay <delay>', 'Delay in seconds', '0')
  .option('-r, --retry-limit <retryLimit>', 'Retry limit', '3')
  .option('-e, --expire-in <expireIn>', 'Expire in seconds', '900')
  .option('-k, --singleton-key <singletonKey>', 'Singleton key')
  .action(async (queueName, data, options) => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      let jobData = {}
      if (data) {
        try {
          jobData = JSON.parse(data)
        } catch (error) {
          jobData = { message: data }
        }
      }
      
      const jobOptions = {
        priority: parseInt(options.priority) || 0,
        retryLimit: parseInt(options.retryLimit) || 3,
        expireInSeconds: parseInt(options.expireIn) || 900,
        singletonKey: options.singletonKey
      }
      
      if (options.delay && parseInt(options.delay) > 0) {
        jobOptions.startAfter = new Date(Date.now() + parseInt(options.delay) * 1000)
      }
      
      const jobId = await swiftQueue.send(queueName, jobData, jobOptions)
      console.log(`✓ Job sent to queue "${queueName}" with ID: ${jobId}`)
      
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Failed to send job:', error.message)
      process.exit(1)
    }
  })

// Work command
program
  .command('work <queue>')
  .description('Process jobs from a queue')
  .option('-b, --batch-size <batchSize>', 'Batch size', '1')
  .option('-i, --interval <interval>', 'Polling interval in ms', '2000')
  .option('-t, --timeout <timeout>', 'Worker timeout in seconds', '30')
  .action(async (queueName, options) => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      console.log(`🔄 Starting worker for queue "${queueName}"...`)
      console.log(`   Batch size: ${options.batchSize}`)
      console.log(`   Polling interval: ${options.interval}ms`)
      console.log('   Press Ctrl+C to stop')
      
      await swiftQueue.work(queueName, async (jobs) => {
        for (const job of jobs) {
          console.log(`📋 Processing job ${job.id}`)
          console.log(`   Data: ${JSON.stringify(job.data)}`)
          console.log(`   Created: ${job.createdOn}`)
          
          // Simulate work
          await new Promise(resolve => setTimeout(resolve, 1000))
          
          console.log(`✓ Job ${job.id} completed`)
        }
      }, {
        batchSize: parseInt(options.batchSize) || 1,
        pollingInterval: parseInt(options.interval) || 2000
      })
      
      // Keep process running
      process.on('SIGINT', async () => {
        console.log('\n🛑 Stopping worker...')
        await swiftQueue.stop()
        process.exit(0)
      })
      
    } catch (error) {
      console.error('✗ Worker failed:', error.message)
      process.exit(1)
    }
  })

// Purge command
program
  .command('purge <queue>')
  .description('Purge all jobs from a queue')
  .option('-f, --force', 'Force purge without confirmation')
  .action(async (queueName, options) => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      if (!options.force) {
        const readline = require('readline')
        const rl = readline.createInterface({
          input: process.stdin,
          output: process.stdout
        })
        
        const answer = await new Promise(resolve => {
          rl.question(`Are you sure you want to purge all jobs from queue "${queueName}"? (y/N): `, resolve)
        })
        
        rl.close()
        
        if (answer.toLowerCase() !== 'y' && answer.toLowerCase() !== 'yes') {
          console.log('Operation cancelled')
          await swiftQueue.stop()
          return
        }
      }
      
      await swiftQueue.purgeQueue(queueName)
      console.log(`✓ Queue "${queueName}" purged successfully`)
      
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Failed to purge queue:', error.message)
      process.exit(1)
    }
  })

// Maintenance command
program
  .command('maintain')
  .description('Run maintenance tasks')
  .action(async () => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      console.log('🔧 Running maintenance tasks...')
      const result = await swiftQueue.maintain()
      
      console.log('✓ Maintenance completed:')
      console.log(`   Expired jobs: ${result.expiredJobs}`)
      console.log(`   Archived jobs: ${result.archivedJobs}`)
      console.log(`   Deleted jobs: ${result.deletedJobs}`)
      
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Maintenance failed:', error.message)
      process.exit(1)
    }
  })

// Schedule command
program
  .command('schedule <queue> <cron> [data]')
  .description('Schedule a recurring job')
  .option('-tz, --timezone <timezone>', 'Timezone', 'UTC')
  .action(async (queueName, cron, data, options) => {
    const swiftQueue = createSwiftQueue(program.opts())
    try {
      await swiftQueue.start()
      
      let jobData = {}
      if (data) {
        try {
          jobData = JSON.parse(data)
        } catch (error) {
          jobData = { message: data }
        }
      }
      
      await swiftQueue.schedule(queueName, cron, jobData, {
        tz: options.timezone || 'UTC'
      })
      
      console.log(`✓ Job scheduled for queue "${queueName}"`)
      console.log(`   Cron: ${cron}`)
      console.log(`   Timezone: ${options.timezone || 'UTC'}`)
      
      await swiftQueue.stop()
    } catch (error) {
      console.error('✗ Failed to schedule job:', error.message)
      process.exit(1)
    }
  })

// Helper function to create Swift Queue MySQL instance
function createSwiftQueue(options) {
  const config = {}
  
  if (options.connectionString) {
    config.connectionString = options.connectionString
  } else {
    config.host = options.host
    config.port = parseInt(options.port)
    config.user = options.user
    config.password = options.password
    config.database = options.database
  }
  
  config.schema = options.schema
  
  return new SwiftQueueMySQL(config)
}

// Parse command line arguments
program.parse(process.argv)

// Show help if no command provided
if (!process.argv.slice(2).length) {
  program.outputHelp()
}
