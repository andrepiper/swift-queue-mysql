const assert = require('assert')
const SwiftQueueMySQL = require('../src/index')

describe('Reliability Tests', function() {
  this.timeout(20000)
  
  let boss

  before(async function() {
    boss = new SwiftQueueMySQL({
      host: process.env.MYSQL_HOST || 'localhost',
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || 'password',
      database: process.env.MYSQL_DATABASE || 'mysql_boss_test'
    })
    
    await boss.start()
  })

  after(async function() {
    if (boss) {
      await boss.clearStorage()
      await boss.stop()
    }
  })

  beforeEach(async function() {
    await boss.clearStorage()
  })

  describe('Retry Logic', function() {
    it('should retry failed jobs', async function() {
      const queueName = 'retry-queue'
      await boss.createQueue(queueName, {
        retryLimit: 3,
        retryDelay: 1,
        retryBackoff: false
      })
      
      const jobId = await boss.send(queueName, { test: 'retry' })
      
      // Simulate job failure
      await boss.fail(jobId, { error: 'test failure' })
      
      // Retry the job
      await boss.retry(jobId)
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'retry')
      assert.strictEqual(job.retryCount, 1)
    })

    it('should handle exponential backoff', async function() {
      const queueName = 'backoff-queue'
      await boss.createQueue(queueName, {
        retryLimit: 3,
        retryDelay: 1,
        retryBackoff: true
      })
      
      const jobId = await boss.send(queueName, { test: 'backoff' })
      
      // Fail and retry multiple times
      await boss.fail(jobId, { error: 'test failure' })
      await boss.retry(jobId)
      
      await boss.fail(jobId, { error: 'test failure' })
      await boss.retry(jobId)
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.retryCount, 2)
    })

    it('should respect retry limits', async function() {
      const queueName = 'limit-queue'
      await boss.createQueue(queueName, {
        retryLimit: 2
      })
      
      const jobId = await boss.send(queueName, { test: 'limit' })
      
      // Exceed retry limit
      await boss.fail(jobId, { error: 'test failure' })
      await boss.retry(jobId)
      await boss.fail(jobId, { error: 'test failure' })
      await boss.retry(jobId)
      await boss.fail(jobId, { error: 'test failure' })
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'failed')
      assert.strictEqual(job.retryCount, 2)
    })
  })

  describe('Dead Letter Queue', function() {
    it('should handle dead letter queue', async function() {
      const mainQueue = 'main-queue'
      const deadLetterQueue = 'dead-letter-queue'
      
      await boss.createQueue(deadLetterQueue)
      await boss.createQueue(mainQueue, {
        deadLetter: deadLetterQueue,
        retryLimit: 1
      })
      
      const jobId = await boss.send(mainQueue, { test: 'dead letter' })
      
      // Fail job beyond retry limit
      await boss.fail(jobId, { error: 'permanent failure' })
      await boss.retry(jobId)
      await boss.fail(jobId, { error: 'permanent failure' })
      
      // Job should be moved to dead letter queue
      const deadLetterJobs = await boss.fetch(deadLetterQueue)
      assert.strictEqual(deadLetterJobs.length, 1)
      assert.strictEqual(deadLetterJobs[0].data.test, 'dead letter')
    })
  })

  describe('Job Archival', function() {
    it('should archive completed jobs', async function() {
      const queueName = 'archive-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'archive' })
      const jobs = await boss.fetch(queueName)
      
      await boss.complete(jobs[0].id, { result: 'success' })
      
      // Run archival
      await boss.archive()
      
      // Job should be archived
      const archivedJob = await boss.getJobById(queueName, jobId, { includeArchive: true })
      assert(archivedJob)
      assert.strictEqual(archivedJob.state, 'completed')
    })

    it('should clean up old archived jobs', async function() {
      const queueName = 'cleanup-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'cleanup' })
      const jobs = await boss.fetch(queueName)
      
      await boss.complete(jobs[0].id, { result: 'success' })
      await boss.archive()
      
      // Run cleanup
      await boss.drop()
      
      // Job should still exist as it's not old enough
      const archivedJob = await boss.getJobById(queueName, jobId, { includeArchive: true })
      assert(archivedJob)
    })
  })

  describe('Concurrency Control', function() {
    it('should handle concurrent job processing', async function() {
      const queueName = 'concurrent-queue'
      await boss.createQueue(queueName)
      
      // Send multiple jobs
      const jobIds = []
      for (let i = 0; i < 5; i++) {
        const jobId = await boss.send(queueName, { index: i })
        jobIds.push(jobId)
      }
      
      // Process jobs concurrently
      const jobs = await boss.fetch(queueName, { batchSize: 5 })
      assert.strictEqual(jobs.length, 5)
      
      // Complete all jobs
      await boss.complete(jobIds)
      
      // Verify all jobs are completed
      for (const jobId of jobIds) {
        const job = await boss.getJobById(queueName, jobId)
        assert.strictEqual(job.state, 'completed')
      }
    })

    it('should handle worker team size', function(done) {
      const queueName = 'team-queue'
      let processedJobs = 0
      let activeWorkers = 0
      let maxActiveWorkers = 0
      
      boss.createQueue(queueName).then(async () => {
        await boss.work(queueName, async (jobs) => {
          activeWorkers++
          maxActiveWorkers = Math.max(maxActiveWorkers, activeWorkers)
          
          // Simulate work
          await new Promise(resolve => setTimeout(resolve, 100))
          
          processedJobs += jobs.length
          activeWorkers--
          
          if (processedJobs === 10) {
            assert(maxActiveWorkers <= 3) // Should respect team size
            done()
          }
        }, {
          teamSize: 3,
          batchSize: 1
        })
        
        // Send multiple jobs
        for (let i = 0; i < 10; i++) {
          await boss.send(queueName, { index: i })
        }
      }).catch(done)
    })
  })

  describe('Error Handling', function() {
    it('should handle worker errors gracefully', function(done) {
      const queueName = 'error-queue'
      let errorCount = 0
      
      boss.on('error', (error) => {
        errorCount++
        if (errorCount === 1) {
          assert(error.message.includes('test error'))
          done()
        }
      })
      
      boss.createQueue(queueName).then(async () => {
        await boss.work(queueName, async (jobs) => {
          throw new Error('test error')
        })
        
        await boss.send(queueName, { test: 'error' })
      }).catch(done)
    })

    it('should handle database disconnection', async function() {
      // This test would require mocking database disconnection
      // For now, we'll test that the boss can be restarted
      await boss.stop()
      await boss.start()
      
      const queueName = 'reconnect-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'reconnect' })
      const jobs = await boss.fetch(queueName)
      
      assert.strictEqual(jobs.length, 1)
      assert.strictEqual(jobs[0].id, jobId)
    })
  })

  describe('Maintenance Tasks', function() {
    it('should run maintenance tasks', async function() {
      const queueName = 'maintenance-queue'
      await boss.createQueue(queueName)
      
      // Create some jobs in different states
      const jobId1 = await boss.send(queueName, { test: 'maintenance1' })
      const jobId2 = await boss.send(queueName, { test: 'maintenance2' })
      
      const jobs = await boss.fetch(queueName, { batchSize: 2 })
      
      // Complete one job
      await boss.complete(jobs[0].id, { result: 'success' })
      
      // Fail another job
      await boss.fail(jobs[1].id, { error: 'test failure' })
      
      // Run maintenance
      const maintenanceResult = await boss.maintain()
      
      assert(typeof maintenanceResult === 'object')
      assert(typeof maintenanceResult.expiredJobs === 'number')
      assert(typeof maintenanceResult.archivedJobs === 'number')
      assert(typeof maintenanceResult.deletedJobs === 'number')
    })
  })

  describe('State Consistency', function() {
    it('should maintain consistent job states', async function() {
      const queueName = 'consistency-queue'
      await boss.createQueue(queueName)
      
      // Send multiple jobs
      const jobIds = []
      for (let i = 0; i < 3; i++) {
        const jobId = await boss.send(queueName, { index: i })
        jobIds.push(jobId)
      }
      
      // Fetch and process jobs
      const jobs = await boss.fetch(queueName, { batchSize: 3 })
      assert.strictEqual(jobs.length, 3)
      
      // Complete some jobs
      await boss.complete([jobIds[0], jobIds[1]])
      
      // Fail one job
      await boss.fail(jobIds[2], { error: 'test failure' })
      
      // Count states
      const states = await boss.countStates()
      assert.strictEqual(states.completed, 2)
      assert.strictEqual(states.failed, 1)
      assert.strictEqual(states.queues[queueName].completed, 2)
      assert.strictEqual(states.queues[queueName].failed, 1)
    })
  })
})
