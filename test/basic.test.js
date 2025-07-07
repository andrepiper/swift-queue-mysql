const assert = require('assert')
const MysqlBoss = require('../src/index')

describe('MysqlBoss Basic Tests', function() {
  this.timeout(10000)
  
  let boss

  before(async function() {
    boss = new MysqlBoss({
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

  describe('Queue Operations', function() {
    it('should create a queue', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const queue = await boss.getQueue(queueName)
      assert.strictEqual(queue.name, queueName)
    })

    it('should delete a queue', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      await boss.deleteQueue(queueName)
      
      const queue = await boss.getQueue(queueName)
      assert.strictEqual(queue, null)
    })

    it('should get queue size', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const initialSize = await boss.getQueueSize(queueName)
      assert.strictEqual(initialSize, 0)
      
      await boss.send(queueName, { test: 'data' })
      
      const sizeAfterSend = await boss.getQueueSize(queueName)
      assert.strictEqual(sizeAfterSend, 1)
    })
  })

  describe('Job Operations', function() {
    it('should send and receive a job', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const testData = { message: 'test job' }
      const jobId = await boss.send(queueName, testData)
      
      assert(jobId)
      assert(typeof jobId === 'string')
      
      const jobs = await boss.fetch(queueName)
      assert.strictEqual(jobs.length, 1)
      assert.strictEqual(jobs[0].id, jobId)
      assert.deepStrictEqual(jobs[0].data, testData)
    })

    it('should complete a job', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'data' })
      const jobs = await boss.fetch(queueName)
      
      await boss.complete(jobs[0].id, { result: 'success' })
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'completed')
    })

    it('should fail a job', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'data' })
      const jobs = await boss.fetch(queueName)
      
      await boss.fail(jobs[0].id, { error: 'test error' })
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'failed')
    })

    it('should cancel a job', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'data' })
      await boss.cancel(jobId)
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'cancelled')
    })

    it('should retry a job', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const jobId = await boss.send(queueName, { test: 'data' })
      await boss.retry(jobId)
      
      const job = await boss.getJobById(queueName, jobId)
      assert.strictEqual(job.state, 'retry')
      assert.strictEqual(job.retryCount, 1)
    })
  })

  describe('Worker Operations', function() {
    it('should process jobs with work()', function(done) {
      const queueName = 'test-queue'
      let jobsProcessed = 0
      
      boss.createQueue(queueName).then(async () => {
        await boss.work(queueName, async (jobs) => {
          jobsProcessed += jobs.length
          if (jobsProcessed === 3) {
            done()
          }
        })
        
        // Send test jobs
        await boss.send(queueName, { test: 1 })
        await boss.send(queueName, { test: 2 })
        await boss.send(queueName, { test: 3 })
      }).catch(done)
    })

    it('should handle job batches', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      // Send multiple jobs
      await boss.send(queueName, { test: 1 })
      await boss.send(queueName, { test: 2 })
      await boss.send(queueName, { test: 3 })
      
      const jobs = await boss.fetch(queueName, { batchSize: 2 })
      assert.strictEqual(jobs.length, 2)
    })
  })

  describe('Priority and Scheduling', function() {
    it('should handle job priority', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      // Send jobs with different priorities
      await boss.send(queueName, { priority: 'low' }, { priority: 1 })
      await boss.send(queueName, { priority: 'high' }, { priority: 10 })
      await boss.send(queueName, { priority: 'medium' }, { priority: 5 })
      
      const jobs = await boss.fetch(queueName, { batchSize: 3 })
      
      // Should be ordered by priority (highest first)
      assert.strictEqual(jobs[0].priority, 10)
      assert.strictEqual(jobs[1].priority, 5)
      assert.strictEqual(jobs[2].priority, 1)
    })

    it('should handle delayed jobs', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      const futureTime = new Date(Date.now() + 1000)
      await boss.send(queueName, { test: 'delayed' }, { startAfter: futureTime })
      
      // Should not be available immediately
      const immediateJobs = await boss.fetch(queueName)
      assert.strictEqual(immediateJobs.length, 0)
      
      // Wait for the delay
      await new Promise(resolve => setTimeout(resolve, 1100))
      
      const delayedJobs = await boss.fetch(queueName)
      assert.strictEqual(delayedJobs.length, 1)
    })
  })

  describe('State Management', function() {
    it('should count job states', async function() {
      const queueName = 'test-queue'
      await boss.createQueue(queueName)
      
      await boss.send(queueName, { test: 1 })
      await boss.send(queueName, { test: 2 })
      
      const states = await boss.countStates()
      assert.strictEqual(states.created, 2)
      assert.strictEqual(states.queues[queueName].created, 2)
    })
  })

  describe('Pub/Sub', function() {
    it('should publish and subscribe to events', async function() {
      const queue1 = 'subscriber-1'
      const queue2 = 'subscriber-2'
      
      await boss.createQueue(queue1)
      await boss.createQueue(queue2)
      
      await boss.subscribe('test-event', queue1)
      await boss.subscribe('test-event', queue2)
      
      await boss.publish('test-event', { message: 'hello' })
      
      const jobs1 = await boss.fetch(queue1)
      const jobs2 = await boss.fetch(queue2)
      
      assert.strictEqual(jobs1.length, 1)
      assert.strictEqual(jobs2.length, 1)
      assert.strictEqual(jobs1[0].data.message, 'hello')
      assert.strictEqual(jobs2[0].data.message, 'hello')
    })
  })
})
