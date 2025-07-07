const assert = require('assert')
const SwiftQueueMySQL = require('../src/index')

describe('Scheduling Tests', function() {
  this.timeout(15000)
  
  let boss

  before(async function() {
    boss = new SwiftQueueMySQL({
      host: process.env.MYSQL_HOST || 'localhost',
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || 'password',
      database: process.env.MYSQL_DATABASE || 'swift_queue_test'
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

  describe('Cron Scheduling', function() {
    it('should schedule a job', async function() {
      const queueName = 'scheduled-queue'
      await boss.createQueue(queueName)
      
      await boss.schedule(queueName, '* * * * *', { message: 'scheduled job' })
      
      const schedules = await boss.getSchedules()
      assert.strictEqual(schedules.length, 1)
      assert.strictEqual(schedules[0].name, queueName)
      assert.strictEqual(schedules[0].cron, '* * * * *')
    })

    it('should unschedule a job', async function() {
      const queueName = 'scheduled-queue'
      await boss.createQueue(queueName)
      
      await boss.schedule(queueName, '* * * * *', { message: 'scheduled job' })
      await boss.unschedule(queueName)
      
      const schedules = await boss.getSchedules()
      assert.strictEqual(schedules.length, 0)
    })

    it('should handle timezone in scheduling', async function() {
      const queueName = 'scheduled-queue'
      await boss.createQueue(queueName)
      
      await boss.schedule(queueName, '0 12 * * *', { message: 'noon job' }, { 
        tz: 'America/New_York' 
      })
      
      const schedules = await boss.getSchedules()
      assert.strictEqual(schedules.length, 1)
      assert.strictEqual(schedules[0].timezone, 'America/New_York')
    })
  })

  describe('Job Timing', function() {
    it('should handle startAfter option', async function() {
      const queueName = 'timing-queue'
      await boss.createQueue(queueName)
      
      const startTime = new Date(Date.now() + 500)
      await boss.send(queueName, { message: 'delayed' }, { startAfter: startTime })
      
      // Should not be available immediately
      const immediateJobs = await boss.fetch(queueName)
      assert.strictEqual(immediateJobs.length, 0)
      
      // Wait for the delay
      await new Promise(resolve => setTimeout(resolve, 600))
      
      const delayedJobs = await boss.fetch(queueName)
      assert.strictEqual(delayedJobs.length, 1)
    })

    it('should handle sendAfter method', async function() {
      const queueName = 'timing-queue'
      await boss.createQueue(queueName)
      
      await boss.sendAfter(queueName, { message: 'delayed' }, {}, 1) // 1 second
      
      // Should not be available immediately
      const immediateJobs = await boss.fetch(queueName)
      assert.strictEqual(immediateJobs.length, 0)
      
      // Wait for the delay
      await new Promise(resolve => setTimeout(resolve, 1100))
      
      const delayedJobs = await boss.fetch(queueName)
      assert.strictEqual(delayedJobs.length, 1)
    })
  })

  describe('Singleton Jobs', function() {
    it('should prevent duplicate singleton jobs', async function() {
      const queueName = 'singleton-queue'
      await boss.createQueue(queueName)
      
      const singletonKey = 'unique-task'
      
      // Send multiple jobs with same singleton key
      await boss.send(queueName, { task: 1 }, { singletonKey })
      await boss.send(queueName, { task: 2 }, { singletonKey })
      await boss.send(queueName, { task: 3 }, { singletonKey })
      
      const jobs = await boss.fetch(queueName, { batchSize: 10 })
      
      // Should only have one job due to singleton constraint
      assert.strictEqual(jobs.length, 1)
    })

    it('should handle singleton with time window', async function() {
      const queueName = 'singleton-queue'
      await boss.createQueue(queueName)
      
      const singletonKey = 'time-limited-task'
      
      // Send job with singleton time window
      await boss.send(queueName, { task: 1 }, { 
        singletonKey, 
        singletonSeconds: 2 
      })
      
      // This should be blocked by singleton constraint
      await boss.send(queueName, { task: 2 }, { 
        singletonKey, 
        singletonSeconds: 2 
      })
      
      const jobs = await boss.fetch(queueName, { batchSize: 10 })
      assert.strictEqual(jobs.length, 1)
    })
  })

  describe('Job Expiration', function() {
    it('should handle job expiration', async function() {
      const queueName = 'expiration-queue'
      await boss.createQueue(queueName)
      
      // Send job with short expiration
      await boss.send(queueName, { message: 'expires soon' }, { 
        expireInSeconds: 1 
      })
      
      const jobs = await boss.fetch(queueName)
      assert.strictEqual(jobs.length, 1)
      
      // Wait for expiration
      await new Promise(resolve => setTimeout(resolve, 1500))
      
      // Run expiration maintenance
      await boss.expire()
      
      const job = await boss.getJobById(queueName, jobs[0].id)
      assert.strictEqual(job.state, 'failed')
    })
  })

  describe('Debouncing and Throttling', function() {
    it('should debounce jobs', async function() {
      const queueName = 'debounce-queue'
      await boss.createQueue(queueName)
      
      const debounceKey = 'debounced-task'
      
      // Send multiple debounced jobs rapidly
      await boss.sendDebounced(queueName, { task: 1 }, { singletonSeconds: 2 }, debounceKey)
      await boss.sendDebounced(queueName, { task: 2 }, { singletonSeconds: 2 }, debounceKey)
      await boss.sendDebounced(queueName, { task: 3 }, { singletonSeconds: 2 }, debounceKey)
      
      const jobs = await boss.fetch(queueName, { batchSize: 10 })
      
      // Should only have one job due to debouncing
      assert.strictEqual(jobs.length, 1)
    })

    it('should throttle jobs', async function() {
      const queueName = 'throttle-queue'
      await boss.createQueue(queueName)
      
      const throttleKey = 'throttled-task'
      
      // Send multiple throttled jobs
      await boss.sendThrottled(queueName, { task: 1 }, { singletonSeconds: 2 }, throttleKey)
      await boss.sendThrottled(queueName, { task: 2 }, { singletonSeconds: 2 }, throttleKey)
      await boss.sendThrottled(queueName, { task: 3 }, { singletonSeconds: 2 }, throttleKey)
      
      const jobs = await boss.fetch(queueName, { batchSize: 10 })
      
      // Should only have one job due to throttling
      assert.strictEqual(jobs.length, 1)
    })
  })
})
