const EventEmitter = require('node:events')
const { randomUUID } = require('node:crypto')
const { serializeError } = require('serialize-error')
const { v4: uuidv4 } = require('uuid')
const plans = require('./plans')
const Attorney = require('./attorney')
const Worker = require('./worker')
const { delay, resolveWithinSeconds } = require('./tools')

const events = {
  error: 'error',
  job: 'job',
  work: 'work',
  stop: 'stop',
  wip: 'wip',
  insert: 'insert',
  monitor: 'monitor'
}

class Manager extends EventEmitter {
  constructor (db, config) {
    super()

    this.config = config
    this.db = db

    this.events = events
    this.wipTs = Date.now()
    this.workers = new Map()

    this.nextJobCommand = plans.fetchNextJob(config.schema)
    this.insertJobCommand = plans.insertJob(config.schema)
    this.insertJobsCommand = plans.insertJobs(config.schema)
    this.completeJobsCommand = plans.completeJobs(config.schema)
    this.cancelJobsCommand = plans.cancelJobs(config.schema)
    this.resumeJobsCommand = plans.resumeJobs(config.schema)
    this.deleteJobsCommand = plans.deleteJobs(config.schema)
    this.retryJobsCommand = plans.retryJobs(config.schema)
    this.failJobsByIdCommand = plans.failJobsById(config.schema)
    this.getJobByIdCommand = plans.getJobById(config.schema)
    this.getArchivedJobByIdCommand = plans.getArchivedJobById(config.schema)
    this.getQueuesCommand = plans.getQueues(config.schema)
    this.getQueueCommand = plans.getQueueByName(config.schema)
    this.getQueueSizeCommand = plans.getQueueSize(config.schema)
    this.purgeQueueCommand = plans.purgeQueue(config.schema)
    this.deleteQueueCommand = plans.deleteQueue(config.schema)
    this.updateQueueCommand = plans.updateQueue(config.schema)
    this.createQueueCommand = plans.createQueue(config.schema)
    this.clearStorageCommand = plans.clearStorage(config.schema)
    this.getQueuesForEventCommand = plans.getQueuesForEvent(config.schema)
    this.subscribeCommand = plans.subscribe(config.schema)
    this.unsubscribeCommand = plans.unsubscribe(config.schema)

    // exported api to index
    this.functions = [
      this.complete,
      this.cancel,
      this.resume,
      this.retry,
      this.deleteJob,
      this.fail,
      this.fetch,
      this.work,
      this.offWork,
      this.notifyWorker,
      this.publish,
      this.subscribe,
      this.unsubscribe,
      this.insert,
      this.send,
      this.sendDebounced,
      this.sendThrottled,
      this.sendAfter,
      this.createQueue,
      this.updateQueue,
      this.deleteQueue,
      this.purgeQueue,
      this.getQueueSize,
      this.getQueue,
      this.getQueues,
      this.clearStorage,
      this.getJobById
    ]
  }

  async send (name, data, options = {}) {
    const { sending } = Attorney.checkSendArgs([name, data, options], this.config)
    
    const job = {
      id: sending.options.id || uuidv4(),
      name: sending.name,
      data: sending.data,
      priority: sending.options.priority || 0,
      retryLimit: sending.options.retryLimit || this.config.retryLimit || 2,
      retryDelay: sending.options.retryDelay || this.config.retryDelay || 0,
      retryBackoff: sending.options.retryBackoff || this.config.retryBackoff || false,
      startAfter: sending.options.startAfter || new Date(),
      expireInSeconds: sending.options.expireInSeconds || this.config.expireInSeconds || 900,
      keepUntil: sending.options.keepUntil || new Date(Date.now() + 14 * 24 * 60 * 60 * 1000),
      singletonKey: sending.options.singletonKey || null,
      singletonOn: sending.options.singletonSeconds ? new Date(Math.floor(Date.now() / (sending.options.singletonSeconds * 1000)) * sending.options.singletonSeconds * 1000) : null,
      deadLetter: sending.options.deadLetter || null,
      policy: sending.options.policy || null
    }

    const values = [
      job.id,
      job.name,
      job.priority,
      JSON.stringify(job.data),
      job.retryLimit,
      job.retryDelay,
      job.retryBackoff,
      job.startAfter,
      job.singletonKey,
      job.singletonOn,
      job.expireInSeconds,
      job.keepUntil,
      job.deadLetter,
      job.policy
    ]

    await this.db.executeSql(this.insertJobCommand, values)
    
    this.emit(events.insert, job)
    
    return job.id
  }

  async sendAfter (name, data, options, after) {
    const startAfter = after instanceof Date ? after : new Date(Date.now() + after * 1000)
    return this.send(name, data, { ...options, startAfter })
  }

  async sendDebounced (name, data, options, key) {
    const singletonKey = key || `debounce_${name}`
    const singletonSeconds = options.singletonSeconds || 60
    return this.send(name, data, { ...options, singletonKey, singletonSeconds })
  }

  async sendThrottled (name, data, options, key) {
    const singletonKey = key || `throttle_${name}`
    const singletonSeconds = options.singletonSeconds || 60
    return this.send(name, data, { ...options, singletonKey, singletonSeconds })
  }

  async insert (jobs) {
    if (!Array.isArray(jobs)) {
      throw new Error('jobs must be an array')
    }

    const values = jobs.map(job => [
      job.name,
      job.priority || 0,
      JSON.stringify(job.data || {}),
      'created',
      job.retryLimit || 2,
      job.retryDelay || 0,
      job.retryBackoff || false,
      job.startAfter || new Date(),
      job.singletonKey || null,
      job.singletonOn || null,
      job.expireInSeconds || 900,
      job.keepUntil || new Date(Date.now() + 14 * 24 * 60 * 60 * 1000),
      job.deadLetter || null,
      job.policy || null
    ])

    await this.db.executeSql(this.insertJobsCommand, [values])
  }

  async fetch (name, options = {}) {
    Attorney.checkFetchArgs(name, options)
    const db = options.db || this.db
    const nextJobSql = this.nextJobCommand({ ...options })

    let result

    try {
      result = await db.executeSql(nextJobSql, [name, options.batchSize || 1])
    } catch (err) {
      // Handle MySQL-specific errors
      if (err.code === 'ER_LOCK_WAIT_TIMEOUT') {
        return []
      }
      throw err
    }

    if (result?.rows?.length) {
      // Update jobs to active state
      const ids = result.rows.map(job => job.id)
      const updateSql = `
        UPDATE \`${this.config.schema}\`.\`job\` 
        SET \`state\` = 'active', \`started_on\` = NOW()
        WHERE \`id\` IN (${ids.map(() => '?').join(',')})
      `
      await db.executeSql(updateSql, ids)
    }

    return result?.rows || []
  }

  async complete (id, data) {
    const ids = Array.isArray(id) ? id : [id]
    const output = data ? JSON.stringify(data) : null
    const sql = `
      UPDATE \`${this.config.schema}\`.\`job\` 
      SET \`state\` = 'completed', 
          \`completed_on\` = NOW(),
          \`output\` = ?
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, [output, ...ids])
  }

  async cancel (id) {
    const ids = Array.isArray(id) ? id : [id]
    const sql = `
      UPDATE \`${this.config.schema}\`.\`job\` 
      SET \`state\` = 'cancelled', 
          \`completed_on\` = NOW()
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, ids)
  }

  async resume (id) {
    const ids = Array.isArray(id) ? id : [id]
    const sql = `
      UPDATE \`${this.config.schema}\`.\`job\` 
      SET \`state\` = 'created', 
          \`completed_on\` = NULL,
          \`started_on\` = NULL
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, ids)
  }

  async retry (id) {
    const ids = Array.isArray(id) ? id : [id]
    const sql = `
      UPDATE \`${this.config.schema}\`.\`job\` 
      SET \`state\` = 'retry', 
          \`retry_count\` = \`retry_count\` + 1,
          \`start_after\` = CASE 
            WHEN \`retry_backoff\` = 0 THEN DATE_ADD(NOW(), INTERVAL \`retry_delay\` SECOND)
            ELSE DATE_ADD(NOW(), INTERVAL (\`retry_delay\` * POW(2, \`retry_count\`)) SECOND)
          END,
          \`completed_on\` = NULL
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, ids)
  }

  async deleteJob (id) {
    const ids = Array.isArray(id) ? id : [id]
    const sql = `
      DELETE FROM \`${this.config.schema}\`.\`job\` 
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, ids)
  }

  async fail (id, data) {
    const ids = Array.isArray(id) ? id : [id]
    const output = data ? JSON.stringify(data) : JSON.stringify({ message: 'Job failed' })
    const sql = `
      UPDATE \`${this.config.schema}\`.\`job\` 
      SET \`state\` = 'failed', 
          \`completed_on\` = NOW(),
          \`output\` = ?
      WHERE \`id\` IN (${ids.map(() => '?').join(',')})
    `
    await this.db.executeSql(sql, [output, ...ids])
  }

  async work (name, ...args) {
    const { options, callback } = Attorney.checkWorkArgs(name, args, this.config)
    
    const id = randomUUID({ disableEntropyCache: true })
    const pollingInterval = options.pollingInterval || this.config.pollingInterval || 2000
    const batchSize = options.batchSize || 1
    const includeMetadata = options.includeMetadata || false
    const priority = options.priority || 0

    const fetch = () => this.fetch(name, { batchSize, includeMetadata, priority })

    const onFetch = async (jobs) => {
      if (!jobs.length) {
        return
      }

      this.emitWip(name)

      const maxExpiration = jobs.reduce((acc, i) => Math.max(acc, i.expireInSeconds), 0)
      const jobIds = jobs.map(job => job.id)

      try {
        const result = await resolveWithinSeconds(callback(jobs), maxExpiration)
        
        if (result && typeof result === 'object' && result.failed) {
          await this.fail(jobIds, result.failed)
        } else {
          await this.complete(jobIds, result)
        }
      } catch (err) {
        const serializedError = serializeError(err)
        await this.fail(jobIds, serializedError)
        this.emit(events.error, err)
      }
    }

    const onError = (err) => {
      this.emit(events.error, err)
    }

    const worker = new Worker({ 
      id, 
      name, 
      options, 
      interval: pollingInterval, 
      fetch, 
      onFetch, 
      onError 
    })

    this.workers.set(id, worker)
    worker.start()

    this.emit(events.work, { id, name, options })

    return id
  }

  async offWork (id) {
    const worker = this.workers.get(id)
    if (worker) {
      worker.stop()
      this.workers.delete(id)
      this.emit(events.stop, { id })
    }
  }

  async notifyWorker (id) {
    const worker = this.workers.get(id)
    if (worker) {
      worker.notify()
    }
  }

  async publish (event, data, options = {}) {
    const queues = await this.db.executeSql(this.getQueuesForEventCommand, [event])
    
    for (const queue of queues.rows) {
      await this.send(queue.name, data, options)
    }
  }

  async subscribe (event, name, options = {}) {
    await this.db.executeSql(this.subscribeCommand, [event, name])
  }

  async unsubscribe (event, name) {
    await this.db.executeSql(this.unsubscribeCommand, [event, name])
  }

  async createQueue (name, options = {}) {
    Attorney.assertQueueName(name)
    Attorney.assertQueueOptions(options)
    
    const queueOptions = JSON.stringify(options)
    await this.db.executeSql(this.createQueueCommand, [name, queueOptions])
  }

  async updateQueue (name, options = {}) {
    Attorney.assertQueueName(name)
    Attorney.assertQueueOptions(options)
    
    const values = [
      options.policy || null,
      options.retryLimit || null,
      options.retryDelay || null,
      options.retryBackoff || null,
      options.expireInSeconds || null,
      options.retentionMinutes || null,
      options.deadLetter || null,
      name
    ]
    
    await this.db.executeSql(this.updateQueueCommand, values)
  }

  async deleteQueue (name) {
    Attorney.assertQueueName(name)
    await this.db.executeSql(this.deleteQueueCommand, [name])
  }

  async getQueues () {
    const result = await this.db.executeSql(this.getQueuesCommand)
    return result.rows
  }

  async getQueue (name) {
    Attorney.assertQueueName(name)
    const result = await this.db.executeSql(this.getQueueCommand, [name])
    return result.rows[0] || null
  }

  async getQueueSize (name) {
    Attorney.assertQueueName(name)
    const result = await this.db.executeSql(this.getQueueSizeCommand, [name])
    return result.rows[0]?.size || 0
  }

  async purgeQueue (name) {
    Attorney.assertQueueName(name)
    await this.db.executeSql(this.purgeQueueCommand, [name])
  }

  async clearStorage () {
    await this.db.executeSql(this.clearStorageCommand)
  }

  async getJobById (name, id, options = {}) {
    Attorney.assertQueueName(name)

    const db = options.db || this.db

    const result1 = await db.executeSql(this.getJobByIdCommand, [name, id])

    if (result1?.rows?.length === 1) {
      return result1.rows[0]
    } else if (options.includeArchive) {
      const result2 = await db.executeSql(this.getArchivedJobByIdCommand, [name, id])
      return result2?.rows[0] || null
    } else {
      return null
    }
  }

  emitWip (name) {
    this.wipTs = Date.now()
    this.emit(events.wip, { name })
  }

  getWipData ({ includeInternal = true } = {}) {
    const data = []
    
    for (const [id, worker] of this.workers) {
      if (includeInternal || !worker.name.startsWith('__')) {
        data.push({
          id,
          name: worker.name,
          state: worker.state,
          options: worker.options,
          createdOn: worker.createdOn,
          lastJobStartedOn: worker.lastJobStartedOn,
          lastJobEndedOn: worker.lastJobEndedOn,
          lastError: worker.lastError,
          lastErrorOn: worker.lastErrorOn
        })
      }
    }
    
    return data
  }

  stop () {
    for (const [id, worker] of this.workers) {
      worker.stop()
    }
    this.workers.clear()
  }
}

module.exports = Manager
