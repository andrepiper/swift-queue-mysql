const EventEmitter = require('node:events')
const plans = require('./plans')
const Attorney = require('./attorney')
const Contractor = require('./contractor')
const Manager = require('./manager')
const Timekeeper = require('./timekeeper')
const Boss = require('./boss')
const Db = require('./db')
const { delay } = require('./tools')

const events = {
  error: 'error',
  stopped: 'stopped'
}

class SwiftQueueMySQL extends EventEmitter {
  #stoppingOn
  #stopped
  #starting
  #started
  #config
  #db
  #boss
  #contractor
  #manager
  #timekeeper

  static getConstructionPlans (schema) {
    return Contractor.constructionPlans(schema)
  }

  static getMigrationPlans (schema, version) {
    return Contractor.migrationPlans(schema, version)
  }

  static getRollbackPlans (schema, version) {
    return Contractor.rollbackPlans(schema, version)
  }

  static states = plans.JOB_STATES
  static policies = plans.QUEUE_POLICIES

  constructor (value) {
    super()

    this.#stoppingOn = null
    this.#stopped = true

    const config = Attorney.getConfig(value)
    this.#config = config

    this.#db = new Db(config)
    this.#contractor = new Contractor(this.#db, config)
    this.#manager = new Manager(this.#db, config)
    this.#boss = new Boss(this.#db, { ...config, manager: this.#manager })
    this.#timekeeper = new Timekeeper(this.#db, { ...config, manager: this.#manager })

    this.#manager.config.manager = this.#manager
    this.#manager.config.boss = this.#boss
    this.#manager.config.timekeeper = this.#timekeeper

    // Proxy methods from subsystems
    this.#manager.functions.forEach(func => {
      this[func.name] = func.bind(this.#manager)
    })

    this.#boss.functions.forEach(func => {
      this[func.name] = func.bind(this.#boss)
    })

    this.#timekeeper.functions.forEach(func => {
      this[func.name] = func.bind(this.#timekeeper)
    })

    // Error handling
    this.#db.on('error', err => this.emit(events.error, err))
    this.#manager.on('error', err => this.emit(events.error, err))
    this.#boss.on('error', err => this.emit(events.error, err))
    this.#timekeeper.on('error', err => this.emit(events.error, err))

    // Forward events
    this.#manager.on('wip', data => this.emit('wip', data))
    this.#manager.on('job', data => this.emit('job', data))
    this.#boss.on('maintenance', data => this.emit('maintenance', data))
    this.#boss.on('monitor-states', data => this.emit('monitor-states', data))
    this.#timekeeper.on('schedule', data => this.emit('schedule', data))
    this.#timekeeper.on('clock-skew', data => this.emit('clock-skew', data))
  }

  async start () {
    if (this.#started) {
      return
    }

    if (this.#starting) {
      return
    }

    this.#starting = true

    try {
      await this.#db.open()
      await this.#contractor.ensureCurrentVersion()
      
      await this.#boss.start()
      await this.#timekeeper.start()
      
      this.#started = true
      this.#stopped = false
    } catch (err) {
      this.emit(events.error, err)
      throw err
    } finally {
      this.#starting = false
    }
  }

  async stop ({ timeout = 30000, wait = true } = {}) {
    if (this.#stopped) {
      return
    }

    this.#stoppingOn = Date.now()

    return new Promise((resolve, reject) => {
      const shutdown = async () => {
        try {
          await this.#boss.stop()
          await this.#timekeeper.stop()
          this.#manager.stop()
          await this.#db.close()
          
          this.#stopped = true
          this.#started = false
          this.emit(events.stopped)
          
          resolve()
        } catch (err) {
          reject(err)
        }
      }

      if (!wait) {
        resolve()
      }

      setImmediate(async () => {
        try {
          if (this.#config.__test__throw_stop_monitor) {
            throw new Error(this.#config.__test__throw_stop_monitor)
          }

          const isWip = () => this.#manager.getWipData({ includeInternal: false }).length > 0

          while ((Date.now() - this.#stoppingOn) < timeout && isWip()) {
            await delay(500)
          }

          await shutdown()
        } catch (err) {
          reject(err)
          this.emit(events.error, err)
        }
      })
    })
  }

  getDb () {
    if (this.#db) {
      return this.#db
    }

    if (this.#config.db) {
      return this.#config.db
    }

    const db = new Db(this.#config)
    db.isOurs = true
    return db
  }

  getConfig () {
    return { ...this.#config }
  }

  get stopped () {
    return this.#stopped
  }

  get started () {
    return this.#started
  }

  // Convenience methods
  async createJob (name, data, options) {
    return this.send(name, data, options)
  }

  async getJob (name, id, options) {
    return this.getJobById(name, id, options)
  }

  async deleteJob (name, id) {
    return this.deleteJob(id)
  }

  async completeJob (id, data) {
    return this.complete(id, data)
  }

  async failJob (id, data) {
    return this.fail(id, data)
  }

  async cancelJob (id) {
    return this.cancel(id)
  }

  async resumeJob (id) {
    return this.resume(id)
  }

  async retryJob (id) {
    return this.retry(id)
  }

  // Queue management
  async createQueue (name, options) {
    return this.#manager.createQueue(name, options)
  }

  async deleteQueue (name) {
    return this.#manager.deleteQueue(name)
  }

  async getQueues () {
    return this.#manager.getQueues()
  }

  async getQueue (name) {
    return this.#manager.getQueue(name)
  }

  async getQueueSize (name) {
    return this.#manager.getQueueSize(name)
  }

  async purgeQueue (name) {
    return this.#manager.purgeQueue(name)
  }

  // Scheduling
  async schedule (name, cron, data, options) {
    return this.#timekeeper.schedule(name, cron, data, options)
  }

  async unschedule (name) {
    return this.#timekeeper.unschedule(name)
  }

  async getSchedules () {
    return this.#timekeeper.getSchedules()
  }

  // Pub/Sub
  async publish (event, data, options) {
    return this.#manager.publish(event, data, options)
  }

  async subscribe (event, name, options) {
    return this.#manager.subscribe(event, name, options)
  }

  async unsubscribe (event, name) {
    return this.#manager.unsubscribe(event, name)
  }

  // Monitoring
  async getWipData (options) {
    return this.#manager.getWipData(options)
  }

  async countStates () {
    return this.#boss.countStates()
  }

  // Maintenance
  async maintain () {
    return this.#boss.maintain()
  }

  async archive () {
    return this.#boss.archive()
  }

  async drop () {
    return this.#boss.drop()
  }

  async expire () {
    return this.#boss.expire()
  }

  async clearStorage () {
    return this.#manager.clearStorage()
  }
}

module.exports = SwiftQueueMySQL
