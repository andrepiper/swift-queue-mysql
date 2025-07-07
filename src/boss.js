const EventEmitter = require('node:events')
const plans = require('./plans')
const { delay } = require('./tools')

const events = {
  error: 'error',
  maintenance: 'maintenance',
  monitorStates: 'monitor-states'
}

class Boss extends EventEmitter {
  constructor (db, config) {
    super()

    this.db = db
    this.config = config
    this.manager = config.manager

    this.maintenanceIntervalSeconds = config.maintenanceIntervalSeconds
    this.monitorStateIntervalSeconds = config.monitorStateIntervalSeconds

    this.events = events

    this.failJobsByTimeoutCommand = plans.failJobsByTimeout(config.schema)
    this.archiveCommand = plans.archive(config.schema, config.archiveInterval || 86400, config.archiveFailedInterval || 86400)
    this.dropCommand = plans.drop(config.schema, config.deleteAfter || 86400)
    this.trySetMaintenanceTimeCommand = plans.trySetMaintenanceTime(config.schema)
    this.trySetMonitorTimeCommand = plans.trySetMonitorTime(config.schema)
    this.countStatesCommand = plans.countStates(config.schema)

    this.functions = [
      this.expire,
      this.archive,
      this.drop,
      this.countStates,
      this.maintain
    ]

    this.monitoring = false
    this.maintaining = false
    this.stopped = false
  }

  async start () {
    this.stopped = false
    this.startMaintenanceTimer()
    this.startMonitoringTimer()
  }

  async stop () {
    this.stopped = true
    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer)
    }
    if (this.monitoringTimer) {
      clearInterval(this.monitoringTimer)
    }
  }

  startMaintenanceTimer () {
    if (this.maintenanceIntervalSeconds > 0) {
      this.maintenanceTimer = setInterval(() => {
        this.onSupervise()
      }, this.maintenanceIntervalSeconds * 1000)
    }
  }

  startMonitoringTimer () {
    if (this.monitorStateIntervalSeconds > 0) {
      this.monitoringTimer = setInterval(() => {
        this.onMonitor()
      }, this.monitorStateIntervalSeconds * 1000)
    }
  }

  async onSupervise () {
    try {
      if (this.maintaining) {
        return
      }

      this.maintaining = true

      if (this.config.__test__delay_maintenance && !this.stopped) {
        this.__testDelayPromise = delay(this.config.__test__delay_maintenance)
        await this.__testDelayPromise
      }

      if (this.config.__test__throw_maint) {
        throw new Error(this.config.__test__throw_maint)
      }

      if (this.stopped) {
        return
      }

      const { rows } = await this.db.executeSql(this.trySetMaintenanceTimeCommand, [this.config.maintenanceIntervalSeconds])

      if (rows.length === 1 && !this.stopped) {
        const result = await this.maintain()
        this.emit(events.maintenance, result)
      }
    } catch (err) {
      this.emit(events.error, err)
    } finally {
      this.maintaining = false
    }
  }

  async onMonitor () {
    try {
      if (this.monitoring) {
        return
      }

      this.monitoring = true

      if (this.config.__test__delay_monitor) {
        await delay(this.config.__test__delay_monitor)
      }

      if (this.config.__test__throw_monitor) {
        throw new Error(this.config.__test__throw_monitor)
      }

      if (this.stopped) {
        return
      }

      const { rows } = await this.db.executeSql(this.trySetMonitorTimeCommand, [this.config.monitorStateIntervalSeconds])

      if (rows.length === 1 && !this.stopped) {
        const states = await this.countStates()
        this.emit(events.monitorStates, states)
      }
    } catch (err) {
      this.emit(events.error, err)
    } finally {
      this.monitoring = false
    }
  }

  async maintain () {
    const results = {
      expiredJobs: 0,
      archivedJobs: 0,
      deletedJobs: 0
    }

    try {
      // Expire jobs
      const expiredResult = await this.expire()
      results.expiredJobs = expiredResult.rowCount || 0

      // Archive jobs
      const archivedResult = await this.archive()
      results.archivedJobs = archivedResult.rowCount || 0

      // Delete old archived jobs
      const deletedResult = await this.drop()
      results.deletedJobs = deletedResult.rowCount || 0
    } catch (err) {
      this.emit(events.error, err)
    }

    return results
  }

  async countStates () {
    const stateCountDefault = { ...plans.JOB_STATES }

    for (const key of Object.keys(stateCountDefault)) {
      stateCountDefault[key] = 0
    }

    const counts = await this.db.executeSql(this.countStatesCommand)

    const states = counts.rows.reduce((acc, item) => {
      if (item.name) {
        acc.queues[item.name] = acc.queues[item.name] || { ...stateCountDefault }
      }

      const queue = item.name ? acc.queues[item.name] : acc
      const state = item.state || 'all'

      // Parse count as number
      queue[state] = parseInt(item.size) || 0

      return acc
    }, { ...stateCountDefault, queues: {} })

    return states
  }

  async expire () {
    return await this.db.executeSql(this.failJobsByTimeoutCommand)
  }

  async archive () {
    return await this.db.executeSql(this.archiveCommand)
  }

  async drop () {
    return await this.db.executeSql(this.dropCommand)
  }
}

module.exports = Boss
