const EventEmitter = require('node:events')
const cronParser = require('cron-parser')
const plans = require('./plans')
const Attorney = require('./attorney')
const { delay } = require('./tools')

const EVENTS = {
  error: 'error',
  clockSkew: 'clock-skew',
  schedule: 'schedule'
}

class Timekeeper extends EventEmitter {
  constructor (db, config) {
    super()

    this.db = db
    this.config = config
    this.manager = config.manager
    this.skewMonitorIntervalMs = config.clockMonitorIntervalSeconds * 1000
    this.cronMonitorIntervalMs = config.cronMonitorIntervalSeconds * 1000
    this.clockSkew = 0

    this.events = EVENTS

    this.getTimeCommand = plans.getTime(config.schema)
    this.getQueueCommand = plans.getQueueByName(config.schema)
    this.getSchedulesCommand = plans.getSchedules(config.schema)
    this.scheduleCommand = plans.schedule(config.schema)
    this.unscheduleCommand = plans.unschedule(config.schema)
    this.trySetCronTimeCommand = plans.trySetCronTime(config.schema)

    this.functions = [
      this.schedule,
      this.unschedule,
      this.getSchedules,
      this.cacheClockSkew
    ]

    this.cronTimerActive = false
    this.clockSkewTimerActive = false
    this.stopped = false
  }

  async start () {
    this.stopped = false
    
    // setting the archive config too low breaks the cron 60s debounce interval so don't even try
    if (this.config.archiveInterval < 60) {
      return
    }

    this.startClockSkewMonitoring()
    this.startCronMonitoring()
  }

  async stop () {
    this.stopped = true
    
    if (this.clockSkewTimer) {
      clearInterval(this.clockSkewTimer)
    }
    
    if (this.cronTimer) {
      clearInterval(this.cronTimer)
    }
  }

  startClockSkewMonitoring () {
    this.clockSkewTimer = setInterval(() => {
      this.cacheClockSkew()
    }, this.skewMonitorIntervalMs)
  }

  startCronMonitoring () {
    this.cronTimer = setInterval(() => {
      this.onCron()
    }, this.cronMonitorIntervalMs)
  }

  async onCron () {
    try {
      if (this.cronTimerActive) {
        return
      }

      this.cronTimerActive = true

      if (this.config.__test__force_cron_monitoring_error) {
        throw new Error(this.config.__test__force_cron_monitoring_error)
      }

      if (this.stopped) {
        return
      }

      const { rows } = await this.db.executeSql(this.trySetCronTimeCommand, [this.config.archiveInterval])

      if (rows.length === 1 && !this.stopped) {
        await this.cron()
      }
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this.cronTimerActive = false
    }
  }

  async cron () {
    const schedules = await this.getSchedules()

    for (const schedule of schedules) {
      try {
        const { name, cron, timezone, data, options } = schedule
        
        if (this.stopped) {
          break
        }

        const interval = cronParser.parseExpression(cron, { tz: timezone })
        
        const prevTime = interval.prev()
        const nextTime = interval.next()

        const now = Date.now() + this.clockSkew

        // Check if we should fire this cron job
        if (prevTime.toDate().getTime() <= now && now < nextTime.toDate().getTime()) {
          await this.manager.send(name, data, options)
          this.emit(this.events.schedule, { name, cron, timezone })
        }
      } catch (err) {
        this.emit(this.events.error, err)
      }
    }
  }

  async cacheClockSkew () {
    let skew = 0

    try {
      if (this.config.__test__force_clock_monitoring_error) {
        throw new Error(this.config.__test__force_clock_monitoring_error)
      }

      const { rows } = await this.db.executeSql(this.getTimeCommand)

      const local = Date.now()

      const dbTime = parseFloat(rows[0].time)

      skew = dbTime - local

      const skewSeconds = Math.abs(skew) / 1000

      if (skewSeconds >= 60 || this.config.__test__force_clock_skew_warning) {
        Attorney.warnClockSkew(`Instance clock is ${skewSeconds}s ${skew > 0 ? 'slower' : 'faster'} than database.`)
      }
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this.clockSkew = skew
    }
  }

  async getSchedules () {
    const { rows } = await this.db.executeSql(this.getSchedulesCommand)
    return rows
  }

  async schedule (name, cron, data, options = {}) {
    const { tz = 'UTC' } = options

    cronParser.parseExpression(cron, { tz })

    Attorney.checkSendArgs([name, data, options], this.config)

    const values = [name, cron, tz, JSON.stringify(data), JSON.stringify(options)]

    try {
      await this.db.executeSql(this.scheduleCommand, values)
    } catch (err) {
      if (err.code === 'ER_NO_REFERENCED_ROW_2') {
        err.message = `Queue ${name} not found`
      }

      throw err
    }
  }

  async unschedule (name) {
    await this.db.executeSql(this.unscheduleCommand, [name])
  }
}

module.exports = Timekeeper
