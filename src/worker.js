const { delay } = require('./tools')

const WORKER_STATES = {
  created: 'created',
  active: 'active',
  stopping: 'stopping',
  stopped: 'stopped'
}

class Worker {
  constructor ({ id, name, options, interval, fetch, onFetch, onError }) {
    this.id = id
    this.name = name
    this.options = options
    this.interval = interval
    this.fetch = fetch
    this.onFetch = onFetch
    this.onError = onError
    this.state = WORKER_STATES.created
    this.stopping = false
    this.beenNotified = false
    this.loopDelayPromise = null
    this.createdOn = new Date()
    this.lastJobStartedOn = null
    this.lastJobEndedOn = null
    this.lastError = null
    this.lastErrorOn = null
  }

  start () {
    this.state = WORKER_STATES.active
    this.loop()
  }

  stop () {
    this.stopping = true
    this.state = WORKER_STATES.stopping

    if (this.loopDelayPromise) {
      this.loopDelayPromise.abort()
    }
  }

  notify () {
    this.beenNotified = true

    if (this.loopDelayPromise) {
      this.loopDelayPromise.abort()
    }
  }

  async loop () {
    while (!this.stopping) {
      const start = Date.now()
      
      try {
        const jobs = await this.fetch()
        
        if (jobs && jobs.length > 0) {
          this.lastJobStartedOn = new Date()
          this.beenNotified = false
          
          await this.onFetch(jobs)
          
          this.lastJobEndedOn = new Date()
        }
      } catch (err) {
        this.lastError = err
        this.lastErrorOn = new Date()
        this.onError(err)
      }

      if (this.stopping) {
        break
      }

      const duration = Date.now() - start
      
      if (!this.stopping && !this.beenNotified && (this.interval - duration) > 100) {
        this.loopDelayPromise = delay(this.interval - duration)
        
        try {
          await this.loopDelayPromise
        } catch (err) {
          // Delay was aborted, continue loop
        }
      }
    }

    this.state = WORKER_STATES.stopped
  }
}

module.exports = Worker
module.exports.WORKER_STATES = WORKER_STATES
