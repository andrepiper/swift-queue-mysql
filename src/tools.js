async function delay (ms) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, ms)
    
    // Add abort capability
    const promise = {
      then: (onFulfilled, onRejected) => {
        return new Promise((resolve, reject) => {
          timeout.unref ? timeout.unref() : null
          setTimeout(() => {
            if (onFulfilled) onFulfilled()
            resolve()
          }, ms)
        }).then(onFulfilled, onRejected)
      },
      catch: (onRejected) => {
        return promise.then(null, onRejected)
      },
      abort: () => {
        clearTimeout(timeout)
        reject(new Error('Delay aborted'))
      }
    }
    
    return promise
  })
}

async function resolveWithinSeconds (promise, seconds) {
  const timeoutPromise = new Promise((resolve, reject) => {
    setTimeout(() => {
      reject(new Error(`Operation timed out after ${seconds} seconds`))
    }, seconds * 1000)
  })

  return Promise.race([promise, timeoutPromise])
}

function parseInterval (interval) {
  if (typeof interval === 'number') {
    return interval
  }
  
  if (typeof interval === 'string') {
    const match = interval.match(/^(\d+)\s*(s|sec|second|seconds|m|min|minute|minutes|h|hour|hours|d|day|days)$/i)
    if (match) {
      const value = parseInt(match[1])
      const unit = match[2].toLowerCase()
      
      switch (unit) {
        case 's':
        case 'sec':
        case 'second':
        case 'seconds':
          return value
        case 'm':
        case 'min':
        case 'minute':
        case 'minutes':
          return value * 60
        case 'h':
        case 'hour':
        case 'hours':
          return value * 3600
        case 'd':
        case 'day':
        case 'days':
          return value * 86400
        default:
          throw new Error(`Unknown time unit: ${unit}`)
      }
    }
  }
  
  throw new Error(`Invalid interval format: ${interval}`)
}

function formatInterval (seconds) {
  if (seconds < 60) {
    return `${seconds}s`
  } else if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m`
  } else if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h`
  } else {
    return `${Math.floor(seconds / 86400)}d`
  }
}

function randomBetween (min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function createDebounce (func, wait) {
  let timeout
  return function executedFunction (...args) {
    const later = () => {
      clearTimeout(timeout)
      func(...args)
    }
    clearTimeout(timeout)
    timeout = setTimeout(later, wait)
  }
}

function createThrottle (func, limit) {
  let inThrottle
  return function executedFunction (...args) {
    if (!inThrottle) {
      func.apply(this, args)
      inThrottle = true
      setTimeout(() => inThrottle = false, limit)
    }
  }
}

module.exports = {
  delay,
  resolveWithinSeconds,
  parseInterval,
  formatInterval,
  randomBetween,
  createDebounce,
  createThrottle
}
