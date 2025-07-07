const assert = require('node:assert')
const { v4: uuidv4 } = require('uuid')

const POLICY = {
  ARCHIVE_COMPLETED_JOBS_SECONDS: 86400,
  ARCHIVE_FAILED_JOBS_SECONDS: 86400,
  ARCHIVE_SECONDS: 86400,
  DELETE_AFTER_SECONDS: 86400,
  CRON_DEBOUNCE_SECONDS: 60,
  MAINTENANCE_INTERVAL_SECONDS: 300,
  MONITOR_INTERVAL_SECONDS: 60,
  CLOCK_MONITOR_INTERVAL_SECONDS: 60,
  POLLING_INTERVAL_SECONDS: 2,
  EXPIRATION_SECONDS: 900,
  RETENTION_MINUTES: 20160,
  COMPLETION_JOB_TTL_SECONDS: 60,
  MAX_EXPIRATION_HOURS: 24,
  DATABASE_MANAGER_SEND_DEBOUNCE_SECONDS: 1,
  WORKER_FETCH_DEBOUNCE_SECONDS: 1,
  THROTTLE_WORK_DEBOUNCE_SECONDS: 1,
  SINGLETON_WORK_DEBOUNCE_SECONDS: 1,
  STATELY_WORK_DEBOUNCE_SECONDS: 1,
  BATCHED_WORK_DEBOUNCE_SECONDS: 1,
  THROTTLE_DEFAULTS: {
    SECONDS: 60,
    MAX: 1,
    INTERVAL: 'seconds'
  },
  CRON_WORKERS: 4,
  MANAGER_WORKERS: 1,
  STATELY_WORKERS: 1,
  SINGLETON_WORKERS: 1,
  THROTTLE_WORKERS: 1,
  BATCHED_WORKERS: 1,
  FETCH_POLL_INTERVAL_SECONDS: 2,
  FETCH_BATCH_SIZE: 1
}

const WARNINGS = {
  ARCHIVE_INTERVAL_TOO_SHORT: {
    message: 'Archive interval is set less than 60s. Cron processing is disabled.',
    code: 'ARCHIVE_INTERVAL_TOO_SHORT'
  },
  CLOCK_SKEW: {
    message: 'Instance clock is skewed',
    code: 'CLOCK_SKEW'
  }
}

module.exports = {
  getConfig,
  checkSendArgs,
  checkWorkArgs,
  checkFetchArgs,
  assertQueueName,
  assertQueueOptions,
  assertBatchSize,
  assertTimeout,
  assertInterval,
  assertRetentionConfig,
  assertArchiveConfig,
  assertMaintenanceConfig,
  assertMonitorConfig,
  assertPollingConfig,
  assertIncludeMetadata,
  assertWorkerOptions,
  assertCronExpression,
  assertScheduleOptions,
  assertSubscriptionOptions,
  assertQueuePolicy,
  warnClockSkew,
  POLICY,
  WARNINGS
}

function getConfig (value) {
  const config = {
    schema: 'mysql_boss',
    user: 'root',
    password: '',
    host: 'localhost',
    port: 3306,
    database: 'mysql_boss',
    ssl: false,
    connectionString: null,
    application_name: 'mysql-boss',
    max: 10,
    connectionTimeoutMillis: 30000,
    idleTimeoutMillis: 30000,
    query_timeout: 0,
    statement_timeout: 0,
    lock_timeout: 0,
    max_connections: 10,
    poolSize: 10,
    newJobCheckInterval: POLICY.POLLING_INTERVAL_SECONDS * 1000,
    newJobCheckIntervalSeconds: POLICY.POLLING_INTERVAL_SECONDS,
    pollingInterval: POLICY.POLLING_INTERVAL_SECONDS * 1000,
    pollingIntervalSeconds: POLICY.POLLING_INTERVAL_SECONDS,
    archiveCompletedJobsEvery: POLICY.ARCHIVE_COMPLETED_JOBS_SECONDS * 1000,
    archiveFailedJobsEvery: POLICY.ARCHIVE_FAILED_JOBS_SECONDS * 1000,
    archiveInterval: POLICY.ARCHIVE_SECONDS,
    deleteAfter: POLICY.DELETE_AFTER_SECONDS,
    deleteAfterSeconds: POLICY.DELETE_AFTER_SECONDS,
    deleteAfterDays: POLICY.DELETE_AFTER_SECONDS / 86400,
    deleteAfterHours: POLICY.DELETE_AFTER_SECONDS / 3600,
    deleteAfterMinutes: POLICY.DELETE_AFTER_SECONDS / 60,
    maintenanceIntervalSeconds: POLICY.MAINTENANCE_INTERVAL_SECONDS,
    maintenanceIntervalMinutes: POLICY.MAINTENANCE_INTERVAL_SECONDS / 60,
    monitorStateIntervalSeconds: POLICY.MONITOR_INTERVAL_SECONDS,
    monitorStateIntervalMinutes: POLICY.MONITOR_INTERVAL_SECONDS / 60,
    clockMonitorIntervalSeconds: POLICY.CLOCK_MONITOR_INTERVAL_SECONDS,
    clockMonitorIntervalMinutes: POLICY.CLOCK_MONITOR_INTERVAL_SECONDS / 60,
    ...value
  }

  if (typeof value === 'string') {
    config.connectionString = value
    // Parse MySQL connection string
    const match = value.match(/^mysql:\/\/(?:([^:]+):([^@]+)@)?([^:\/]+)(?::(\d+))?\/(.+)$/)
    if (match) {
      config.user = match[1] || config.user
      config.password = match[2] || config.password
      config.host = match[3] || config.host
      config.port = parseInt(match[4]) || config.port
      config.database = match[5] || config.database
    }
  }

  // Validate configuration
  assertArchiveConfig(config)
  assertMaintenanceConfig(config)
  assertMonitorConfig(config)
  assertPollingConfig(config)

  return config
}

function checkSendArgs (args, defaults) {
  const [name, data, options] = args

  assertQueueName(name)

  const sending = {
    name,
    data: data || {},
    options: { ...defaults, ...options }
  }

  if (sending.options.singletonKey) {
    assert(typeof sending.options.singletonKey === 'string', 'singletonKey must be a string')
    assert(sending.options.singletonKey.length <= 255, 'singletonKey cannot exceed 255 characters')
  }

  if (sending.options.singletonSeconds) {
    assert(Number.isInteger(sending.options.singletonSeconds) && sending.options.singletonSeconds > 0, 'singletonSeconds must be a positive integer')
  }

  if (sending.options.priority) {
    assert(Number.isInteger(sending.options.priority), 'priority must be an integer')
    assert(sending.options.priority >= -32768 && sending.options.priority <= 32767, 'priority must be between -32768 and 32767')
  }

  if (sending.options.startAfter) {
    assert(Number.isInteger(sending.options.startAfter) || sending.options.startAfter instanceof Date, 'startAfter must be a Date or integer')
  }

  if (sending.options.expireInSeconds) {
    assert(Number.isInteger(sending.options.expireInSeconds) && sending.options.expireInSeconds > 0, 'expireInSeconds must be a positive integer')
    assert(sending.options.expireInSeconds <= POLICY.MAX_EXPIRATION_HOURS * 3600, `expireInSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`)
  }

  if (sending.options.retryLimit) {
    assert(Number.isInteger(sending.options.retryLimit) && sending.options.retryLimit >= 0, 'retryLimit must be a non-negative integer')
  }

  if (sending.options.retryDelay) {
    assert(Number.isInteger(sending.options.retryDelay) && sending.options.retryDelay >= 0, 'retryDelay must be a non-negative integer')
  }

  if (sending.options.retryBackoff) {
    assert(typeof sending.options.retryBackoff === 'boolean', 'retryBackoff must be a boolean')
  }

  if (sending.options.keepUntil) {
    assert(Number.isInteger(sending.options.keepUntil) || sending.options.keepUntil instanceof Date, 'keepUntil must be a Date or integer')
  }

  if (sending.options.deadLetter) {
    assertQueueName(sending.options.deadLetter)
  }

  return sending
}

function checkWorkArgs (name, args, defaults) {
  assertQueueName(name)

  const options = { ...defaults, ...args[1] }

  if (args.length < 1 || args.length > 2) {
    throw new Error('work() requires a callback and optional options')
  }

  if (typeof args[0] !== 'function') {
    throw new Error('work() requires a callback function')
  }

  const callback = args[0]

  if (options.teamSize) {
    assert(Number.isInteger(options.teamSize) && options.teamSize > 0, 'teamSize must be a positive integer')
  }

  if (options.teamConcurrency) {
    assert(Number.isInteger(options.teamConcurrency) && options.teamConcurrency > 0, 'teamConcurrency must be a positive integer')
  }

  if (options.batchSize) {
    assertBatchSize(options.batchSize)
  }

  if (options.pollingInterval) {
    assertInterval(options.pollingInterval)
  }

  if (options.includeMetadata !== undefined) {
    assertIncludeMetadata(options.includeMetadata)
  }

  return { options, callback }
}

function checkFetchArgs (name, options) {
  assertQueueName(name)

  if (options.batchSize) {
    assertBatchSize(options.batchSize)
  }

  if (options.includeMetadata !== undefined) {
    assertIncludeMetadata(options.includeMetadata)
  }

  return options
}

function assertQueueName (name) {
  assert(typeof name === 'string', 'queue name must be a string')
  assert(name.length > 0, 'queue name cannot be empty')
  assert(name.length <= 255, 'queue name cannot exceed 255 characters')
  assert(!/[^a-zA-Z0-9_\-.]/.test(name), 'queue name can only contain alphanumeric characters, underscores, hyphens, and dots')
}

function assertQueueOptions (options) {
  if (options.policy) {
    assertQueuePolicy(options.policy)
  }

  if (options.retryLimit !== undefined) {
    assert(Number.isInteger(options.retryLimit) && options.retryLimit >= 0, 'retryLimit must be a non-negative integer')
  }

  if (options.retryDelay !== undefined) {
    assert(Number.isInteger(options.retryDelay) && options.retryDelay >= 0, 'retryDelay must be a non-negative integer')
  }

  if (options.retryBackoff !== undefined) {
    assert(typeof options.retryBackoff === 'boolean', 'retryBackoff must be a boolean')
  }

  if (options.expireInSeconds !== undefined) {
    assert(Number.isInteger(options.expireInSeconds) && options.expireInSeconds > 0, 'expireInSeconds must be a positive integer')
  }

  if (options.retentionMinutes !== undefined) {
    assert(Number.isInteger(options.retentionMinutes) && options.retentionMinutes > 0, 'retentionMinutes must be a positive integer')
  }

  if (options.deadLetter !== undefined) {
    assertQueueName(options.deadLetter)
  }
}

function assertBatchSize (batchSize) {
  assert(Number.isInteger(batchSize) && batchSize > 0, 'batchSize must be a positive integer')
  assert(batchSize <= 1000, 'batchSize cannot exceed 1000')
}

function assertTimeout (timeout) {
  assert(Number.isInteger(timeout) && timeout > 0, 'timeout must be a positive integer')
}

function assertInterval (interval) {
  assert(Number.isInteger(interval) && interval > 0, 'interval must be a positive integer')
}

function assertRetentionConfig (config) {
  if (config.retentionMinutes !== undefined) {
    assert(Number.isInteger(config.retentionMinutes) && config.retentionMinutes > 0, 'retentionMinutes must be a positive integer')
  }
}

function assertArchiveConfig (config) {
  if (config.archiveInterval !== undefined) {
    assert(Number.isInteger(config.archiveInterval) && config.archiveInterval > 0, 'archiveInterval must be a positive integer')
    
    if (config.archiveInterval < POLICY.CRON_DEBOUNCE_SECONDS) {
      console.warn(WARNINGS.ARCHIVE_INTERVAL_TOO_SHORT.message)
    }
  }
}

function assertMaintenanceConfig (config) {
  if (config.maintenanceIntervalSeconds !== undefined) {
    assert(Number.isInteger(config.maintenanceIntervalSeconds) && config.maintenanceIntervalSeconds > 0, 'maintenanceIntervalSeconds must be a positive integer')
    assert(config.maintenanceIntervalSeconds <= POLICY.MAX_EXPIRATION_HOURS * 3600, `maintenanceIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`)
  }
}

function assertMonitorConfig (config) {
  if (config.monitorStateIntervalSeconds !== undefined) {
    assert(Number.isInteger(config.monitorStateIntervalSeconds) && config.monitorStateIntervalSeconds > 0, 'monitorStateIntervalSeconds must be a positive integer')
    assert(config.monitorStateIntervalSeconds <= POLICY.MAX_EXPIRATION_HOURS * 3600, `monitorStateIntervalSeconds cannot exceed ${POLICY.MAX_EXPIRATION_HOURS} hours`)
  }
}

function assertPollingConfig (config) {
  if (config.pollingIntervalSeconds !== undefined) {
    assert(Number.isInteger(config.pollingIntervalSeconds) && config.pollingIntervalSeconds > 0, 'pollingIntervalSeconds must be a positive integer')
  }
}

function assertIncludeMetadata (includeMetadata) {
  assert(typeof includeMetadata === 'boolean', 'includeMetadata must be a boolean')
}

function assertWorkerOptions (options) {
  if (options.teamSize !== undefined) {
    assert(Number.isInteger(options.teamSize) && options.teamSize > 0, 'teamSize must be a positive integer')
  }

  if (options.teamConcurrency !== undefined) {
    assert(Number.isInteger(options.teamConcurrency) && options.teamConcurrency > 0, 'teamConcurrency must be a positive integer')
  }

  if (options.batchSize !== undefined) {
    assertBatchSize(options.batchSize)
  }

  if (options.pollingInterval !== undefined) {
    assertInterval(options.pollingInterval)
  }
}

function assertCronExpression (cron) {
  assert(typeof cron === 'string', 'cron expression must be a string')
  assert(cron.length > 0, 'cron expression cannot be empty')
}

function assertScheduleOptions (options) {
  if (options.tz !== undefined) {
    assert(typeof options.tz === 'string', 'timezone must be a string')
  }
}

function assertSubscriptionOptions (options) {
  if (options.newJobCheckInterval !== undefined) {
    assertInterval(options.newJobCheckInterval)
  }
}

function assertQueuePolicy (policy) {
  const validPolicies = ['standard', 'short', 'singleton', 'stately']
  assert(validPolicies.includes(policy), `queue policy must be one of: ${validPolicies.join(', ')}`)
}

function warnClockSkew (message) {
  console.warn(`${WARNINGS.CLOCK_SKEW.message}: ${message}`)
}
