const { v4: uuidv4 } = require('uuid')

const DEFAULT_SCHEMA = 'swift_queue'
const MIGRATE_RACE_MESSAGE = 'Duplicate entry'
const CREATE_RACE_MESSAGE = 'already exists'

const JOB_STATES = Object.freeze({
  created: 'created',
  retry: 'retry',
  active: 'active',
  completed: 'completed',
  cancelled: 'cancelled',
  failed: 'failed'
})

const QUEUE_POLICIES = Object.freeze({
  standard: 'standard',
  short: 'short',
  singleton: 'singleton',
  stately: 'stately'
})

module.exports = {
  create,
  insertVersion,
  getVersion,
  setVersion,
  versionTableExists,
  fetchNextJob,
  completeJobs,
  cancelJobs,
  resumeJobs,
  deleteJobs,
  retryJobs,
  failJobsById,
  failJobsByTimeout,
  insertJob,
  insertJobs,
  getTime,
  getSchedules,
  schedule,
  unschedule,
  subscribe,
  unsubscribe,
  getQueuesForEvent,
  archive,
  drop,
  countStates,
  updateQueue,
  createQueue,
  deleteQueue,
  getQueues,
  getQueueByName,
  getQueueSize,
  purgeQueue,
  clearStorage,
  trySetMaintenanceTime,
  trySetMonitorTime,
  trySetCronTime,
  locked,
  assertMigration,
  getArchivedJobById,
  getJobById,
  QUEUE_POLICIES,
  JOB_STATES,
  MIGRATE_RACE_MESSAGE,
  CREATE_RACE_MESSAGE,
  DEFAULT_SCHEMA
}

const assert = require('node:assert')

function create (schema, version) {
  const commands = [
    createDatabase(schema),
    useDatabase(schema),
    createTableVersion(schema),
    createTableQueue(schema),
    createTableSchedule(schema),
    createTableSubscription(schema),
    createTableJob(schema),
    createTableArchive(schema),
    createIndexes(schema),
    createProcedures(schema),
    insertVersion(schema, version)
  ]

  return commands.join(';\n\n') + ';'
}

function createDatabase (schema) {
  return `CREATE DATABASE IF NOT EXISTS \`${schema}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
}

function useDatabase (schema) {
  return `USE \`${schema}\``
}

function createTableVersion (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`version\` (
      \`version\` INT PRIMARY KEY,
      \`maintained_on\` TIMESTAMP NULL,
      \`cron_on\` TIMESTAMP NULL,
      \`monitored_on\` TIMESTAMP NULL,
      \`created_at\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      \`updated_at\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
  `
}

function createTableQueue (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`queue\` (
      \`name\` VARCHAR(255) NOT NULL,
      \`policy\` VARCHAR(50) DEFAULT 'standard',
      \`retry_limit\` INT DEFAULT 3,
      \`retry_delay\` INT DEFAULT 0,
      \`retry_backoff\` BOOLEAN DEFAULT FALSE,
      \`expire_seconds\` INT DEFAULT 900,
      \`retention_minutes\` INT DEFAULT 20160,
      \`dead_letter\` VARCHAR(255) NULL,
      \`partition_name\` VARCHAR(255) NULL,
      \`created_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      \`updated_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (\`name\`),
      FOREIGN KEY (\`dead_letter\`) REFERENCES \`${schema}\`.\`queue\`(\`name\`) ON DELETE SET NULL
    ) ENGINE=InnoDB
  `
}

function createTableSchedule (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`schedule\` (
      \`name\` VARCHAR(255) NOT NULL,
      \`cron\` VARCHAR(255) NOT NULL,
      \`timezone\` VARCHAR(100) DEFAULT 'UTC',
      \`data\` JSON,
      \`options\` JSON,
      \`created_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      \`updated_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (\`name\`),
      FOREIGN KEY (\`name\`) REFERENCES \`${schema}\`.\`queue\`(\`name\`) ON DELETE CASCADE
    ) ENGINE=InnoDB
  `
}

function createTableSubscription (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`subscription\` (
      \`event\` VARCHAR(255) NOT NULL,
      \`name\` VARCHAR(255) NOT NULL,
      \`created_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      \`updated_on\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (\`event\`, \`name\`),
      FOREIGN KEY (\`name\`) REFERENCES \`${schema}\`.\`queue\`(\`name\`) ON DELETE CASCADE
    ) ENGINE=InnoDB
  `
}

function createTableJob (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`job\` (
      \`id\` VARCHAR(36) NOT NULL DEFAULT (UUID()),
      \`name\` VARCHAR(255) NOT NULL,
      \`priority\` INT NOT NULL DEFAULT 0,
      \`data\` JSON,
      \`state\` ENUM('created', 'retry', 'active', 'completed', 'cancelled', 'failed') NOT NULL DEFAULT 'created',
      \`retry_limit\` INT NOT NULL DEFAULT 2,
      \`retry_count\` INT NOT NULL DEFAULT 0,
      \`retry_delay\` INT NOT NULL DEFAULT 0,
      \`retry_backoff\` BOOLEAN NOT NULL DEFAULT FALSE,
      \`start_after\` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      \`started_on\` TIMESTAMP NULL,
      \`singleton_key\` VARCHAR(255) NULL,
      \`singleton_on\` TIMESTAMP NULL,
      \`expire_in_seconds\` INT NOT NULL DEFAULT 900,
      \`created_on\` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      \`completed_on\` TIMESTAMP NULL,
      \`keep_until\` TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP + INTERVAL 14 DAY),
      \`output\` JSON,
      \`dead_letter\` VARCHAR(255) NULL,
      \`policy\` VARCHAR(50) NULL,
      PRIMARY KEY (\`id\`),
      INDEX \`idx_job_name_state\` (\`name\`, \`state\`),
      INDEX \`idx_job_fetch\` (\`name\`, \`state\`, \`start_after\`, \`priority\`),
      INDEX \`idx_job_singleton\` (\`name\`, \`singleton_key\`, \`singleton_on\`),
      INDEX \`idx_job_created_on\` (\`created_on\`),
      INDEX \`idx_job_completed_on\` (\`completed_on\`),
      INDEX \`idx_job_keep_until\` (\`keep_until\`)
    ) ENGINE=InnoDB
  `
}

function createTableArchive (schema) {
  return `
    CREATE TABLE IF NOT EXISTS \`${schema}\`.\`archive\` (
      \`id\` VARCHAR(36) NOT NULL,
      \`name\` VARCHAR(255) NOT NULL,
      \`priority\` INT NOT NULL DEFAULT 0,
      \`data\` JSON,
      \`state\` ENUM('created', 'retry', 'active', 'completed', 'cancelled', 'failed') NOT NULL,
      \`retry_limit\` INT NOT NULL DEFAULT 2,
      \`retry_count\` INT NOT NULL DEFAULT 0,
      \`retry_delay\` INT NOT NULL DEFAULT 0,
      \`retry_backoff\` BOOLEAN NOT NULL DEFAULT FALSE,
      \`start_after\` TIMESTAMP NOT NULL,
      \`started_on\` TIMESTAMP NULL,
      \`singleton_key\` VARCHAR(255) NULL,
      \`singleton_on\` TIMESTAMP NULL,
      \`expire_in_seconds\` INT NOT NULL DEFAULT 900,
      \`created_on\` TIMESTAMP NOT NULL,
      \`completed_on\` TIMESTAMP NULL,
      \`keep_until\` TIMESTAMP NOT NULL,
      \`output\` JSON,
      \`dead_letter\` VARCHAR(255) NULL,
      \`policy\` VARCHAR(50) NULL,
      \`archived_on\` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (\`id\`),
      INDEX \`idx_archive_name\` (\`name\`),
      INDEX \`idx_archive_archived_on\` (\`archived_on\`)
    ) ENGINE=InnoDB
  `
}

function createIndexes (schema) {
  return `
    -- Additional indexes for performance
    CREATE INDEX \`idx_job_policy_short\` ON \`${schema}\`.\`job\` (\`name\`, \`policy\`, \`state\`, \`start_after\`) 
    WHERE \`policy\` = 'short';
    
    CREATE INDEX \`idx_job_policy_singleton\` ON \`${schema}\`.\`job\` (\`name\`, \`singleton_key\`, \`state\`, \`start_after\`) 
    WHERE \`policy\` = 'singleton';
    
    CREATE INDEX \`idx_job_policy_stately\` ON \`${schema}\`.\`job\` (\`name\`, \`policy\`, \`state\`, \`start_after\`) 
    WHERE \`policy\` = 'stately'
  `
}

function createProcedures (schema) {
  return `
    -- Procedure to create queue
    DELIMITER $$
    CREATE PROCEDURE \`${schema}\`.\`create_queue\`(
      IN queue_name VARCHAR(255),
      IN options JSON
    )
    BEGIN
      INSERT IGNORE INTO \`${schema}\`.\`queue\` (
        \`name\`,
        \`policy\`,
        \`retry_limit\`,
        \`retry_delay\`,
        \`retry_backoff\`,
        \`expire_seconds\`,
        \`retention_minutes\`,
        \`dead_letter\`
      ) VALUES (
        queue_name,
        JSON_UNQUOTE(JSON_EXTRACT(options, '$.policy')),
        JSON_EXTRACT(options, '$.retryLimit'),
        JSON_EXTRACT(options, '$.retryDelay'),
        JSON_EXTRACT(options, '$.retryBackoff'),
        JSON_EXTRACT(options, '$.expireInSeconds'),
        JSON_EXTRACT(options, '$.retentionMinutes'),
        JSON_UNQUOTE(JSON_EXTRACT(options, '$.deadLetter'))
      );
    END$$
    DELIMITER ;
    
    -- Procedure to delete queue
    DELIMITER $$
    CREATE PROCEDURE \`${schema}\`.\`delete_queue\`(
      IN queue_name VARCHAR(255)
    )
    BEGIN
      DELETE FROM \`${schema}\`.\`queue\` WHERE \`name\` = queue_name;
    END$$
    DELIMITER ;
  `
}

const baseJobColumns = 'id, name, data, expire_in_seconds as expireInSeconds'
const allJobColumns = `${baseJobColumns},
  policy,
  state,
  priority,
  retry_limit as retryLimit,
  retry_count as retryCount,
  retry_delay as retryDelay,
  retry_backoff as retryBackoff,
  start_after as startAfter,
  started_on as startedOn,
  singleton_key as singletonKey,
  singleton_on as singletonOn,
  expire_in_seconds as expireInSeconds,
  created_on as createdOn,
  completed_on as completedOn,
  keep_until as keepUntil,
  dead_letter as deadLetter,
  output`

function insertVersion (schema, version) {
  return `INSERT INTO \`${schema}\`.\`version\` (version) VALUES (${version}) ON DUPLICATE KEY UPDATE version = ${version}`
}

function getVersion (schema) {
  return `SELECT version FROM \`${schema}\`.\`version\` LIMIT 1`
}

function setVersion (schema, version) {
  return `UPDATE \`${schema}\`.\`version\` SET version = ${version}`
}

function versionTableExists (schema) {
  return `SELECT 1 FROM information_schema.tables WHERE table_schema = '${schema}' AND table_name = 'version'`
}

// MySQL doesn't have SKIP LOCKED, so we use a different approach
function fetchNextJob (schema) {
  return function (options = {}) {
    const { batchSize = 1, includeMetadata = false } = options
    
    return `
      SELECT ${includeMetadata ? allJobColumns : baseJobColumns}
      FROM \`${schema}\`.\`job\` 
      WHERE \`name\` = ? 
        AND \`state\` IN ('created', 'retry')
        AND \`start_after\` <= NOW()
      ORDER BY \`priority\` DESC, \`created_on\` ASC
      LIMIT ${batchSize}
      FOR UPDATE
    `
  }
}

function completeJobs (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'completed', 
        \`completed_on\` = NOW(),
        \`output\` = ?
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function cancelJobs (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'cancelled', 
        \`completed_on\` = NOW()
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function resumeJobs (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'created', 
        \`completed_on\` = NULL,
        \`started_on\` = NULL
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function deleteJobs (schema) {
  return `
    DELETE FROM \`${schema}\`.\`job\` 
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function retryJobs (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'retry', 
        \`retry_count\` = \`retry_count\` + 1,
        \`start_after\` = CASE 
          WHEN \`retry_backoff\` = 0 THEN DATE_ADD(NOW(), INTERVAL \`retry_delay\` SECOND)
          ELSE DATE_ADD(NOW(), INTERVAL (\`retry_delay\` * POW(2, \`retry_count\`)) SECOND)
        END,
        \`completed_on\` = NULL
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function failJobsById (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'failed', 
        \`completed_on\` = NOW(),
        \`output\` = ?
    WHERE \`id\` IN (${Array(arguments.length - 1).fill('?').join(',')})
  `
}

function failJobsByTimeout (schema) {
  return `
    UPDATE \`${schema}\`.\`job\` 
    SET \`state\` = 'failed', 
        \`completed_on\` = NOW(),
        \`output\` = '{"value": {"message": "job failed by timeout in active state"}}'
    WHERE \`state\` = 'active' 
      AND \`started_on\` < DATE_SUB(NOW(), INTERVAL \`expire_in_seconds\` SECOND)
  `
}

function insertJob (schema) {
  return `
    INSERT INTO \`${schema}\`.\`job\` (
      \`id\`,
      \`name\`,
      \`priority\`,
      \`data\`,
      \`state\`,
      \`retry_limit\`,
      \`retry_delay\`,
      \`retry_backoff\`,
      \`start_after\`,
      \`singleton_key\`,
      \`singleton_on\`,
      \`expire_in_seconds\`,
      \`keep_until\`,
      \`dead_letter\`,
      \`policy\`
    ) VALUES (
      COALESCE(?, UUID()),
      ?,
      ?,
      ?,
      'created',
      COALESCE(?, 2),
      COALESCE(?, 0),
      COALESCE(?, 0),
      COALESCE(?, NOW()),
      ?,
      ?,
      COALESCE(?, 900),
      COALESCE(?, DATE_ADD(NOW(), INTERVAL 14 DAY)),
      ?,
      ?
    )
  `
}

function insertJobs (schema) {
  return `
    INSERT INTO \`${schema}\`.\`job\` (
      \`name\`,
      \`priority\`,
      \`data\`,
      \`state\`,
      \`retry_limit\`,
      \`retry_delay\`,
      \`retry_backoff\`,
      \`start_after\`,
      \`singleton_key\`,
      \`singleton_on\`,
      \`expire_in_seconds\`,
      \`keep_until\`,
      \`dead_letter\`,
      \`policy\`
    ) VALUES ?
  `
}

function getTime (schema) {
  return 'SELECT UNIX_TIMESTAMP() * 1000 as time'
}

function getSchedules (schema) {
  return `
    SELECT \`name\`, \`cron\`, \`timezone\`, \`data\`, \`options\`
    FROM \`${schema}\`.\`schedule\`
  `
}

function schedule (schema) {
  return `
    INSERT INTO \`${schema}\`.\`schedule\` (\`name\`, \`cron\`, \`timezone\`, \`data\`, \`options\`)
    VALUES (?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      \`cron\` = VALUES(\`cron\`),
      \`timezone\` = VALUES(\`timezone\`),
      \`data\` = VALUES(\`data\`),
      \`options\` = VALUES(\`options\`),
      \`updated_on\` = NOW()
  `
}

function unschedule (schema) {
  return `DELETE FROM \`${schema}\`.\`schedule\` WHERE \`name\` = ?`
}

function subscribe (schema) {
  return `
    INSERT IGNORE INTO \`${schema}\`.\`subscription\` (\`event\`, \`name\`)
    VALUES (?, ?)
  `
}

function unsubscribe (schema) {
  return `
    DELETE FROM \`${schema}\`.\`subscription\` 
    WHERE \`event\` = ? AND \`name\` = ?
  `
}

function getQueuesForEvent (schema) {
  return `
    SELECT \`name\` 
    FROM \`${schema}\`.\`subscription\` 
    WHERE \`event\` = ?
  `
}

function archive (schema, completedInterval, failedInterval) {
  return `
    INSERT INTO \`${schema}\`.\`archive\` 
    SELECT *, NOW() as \`archived_on\` 
    FROM \`${schema}\`.\`job\` 
    WHERE (\`state\` != 'failed' AND \`completed_on\` < DATE_SUB(NOW(), INTERVAL ${completedInterval} SECOND))
       OR (\`state\` = 'failed' AND \`completed_on\` < DATE_SUB(NOW(), INTERVAL ${failedInterval} SECOND))
  `
}

function drop (schema, interval) {
  return `
    DELETE FROM \`${schema}\`.\`archive\` 
    WHERE \`archived_on\` < DATE_SUB(NOW(), INTERVAL ${interval} SECOND)
  `
}

function countStates (schema) {
  return `
    SELECT 
      \`name\`,
      \`state\`,
      COUNT(*) as \`size\`
    FROM \`${schema}\`.\`job\`
    GROUP BY \`name\`, \`state\`
    
    UNION ALL
    
    SELECT 
      NULL as \`name\`,
      \`state\`,
      COUNT(*) as \`size\`
    FROM \`${schema}\`.\`job\`
    GROUP BY \`state\`
    
    UNION ALL
    
    SELECT 
      NULL as \`name\`,
      'all' as \`state\`,
      COUNT(*) as \`size\`
    FROM \`${schema}\`.\`job\`
  `
}

function updateQueue (schema) {
  return `
    UPDATE \`${schema}\`.\`queue\` 
    SET \`policy\` = COALESCE(?, \`policy\`),
        \`retry_limit\` = COALESCE(?, \`retry_limit\`),
        \`retry_delay\` = COALESCE(?, \`retry_delay\`),
        \`retry_backoff\` = COALESCE(?, \`retry_backoff\`),
        \`expire_seconds\` = COALESCE(?, \`expire_seconds\`),
        \`retention_minutes\` = COALESCE(?, \`retention_minutes\`),
        \`dead_letter\` = COALESCE(?, \`dead_letter\`),
        \`updated_on\` = NOW()
    WHERE \`name\` = ?
  `
}

function createQueue (schema) {
  return `CALL \`${schema}\`.\`create_queue\`(?, ?)`
}

function deleteQueue (schema) {
  return `CALL \`${schema}\`.\`delete_queue\`(?)`
}

function getQueues (schema) {
  return `
    SELECT
      \`name\`,
      \`policy\`,
      \`retry_limit\` as \`retryLimit\`,
      \`retry_delay\` as \`retryDelay\`,
      \`retry_backoff\` as \`retryBackoff\`,
      \`expire_seconds\` as \`expireInSeconds\`,
      \`retention_minutes\` as \`retentionMinutes\`,
      \`dead_letter\` as \`deadLetter\`,
      \`created_on\` as \`createdOn\`,
      \`updated_on\` as \`updatedOn\`
    FROM \`${schema}\`.\`queue\`
  `
}

function getQueueByName (schema) {
  return `
    SELECT
      \`name\`,
      \`policy\`,
      \`retry_limit\` as \`retryLimit\`,
      \`retry_delay\` as \`retryDelay\`,
      \`retry_backoff\` as \`retryBackoff\`,
      \`expire_seconds\` as \`expireInSeconds\`,
      \`retention_minutes\` as \`retentionMinutes\`,
      \`dead_letter\` as \`deadLetter\`,
      \`created_on\` as \`createdOn\`,
      \`updated_on\` as \`updatedOn\`
    FROM \`${schema}\`.\`queue\`
    WHERE \`name\` = ?
  `
}

function getQueueSize (schema) {
  return `
    SELECT COUNT(*) as \`size\`
    FROM \`${schema}\`.\`job\`
    WHERE \`name\` = ?
  `
}

function purgeQueue (schema) {
  return `DELETE FROM \`${schema}\`.\`job\` WHERE \`name\` = ?`
}

function clearStorage (schema) {
  return `
    DELETE FROM \`${schema}\`.\`job\`;
    DELETE FROM \`${schema}\`.\`archive\`;
    DELETE FROM \`${schema}\`.\`schedule\`;
    DELETE FROM \`${schema}\`.\`subscription\`;
    DELETE FROM \`${schema}\`.\`queue\`;
  `
}

function trySetMaintenanceTime (schema) {
  return `
    UPDATE \`${schema}\`.\`version\` 
    SET \`maintained_on\` = NOW()
    WHERE \`maintained_on\` IS NULL 
       OR \`maintained_on\` < DATE_SUB(NOW(), INTERVAL ? SECOND)
  `
}

function trySetMonitorTime (schema) {
  return `
    UPDATE \`${schema}\`.\`version\` 
    SET \`monitored_on\` = NOW()
    WHERE \`monitored_on\` IS NULL 
       OR \`monitored_on\` < DATE_SUB(NOW(), INTERVAL ? SECOND)
  `
}

function trySetCronTime (schema) {
  return `
    UPDATE \`${schema}\`.\`version\` 
    SET \`cron_on\` = NOW()
    WHERE \`cron_on\` IS NULL 
       OR \`cron_on\` < DATE_SUB(NOW(), INTERVAL ? SECOND)
  `
}

function locked (schema, commands) {
  if (Array.isArray(commands)) {
    return commands.join(';\n')
  }
  return commands
}

function assertMigration (schema, version) {
  return `
    SELECT version FROM \`${schema}\`.\`version\` 
    WHERE version >= ${version}
  `
}

function getArchivedJobById (schema) {
  return `
    SELECT ${allJobColumns} 
    FROM \`${schema}\`.\`archive\` 
    WHERE \`name\` = ? AND \`id\` = ?
  `
}

function getJobById (schema) {
  return `
    SELECT ${allJobColumns} 
    FROM \`${schema}\`.\`job\` 
    WHERE \`name\` = ? AND \`id\` = ?
  `
}
