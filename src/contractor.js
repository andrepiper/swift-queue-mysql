const plans = require('./plans')
const migrationStore = require('./migrationStore')

class Contractor {
  constructor (db, config) {
    this.db = db
    this.config = config
    this.schema = config.schema
  }

  static constructionPlans (schema) {
    return plans.create(schema || plans.DEFAULT_SCHEMA, migrationStore.getVersion())
  }

  static migrationPlans (schema, version) {
    return migrationStore.get(schema || plans.DEFAULT_SCHEMA, version)
  }

  static rollbackPlans (schema, version) {
    return migrationStore.rollback(schema || plans.DEFAULT_SCHEMA, version)
  }

  async create () {
    const commands = plans.create(this.schema, migrationStore.getVersion())
    
    // Split commands and execute them individually for MySQL
    const commandArray = commands.split(';').filter(cmd => cmd.trim())
    
    for (const command of commandArray) {
      const trimmedCommand = command.trim()
      if (trimmedCommand) {
        try {
          await this.db.executeSql(trimmedCommand)
        } catch (err) {
          if (err.code === 'ER_TABLE_EXISTS_ERROR' || err.code === 'ER_DB_CREATE_EXISTS') {
            // Ignore table/database already exists errors
            continue
          }
          throw err
        }
      }
    }
  }

  async migrate (version) {
    const migrations = migrationStore.get(this.schema, version)
    
    if (migrations && migrations.length > 0) {
      for (const migration of migrations) {
        await this.db.executeSql(migration)
      }
    }
  }

  async rollback (version) {
    const rollbacks = migrationStore.rollback(this.schema, version)
    
    if (rollbacks && rollbacks.length > 0) {
      for (const rollback of rollbacks) {
        await this.db.executeSql(rollback)
      }
    }
  }

  async version () {
    try {
      const versionExists = await this.db.executeSql(plans.versionTableExists(this.schema))
      
      if (versionExists.rows.length === 0) {
        return null
      }

      const result = await this.db.executeSql(plans.getVersion(this.schema))
      return result.rows[0]?.version || null
    } catch (err) {
      return null
    }
  }

  async isInstalled () {
    try {
      const version = await this.version()
      return version !== null
    } catch (err) {
      return false
    }
  }

  async ensureCurrentVersion () {
    const currentVersion = await this.version()
    const latestVersion = migrationStore.getVersion()
    
    if (currentVersion === null) {
      await this.create()
    } else if (currentVersion < latestVersion) {
      await this.migrate(currentVersion)
    }
  }
}

module.exports = Contractor
