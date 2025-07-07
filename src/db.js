const EventEmitter = require('node:events')
const mysql = require('mysql2/promise')

class Db extends EventEmitter {
  constructor (config) {
    super()

    // Convert pg-boss style config to MySQL config
    const mysqlConfig = {
      host: config.host || 'localhost',
      port: config.port || 3306,
      user: config.user || config.username,
      password: config.password,
      database: config.database,
      connectionLimit: config.max || 10,
      acquireTimeout: config.connectionTimeoutMillis || 60000,
      timeout: config.query_timeout || 0,
      charset: 'utf8mb4',
      timezone: 'Z',
      supportBigNumbers: true,
      bigNumberStrings: true,
      dateStrings: false,
      ...config
    }

    this.config = mysqlConfig
    this.schema = config.schema || 'mysql_boss'
  }

  events = {
    error: 'error'
  }

  async open () {
    this.pool = mysql.createPool(this.config)
    
    // Test connection
    try {
      const connection = await this.pool.getConnection()
      connection.release()
      this.opened = true
    } catch (error) {
      this.emit('error', error)
      throw error
    }
  }

  async close () {
    if (this.pool && this.opened) {
      this.opened = false
      await this.pool.end()
    }
  }

  async executeSql (text, values = []) {
    if (!this.opened) {
      throw new Error('Database connection not open')
    }

    try {
      // MySQL uses ? placeholders, convert $1, $2, etc to ?
      const mysqlText = text.replace(/\$(\d+)/g, '?')
      
      const [rows, fields] = await this.pool.execute(mysqlText, values)
      
      // Return pg-boss compatible result format
      return {
        rows: Array.isArray(rows) ? rows : [rows],
        rowCount: Array.isArray(rows) ? rows.length : (rows.affectedRows || 0),
        fields
      }
    } catch (error) {
      this.emit('error', error)
      throw error
    }
  }

  async query (text, values = []) {
    return this.executeSql(text, values)
  }

  // Helper method to escape identifiers
  escapeId (identifier) {
    return mysql.escapeId(identifier)
  }

  // Helper method to escape values
  escape (value) {
    return mysql.escape(value)
  }
}

module.exports = Db
