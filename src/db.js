const EventEmitter = require('node:events')
const mysql = require('mysql2/promise')

class Db extends EventEmitter {
  constructor (config) {
    super()

    // Store the original config for our use
    this.originalConfig = config
    this.autoCreateDatabase = config.autoCreateDatabase || false
    this.schema = config.schema || 'swift_queue'

    // Create clean MySQL config with only valid MySQL options
    this.config = this.createMySQLConfig(config)
  }

  createMySQLConfig (config) {
    // Only include valid MySQL connection options - explicitly whitelist them
    const mysqlConfig = {}
    
    // Connection settings
    if (config.host) mysqlConfig.host = config.host
    if (config.port) mysqlConfig.port = config.port
    if (config.user || config.username) mysqlConfig.user = config.user || config.username
    if (config.password !== undefined) mysqlConfig.password = config.password
    if (config.database) mysqlConfig.database = config.database
    
    // Pool settings
    if (config.max) mysqlConfig.connectionLimit = config.max
    
    // MySQL-specific settings
    mysqlConfig.charset = 'utf8mb4'
    mysqlConfig.timezone = 'Z'
    mysqlConfig.supportBigNumbers = true
    mysqlConfig.bigNumberStrings = true
    mysqlConfig.dateStrings = false
    
    // Default values for required fields
    if (!mysqlConfig.host) mysqlConfig.host = 'localhost'
    if (!mysqlConfig.port) mysqlConfig.port = 3306
    if (!mysqlConfig.connectionLimit) mysqlConfig.connectionLimit = 10

    return mysqlConfig
  }

  events = {
    error: 'error'
  }

  async open () {
    // Debug: log the config being passed to MySQL
    console.log('Debug: MySQL config keys:', Object.keys(this.config))
    
    // Try to create pool with the specified database
    try {
      this.pool = mysql.createPool(this.config)
      
      // Test connection
      const connection = await this.pool.getConnection()
      connection.release()
      this.opened = true
      console.log(`✓ Connected to database '${this.config.database}'`)
    } catch (error) {
      // If database doesn't exist and auto-creation is enabled, create it
      if (error.code === 'ER_BAD_DB_ERROR' && this.autoCreateDatabase) {
        console.log(`Database '${this.config.database}' does not exist. Creating...`)
        await this.createDatabase()
        // Retry with the created database
        this.pool = mysql.createPool(this.config)
        const connection = await this.pool.getConnection()
        connection.release()
        this.opened = true
        console.log(`✓ Created and connected to database '${this.config.database}'`)
      } else {
        this.emit('error', error)
        throw error
      }
    }
  }
  
  async createDatabase () {
    // Create temporary pool without database specified
    const tempConfig = { ...this.config }
    delete tempConfig.database
    
    const tempPool = mysql.createPool(tempConfig)
    
    try {
      const connection = await tempPool.getConnection()
      
      // Create database
      const sql = `CREATE DATABASE IF NOT EXISTS \`${this.config.database}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
      await connection.query(sql)
      
      connection.release()
      console.log(`✓ Database '${this.config.database}' created successfully`)
    } catch (error) {
      console.error(`✗ Failed to create database '${this.config.database}':`, error.message)
      throw error
    } finally {
      await tempPool.end()
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
      
      // Check if this is a DDL statement that cannot be prepared
      const isDDL = /^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|USE)\s/i.test(mysqlText)
      
      let rows, fields
      if (isDDL || values.length === 0) {
        // Use query() for DDL statements or statements without parameters
        [rows, fields] = await this.pool.query(mysqlText, values)
      } else {
        // Use execute() for DML statements with parameters
        [rows, fields] = await this.pool.execute(mysqlText, values)
      }
      
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
