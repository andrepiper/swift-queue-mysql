const MysqlBoss = require('../src/index')

// Global test hooks
before(async function() {
  this.timeout(10000)
  
  // Create test database if it doesn't exist
  const adminBoss = new MysqlBoss({
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'password',
    database: 'mysql' // Connect to mysql database to create test database
  })
  
  try {
    await adminBoss.start()
    
    const testDb = process.env.MYSQL_DATABASE || 'mysql_boss_test'
    await adminBoss.getDb().executeSql(`CREATE DATABASE IF NOT EXISTS \`${testDb}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`)
    
    await adminBoss.stop()
  } catch (error) {
    console.error('Failed to create test database:', error)
  }
})

after(async function() {
  this.timeout(10000)
  
  // Clean up test database
  const adminBoss = new MysqlBoss({
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'password',
    database: 'mysql'
  })
  
  try {
    await adminBoss.start()
    
    const testDb = process.env.MYSQL_DATABASE || 'mysql_boss_test'
    await adminBoss.getDb().executeSql(`DROP DATABASE IF EXISTS \`${testDb}\``)
    
    await adminBoss.stop()
  } catch (error) {
    console.error('Failed to clean up test database:', error)
  }
})

// Helper function to wait for condition
global.waitForCondition = async function(condition, timeout = 5000, interval = 100) {
  const start = Date.now()
  
  while (Date.now() - start < timeout) {
    if (await condition()) {
      return true
    }
    await new Promise(resolve => setTimeout(resolve, interval))
  }
  
  throw new Error(`Condition not met within ${timeout}ms`)
}

// Helper function to create test boss instance
global.createTestBoss = function(options = {}) {
  return new MysqlBoss({
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'password',
    database: process.env.MYSQL_DATABASE || 'mysql_boss_test',
    ...options
  })
}

// Helper function to clean up test data
global.cleanupTestData = async function(boss) {
  try {
    await boss.clearStorage()
  } catch (error) {
    console.error('Failed to cleanup test data:', error)
  }
}
