const assert = require('node:assert')
const plans = require('./plans')

const CURRENT_VERSION = 1

const migrations = new Map()

// Version 1 - Initial MySQL schema
migrations.set(1, {
  version: 1,
  previous: 0,
  install: [
    (schema) => plans.create(schema, 1)
  ],
  rollback: [
    (schema) => `DROP DATABASE IF EXISTS \`${schema}\``
  ]
})

function getVersion () {
  return CURRENT_VERSION
}

function get (schema, version) {
  const migration = migrations.get(version + 1)
  if (!migration) {
    return null
  }

  return migration.install.map(fn => fn(schema))
}

function rollback (schema, version) {
  const migration = migrations.get(version)
  if (!migration) {
    return null
  }

  return migration.rollback.map(fn => fn(schema))
}

function getAll (schema) {
  const result = []
  
  for (const [version, migration] of migrations) {
    result.push({
      version,
      previous: migration.previous,
      install: migration.install.map(fn => fn(schema)),
      rollback: migration.rollback.map(fn => fn(schema))
    })
  }
  
  return result.sort((a, b) => a.version - b.version)
}

function migrate (value, version, userMigrations) {
  let schema, config

  if (typeof value === 'string') {
    config = null
    schema = value
  } else {
    config = value
    schema = config.schema
  }

  userMigrations = userMigrations || getAll(schema)

  const result = userMigrations
    .filter(i => i.previous >= version)
    .sort((a, b) => a.version - b.version)
    .reduce((acc, i) => {
      acc.install = acc.install.concat(i.install)
      acc.version = i.version
      return acc
    }, { install: [], version })

  assert(result.install.length > 0, `Version ${version} not found.`)

  return flatten(schema, result.install, result.version)
}

function flatten (schema, commands, version) {
  const flattened = commands.flat()
  
  // Add version update at the end
  flattened.push(plans.setVersion(schema, version))
  
  return flattened
}

module.exports = {
  getVersion,
  get,
  rollback,
  getAll,
  migrate,
  flatten
}
