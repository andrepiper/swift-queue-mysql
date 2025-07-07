# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-15

### Added
- Initial release of MySQL Boss
- Complete MySQL-compatible job queue implementation
- Support for job priorities and scheduling
- Cron-based job scheduling with timezone support
- Pub/sub event system
- Dead letter queue support
- Job retry with exponential backoff
- Singleton and throttled job support
- Queue policies (standard, short, singleton, stately)
- Comprehensive test suite
- CLI tool for queue management
- Web-based monitoring dashboard
- Docker support with docker-compose
- Extensive documentation and examples

### Core Features
- **Database Operations**: MySQL-specific SQL queries and schemas
- **Job Management**: Send, fetch, complete, fail, cancel, resume, retry jobs
- **Queue Management**: Create, delete, update, purge queues
- **Worker System**: Multi-worker support with team size and concurrency control
- **Scheduling**: Cron-based recurring jobs with timezone support
- **Monitoring**: Real-time job state monitoring and statistics
- **Maintenance**: Automatic job archival and cleanup
- **Reliability**: Retry logic, dead letter queues, job expiration

### Technical Details
- Uses MySQL 5.7+ or MySQL 8.0+
- Node.js 16+ support
- Connection pooling with mysql2
- JSON data type for job payload storage
- UUID-based job identification
- Optimistic locking for job processing
- Configurable retention and archival policies

### Documentation
- Complete API documentation
- Usage examples and tutorials
- Docker deployment guide
- Performance optimization tips
- Migration guide from pg-boss

### Development Tools
- Comprehensive test suite with reliability tests
- CLI tool for development and operations
- Web dashboard for monitoring and management
- Docker development environment
- ESLint configuration for code quality

## [Unreleased]

### Planned Features
- Batch job operations
- Job dependencies and workflows
- Advanced queue routing
- Metrics and alerting integration
- Horizontal scaling support
- Job result streaming
- Advanced monitoring and analytics
