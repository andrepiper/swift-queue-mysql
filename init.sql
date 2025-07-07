-- MySQL initialization script for swift-queue-mysql
-- This script is run when the MySQL container starts up

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS swift_queue CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create a dedicated user for the queue system
CREATE USER IF NOT EXISTS 'queue_user'@'%' IDENTIFIED BY 'queue_password';
GRANT ALL PRIVILEGES ON swift_queue.* TO 'queue_user'@'%';

-- Grant necessary privileges for the queue system
GRANT SELECT, INSERT, UPDATE, DELETE ON swift_queue.* TO 'queue_user'@'%';
GRANT CREATE, ALTER, DROP, INDEX ON swift_queue.* TO 'queue_user'@'%';
GRANT EXECUTE ON swift_queue.* TO 'queue_user'@'%';

-- Flush privileges to ensure they take effect
FLUSH PRIVILEGES;

-- Switch to the swift_queue database
USE swift_queue;

-- Set timezone to UTC for consistency
SET time_zone = '+00:00';

-- Enable event scheduler for maintenance tasks
SET GLOBAL event_scheduler = ON;

-- Create some initial configuration
INSERT INTO mysql.time_zone_name (Name, Time_zone_id) VALUES ('UTC', 1) ON DUPLICATE KEY UPDATE Time_zone_id = 1;

-- Ensure proper character set and collation
ALTER DATABASE swift_queue CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
