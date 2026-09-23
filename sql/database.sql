-- ============================================================
-- SNOWGUARD DATABASE SETUP
-- ============================================================

-- Create the main SnowGuard database
CREATE DATABASE IF NOT EXISTS SNOWGUARD_DB;

-- Use the database
USE DATABASE SNOWGUARD_DB;

-- Create project schemas
CREATE SCHEMA IF NOT EXISTS RAW;

CREATE SCHEMA IF NOT EXISTS PROCESSED;

CREATE SCHEMA IF NOT EXISTS QUALITY;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Verify
SHOW SCHEMAS;