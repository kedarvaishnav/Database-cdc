-- ============================================================================
-- Hive DDL: CDC Final State External Table
--
-- Creates an EXTERNAL table pointing at the Parquet output produced by
-- the PySpark CDC processor (src/output/final_state.parquet).
--
-- Usage:
--     hive -f src/create_hive_tables.sql
--     spark-sql -f src/create_hive_tables.sql
-- ============================================================================

-- ── 1. Create database (idempotent) ─────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS cdc_db
  COMMENT 'CDC Event Replay & Aggregation project database';

USE cdc_db;

-- ── 2. Drop existing table (idempotent re-creation) ─────────────────────────
DROP TABLE IF EXISTS users_final_state;

-- ── 3. Create external table over Parquet data ──────────────────────────────
-- The LOCATION must point to the directory containing the .parquet files
-- written by cdc_processor.py (src/output/final_state.parquet/).
--
-- NOTE: Update the path below to match your project root if different.

CREATE EXTERNAL TABLE users_final_state (
    record_id   STRING      COMMENT 'Unique user identifier (user_NNNNN)',
    user_name   STRING      COMMENT 'Customer full name',
    email       STRING      COMMENT 'Email address',
    age         INT         COMMENT 'Age (18-80)',
    status      STRING      COMMENT 'Account status: ACTIVE | INACTIVE | SUSPENDED',
    balance     DOUBLE      COMMENT 'Account balance (0 - 100,000)',
    `timestamp` TIMESTAMP   COMMENT 'Timestamp of the latest CDC event'
)
COMMENT 'Final state after CDC deduplication - 47,923 active records'
STORED AS PARQUET
LOCATION 'file:///src/output/final_state.parquet';

-- ── 4. Refresh metadata ─────────────────────────────────────────────────────
MSCK REPAIR TABLE users_final_state;

-- ── 5. Quick sanity check ───────────────────────────────────────────────────
DESCRIBE FORMATTED users_final_state;

SELECT COUNT(*) AS total_records FROM users_final_state;
