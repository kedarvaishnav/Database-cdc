-- ============================================================================
-- CDC Validation Queries
--
-- Six queries that prove the CDC pipeline produced correct, deduplicated data.
-- Run after loading Parquet data into the Hive table.
--
-- Usage:
--     hive -f src/validate_cdc.sql
--     spark-sql -f src/validate_cdc.sql
-- ============================================================================

USE cdc_db;

-- ─────────────────────────────────────────────────────────────────────────────
-- Query 1: Total record count
-- Expected: ~47,923
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 1: Total Record Count ===' AS label;
SELECT COUNT(*) AS total_records
FROM   users_final_state;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 2: Duplicate check (total vs distinct record_ids)
-- Expected: duplicates = 0
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 2: Duplicate Check ===' AS label;
SELECT COUNT(*)                   AS total,
       COUNT(DISTINCT record_id)  AS unique_ids,
       COUNT(*) - COUNT(DISTINCT record_id) AS duplicates
FROM   users_final_state;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 3: Status distribution
-- Shows count of ACTIVE, INACTIVE, SUSPENDED accounts
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 3: Status Distribution ===' AS label;
SELECT   status,
         COUNT(*)  AS count
FROM     users_final_state
GROUP BY status
ORDER BY count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 4: High-value customers (balance > 50,000)
-- Top 10 by balance
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 4: High-Value Customers ===' AS label;
SELECT   record_id,
         user_name,
         email,
         balance
FROM     users_final_state
WHERE    balance > 50000
ORDER BY balance DESC
LIMIT    10;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 5: Timestamp range (min / max)
-- Expected: dates between Jan 1-31, 2024
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 5: Timestamp Range ===' AS label;
SELECT MIN(`timestamp`) AS earliest,
       MAX(`timestamp`) AS latest
FROM   users_final_state;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 6: Sample record (all fields)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT '=== Query 6: Sample Record ===' AS label;
SELECT *
FROM   users_final_state
LIMIT  1;
