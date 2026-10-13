-- ========================================================
-- CDC Validation Queries (Task 2.2)
-- Verifies the reconstructed state across volume, uniqueness,
-- distributions, and field integrity.
-- ========================================================

-- Query 1: Total record count
SELECT COUNT(*) as total_records FROM cdc_db.users_final_state;

-- Query 2: Duplicate check
SELECT 
    COUNT(*) as total, 
    COUNT(DISTINCT record_id) as unique_ids, 
    (COUNT(*) - COUNT(DISTINCT record_id)) as duplicates 
FROM cdc_db.users_final_state;

-- Query 3: Status distribution
SELECT status, COUNT(*) as count 
FROM cdc_db.users_final_state 
GROUP BY status 
ORDER BY count DESC;

-- Query 4: High-value customers
SELECT record_id, user_name, email, balance 
FROM cdc_db.users_final_state 
WHERE balance > 50000 
ORDER BY balance DESC 
LIMIT 10;

-- Query 5: Timestamp range
SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest 
FROM cdc_db.users_final_state;

-- Query 6: Sample record
SELECT * FROM cdc_db.users_final_state 
LIMIT 1;