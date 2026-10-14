USE cdc_db;

-- 1. Total record count
SELECT COUNT(*) AS total_records FROM users_final_state;

-- 2. Duplicate check (expected: duplicates = 0)
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT record_id) AS unique_ids,
    COUNT(*) - COUNT(DISTINCT record_id) AS duplicates
FROM users_final_state;

-- 3. Status distribution
SELECT 
    status,
    COUNT(*) AS count
FROM users_final_state
GROUP BY status
ORDER BY count DESC;

-- 4. High-value customers (balance > 50,000)
SELECT 
    record_id,
    user_name,
    email,
    balance
FROM users_final_state
WHERE balance > 50000
ORDER BY balance DESC
LIMIT 10;

-- 5. Timestamp range (Jan 1 - Jan 31, 2024)
SELECT 
    MIN(`timestamp`) AS earliest,
    MAX(`timestamp`) AS latest
FROM users_final_state;

-- 6. Sample record
SELECT * FROM users_final_state LIMIT 5;
