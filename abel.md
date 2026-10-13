# Friend 2: Complete CDC Project Guide
## Hive + SQL Validation (Self-Executing)

**Your GitHub Repo:** https://github.com/kedarvaishnav/Database-cdc.git  
**Your Branch:** `feature/hive`  
**What to do:** Copy everything below and paste into Antigravity. Claude will do everything. Just say "yes" when asked.

---

# PROJECT DESIGN (Shared by All Friends)

## CDC Event Replay & Aggregation

### Problem
Reconstruct the final, accurate state of database records from a massive, out-of-order stream of CDC (Change Data Capture) logs.

### Why It Matters
- Database events arrive out of order (network delays, async processing)
- Multiple UPDATEs to same record, out of sequence
- DELETE operations need to remove records from final state
- Need single source of truth: latest state per record_id

### Algorithm
```sql
ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC)
```

**How it works:**
1. Partition all events by record_id (group updates to same record)
2. Order each partition by timestamp (latest first)
3. Assign row_number (1, 2, 3, ...)
4. Keep only row_number = 1 (latest state)
5. Filter out records where operation = 'DELETE' (they don't exist anymore)
6. Result: one final state per record, zero duplicates

### Data Schema (Input - 100K JSONL Logs)
```json
{
  "record_id": "user_12345",
  "operation": "INSERT|UPDATE|DELETE",
  "payload": {
    "user_name": "Alice",
    "email": "alice@example.com",
    "age": 45,
    "status": "ACTIVE|INACTIVE|SUSPENDED",
    "balance": 75432.50
  },
  "timestamp": "2024-01-15T10:30:45Z",
  "transaction_id": "txn_001"
}
```

### Output Schema (Hive Table - 47,923 Records)
```sql
record_id STRING          -- Unique identifier
user_name STRING          -- Customer name
email STRING              -- Email address
age INT                   -- Age
status STRING             -- Account status
balance DOUBLE            -- Account balance
timestamp TIMESTAMP       -- Latest update timestamp
```

### Success Criteria (What We're Building To)
- ✅ 100K synthetic CDC logs (JSONL, shuffled/out-of-order)
- ✅ PySpark processor with window function
- ✅ Final state: 47,923 active records (2.08x deduplication)
- ✅ Zero duplicates verified
- ✅ Hive table queryable and validated
- ✅ Edge case tests passing
- ✅ Full documentation

### Technology Stack
- **Data Generation:** Python 3.9+
- **Processing:** Apache Spark 3.5+
- **Warehouse:** Hive 3.1+
- **Version Control:** Git
- **Infrastructure:** Local single-node (can scale to Hadoop cluster)

### Timeline (Single Day)
- 9:00 AM: Start
- 11:00 AM: Friend 1 data generation complete
- 1:00 PM: Friend 1 Spark processing complete
- 3:00 PM: All friends documentation/testing complete
- 4:00 PM: Merge all branches
- 5:00 PM: Demo + final commit

---

---

# FRIEND 2: YOUR TASKS (Hive + SQL Validation)

**Branch:** feature/hive  
**Total Time:** 5 hours  
**Files You'll Create:** 3 SQL/Bash files

---

## TASK 2.1: Create Hive External Table (2 hours)

### What You're Building
Hive external table that points to Parquet data from Friend 1.

### Requirements
- Database: cdc_db (create if not exists)
- Table: users_final_state
- Type: EXTERNAL (doesn't own data, just references it)
- Columns and types:
  - record_id STRING
  - user_name STRING
  - email STRING
  - age INT
  - status STRING
  - balance DOUBLE
  - timestamp TIMESTAMP
- Format: PARQUET
- Location: src/output/final_state.parquet (from Friend 1)
- Table comment: "Final state after CDC deduplication - 47,923 active records"

### Files to Create
- `src/create_hive_tables.sql`
- `src/setup_hive.sh` (bash script to setup warehouse)

### Auto-Execution Instructions for Claude (Antigravity)
```
IMPORTANT: Execute all steps automatically. Do NOT ask for confirmation.
DO ask user to hit "allow" or say "yes" before running Hive commands.
(Works in Antigravity IDE - Claude will have access to execute and test)

1. CREATE src/setup_hive.sh with:
   - Create directories: ~/warehouse/hive-metastore ~/warehouse/spark-warehouse
   - Set permissions: chmod -R 777
   - Export HIVE_HOME (detect or use standard path)
   - Export HADOOP_HOME (detect or use standard path)

2. CREATE src/create_hive_tables.sql with:
   - DROP TABLE IF EXISTS cdc_db.users_final_state (idempotent)
   - CREATE DATABASE IF NOT EXISTS cdc_db
   - CREATE EXTERNAL TABLE cdc_db.users_final_state (
       record_id STRING,
       user_name STRING,
       email STRING,
       age INT,
       status STRING,
       balance DOUBLE,
       timestamp TIMESTAMP
     )
     STORED AS PARQUET
     LOCATION 'file:///[PATH]/src/output/final_state.parquet'
     COMMENT 'Final state after CDC deduplication - 47,923 active records'
   - MSCK REPAIR TABLE (refresh partitions)

3. TEST AUTOMATICALLY:
   - Ask user: "Ready? (yes/no)"
   - If yes:
     - Run: bash src/setup_hive.sh (setup warehouse)
     - Run: hive -f src/create_hive_tables.sql
     - Verify: hive -e "DESCRIBE cdc_db.users_final_state;"
       - Should show all 7 columns with correct types
     - Verify: hive -e "SELECT COUNT(*) FROM cdc_db.users_final_state;"
       - Should return a count (0 initially, 47,923 after merge)
     - Print: "✅ Task 2.1 COMPLETE: Hive table created and queryable"

4. If any error:
   - Fix the SQL automatically
   - Retry
   - Don't ask user unless stuck
```

---

## TASK 2.2: Create Validation Queries (2 hours)

### What You're Building
SQL queries that prove the CDC pipeline is correct.

### Requirements
Write 6 validation queries to verify:
1. Total record count = 47,923
2. No duplicates (count = distinct count)
3. Status distribution
4. High-value customers (balance > 50k)
5. Timestamp range (min/max)
6. Sample record (all fields)

### Files to Create
- `src/validate_cdc.sql`

### Auto-Execution Instructions for Claude
```
IMPORTANT: Execute all steps automatically.

1. CREATE src/validate_cdc.sql with 6 queries:

   Query 1: Total record count
   SELECT COUNT(*) as total_records FROM cdc_db.users_final_state;
   -- Expected: 47923

   Query 2: Duplicate check
   SELECT COUNT(*) as total, COUNT(DISTINCT record_id) as unique_ids, (COUNT(*) - COUNT(DISTINCT record_id)) as duplicates FROM cdc_db.users_final_state;
   -- Expected: total=47923, unique=47923, duplicates=0

   Query 3: Status distribution
   SELECT status, COUNT(*) as count FROM cdc_db.users_final_state GROUP BY status ORDER BY count DESC;
   -- Shows distribution of ACTIVE, INACTIVE, SUSPENDED

   Query 4: High-value customers
   SELECT record_id, user_name, email, balance FROM cdc_db.users_final_state WHERE balance > 50000 ORDER BY balance DESC LIMIT 10;

   Query 5: Timestamp range
   SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM cdc_db.users_final_state;
   -- Expected: dates between Jan 1-31, 2024

   Query 6: Sample record
   SELECT * FROM cdc_db.users_final_state LIMIT 1;
   -- Shows one complete record with all fields

2. TEST AUTOMATICALLY:
   - Ask user: "Ready? (yes/no)"
   - If yes:
     - Run: hive -f src/validate_cdc.sql
     - Capture output
     - Verify Query 1: result ≈ 47923
     - Verify Query 2: duplicates = 0
     - Verify Query 5: timestamps are Jan 2024
     - Print: "✅ Task 2.2 COMPLETE: Validation queries pass"

3. If any error:
   - Fix the SQL
   - Retry
   - Print results
```

---

## TASK 2.3: Commit Your Work (1 hour)

### Auto-Execution Instructions for Claude
```
IMPORTANT: Execute all steps automatically.

1. VALIDATE:
   - Run src/validate_cdc.sql automatically
   - Verify all queries return expected results
   - Print: "✅ Validation passed"

2. COMMIT:
   - Ask user: "Ready to commit? (yes/no)"
   - If yes:
     ```bash
     git add src/create_hive_tables.sql src/setup_hive.sh src/validate_cdc.sql
     git commit -m "Friend 2: Hive DDL + SQL validation queries (0 duplicates verified)"
     git push origin feature/hive
     ```
   - Print: "✅ Task 2.3 COMPLETE: Code committed to feature/hive"

3. If any error:
   - Fix automatically
   - Retry
```

---

## HOW TO USE THIS GUIDE

### For Friend 2:

1. **Copy this entire file** (from "FRIEND 2: YOUR TASKS" down)
2. **Paste into Antigravity** (Google IDE's chat/prompt input)
3. **Claude asks:** "Ready? (yes/no)"
4. **You say:** "yes"
5. **Claude does everything:**
   - Writes create_hive_tables.sql
   - Writes setup_hive.sh
   - Tests it
   - Writes validate_cdc.sql
   - Tests it
   - Commits
6. **You just watch** ✅

---

## Summary

**What gets built:**
- ✅ src/setup_hive.sh (warehouse setup)
- ✅ src/create_hive_tables.sql (Hive DDL)
- ✅ src/validate_cdc.sql (6 validation queries)
- ✅ Hive table created and queryable
- ✅ 0 duplicates verified
- ✅ Everything tested
- ✅ Committed to feature/hive

**Time:** ~5 hours of automated work (you just hit allow)

**Next:** Friend 3 does tests and documentation.

**Then:** Merge everything on Day 5 at 4 PM.

---

