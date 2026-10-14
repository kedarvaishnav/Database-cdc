# CDC Event Replay & Aggregation Pipeline

A data engineering project that reconstructs the final, accurate state of database records from a massive, out-of-order stream of **Change Data Capture (CDC)** logs using PySpark window functions and Apache Hive / Spark SQL.

## Problem

Database CDC events arrive out of order due to network delays, asynchronous pipelines, and distributed transactions. Multiple updates to the same record arrive at different times, and DELETE operations must properly purge records. This pipeline produces a **single source of truth**: the latest state per `record_id`, with zero duplicates.

## Algorithm

```sql
ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC, transaction_id DESC)
```

1. **Partition** events by `record_id` (groups all updates for that specific record).
2. **Order** by `timestamp` DESC and `transaction_id` DESC (places the newest event first).
3. **Filter** `row_number = 1` (selects only the latest event for each record).
4. **Purge** records where `operation = 'DELETE'`.
5. **Result:** Exactly one final state per active record, with 0 duplicates.

## Project Structure

```
src/
├── generate_cdc_data.py     # Generates 100,000 synthetic JSONL CDC logs
├── cdc_processor.py         # PySpark pipeline: JSONL → deduplicated Parquet
├── test_cdc_validation.py   # Edge-case validation tests (DELETE, UPDATE, out-of-order)
├── create_hive_tables.hql   # Hive / Spark SQL external table DDL
├── setup_hive.sh            # Warehouse directory setup
├── validate_cdc.hql         # 6 SQL validation queries
└── output/                  # Generated data (gitignored)
    ├── cdc_logs.jsonl
    └── final_state.parquet/
```

## Prerequisites

- **Python 3.8+** with `pyspark` (`pip install pyspark`)
- **Java 11+** (OpenJDK 11 installed in Ubuntu WSL)
- **Spark / Hive** (`spark-sql` or `hive` CLI)

## How to Run & Test Manually in Ubuntu WSL

All components run directly inside Ubuntu WSL:

### 1. Open WSL and Navigate to the Project
```bash
cd /home/kedar/database-cdc
```

### 2. Generate CDC Data (100,000 Events)
```bash
python3 src/generate_cdc_data.py
```
*Expected: 100,000 JSONL events generated in `src/output/cdc_logs.jsonl` with ~24 MB size.*

### 3. Run PySpark Deduplication Pipeline
```bash
python3 src/cdc_processor.py
```
*Expected: Reads 100,000 events, computes latest state using `ROW_NUMBER()`, writes deduplicated records to `src/output/final_state.parquet`, reports 0 duplicates in ~13 seconds.*

### 4. Run Automated Validation Tests
```bash
python3 src/test_cdc_validation.py
```
*Expected: 4 passed, 0 failed.*
- `test_delete_removes_record` (verifies deleted records do not appear)
- `test_update_keeps_latest` (verifies only the latest state is retained)
- `test_out_of_order_arrival` (verifies out-of-order events resolve correctly)
- `test_no_duplicates_in_real_output` (verifies 0 duplicates in real Parquet output)

### 5. Create Hive Table & Run Validation Queries

Set up warehouse directory:
```bash
bash src/setup_hive.sh
```

Create Hive external table over Parquet data:
```bash
spark-sql -f src/create_hive_tables.hql
```
*(Or `hive -f src/create_hive_tables.hql` if HDFS NameNode is active)*

Run the 6 validation queries:
```bash
spark-sql -f src/validate_cdc.hql
```
*Queries executed:*
1. Total Record Count (~34,767 records)
2. Duplicate Check (Total = 34,767, Unique = 34,767, Duplicates = 0)
3. Status Distribution (ACTIVE ~70%, INACTIVE ~20%, SUSPENDED ~10%)
4. High-Value Customers (Top 10 balances > $50,000)
5. Timestamp Range (Jan 1 to Jan 31, 2024)
6. Sample Record inspection

## Data Schema

### Input (`cdc_logs.jsonl`)
```json
{
  "record_id": "user_12345",
  "operation": "INSERT | UPDATE | DELETE",
  "payload": {
    "user_name": "Alice Smith",
    "email": "alice.smith123@example.com",
    "age": 45,
    "status": "ACTIVE | INACTIVE | SUSPENDED",
    "balance": 75432.50
  },
  "timestamp": "2024-01-15T10:30:45Z",
  "transaction_id": "txn_0012345678"
}
```

### Output (`final_state.parquet`)
| Column      | Type      | Description                    |
|-------------|-----------|--------------------------------|
| record_id   | STRING    | Unique user identifier         |
| user_name   | STRING    | Customer full name             |
| email       | STRING    | Email address                  |
| age         | INT       | Age (18–80)                    |
| status      | STRING    | ACTIVE, INACTIVE, or SUSPENDED |
| balance     | DOUBLE    | Account balance (0–100,000)    |
| timestamp   | TIMESTAMP | Timestamp of latest CDC event  |

## Team Contributions

| Member  | Branch           | Contribution                     |
|---------|------------------|----------------------------------|
| Kedar   | `feature/spark`  | Data generator + PySpark pipeline |
| Abel    | `feature/hive`   | Hive DDL + SQL validation queries |
| Harsh   | `feature/quality`| Tests + documentation             |
