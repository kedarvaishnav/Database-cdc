# CDC Event Replay & Aggregation Pipeline

A production-grade data engineering project that reconstructs the final, accurate state of database records from a massive, out-of-order stream of **Change Data Capture (CDC)** logs using PySpark window functions and Apache Hive / Spark SQL.

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
├── ARCHITECTURE.md          # In-depth technical architecture and design document
├── README.md                # Project documentation and execution instructions
├── demo.sh                  # Automated end-to-end demonstration script
├── src/
│   ├── generate_cdc_data.py # Generates 100,000 synthetic JSONL CDC logs
│   ├── cdc_processor.py     # PySpark pipeline: JSONL → deduplicated Parquet
│   ├── test_cdc_validation.py # Verification tests for CDC deduplication
│   ├── create_hive_tables.hql # Hive / Spark SQL external table DDL
│   ├── setup_hive.sh        # Warehouse directory setup
│   ├── validate_cdc.hql     # 6 SQL validation queries
│   └── output/              # Generated data (gitignored)
│       ├── cdc_logs.jsonl
│       └── final_state.parquet/
└── tests/
    ├── conftest.py          # Pytest shared fixtures and memory pipeline
    ├── test_cdc_edge_cases.py # Edge-case suite: DELETE, late arrivals, out-of-order
    ├── test_idempotency.py  # Idempotency validation (Run 1 == Run 2)
    └── test_performance.py  # Execution duration & throughput benchmarking
```

## Prerequisites

- **Python 3.8+** with `pyspark` and `pytest` (`pip install pyspark pytest`)
- **Java 11+** (OpenJDK 11 installed in Ubuntu WSL)
- **Spark / Hive** (`spark-sql` or `hive` CLI)

## How to Run & Test in Ubuntu WSL

All components run directly inside Ubuntu WSL:

### 1. Open WSL and Navigate to the Project
```bash
wsl -d Ubuntu
cd /home/kedar/database-cdc
```

### 2. Run the Automated Demo Script
```bash
./demo.sh
```
*(Executes data generation, PySpark processing, test suites, warehouse setup, and SQL queries automatically)*

### 3. Step-by-Step Manual Execution

#### Step 3.1: Generate CDC Data (100,000 Events)
```bash
python3 src/generate_cdc_data.py
```
*Expected: 100,000 JSONL events generated in `src/output/cdc_logs.jsonl` with ~24.5 MB size.*

#### Step 3.2: Run PySpark Deduplication Pipeline
```bash
python3 src/cdc_processor.py
```
*Expected: Reads 100,000 events, computes latest state using `ROW_NUMBER()`, writes deduplicated records to `src/output/final_state.parquet`, reports 0 duplicates in ~13 seconds.*

#### Step 3.3: Run Quality & Edge-Case Test Suites
```bash
pytest tests/ -v
```
*Expected: 6 passed in ~17 seconds.*
- `test_delete_semantics` (verifies deleted records do not appear in output)
- `test_late_arrivals` (verifies older late-arriving events do not overwrite newer state)
- `test_out_of_order_updates` (verifies shuffled updates resolve to newest timestamp)
- `test_no_duplicates` (verifies duplicate event transmissions produce 1 record)
- `test_idempotency` (verifies repeated executions yield identical records)
- `test_performance` (verifies high throughput and execution within SLA)

#### Step 3.4: Create Warehouse Table & Run Analytics Queries
```bash
bash src/setup_hive.sh
spark-sql -f src/create_hive_tables.hql
spark-sql -f src/validate_cdc.hql
```
*Queries executed:*
1. Total Record Count (`34,767` records)
2. Duplicate Check (`total = 34,767`, `unique = 34,767`, `duplicates = 0`)
3. Status Distribution (`ACTIVE ~70%`, `INACTIVE ~20%`, `SUSPENDED ~10%`)
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

| Member        | Branch            | Contribution                                             |
|---------------|-------------------|----------------------------------------------------------|
| Kedar         | `feature/spark`   | Data generator + PySpark window deduplication pipeline   |
| Abel          | `feature/hive`    | Hive / Spark SQL DDL + validation queries                |
| HarshKumar133 | `feature/quality` | Edge-case, idempotency, performance tests + architecture |
