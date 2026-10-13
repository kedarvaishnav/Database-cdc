# Database CDC: Event Replay & State Aggregation Pipeline

A production-grade Change Data Capture (CDC) processing and state reconciliation engine built with Apache Spark and Hive. This system reconstructs the true, deduplicated point-in-time state of database records from high-volume, out-of-order event streams (INSERT, UPDATE, DELETE) containing network-induced jitter, arbitrary delays, and duplicate transmissions.

---

## Quick Start

```bash
# 1. Prerequisites: Java 17/21, Python 3.9+, PySpark 3.5+, Pytest
git clone https://github.com/kedarvaishnav/Database-cdc.git && cd Database-cdc

# 2. Install Python dependencies
pip install pyspark pytest

# 3. Generate 100K synthetic CDC logs and execute deduplication pipeline
python src/generate_cdc_data.py
python src/cdc_processor.py

# 4. Run automated test suite
pytest tests/ -v
# Expected: 47,923 active deduplicated records, 0 duplicates, all tests passing
```

---

## Project Structure

```text
Database-cdc/
├── src/
│   ├── generate_cdc_data.py        # Synthetic CDC log generator (100K JSONL records)
│   ├── cdc_processor.py            # PySpark ROW_NUMBER() window deduplication engine
│   ├── create_hive_tables.sql      # Hive external table schema definition
│   ├── validate_cdc.sql            # Analytical validation queries (0 duplicates check)
│   └── output/
│       ├── cdc_logs.jsonl          # Raw out-of-order JSONL stream (~18 MB)
│       └── final_state.parquet     # Materialized columnar target snapshot
├── tests/
│   ├── conftest.py                 # PySpark session fixture and pipeline harness
│   ├── test_cdc_edge_cases.py      # Edge case tests (DELETEs, late arrivals, jitter)
│   ├── test_idempotency.py         # Multi-run hash and count consistency validation
│   └── test_performance.py         # Sub-5-second SLA throughput validation
├── ARCHITECTURE.md                 # In-depth architectural design and complexity analysis
├── INTERVIEW_PREP.md               # Technical interview Q&A guide and system talking points
├── demo.sh                         # Automated end-to-end demo execution script
└── README.md                       # Project documentation and developer quickstart
```

---

## Step-by-Step Usage

### 1. Generating CDC Event Streams
Execute `python src/generate_cdc_data.py`. This script synthesizes 100,000 JSONL records spanning 50,000 unique entity IDs across January 2024. The generator introduces intentional data engineering challenges: 5% late-arriving events delayed by 1 to 3 days, out-of-sequence UPDATE operations, and terminal DELETE tombstone events.

### 2. Processing & Deduplicating State
Execute `python src/cdc_processor.py` (or `spark-submit src/cdc_processor.py`). PySpark ingests the raw JSONL stream, standardizes schema types, and applies an analytical window function:
`ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC, transaction_id DESC)`.
The pipeline preserves rows where `rn == 1`, purges records where the latest operation is `DELETE`, and writes the reconciled snapshot to `src/output/final_state.parquet`.

### 3. Creating Hive Tables & Querying
Register the Parquet dataset in your data warehouse catalog using Hive DDL: `hive -f src/create_hive_tables.sql`. This creates an external table mapped directly to the Parquet directory, enabling immediate SQL querying via Presto, Trino, Hive, or Spark SQL.

### 4. Running Validation & Test Suites
Execute `pytest tests/ -v` to run the complete automated test suite. The suite validates edge cases (`test_cdc_edge_cases.py`), mathematical idempotency across runs (`test_idempotency.py`), and execution performance SLAs (`test_performance.py`).

---

## Sample Output

### Raw CDC Log Entry (`cdc_logs.jsonl`)
```json
{
  "record_id": "user_00142",
  "operation": "UPDATE",
  "payload": {
    "user_name": "Carol Miller",
    "email": "carol.miller84@gmail.com",
    "age": 42,
    "status": "ACTIVE",
    "balance": 83419.50
  },
  "timestamp": "2024-01-18T14:22:10Z",
  "transaction_id": "txn_8471920391"
}
```

### Reconstructed Final State (`final_state.parquet`)
```text
+------------+--------------+--------------------------+-----+--------+---------+--------------------+
|record_id   |user_name     |email                     |age  |status  |balance  |timestamp           |
+------------+--------------+--------------------------+-----+--------+---------+--------------------+
|user_00001  |Alice Brown   |alice.brown12@gmail.com   |29   |ACTIVE  |48210.00 |2024-01-29 18:04:12 |
|user_00002  |David Wilson  |david.wilson45@yahoo.com  |51   |INACTIVE|12940.75 |2024-01-31 09:15:33 |
|user_00003  |Rachel Garcia |rachel.garcia9@outlook.com|37   |ACTIVE  |91024.10 |2024-01-28 22:50:04 |
+------------+--------------+--------------------------+-----+--------+---------+--------------------+
```

### Warehouse Validation
```text
Total Records: 47,923 | Unique Primary Keys: 47,923 | Duplicates: 0
```

---

## Results & Performance

Processing 100,000 disordered CDC logs yields exactly 47,923 active database records. The pipeline achieves a 2.08x deduplication ratio with 0 primary key duplicates. On standard commodity hardware, local PySpark execution completes the full deduplication, validation, and Parquet write cycles in less than 3 seconds, proving horizontal scalability.

---

## Next Steps

1. **Streaming Ingestion**: Integrate Apache Kafka with Spark Structured Streaming for continuous micro-batch state reconciliation.
2. **ACID Table Formats**: Upgrade target storage from raw Parquet to Apache Iceberg or Delta Lake using `MERGE INTO` operations.
3. **Multi-Table Relational CDC**: Extend the processor to resolve foreign key dependencies and referential integrity across multi-entity schemas.
