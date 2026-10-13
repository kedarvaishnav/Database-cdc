# Database CDC (Change Data Capture) Pipeline 🚀

Production-grade Change Data Capture (CDC) event replay and state reconciliation pipeline built with **PySpark**, **Apache Hive**, and **PyTest**.

## 📌 Problem & Architecture
In distributed systems and microservices, database event streams (`INSERT`, `UPDATE`, `DELETE`) frequently arrive **out-of-order** with duplicates due to asynchronous message brokers (Kafka/Kinesis), retry loops, and network partitions.

This project reconstructs the exact single source of truth for **100,000 raw CDC events**, reconciling them into **47,923 active records** (a 2.08x deduplication ratio) stored in Parquet and queryable through Apache Hive.

### Deduplication Algorithm
```sql
ROW_NUMBER() OVER (
    PARTITION BY record_id 
    ORDER BY timestamp DESC, transaction_id DESC
)
```
- Groups events by `record_id` and orders by `timestamp DESC`.
- Picks `row_number == 1`.
- Filters out records where `operation == 'DELETE'` (tombstones).

---

## 📁 Repository Structure
```
Database-cdc/
├── data/
│   └── cdc_events.jsonl            # 100K raw CDC events (JSON Lines)
├── src/
│   ├── generate_cdc_data.py        # Generates deterministic 100K JSONL events
│   ├── cdc_processor.py            # PySpark window deduplication engine
│   ├── setup_hive.sh               # Warehouse & environment configuration
│   ├── create_hive_tables.sql      # Hive DDL for external table
│   ├── validate_cdc.sql            # 6 analytical verification queries
│   ├── run_hive_validation.py      # Automated runner for Hive DDL & queries
│   └── output/
│       └── final_state.parquet     # 47,923 active deduplicated records
├── tests/
│   ├── test_cdc_edge_cases.py      # 4 edge case tests (out-of-order, deletes)
│   ├── test_idempotency.py         # Idempotency proof across multiple runs
│   └── test_performance.py        # Processing performance threshold test
├── docs/
│   ├── ARCHITECTURE.md             # Technical deep dive into CDC design
│   ├── INTERVIEW_PREP.md           # CDC and Data Engineering interview Q&A
│   └── README.md                   # Supplemental reference guide
├── demo.sh                         # Automated end-to-end execution script
└── pytest.ini                      # PyTest configuration
```

---

## ⚡ Quickstart (One Command)
Run the entire pipeline from end to end:
```bash
./demo.sh
```

---

## 🛠️ Step-by-Step Execution

### 1. Environment & Warehouse Setup
```bash
bash src/setup_hive.sh
```

### 2. Generate 100,000 CDC Events
```bash
.venv/bin/python3 src/generate_cdc_data.py
```
Outputs `data/cdc_events.jsonl` with 100,000 JSON lines (~25 MB).

### 3. Reconcile State with PySpark
```bash
.venv/bin/python3 src/cdc_processor.py
```
Reads the 100K raw events, executes window deduplication, discards DELETE tombstones, and outputs **47,923 records** to `src/output/final_state.parquet`.

### 4. Create Hive External Table & Run Validation Queries
```bash
.venv/bin/python3 src/run_hive_validation.py
```
Executes `src/create_hive_tables.sql` and the 6 queries in `src/validate_cdc.sql`:
1. **Total record count**: Exactly `47,923`
2. **Duplicate check**: Exactly `0` duplicates (`total = unique`)
3. **Status distribution**: Counts across `ACTIVE`, `INACTIVE`, `SUSPENDED`
4. **High-value customers**: Top 10 users with `balance > 50,000`
5. **Timestamp range**: Earliest and latest timestamps within Jan 2024
6. **Sample record**: Complete record verification with all 7 columns

### 5. Run Automated Tests
```bash
.venv/bin/pytest tests/ -v
```

---

## 📊 Verification & Validation Results
- **Raw CDC Events**: 100,000
- **Final Active Records**: 47,923
- **Duplicates**: 0
- **Test Suite**: 6 passed in ~17s