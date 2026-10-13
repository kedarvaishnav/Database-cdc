# Database CDC Project: Pipeline & Verification Guide

This guide details how to generate CDC data, run the PySpark reconciliation pipeline, register external Hive tables, and run validation queries and automated tests.

## Prerequisites
- Linux / WSL Ubuntu environment
- Python 3.10+ (with `.venv` configured)
- OpenJDK 11 or 21
- Apache Spark 3.5+ / PySpark 4.x
- Apache Hive 3.x / 4.x

## Pipeline Architecture
1. **Data Generation**: `python3 src/generate_cdc_data.py`
   - Generates 100,000 JSONL CDC events in `data/cdc_events.jsonl`
2. **State Reconciliation**: `python3 src/cdc_processor.py`
   - Applies `ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC)`
   - Discards DELETE tombstones and outputs 47,923 active records to `src/output/final_state.parquet`
3. **Hive Integration & Validation**: `python3 src/run_hive_validation.py`
   - Creates database `cdc_db` and external table `users_final_state`
   - Executes all 6 validation queries
4. **Test Suite**: `pytest tests/ -v`
   - Runs unit and edge case tests, idempotency checks, and performance benchmarks