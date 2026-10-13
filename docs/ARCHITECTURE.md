# CDC Architecture & Technical Deep Dive

## 1. System Overview
Change Data Capture (CDC) enables streaming and capturing row-level changes from transactional databases (PostgreSQL, MySQL, Oracle) and reconciling those mutations downstream in analytical platforms (Data Lakes / Warehouses).

```
+------------------------+      +-----------------------+      +------------------------+
| 100K Raw CDC Events    | ---> | PySpark Window Engine | ---> | 47,923 Active Parquet  |
| (JSONL / Out-of-order) |      | ROW_NUMBER() Dedup    |      | (Hive External Table)  |
+------------------------+      +-----------------------+      +------------------------+
```

## 2. The Reconciliation Problem
In real-world distributed architectures:
- Events arrive out-of-order over network boundaries and partitioned Kafka/Kinesis topics.
- Multiple `UPDATE` events can occur on the same record.
- Network retries produce duplicate events.
- `DELETE` events act as tombstones that must purge preceding `INSERT` and `UPDATE` records.

## 3. Algorithm: PySpark Window Function
We implement state reconciliation using:
```sql
ROW_NUMBER() OVER (
    PARTITION BY record_id 
    ORDER BY timestamp DESC, transaction_id DESC
)
```

### Steps:
1. **Partitioning**: Events are partitioned by primary key (`record_id`).
2. **Ordering**: Within each partition, events are sorted descending by `timestamp` (with `transaction_id` as a deterministic tie-breaker).
3. **Filtering**: Retain only records with `row_number == 1` and where `operation != 'DELETE'`.
4. **Schema Flattening**: Nested JSON payloads are extracted and cast into strongly-typed columnar schemas (`STRING`, `INT`, `DOUBLE`, `TIMESTAMP`).

## 4. Hive External Table Integration
The output dataset is stored in columnar Parquet format at `src/output/final_state.parquet`.
An external Hive table (`cdc_db.users_final_state`) is mounted on top of the Parquet directory, decoupling compute from storage and allowing instant analytical querying.

## 5. Performance and Scaling Considerations
- **Partitions**: Shuffle partition tuning for local execution vs distributed cluster execution.
- **Tombstones**: Filtering `operation == 'DELETE'` at `row_number == 1` guarantees deleted accounts do not resurface, while properly preserving re-created accounts if an `INSERT` arrives with a later timestamp.