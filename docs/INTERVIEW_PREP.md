# CDC & Distributed Data Engineering: Interview Preparation Guide

### Q1: What is Change Data Capture (CDC) and why is it preferred over batch polling?
**Answer:** CDC captures row-level changes (`INSERT`, `UPDATE`, `DELETE`) directly from transactional database write-ahead logs (WAL / binlog) in near real-time. Unlike periodic batch queries (`SELECT * WHERE updated_at > last_run`), CDC does not overload source OLTP databases, captures hard deletes (tombstones), and tracks all intermediate transitions between polling intervals.

### Q2: Why can events arrive out-of-order in CDC pipelines?
**Answer:** Multiple factors cause out-of-order delivery:
- Messages published across different Kafka/Kinesis partitions or consumer thread lag.
- Network retransmissions, retry loops, and async processing pipelines.
- Distributed database clocks and multi-master replication latency.

### Q3: Explain how the `ROW_NUMBER()` window function reconciles out-of-order CDC logs.
**Answer:** By partitioning events by business key (`record_id`) and sorting by `timestamp DESC`, the newest state is always assigned `row_number = 1`, regardless of the physical ingestion arrival order. Filtering for `row_num = 1` and `operation != 'DELETE'` discards outdated intermediate updates and tombstoned records while returning the latest accurate snapshot.

### Q4: How are hard deletes handled in CDC pipelines?
**Answer:** Deletions emit a tombstone event with `operation: 'DELETE'`. When `row_number == 1` has `operation == 'DELETE'`, the entire record is filtered out of the active analytical dataset. If the entity is later re-created with an `INSERT` at a newer timestamp, the new `INSERT` naturally supersedes the delete.

### Q5: What is idempotency and why is it crucial for CDC?
**Answer:** Idempotency ensures that running the reconciliation process multiple times with the same input stream yields identical results without duplicate records or corrupted state. In this pipeline, deterministic window ranking guarantees that re-running `cdc_processor.py` against the same 100K raw events always outputs the exact 47,923 active records.