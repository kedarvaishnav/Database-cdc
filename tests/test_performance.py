"""
Performance SLA test for the CDC pipeline.

Proves pipeline scalability by measuring execution duration
and asserting duration < 5.0 seconds for synthetic batch processing.
"""

import time
import pytest
from pyspark.sql import SparkSession
from tests.conftest import process_cdc_records


def _generate_performance_batch(num_records: int = 5000) -> list:
    """Generate synthetic records for performance benchmarking."""
    records = []
    for i in range(1, num_records + 1):
        rec_id = f"perf_user_{i % 500:04d}"
        op = "INSERT" if i <= 500 else ("DELETE" if i % 20 == 0 else "UPDATE")
        records.append({
            "record_id": rec_id,
            "operation": op,
            "payload": {
                "user_name": f"Perf User {rec_id}",
                "email": f"{rec_id}@company.com",
                "age": 25 + (i % 40),
                "status": "ACTIVE" if i % 3 != 0 else "INACTIVE",
                "balance": float(50.0 * (i % 100)),
            },
            "timestamp": f"2024-01-{(i % 28) + 1:02d}T12:00:00Z",
            "transaction_id": f"txn_perf_{i:06d}",
        })
    return records


def test_performance(spark: SparkSession):
    """
    Test: test_performance()
    Logic:
      a) Time cdc_processor execution
      b) Measure: start_time, end_time
      c) Duration = end_time - start_time
      d) Assert: duration < 5 seconds
    Why: Proves scalability (in-memory partition processing < 5s)
    """
    records = _generate_performance_batch(5000)

    # Warmup action on spark session
    spark.range(1).count()

    start_time = time.time()
    result_df = process_cdc_records(spark, records)
    total_output = result_df.count()
    end_time = time.time()

    duration = end_time - start_time

    print(f"\n  [PERF METRICS] Processed {len(records):,} input records -> {total_output:,} deduplicated records")
    print(f"  [PERF METRICS] Execution duration: {duration:.3f} seconds (SLA: < 5.0s)")

    assert duration < 5.0, f"Processing took {duration:.2f}s, exceeding 5.0s SLA!"
    assert total_output > 0, "Pipeline should return deduplicated records"
