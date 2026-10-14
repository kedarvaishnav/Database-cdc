"""
Idempotency test for the CDC pipeline.

Proves that running the CDC deduplication pipeline multiple times
on identical input data produces identical output records (no duplicate
processing, deterministic ordering, and row-by-row consistency).
"""

import hashlib
import json
import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tests.conftest import process_cdc_records


def _generate_test_dataset(num_records: int = 150) -> list:
    """Generate deterministic synthetic CDC records for testing."""
    records = []
    for i in range(1, num_records + 1):
        rec_id = f"user_{i % 25:03d}"
        op = "INSERT" if i <= 25 else ("DELETE" if i % 10 == 0 else "UPDATE")
        records.append({
            "record_id": rec_id,
            "operation": op,
            "payload": {
                "user_name": f"User {rec_id}",
                "email": f"{rec_id}@example.com",
                "age": 20 + (i % 50),
                "status": "ACTIVE" if i % 2 == 0 else "INACTIVE",
                "balance": float(100 * (i % 30)),
            },
            "timestamp": f"2024-01-{(i % 28) + 1:02d}T10:00:00Z",
            "transaction_id": f"txn_{i:05d}",
        })
    return records


def _compute_dataframe_checksum(df) -> tuple[int, str]:
    """Compute row count and deterministic hash of all rows in DataFrame."""
    rows = df.orderBy("record_id").collect()
    hasher = hashlib.sha256()
    for row in rows:
        row_str = "|".join(str(val) for val in row.asDict().values())
        hasher.update(row_str.encode("utf-8"))
    return len(rows), hasher.hexdigest()


def test_idempotency(spark: SparkSession):
    """
    Test: test_idempotency()
    Logic:
      a) Run CDC processing on same data TWICE
      b) Save output1.parquet (first run)
      c) Save output2.parquet (second run)
      d) Compare:
         - output1.count() == output2.count()
         - Row-by-row comparison (checksums)
      e) Assert: Both outputs are identical
    """
    tmp_dir = tempfile.mkdtemp(prefix="cdc_idempotency_")
    out1_path = os.path.join(tmp_dir, "output1.parquet")
    out2_path = os.path.join(tmp_dir, "output2.parquet")

    try:
        records = _generate_test_dataset(150)

        # Run 1
        df1 = process_cdc_records(spark, records)
        df1.write.mode("overwrite").parquet(out1_path)

        # Run 2 on identical data
        df2 = process_cdc_records(spark, records)
        df2.write.mode("overwrite").parquet(out2_path)

        # Read both outputs
        res1 = spark.read.parquet(out1_path)
        res2 = spark.read.parquet(out2_path)

        count1 = res1.count()
        count2 = res2.count()

        assert count1 > 0, "Output DataFrame should contain records"
        assert count1 == count2, f"Count mismatch: run 1 has {count1}, run 2 has {count2}"

        # Row-by-row comparison via SHA-256 checksums
        count1_rows, hash1 = _compute_dataframe_checksum(res1)
        count2_rows, hash2 = _compute_dataframe_checksum(res2)

        assert hash1 == hash2, f"Hash mismatch between run 1 ({hash1}) and run 2 ({hash2})"

        # Also verify symmetric difference is empty
        diff1 = res1.subtract(res2).count()
        diff2 = res2.subtract(res1).count()
        assert diff1 == 0 and diff2 == 0, f"Symmetric diff not zero: diff1={diff1}, diff2={diff2}"

        print(f"\n  [PASS] test_idempotency verified: {count1} records, hash={hash1[:12]}...")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
