"""
Edge-case tests for the CDC pipeline.

Validates:
  1. test_delete_semantics: DELETE operations properly eliminate records from final state.
  2. test_late_arrivals: Older events arriving days later do not overwrite newer state.
  3. test_out_of_order_updates: Shuffled updates resolve to the newest timestamp state.
  4. test_no_duplicates: Identical transactions or repeated events produce exactly 1 record.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
try:
    from tests.conftest import process_cdc_records
except ImportError:
    from conftest import process_cdc_records


def _create_record(record_id: str, op: str, ts: str, txn: str, balance: float = 1000.0, status: str = "ACTIVE") -> dict:
    return {
        "record_id": record_id,
        "operation": op,
        "payload": {
            "user_name": f"User {record_id}",
            "email": f"{record_id}@example.com",
            "age": 30,
            "status": status,
            "balance": balance,
        },
        "timestamp": ts,
        "transaction_id": txn,
    }


def test_delete_semantics(spark: SparkSession):
    """
    Test 1: test_delete_semantics()
    Lifecycle: INSERT user_1 -> UPDATE user_1 -> DELETE user_1
    Expected: user_1 NOT in output
    Assert: output_df.filter(col('record_id') == 'user_1').count() == 0
    """
    records = [
        _create_record("user_1", "INSERT", "2024-01-01T10:00:00Z", "txn_001", balance=500.0),
        _create_record("user_1", "UPDATE", "2024-01-02T10:00:00Z", "txn_002", balance=750.0),
        _create_record("user_1", "DELETE", "2024-01-03T10:00:00Z", "txn_003", balance=750.0),
        # Add background padding records to simulate 10-50 records batch
        *[_create_record(f"user_pad_{i}", "INSERT", "2024-01-01T08:00:00Z", f"txn_pad_{i}") for i in range(10)],
    ]

    output_df = process_cdc_records(spark, records)
    assert output_df.filter(col("record_id") == "user_1").count() == 0


def test_late_arrivals(spark: SparkSession):
    """
    Test 2: test_late_arrivals()
    Scenario: UPDATE at T (newer), UPDATE at T-3days (older, late arrival)
    Expected: T (newer) wins
    Assert: output has newer values
    """
    records = [
        # Base insert
        _create_record("user_late", "INSERT", "2024-01-01T10:00:00Z", "txn_100", balance=100.0, status="INACTIVE"),
        # Newer update at T (2024-01-15)
        _create_record("user_late", "UPDATE", "2024-01-15T12:00:00Z", "txn_102", balance=9999.0, status="ACTIVE"),
        # Older event arriving late with timestamp T-3 days (2024-01-12)
        _create_record("user_late", "UPDATE", "2024-01-12T09:00:00Z", "txn_101", balance=500.0, status="SUSPENDED"),
        # Background padding records
        *[_create_record(f"user_pad_{i}", "INSERT", "2024-01-01T08:00:00Z", f"txn_pad_{i}") for i in range(10)],
    ]

    output_df = process_cdc_records(spark, records)
    user_row = output_df.filter(col("record_id") == "user_late").collect()

    assert len(user_row) == 1
    assert user_row[0]["balance"] == 9999.0
    assert user_row[0]["status"] == "ACTIVE"


def test_out_of_order_updates(spark: SparkSession):
    """
    Test 3: test_out_of_order_updates()
    Scenario: INSERT at T1, UPDATE at T3, UPDATE at T2 (arrives T3, T2, T1)
    Expected: T3 (latest) wins
    Assert: output has T3 values
    """
    # Stream arrivals order: T3 arrives first, then T2, then T1
    records = [
        # T3 arrives first in stream
        _create_record("user_ooo", "UPDATE", "2024-01-20T18:00:00Z", "txn_303", balance=3000.0, status="ACTIVE"),
        # T2 arrives second
        _create_record("user_ooo", "UPDATE", "2024-01-10T12:00:00Z", "txn_302", balance=2000.0, status="INACTIVE"),
        # T1 arrives last
        _create_record("user_ooo", "INSERT", "2024-01-01T09:00:00Z", "txn_301", balance=1000.0, status="INACTIVE"),
        # Background padding records
        *[_create_record(f"user_pad_{i}", "INSERT", "2024-01-01T08:00:00Z", f"txn_pad_{i}") for i in range(10)],
    ]

    output_df = process_cdc_records(spark, records)
    user_row = output_df.filter(col("record_id") == "user_ooo").collect()

    assert len(user_row) == 1
    assert user_row[0]["balance"] == 3000.0
    assert user_row[0]["status"] == "ACTIVE"


def test_no_duplicates(spark: SparkSession):
    """
    Test 4: test_no_duplicates()
    Scenario: INSERT with txn_123, INSERT with txn_123 again
    Expected: 1 record, not 2
    Assert: output_df.count() == 1
    """
    records = [
        _create_record("user_dup", "INSERT", "2024-01-01T10:00:00Z", "txn_123", balance=123.0),
        _create_record("user_dup", "INSERT", "2024-01-01T10:00:00Z", "txn_123", balance=123.0),
    ]

    output_df = process_cdc_records(spark, records)
    assert output_df.filter(col("record_id") == "user_dup").count() == 1
