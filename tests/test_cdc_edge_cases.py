"""
CDC Pipeline Edge Case Tests
Tests:
1. Out-of-order event resolution
2. Multiple updates on the same record
3. DELETE tombstone discarding earlier records
4. INSERT after DELETE (account re-creation)
"""

import os
import json
import tempfile
import pytest
from pyspark.sql import SparkSession
from src.cdc_processor import process_cdc_events, get_spark_session

@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("CDC_Edge_Case_Tests")
    yield session
    session.stop()

def test_out_of_order_resolution(spark, tmp_path):
    """Event arriving later in stream with earlier timestamp must not overwrite newer state."""
    input_file = tmp_path / "out_of_order.jsonl"
    output_dir = tmp_path / "out_of_order.parquet"

    events = [
        # Newer update arrives first in the file
        {
            "record_id": "user_ooo",
            "operation": "UPDATE",
            "payload": {"user_name": "Alice Latest", "email": "alice@ex.com", "age": 30, "status": "ACTIVE", "balance": 200.0},
            "timestamp": "2024-01-15T12:00:00Z",
            "transaction_id": "txn_2"
        },
        # Older insert arrives second in the file
        {
            "record_id": "user_ooo",
            "operation": "INSERT",
            "payload": {"user_name": "Alice Old", "email": "alice@ex.com", "age": 29, "status": "ACTIVE", "balance": 100.0},
            "timestamp": "2024-01-10T12:00:00Z",
            "transaction_id": "txn_1"
        }
    ]
    with open(input_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    process_cdc_events(str(input_file), str(output_dir), spark=spark)
    df = spark.read.parquet(str(output_dir))
    assert df.count() == 1
    row = df.collect()[0]
    assert row["user_name"] == "Alice Latest"
    assert row["balance"] == 200.0

def test_multiple_updates(spark, tmp_path):
    """Multiple sequential updates must preserve only the latest state."""
    input_file = tmp_path / "multiple_updates.jsonl"
    output_dir = tmp_path / "multiple_updates.parquet"

    events = [
        {"record_id": "user_mu", "operation": "INSERT", "payload": {"user_name": "Bob", "email": "b@ex.com", "age": 40, "status": "ACTIVE", "balance": 50.0}, "timestamp": "2024-01-01T10:00:00Z", "transaction_id": "txn_1"},
        {"record_id": "user_mu", "operation": "UPDATE", "payload": {"user_name": "Bob", "email": "b@ex.com", "age": 40, "status": "INACTIVE", "balance": 150.0}, "timestamp": "2024-01-05T10:00:00Z", "transaction_id": "txn_2"},
        {"record_id": "user_mu", "operation": "UPDATE", "payload": {"user_name": "Bob Updated", "email": "b@ex.com", "age": 41, "status": "ACTIVE", "balance": 350.0}, "timestamp": "2024-01-20T10:00:00Z", "transaction_id": "txn_3"},
    ]
    with open(input_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    process_cdc_events(str(input_file), str(output_dir), spark=spark)
    df = spark.read.parquet(str(output_dir))
    assert df.count() == 1
    row = df.collect()[0]
    assert row["user_name"] == "Bob Updated"
    assert row["status"] == "ACTIVE"
    assert row["balance"] == 350.0
    assert row["age"] == 41

def test_delete_tombstone(spark, tmp_path):
    """A record whose latest operation is DELETE must not exist in final state."""
    input_file = tmp_path / "delete_test.jsonl"
    output_dir = tmp_path / "delete_test.parquet"

    events = [
        {"record_id": "user_del", "operation": "INSERT", "payload": {"user_name": "Charlie", "email": "c@ex.com", "age": 25, "status": "ACTIVE", "balance": 100.0}, "timestamp": "2024-01-02T10:00:00Z", "transaction_id": "txn_1"},
        {"record_id": "user_del", "operation": "UPDATE", "payload": {"user_name": "Charlie", "email": "c@ex.com", "age": 25, "status": "SUSPENDED", "balance": 50.0}, "timestamp": "2024-01-05T10:00:00Z", "transaction_id": "txn_2"},
        {"record_id": "user_del", "operation": "DELETE", "payload": {"user_name": "Charlie", "email": "c@ex.com", "age": 25, "status": "INACTIVE", "balance": 0.0}, "timestamp": "2024-01-10T10:00:00Z", "transaction_id": "txn_3"},
    ]
    with open(input_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    process_cdc_events(str(input_file), str(output_dir), spark=spark)
    df = spark.read.parquet(str(output_dir))
    assert df.count() == 0

def test_insert_after_delete(spark, tmp_path):
    """An INSERT after a DELETE (re-creation) must be preserved if timestamp is later."""
    input_file = tmp_path / "recreate_test.jsonl"
    output_dir = tmp_path / "recreate_test.parquet"

    events = [
        {"record_id": "user_rec", "operation": "INSERT", "payload": {"user_name": "David Old", "email": "d@ex.com", "age": 20, "status": "ACTIVE", "balance": 10.0}, "timestamp": "2024-01-01T10:00:00Z", "transaction_id": "txn_1"},
        {"record_id": "user_rec", "operation": "DELETE", "payload": {"user_name": "David Old", "email": "d@ex.com", "age": 20, "status": "INACTIVE", "balance": 0.0}, "timestamp": "2024-01-05T10:00:00Z", "transaction_id": "txn_2"},
        {"record_id": "user_rec", "operation": "INSERT", "payload": {"user_name": "David Recreated", "email": "d@ex.com", "age": 21, "status": "ACTIVE", "balance": 500.0}, "timestamp": "2024-01-15T10:00:00Z", "transaction_id": "txn_3"},
    ]
    with open(input_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    process_cdc_events(str(input_file), str(output_dir), spark=spark)
    df = spark.read.parquet(str(output_dir))
    assert df.count() == 1
    row = df.collect()[0]
    assert row["user_name"] == "David Recreated"
    assert row["balance"] == 500.0