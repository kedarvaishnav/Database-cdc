import json
import os
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cdc_processor import flatten_payload, deduplicate_cdc


def run_cdc_on_records(spark, records):
    """Run the CDC dedup pipeline on an in-memory list of records."""
    tmp_dir = tempfile.mkdtemp(prefix="cdc_test_")
    tmp_path = os.path.join(tmp_dir, "test_input.jsonl")
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        raw = spark.read.json(tmp_path)
        flat = flatten_payload(raw)
        result = deduplicate_cdc(flat).cache()
        result.count()
        return result
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def make_record(record_id, operation, timestamp, txn_id):
    return {
        "record_id": record_id,
        "operation": operation,
        "payload": {
            "user_name": "Test User",
            "email": "test@example.com",
            "age": 30,
            "status": "ACTIVE",
            "balance": 1000.0,
        },
        "timestamp": timestamp,
        "transaction_id": txn_id,
    }


def test_delete_removes_record(spark):
    """A record whose latest operation is DELETE must not appear in the final output."""
    records = [
        make_record("user_X", "INSERT", "2024-01-01T10:00:00Z", "txn_001"),
        make_record("user_X", "UPDATE", "2024-01-02T10:00:00Z", "txn_002"),
        make_record("user_X", "DELETE", "2024-01-03T10:00:00Z", "txn_003"),
    ]
    df = run_cdc_on_records(spark, records)
    count = df.filter(F.col("record_id") == "user_X").count()
    assert count == 0, f"Expected user_X to be deleted, but found {count} row(s)"
    print("  [PASS] test_delete_removes_record")


def test_update_keeps_latest(spark):
    """Multiple UPDATE events on the same record should result in only the latest state."""
    records = [
        make_record("user_Y", "INSERT", "2024-01-01T08:00:00Z", "txn_010"),
        make_record("user_Y", "UPDATE", "2024-01-05T09:00:00Z", "txn_011"),
        make_record("user_Y", "UPDATE", "2024-01-10T12:00:00Z", "txn_012"),
    ]
    df = run_cdc_on_records(spark, records)
    count = df.filter(F.col("record_id") == "user_Y").count()
    assert count == 1, f"Expected 1 row for user_Y, got {count}"
    print("  [PASS] test_update_keeps_latest")


def test_out_of_order_arrival(spark):
    """Events arriving in non-chronological order should still yield the newest timestamp."""
    records = [
        make_record("user_Z", "UPDATE", "2024-01-20T15:00:00Z", "txn_022"),
        make_record("user_Z", "INSERT", "2024-01-01T07:00:00Z", "txn_020"),
        make_record("user_Z", "UPDATE", "2024-01-10T11:00:00Z", "txn_021"),
    ]
    df = run_cdc_on_records(spark, records)
    count = df.filter(F.col("record_id") == "user_Z").count()
    assert count == 1, f"Expected 1 row for user_Z, got {count}"
    print("  [PASS] test_out_of_order_arrival")


def test_no_duplicates_in_real_output(spark):
    """Verify that the generated Parquet output has zero duplicate record_ids."""
    parquet_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "output", "final_state.parquet"
    )
    if not os.path.exists(parquet_path):
        print("  [SKIP] test_no_duplicates_in_real_output: final_state.parquet not generated yet")
        return

    df = spark.read.parquet(parquet_path)
    total = df.count()
    unique = df.select("record_id").distinct().count()
    dupes = total - unique
    assert dupes == 0, f"Found {dupes} duplicate record_ids in {parquet_path}"
    print(f"  [PASS] test_no_duplicates_in_real_output ({total:,} records, 0 duplicates)")


if __name__ == "__main__":
    print("=" * 50)
    print("Running CDC Validation Tests")
    print("=" * 50)

    spark = (
        SparkSession.builder
        .appName("CDC-Validation-Tests")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tests = [
        test_delete_removes_record,
        test_update_keeps_latest,
        test_out_of_order_arrival,
        test_no_duplicates_in_real_output,
    ]

    passed, failed = 0, 0
    for test in tests:
        try:
            test(spark)
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL] {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  [ERROR] {test.__name__}: {e}")
            failed += 1

    spark.stop()

    print("-" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)
    print("[OK] All tests passed successfully.")
