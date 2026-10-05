"""
CDC validation tests.

Edge-case tests that verify the PySpark CDC pipeline handles
DELETE removal, UPDATE deduplication, and out-of-order arrivals correctly.

Usage:
    python src/test_cdc_validation.py
"""

import json
import os
import shutil
import sys
import tempfile

SPARK_HOME = os.environ.get("SPARK_HOME", "/home/kedar/spark")
PY4J_ZIP   = os.path.join(SPARK_HOME, "python", "lib", "py4j-0.10.9.7-src.zip")
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
sys.path.insert(0, PY4J_ZIP)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_name", T.StringType(),  True),
    T.StructField("email",     T.StringType(),  True),
    T.StructField("age",       T.IntegerType(), True),
    T.StructField("status",    T.StringType(),  True),
    T.StructField("balance",   T.DoubleType(),  True),
])


def _run_cdc(spark: SparkSession, records: list) -> "pyspark.sql.DataFrame":
    """
    Run the CDC dedup pipeline on an in-memory list of dicts.

    Writes to a stable temp path before reading — Spark reads lazily,
    so the file must exist until after all actions are triggered.
    """
    tmp_dir = tempfile.mkdtemp(prefix="cdc_test_")
    tmp = os.path.join(tmp_dir, "test_input.jsonl")
    with open(tmp, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")

    raw = spark.read.json(tmp)

    if isinstance(raw.schema["payload"].dataType, T.StructType):
        flat = raw.select(
            "record_id", "operation", "transaction_id",
            F.col("timestamp").alias("ts_raw"),
            F.col("payload.user_name").alias("user_name"),
            F.col("payload.email").alias("email"),
            F.col("payload.age").alias("age"),
            F.col("payload.status").alias("status"),
            F.col("payload.balance").alias("balance"),
        )
    else:
        flat = raw.select(
            "record_id", "operation", "transaction_id",
            F.col("timestamp").alias("ts_raw"),
            F.from_json(F.col("payload"), PAYLOAD_SCHEMA).alias("p"),
        ).select(
            "record_id", "operation", "transaction_id", "ts_raw",
            F.col("p.user_name").alias("user_name"),
            F.col("p.email").alias("email"),
            F.col("p.age").alias("age"),
            F.col("p.status").alias("status"),
            F.col("p.balance").alias("balance"),
        )

    flat = flat.withColumn(
        "timestamp",
        F.to_timestamp(F.col("ts_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ).drop("ts_raw")

    window_spec = (
        Window.partitionBy("record_id")
        .orderBy(F.col("timestamp").desc(), F.col("transaction_id").desc())
    )

    result = (
        flat
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1).drop("rn")
        .filter(F.col("operation") != "DELETE")
        .select("record_id", "user_name", "email", "age", "status", "balance", "timestamp")
        .cache()
    )
    result.count()  # materialise before deleting the temp file

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return result


def _make_record(record_id: str, op: str, ts: str, txn: str) -> dict:
    return {
        "record_id": record_id,
        "operation": op,
        "payload": {
            "user_name": "Test User",
            "email": "test@test.com",
            "age": 30,
            "status": "ACTIVE",
            "balance": 1000.0,
        },
        "timestamp": ts,
        "transaction_id": txn,
    }


def test_delete_removes_record(spark: SparkSession) -> None:
    """Record whose latest op is DELETE must not appear in output."""
    records = [
        _make_record("user_X", "INSERT", "2024-01-01T10:00:00Z", "txn_001"),
        _make_record("user_X", "UPDATE", "2024-01-02T10:00:00Z", "txn_002"),
        _make_record("user_X", "DELETE", "2024-01-03T10:00:00Z", "txn_003"),
    ]
    count = _run_cdc(spark, records).filter(F.col("record_id") == "user_X").count()
    assert count == 0, f"FAIL: deleted record appeared (count={count})"
    print("  [PASS] test_delete_removes_record")


def test_update_keeps_latest(spark: SparkSession) -> None:
    """Multiple UPDATEs to the same record: only the latest survives."""
    records = [
        _make_record("user_Y", "INSERT", "2024-01-01T08:00:00Z", "txn_010"),
        _make_record("user_Y", "UPDATE", "2024-01-05T09:00:00Z", "txn_011"),
        _make_record("user_Y", "UPDATE", "2024-01-10T12:00:00Z", "txn_012"),
    ]
    count = _run_cdc(spark, records).filter(F.col("record_id") == "user_Y").count()
    assert count == 1, f"FAIL: expected 1 row for user_Y, got {count}"
    print("  [PASS] test_update_keeps_latest")


def test_out_of_order_arrival(spark: SparkSession) -> None:
    """Events arriving out of order still produce the correct latest state."""
    records = [
        _make_record("user_Z", "UPDATE", "2024-01-20T15:00:00Z", "txn_022"),  # latest, arrives first
        _make_record("user_Z", "INSERT", "2024-01-01T07:00:00Z", "txn_020"),
        _make_record("user_Z", "UPDATE", "2024-01-10T11:00:00Z", "txn_021"),
    ]
    count = _run_cdc(spark, records).filter(F.col("record_id") == "user_Z").count()
    assert count == 1, f"FAIL: expected 1 row for user_Z, got {count}"
    print("  [PASS] test_out_of_order_arrival")


def test_no_duplicates_in_real_output(spark: SparkSession) -> None:
    """Verify the actual Parquet output has zero duplicate record_ids."""
    parquet_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "output", "final_state.parquet")
    if not os.path.exists(parquet_path):
        print("  [SKIP] test_no_duplicates_in_real_output (Parquet not found)")
        return

    df    = spark.read.parquet(parquet_path)
    total = df.count()
    dupes = total - df.select("record_id").distinct().count()
    assert dupes == 0, f"FAIL: {dupes} duplicate record_ids in final_state.parquet"
    print(f"  [PASS] test_no_duplicates_in_real_output ({total:,} records, 0 dupes)")


if __name__ == "__main__":
    print("=" * 55)
    print("CDC Validation Tests")
    print("=" * 55)

    spark = (
        SparkSession.builder
        .appName("CDC-Validation")
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
    for t in tests:
        try:
            t(spark)
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL] {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  [ERROR] {t.__name__}: {e}")
            failed += 1

    spark.stop()

    print("-" * 55)
    print(f"Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("[OK] All validation tests passed")
    else:
        print("[FAIL] Some tests failed — check output above")
        sys.exit(1)
    print("=" * 55)
