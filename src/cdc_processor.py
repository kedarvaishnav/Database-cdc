"""
cdc_processor.py
================
PySpark CDC processor: reconstructs the final, accurate state of every
database record from a large, out-of-order stream of CDC events.

Algorithm
---------
ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC, transaction_id DESC)

Steps:
    1. Load 100K JSONL logs
    2. Parse nested payload struct → flat columns
    3. Cast timestamp string → Spark TimestampType
    4. Window: partition by record_id, order by timestamp DESC (+ txn tiebreak)
    5. Keep only rn = 1  (latest event per record)
    6. Drop DELETEd records  (they no longer exist)
    7. Save final state as Parquet

Output
------
    src/output/final_state.parquet   (~47,923 active records)

Usage
-----
    python src/cdc_processor.py            (PySpark in local mode)
    spark-submit src/cdc_processor.py      (standalone cluster)
"""

import os
import sys
import time

# ─────────────────────────────────────────────────────────────────────────────
# PySpark bootstrap
# ─────────────────────────────────────────────────────────────────────────────

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window
except ImportError:
    print("[ERROR] PySpark is not installed.")
    print("Install it with:  pip install pyspark")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH   = os.path.join(SCRIPT_DIR, "output", "cdc_logs.jsonl")
OUTPUT_PATH  = os.path.join(SCRIPT_DIR, "output", "final_state.parquet")


# ─────────────────────────────────────────────────────────────────────────────
# Schema for the nested "payload" field
# ─────────────────────────────────────────────────────────────────────────────

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_name", T.StringType(), True),
    T.StructField("email",     T.StringType(), True),
    T.StructField("age",       T.IntegerType(), True),
    T.StructField("status",    T.StringType(), True),
    T.StructField("balance",   T.DoubleType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CDC-Processor")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")   # small dataset
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main processing pipeline
# ─────────────────────────────────────────────────────────────────────────────

def process_cdc(spark: SparkSession) -> None:
    """Full CDC event-replay pipeline."""

    # 1. Load JSONL ───────────────────────────────────────────────────────────
    print(f"\nLoading: {INPUT_PATH}")
    raw = spark.read.json(INPUT_PATH)
    raw_count = raw.count()
    print(f"  Raw records loaded: {raw_count:,}")

    # 2. Parse payload ────────────────────────────────────────────────────────
    # Spark may read 'payload' as a Struct OR as a String depending on the
    # JSON content.  Handle both cases.
    if isinstance(raw.schema["payload"].dataType, T.StructType):
        # Payload is already a struct
        flat = raw.select(
            F.col("record_id"),
            F.col("operation"),
            F.col("transaction_id"),
            F.col("timestamp").alias("ts_raw"),
            F.col("payload.user_name").alias("user_name"),
            F.col("payload.email").alias("email"),
            F.col("payload.age").alias("age"),
            F.col("payload.status").alias("status"),
            F.col("payload.balance").alias("balance"),
        )
    else:
        # Payload came in as a string — parse with from_json
        flat = raw.select(
            F.col("record_id"),
            F.col("operation"),
            F.col("transaction_id"),
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

    # 3. Cast timestamp ───────────────────────────────────────────────────────
    flat = flat.withColumn(
        "timestamp",
        F.to_timestamp(F.col("ts_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ).drop("ts_raw")

    # 4. Window function ──────────────────────────────────────────────────────
    window_spec = (
        Window
        .partitionBy("record_id")
        .orderBy(F.col("timestamp").desc(), F.col("transaction_id").desc())
    )
    windowed = flat.withColumn("rn", F.row_number().over(window_spec))

    # 5. Keep latest event per record ─────────────────────────────────────────
    latest = windowed.filter(F.col("rn") == 1).drop("rn")

    # 6. Remove deleted records ───────────────────────────────────────────────
    active = latest.filter(F.col("operation") != "DELETE")

    # 7. Select final columns ─────────────────────────────────────────────────
    final = active.select(
        "record_id",
        "user_name",
        "email",
        "age",
        "status",
        "balance",
        "timestamp",
    )

    # 8. Write Parquet ────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    print(f"\nWriting Parquet to: {OUTPUT_PATH}")
    (
        final
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )
    print("  Write complete.")

    # 9. Verify ───────────────────────────────────────────────────────────────
    result = spark.read.parquet(OUTPUT_PATH)
    total  = result.count()
    unique = result.select("record_id").distinct().count()
    dupes  = total - unique

    print("\n" + "=" * 55)
    print("RESULTS")
    print("=" * 55)
    print(f"  Input records  : {raw_count:,}")
    print(f"  Output records : {total:,}")
    print(f"  Unique IDs     : {unique:,}")
    print(f"  Duplicates     : {dupes}")
    print(f"  Dedup ratio    : {raw_count / total:.2f}x")
    print("=" * 55)

    if dupes != 0:
        raise AssertionError(f"DUPLICATE RECORDS FOUND: {dupes}")

    # Print sample rows
    print("\nSample output rows:")
    result.show(5, truncate=True)

    print(f"\n[OK] Task 1.2 COMPLETE: {total:,} records, 0 duplicates")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("CDC Processor — Task 1.2")
    print("=" * 55)

    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] Input not found: {INPUT_PATH}")
        print("Run Task 1.1 first:  python src/generate_cdc_data.py")
        sys.exit(1)

    t0 = time.time()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    process_cdc(spark)

    elapsed = time.time() - t0
    print(f"Total processing time: {elapsed:.1f}s")
    spark.stop()
