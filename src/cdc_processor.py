"""
PySpark CDC processor.

Reconstructs the latest state of every database record from an
out-of-order stream of CDC events using ROW_NUMBER() window deduplication.

Algorithm: PARTITION BY record_id ORDER BY timestamp DESC, transaction_id DESC
           → keep rn=1, drop DELETEs → write Parquet

Usage:
    python src/cdc_processor.py
    spark-submit src/cdc_processor.py
Output:
    src/output/final_state.parquet  (~47,923 active records)
"""

import os
import sys
import time

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window
except ImportError:
    print("[ERROR] PySpark not found. Run: pip install pyspark")
    sys.exit(1)

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH  = os.path.join(SCRIPT_DIR, "output", "cdc_logs.jsonl")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "output", "final_state.parquet")

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_name", T.StringType(),  True),
    T.StructField("email",     T.StringType(),  True),
    T.StructField("age",       T.IntegerType(), True),
    T.StructField("status",    T.StringType(),  True),
    T.StructField("balance",   T.DoubleType(),  True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CDC-Processor")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def process_cdc(spark: SparkSession) -> None:
    """Run the full CDC dedup pipeline and write Parquet output."""
    print(f"\nLoading: {INPUT_PATH}")
    raw = spark.read.json(INPUT_PATH)
    raw_count = raw.count()
    print(f"  Raw records: {raw_count:,}")

    # Payload arrives as Struct or String depending on Spark's JSON inference
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
        Window
        .partitionBy("record_id")
        .orderBy(F.col("timestamp").desc(), F.col("transaction_id").desc())
    )

    final = (
        flat
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1).drop("rn")
        .filter(F.col("operation") != "DELETE")
        .select("record_id", "user_name", "email", "age", "status", "balance", "timestamp")
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    print(f"\nWriting Parquet: {OUTPUT_PATH}")
    final.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

    result = spark.read.parquet(OUTPUT_PATH)
    total  = result.count()
    unique = result.select("record_id").distinct().count()
    dupes  = total - unique

    print(f"\n{'='*50}")
    print(f"  Input  : {raw_count:,}")
    print(f"  Output : {total:,}")
    print(f"  Dedup  : {raw_count / total:.2f}x")
    print(f"  Dupes  : {dupes}")
    print(f"{'='*50}")

    if dupes != 0:
        raise AssertionError(f"Duplicate records found: {dupes}")

    result.show(5, truncate=True)
    print(f"\n[OK] {total:,} records, 0 duplicates")


if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] Input not found: {INPUT_PATH}")
        print("Run first: python src/generate_cdc_data.py")
        sys.exit(1)

    t0 = time.time()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    process_cdc(spark)
    print(f"Total time: {time.time() - t0:.1f}s")
    spark.stop()
