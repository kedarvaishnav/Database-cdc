#!/usr/bin/env python3
"""
CDC PySpark Processor
Reads raw Change Data Capture events (JSONL), reconstructs the latest state using:
ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC)
Filters out DELETE tombstones and outputs the reconciled dataset as Parquet.
"""

import sys
import os
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

def get_spark_session(app_name="CDC_Event_Processor"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def process_cdc_events(input_path="data/cdc_events.jsonl", output_path="src/output/final_state.parquet", spark=None):
    start_time = time.time()
    created_spark = False
    if spark is None:
        spark = get_spark_session()
        created_spark = True

    print(f"Reading raw CDC events from: {input_path}")
    raw_df = spark.read.json(input_path)
    total_events = raw_df.count()
    print(f"Total raw events ingested: {total_events}")

    # Standardize timestamp to TimestampType
    events_df = raw_df.withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Apply ROW_NUMBER() window function
    # Order by ts DESC, and tie-break by transaction_id DESC
    window_spec = Window.partitionBy("record_id").orderBy(col("ts").desc(), col("transaction_id").desc())
    ranked_df = events_df.withColumn("row_num", row_number().over(window_spec))

    # Reconcile state: Keep row_number == 1 and filter out tombstoned DELETEs
    latest_state_df = ranked_df.filter(
        (col("row_num") == 1) & (col("operation") != "DELETE")
    )

    # Flatten payload struct and cast columns to exact schema
    final_df = latest_state_df.select(
        col("record_id").cast(StringType()).alias("record_id"),
        col("payload.user_name").cast(StringType()).alias("user_name"),
        col("payload.email").cast(StringType()).alias("email"),
        col("payload.age").cast(IntegerType()).alias("age"),
        col("payload.status").cast(StringType()).alias("status"),
        col("payload.balance").cast(DoubleType()).alias("balance"),
        col("ts").cast(TimestampType()).alias("timestamp")
    )

    final_count = final_df.count()
    duration = time.time() - start_time
    print(f"Deduplication complete in {duration:.2f} seconds.")
    print(f"Final active record count: {final_count} (from {total_events} raw events)")

    # Ensure output directory exists and write Parquet
    print(f"Writing final state to Parquet: {output_path}")
    final_df.write.mode("overwrite").parquet(output_path)
    print("✅ Parquet write complete.")

    if created_spark:
        spark.stop()

    return final_count

if __name__ == "__main__":
    in_file = sys.argv[1] if len(sys.argv) > 1 else "data/cdc_events.jsonl"
    out_file = sys.argv[2] if len(sys.argv) > 2 else "src/output/final_state.parquet"
    process_cdc_events(in_file, out_file)