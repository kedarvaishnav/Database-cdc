"""
Pytest configuration and shared fixtures for CDC pipeline testing.
"""

import json
import os
import sys
import tempfile
import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("balance", T.DoubleType(), True),
])


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession fixture for testing."""
    session = (
        SparkSession.builder
        .appName("CDC-Testing-Friend3")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def process_cdc_records(spark: SparkSession, records: list):
    """
    Execute the CDC deduplication pipeline on an in-memory list of dict records.
    
    Algorithm:
      1. Flatten payload
      2. Parse ISO timestamps
      3. Window partition by record_id, order by timestamp DESC, transaction_id DESC
      4. Filter rn == 1 (keep latest state)
      5. Filter operation != 'DELETE' (tombstone removal)
    """
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        temp_path = f.name

    try:
        raw = spark.read.json(temp_path)

        if "payload" in raw.columns and isinstance(raw.schema["payload"].dataType, T.StructType):
            flat = raw.select(
                "record_id",
                "operation",
                "transaction_id",
                F.col("timestamp").alias("ts_raw"),
                F.col("payload.user_name").alias("user_name"),
                F.col("payload.email").alias("email"),
                F.col("payload.age").alias("age"),
                F.col("payload.status").alias("status"),
                F.col("payload.balance").alias("balance"),
            )
        else:
            flat = raw.select(
                "record_id",
                "operation",
                "transaction_id",
                F.col("timestamp").alias("ts_raw"),
                F.from_json(F.col("payload"), PAYLOAD_SCHEMA).alias("p"),
            ).select(
                "record_id",
                "operation",
                "transaction_id",
                "ts_raw",
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

        final_df = (
            flat.withColumn("rn", F.row_number().over(window_spec))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .filter(F.col("operation") != "DELETE")
            .select("record_id", "user_name", "email", "age", "status", "balance", "timestamp")
        )
        # Materialize in memory so temp file can be cleaned safely
        rows = final_df.collect()
        schema = final_df.schema
        return spark.createDataFrame(rows, schema) if rows else spark.createDataFrame([], schema)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
