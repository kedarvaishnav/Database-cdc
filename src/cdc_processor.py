import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.environ.get(
    "CDC_INPUT_PATH", os.path.join(SCRIPT_DIR, "output", "cdc_logs.jsonl")
)
OUTPUT_PATH = os.environ.get(
    "CDC_OUTPUT_PATH", os.path.join(SCRIPT_DIR, "output", "final_state.parquet")
)

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("balance", T.DoubleType(), True),
])

def build_spark():
    return (
        SparkSession.builder
        .appName("CDC-Processor")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def flatten_payload(raw):
    """Flatten all payload fields and parse the event timestamp.

    Keeping payload fields dynamic allows the same reducer to process different
    source schemas, such as customer records and Stack Exchange revisions.
    """
    if "payload" not in raw.columns:
        raise ValueError("CDC input must contain a 'payload' field")

    payload_type = raw.schema["payload"].dataType
    if isinstance(payload_type, T.StructType):
        payload_columns = []
        for field in payload_type.fields:
            value = F.col("payload").getField(field.name)
            # Keep the established customer Parquet/Hive schema stable while
            # leaving all other source payload fields inferred dynamically.
            if field.name == "age":
                value = value.cast(T.IntegerType())
            elif field.name == "balance":
                value = value.cast(T.DoubleType())
            payload_columns.append(value.alias(field.name))
    elif isinstance(payload_type, T.StringType):
        # Backward compatibility for JSONL files whose payload is JSON text.
        parsed = raw.withColumn("_parsed_payload", F.from_json("payload", PAYLOAD_SCHEMA))
        payload_columns = [
            F.col("_parsed_payload").getField(field.name).alias(field.name)
            for field in PAYLOAD_SCHEMA.fields
        ]
        raw = parsed
    else:
        raise TypeError(f"Unsupported payload type: {payload_type.simpleString()}")

    flat = raw.select(
        "record_id", "operation", "transaction_id",
        F.col("timestamp").alias("ts_raw"),
        *payload_columns,
    )

    return flat.withColumn(
        "timestamp",
        F.to_timestamp(F.col("ts_raw"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ).drop("ts_raw")


def deduplicate_cdc(flat):
    """Keep the latest non-deleted record per record_id using ROW_NUMBER."""
    window_spec = (
        Window.partitionBy("record_id")
        .orderBy(F.col("timestamp").desc(), F.col("transaction_id").desc())
    )

    output_columns = [
        name for name in flat.columns if name not in {"operation", "transaction_id"}
    ]

    return (
        flat
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .filter(F.col("operation") != "DELETE")
        .select(*output_columns)
    )


def process_cdc(spark):
    print(f"\nLoading CDC logs from: {INPUT_PATH}")
    raw = spark.read.json(INPUT_PATH)
    raw_count = raw.count()
    print(f"Loaded {raw_count:,} raw records.")

    flat = flatten_payload(raw)
    final = deduplicate_cdc(flat)

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    print(f"Writing deduplicated output to Parquet: {OUTPUT_PATH}")
    final.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

    result = spark.read.parquet(OUTPUT_PATH)
    total = result.count()
    unique = result.select("record_id").distinct().count()
    dupes = total - unique

    print("\n" + "=" * 50)
    print(f"Input records  : {raw_count:,}")
    print(f"Output records : {total:,}")
    print(f"Dedup ratio    : {raw_count / total:.2f}x")
    print(f"Duplicates     : {dupes}")
    print("=" * 50)

    if dupes != 0:
        raise AssertionError(f"Duplicate records found: {dupes}")

    result.show(5, truncate=True)
    print(f"\n[OK] Pipeline complete: {total:,} active records, 0 duplicates.")


if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] Input not found: {INPUT_PATH}")
        print("Please run first: python src/generate_cdc_data.py")
        sys.exit(1)

    t0 = time.time()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    process_cdc(spark)

    print(f"Total time: {time.time() - t0:.1f}s")
    spark.stop()
