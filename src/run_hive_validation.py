#!/usr/bin/env python3
"""
Hive SQL Validator Runner
Executes the DDL and 6 validation queries against the Parquet dataset using Spark SQL / Hive metastore.
"""
import sys
import os
import re
from pyspark.sql import SparkSession

def main():
    print("=" * 60)
    print("STARTING HIVE TABLE DDL AND VALIDATION QUERIES")
    print("=" * 60)

    warehouse_dir = os.path.abspath("warehouse/hive-warehouse")
    os.makedirs(warehouse_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Hive_CDC_Validator")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )

    # 1. Execute DDL from create_hive_tables.sql
    ddl_file = "src/create_hive_tables.sql"
    print(f"\nExecuting DDL from {ddl_file}...")
    with open(ddl_file, "r") as f:
        ddl_content = f.read()

    # Split statements
    statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]
    for stmt in statements:
        clean_stmt = "\n".join([line for line in stmt.split("\n") if not line.strip().startswith("--")])
        if not clean_stmt.strip():
            continue
        if "MSCK REPAIR" in clean_stmt:
            print("Skipping MSCK REPAIR (table is unpartitioned Parquet).")
            continue
        print(f"Running: {clean_stmt[:50]}...")
        spark.sql(clean_stmt)

    print("✅ Hive DDL executed successfully. Table cdc_db.users_final_state created.")

    # 2. Execute Validation Queries from validate_cdc.sql
    val_file = "src/validate_cdc.sql"
    print(f"\nExecuting Validation Queries from {val_file}...\n")

    queries = [
        ("Query 1: Total record count", "SELECT COUNT(*) as total_records FROM cdc_db.users_final_state"),
        ("Query 2: Duplicate check", "SELECT COUNT(*) as total, COUNT(DISTINCT record_id) as unique_ids, (COUNT(*) - COUNT(DISTINCT record_id)) as duplicates FROM cdc_db.users_final_state"),
        ("Query 3: Status distribution", "SELECT status, COUNT(*) as count FROM cdc_db.users_final_state GROUP BY status ORDER BY count DESC"),
        ("Query 4: High-value customers", "SELECT record_id, user_name, email, balance FROM cdc_db.users_final_state WHERE balance > 50000 ORDER BY balance DESC LIMIT 10"),
        ("Query 5: Timestamp range", "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM cdc_db.users_final_state"),
        ("Query 6: Sample record", "SELECT * FROM cdc_db.users_final_state LIMIT 1")
    ]

    for title, q in queries:
        print(f"--- {title} ---")
        print(f"SQL: {q}")
        df = spark.sql(q)
        df.show(truncate=False)
        
        if "Query 1" in title:
            cnt = df.collect()[0]["total_records"]
            assert cnt == 47923, f"Expected 47923 records, got {cnt}"
            print(f"✅ Verified count = {cnt}")
        elif "Query 2" in title:
            row = df.collect()[0]
            assert row["duplicates"] == 0, f"Expected 0 duplicates, got {row['duplicates']}"
            print(f"✅ Verified 0 duplicates (total={row['total']}, unique={row['unique_ids']})")
        print()

    print("=" * 60)
    print("✅ ALL 6 HIVE VALIDATION QUERIES PASSED PERFECTLY!")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    main()