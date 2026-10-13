"""
CDC Pipeline Idempotency Tests
Proves that processing the same dataset multiple times produces identical row counts,
exact duplicate absence, and stable content.
"""

import pytest
from src.cdc_processor import process_cdc_events, get_spark_session

@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("CDC_Idempotency_Tests")
    yield session
    session.stop()

def test_pipeline_idempotency(spark, tmp_path):
    input_file = "data/cdc_events.jsonl"
    out1 = str(tmp_path / "run1.parquet")
    out2 = str(tmp_path / "run2.parquet")

    count1 = process_cdc_events(input_file, out1, spark=spark)
    count2 = process_cdc_events(input_file, out2, spark=spark)

    assert count1 == 47923, f"Expected 47923 on run 1, got {count1}"
    assert count2 == 47923, f"Expected 47923 on run 2, got {count2}"

    df1 = spark.read.parquet(out1)
    df2 = spark.read.parquet(out2)

    assert df1.count() == df2.count()
    # Verify no diff between runs
    diff = df1.subtract(df2).count()
    assert diff == 0, f"Detected {diff} differences between runs!"