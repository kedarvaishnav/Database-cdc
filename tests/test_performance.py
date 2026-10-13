"""
CDC Pipeline Performance Tests
Ensures pipeline processes the 100K record dataset quickly and efficiently.
"""

import time
import pytest
from src.cdc_processor import process_cdc_events, get_spark_session

@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("CDC_Performance_Tests")
    yield session
    session.stop()

def test_performance_under_threshold(spark, tmp_path):
    input_file = "data/cdc_events.jsonl"
    out = str(tmp_path / "perf.parquet")

    start = time.time()
    count = process_cdc_events(input_file, out, spark=spark)
    duration = time.time() - start

    print(f"\nProcessing 100K CDC events took: {duration:.2f} seconds")
    assert count == 47923
    # Generous threshold to allow for local CPU variability while verifying performance
    assert duration < 20.0, f"Processing took {duration:.2f}s, exceeding threshold!"