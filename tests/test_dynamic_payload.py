"""Integration coverage for real revision data and dynamic payload fields."""

import json
import os

try:
    from src.cdc_processor import deduplicate_cdc, flatten_payload
except ImportError:
    from cdc_processor import deduplicate_cdc, flatten_payload


def test_stackoverflow_sample_reduces_revisions_to_latest_post(spark):
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sample_path = os.path.join(repo_root, "data", "stackoverflow_revisions.jsonl")

    with open(sample_path, encoding="utf-8") as sample_file:
        events = [json.loads(line) for line in sample_file if line.strip()]

    expected_by_post = {}
    for event in events:
        previous = expected_by_post.get(event["record_id"])
        key = (event["timestamp"], event["transaction_id"])
        if previous is None or key > (previous["timestamp"], previous["transaction_id"]):
            expected_by_post[event["record_id"]] = event
    assert len(events) > len(expected_by_post)
    assert len(expected_by_post) == 2

    raw = spark.read.json(sample_path)
    result = deduplicate_cdc(flatten_payload(raw))

    actual = result.collect()

    assert len(actual) == len(expected_by_post)
    for row in actual:
        expected = expected_by_post[row["record_id"]]
        assert row["revision_number"] == expected["payload"]["revision_number"]
        assert row["body"] == expected["payload"]["body"]
        assert row["content_license"] == expected["payload"]["content_license"]
    assert "author_display_name" in result.columns
