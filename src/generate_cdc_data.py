"""
CDC log generator.

Produces 100,000 synthetic JSONL records simulating an out-of-order
CDC event stream (INSERT / UPDATE / DELETE) for January 2024.

Usage:  python src/generate_cdc_data.py
Output: src/output/cdc_logs.jsonl (~15-20 MB)
"""

import json
import os
import random
import string
from datetime import datetime, timedelta

NUM_RECORDS = 100_000
NUM_UNIQUE_IDS = 50_000
LATE_ARRIVAL_FRACTION = 0.05
LATE_ARRIVAL_DAYS = (1, 3)

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nate", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zoe", "Aaron", "Bella", "Chris", "Diana",
]

LAST_NAMES = [
    "Smith", "Jones", "Williams", "Brown", "Davis", "Miller", "Wilson",
    "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris",
    "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "proton.me"]
STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
STATUS_WEIGHTS = [0.70, 0.20, 0.10]

JAN_START = datetime(2024, 1, 1, 0, 0, 0)
JAN_END = datetime(2024, 1, 31, 23, 59, 59)
JAN_SECONDS = int((JAN_END - JAN_START).total_seconds())


def _random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _random_email(name):
    slug = name.lower().replace(" ", ".")
    return f"{slug}{random.randint(1, 999)}@{random.choice(EMAIL_DOMAINS)}"


def _random_payload():
    name = _random_name()
    return {
        "user_name": name,
        "email": _random_email(name),
        "age": random.randint(18, 80),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "balance": round(random.uniform(0.0, 100_000.0), 2),
    }


def _random_timestamp(base_ts, is_late):
    ts = base_ts
    if is_late:
        ts = ts - timedelta(days=random.randint(*LATE_ARRIVAL_DAYS))
        if ts < JAN_START:
            ts = JAN_START
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _txn_id():
    return "txn_" + "".join(random.choices(string.digits, k=10))


def generate_cdc_logs(num_records=NUM_RECORDS, num_unique_ids=NUM_UNIQUE_IDS, output_path=None):
    """Generate CDC log entries and write as JSONL. Returns the output path."""
    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "output", "cdc_logs.jsonl")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    random.seed(42)

    record_ids = [f"user_{i:05d}" for i in range(1, num_unique_ids + 1)]
    records = []

    insert_ids = random.sample(record_ids, min(20_000, num_unique_ids))
    for rid in insert_ids:
        offset = random.randint(0, JAN_SECONDS)
        records.append({
            "record_id": rid,
            "operation": "INSERT",
            "payload": _random_payload(),
            "timestamp": _random_timestamp(JAN_START + timedelta(seconds=offset),
                                           random.random() < LATE_ARRIVAL_FRACTION),
            "transaction_id": _txn_id(),
        })

    remaining = num_records - len(records)
    update_target = int(remaining * 0.75)
    delete_target = remaining - update_target

    for _ in range(update_target):
        rid = random.choice(record_ids)
        offset = random.randint(0, JAN_SECONDS)
        records.append({
            "record_id": rid,
            "operation": "UPDATE",
            "payload": _random_payload(),
            "timestamp": _random_timestamp(JAN_START + timedelta(seconds=offset),
                                           random.random() < LATE_ARRIVAL_FRACTION),
            "transaction_id": _txn_id(),
        })

    for rid in random.sample(record_ids, min(delete_target, num_unique_ids)):
        offset = random.randint(0, JAN_SECONDS)
        records.append({
            "record_id": rid,
            "operation": "DELETE",
            "payload": _random_payload(),
            "timestamp": _random_timestamp(JAN_START + timedelta(seconds=offset),
                                           random.random() < LATE_ARRIVAL_FRACTION),
            "transaction_id": _txn_id(),
        })

    random.shuffle(records)

    with open(output_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    return output_path


def validate_output(path):
    """Quick sanity check on the generated JSONL file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Output file not found: {path}")

    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  File size: {size_mb:.2f} MB")

    op_counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    unique_ids = set()
    total = 0

    with open(path, encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON at line {i+1}: {e}")
            op = rec.get("operation", "UNKNOWN")
            if op in op_counts:
                op_counts[op] += 1
            unique_ids.add(rec.get("record_id"))
            total += 1

    print(f"  Total records : {total:,}")
    print(f"  Unique IDs    : {len(unique_ids):,}")
    print(f"  INSERT        : {op_counts['INSERT']:,}")
    print(f"  UPDATE        : {op_counts['UPDATE']:,}")
    print(f"  DELETE        : {op_counts['DELETE']:,}")

    assert total == NUM_RECORDS, f"Expected {NUM_RECORDS} records, got {total}"
    assert size_mb >= 10, f"File too small ({size_mb:.2f} MB)"
    assert op_counts["INSERT"] > 0, "No INSERT records found"
    assert op_counts["UPDATE"] > 0, "No UPDATE records found"
    assert op_counts["DELETE"] > 0, "No DELETE records found"


if __name__ == "__main__":
    print("=" * 60)
    print("CDC Data Generator")
    print("=" * 60)

    out = generate_cdc_logs()
    print(f"\nGenerated: {out}")

    print("\nValidating output...")
    validate_output(out)

    print("\n[OK] 100K logs generated and validated")
    print("=" * 60)
