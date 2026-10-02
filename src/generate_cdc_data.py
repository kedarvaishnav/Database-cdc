"""
generate_cdc_data.py
====================
Generates 100,000 synthetic CDC (Change Data Capture) logs simulating
real-world out-of-order database event streams.

Schema (JSONL output):
    {
        "record_id":      "user_XXXXX",
        "operation":      "INSERT" | "UPDATE" | "DELETE",
        "payload": {
            "user_name":  str,
            "email":      str,
            "age":        int (18-80),
            "status":     "ACTIVE" | "INACTIVE" | "SUSPENDED",
            "balance":    float (0-100000)
        },
        "timestamp":      "2024-01-DD THH:MM:SSZ",
        "transaction_id": "txn_XXXXXXXXXX"
    }

Output:
    src/output/cdc_logs.jsonl   (15-20 MB)

Usage:
    python src/generate_cdc_data.py
"""

import json
import os
import random
import string
import sys
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

NUM_RECORDS = 100_000
NUM_UNIQUE_IDS = 50_000
LATE_ARRIVAL_FRACTION = 0.05   # 5 % of records arrive late
LATE_ARRIVAL_DAYS = (1, 3)     # 1-3 days behind true event time

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
STATUS_WEIGHTS = [0.70, 0.20, 0.10]   # 70 % ACTIVE

# January 2024 boundaries
JAN_START = datetime(2024, 1, 1, 0, 0, 0)
JAN_END   = datetime(2024, 1, 31, 23, 59, 59)
JAN_SECONDS = int((JAN_END - JAN_START).total_seconds())


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _random_email(name: str) -> str:
    slug = name.lower().replace(" ", ".")
    suffix = random.randint(1, 999)
    domain = random.choice(EMAIL_DOMAINS)
    return f"{slug}{suffix}@{domain}"


def _random_payload(record_id: str) -> dict:
    name = _random_name()
    return {
        "user_name": name,
        "email": _random_email(name),
        "age": random.randint(18, 80),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "balance": round(random.uniform(0.0, 100_000.0), 2),
    }


def _random_timestamp(base_ts: datetime, is_late: bool) -> str:
    """Return ISO-8601 UTC timestamp.

    If *is_late*, subtract 1-3 days from the event's natural position to
    simulate a late-arriving record.
    """
    ts = base_ts
    if is_late:
        lag_days = random.randint(*LATE_ARRIVAL_DAYS)
        ts = ts - timedelta(days=lag_days)
        # Clamp to window start
        if ts < JAN_START:
            ts = JAN_START
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _txn_id() -> str:
    return "txn_" + "".join(random.choices(string.digits, k=10))


# ─────────────────────────────────────────────────────────────────────────────
# Core generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_cdc_logs(
    num_records: int = NUM_RECORDS,
    num_unique_ids: int = NUM_UNIQUE_IDS,
    output_path: str = None,
) -> str:
    """Generate *num_records* CDC log entries and write them as JSONL.

    Returns the path of the written file.
    """
    if output_path is None:
        # Write next to this script's parent → src/output/cdc_logs.jsonl
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "output", "cdc_logs.jsonl")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    random.seed(42)   # reproducible

    # Build pool of record IDs
    record_ids = [f"user_{i:05d}" for i in range(1, num_unique_ids + 1)]

    records = []

    # ── Phase 1: INSERT (one per unique ID, up to 20 K) ──────────────────────
    insert_ids = random.sample(record_ids, min(20_000, num_unique_ids))
    insert_count = 0
    for rid in insert_ids:
        offset = random.randint(0, JAN_SECONDS)
        base_ts = JAN_START + timedelta(seconds=offset)
        is_late = random.random() < LATE_ARRIVAL_FRACTION
        records.append({
            "record_id": rid,
            "operation": "INSERT",
            "payload": _random_payload(rid),
            "timestamp": _random_timestamp(base_ts, is_late),
            "transaction_id": _txn_id(),
        })
        insert_count += 1

    remaining = num_records - insert_count   # ~80 K

    # ── Phase 2: UPDATE (60 % of remaining ≈ 48 K) ───────────────────────────
    update_target = int(remaining * 0.75)    # fill most remaining with UPDATEs
    delete_target = remaining - update_target

    for _ in range(update_target):
        rid = random.choice(record_ids)
        offset = random.randint(0, JAN_SECONDS)
        base_ts = JAN_START + timedelta(seconds=offset)
        is_late = random.random() < LATE_ARRIVAL_FRACTION
        records.append({
            "record_id": rid,
            "operation": "UPDATE",
            "payload": _random_payload(rid),
            "timestamp": _random_timestamp(base_ts, is_late),
            "transaction_id": _txn_id(),
        })

    # ── Phase 3: DELETE (~10 K) ───────────────────────────────────────────────
    delete_ids = random.sample(record_ids, min(delete_target, num_unique_ids))
    for rid in delete_ids:
        offset = random.randint(0, JAN_SECONDS)
        base_ts = JAN_START + timedelta(seconds=offset)
        is_late = random.random() < LATE_ARRIVAL_FRACTION
        records.append({
            "record_id": rid,
            "operation": "DELETE",
            "payload": _random_payload(rid),
            "timestamp": _random_timestamp(base_ts, is_late),
            "transaction_id": _txn_id(),
        })

    # ── Shuffle (arrival order ≠ timestamp order) ─────────────────────────────
    random.shuffle(records)

    # ── Write JSONL ───────────────────────────────────────────────────────────
    with open(output_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────

def validate_output(path: str) -> None:
    """Quick sanity check on the generated file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Output file not found: {path}")

    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  File size: {size_mb:.2f} MB")

    op_counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    total = 0
    unique_ids = set()

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
            op_counts[op] = op_counts.get(op, 0) + 1
            unique_ids.add(rec.get("record_id"))
            total += 1

    print(f"  Total records : {total:,}")
    print(f"  Unique IDs    : {len(unique_ids):,}")
    print(f"  INSERT        : {op_counts['INSERT']:,}")
    print(f"  UPDATE        : {op_counts['UPDATE']:,}")
    print(f"  DELETE        : {op_counts['DELETE']:,}")

    assert total == NUM_RECORDS,      f"Expected {NUM_RECORDS} records, got {total}"
    assert size_mb >= 10,             f"File too small ({size_mb:.2f} MB)"
    assert op_counts["INSERT"] > 0,   "No INSERT records found"
    assert op_counts["UPDATE"] > 0,   "No UPDATE records found"
    assert op_counts["DELETE"] > 0,   "No DELETE records found"


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("CDC Data Generator — Task 1.1")
    print("=" * 60)

    out = generate_cdc_logs()
    print(f"\nGenerated: {out}")

    print("\nValidating output...")
    validate_output(out)

    print("\n[OK] Task 1.1 COMPLETE: 100K logs generated and validated")
    print("=" * 60)
