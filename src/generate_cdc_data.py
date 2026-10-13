#!/usr/bin/env python3
"""
CDC Synthetic Data Generator
Generates exactly 100,000 Change Data Capture (CDC) events in JSON Lines (JSONL) format
with out-of-order timestamps, updates, and delete tombstones, such that
reconciliation produces exactly 47,923 active records.
"""

import os
import sys
import json
import random
from datetime import datetime, timedelta

def generate_cdc_events(output_file="data/cdc_events.jsonl", total_events=100000, target_active=47923, seed=42):
    random.seed(seed)
    
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # We want exactly `target_active` unique records whose latest event is NOT a DELETE.
    # We also include a pool of deleted records whose latest event IS a DELETE.
    target_deleted = 3500
    
    first_names = [
        "Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah",
        "Ian", "Julia", "Kevin", "Laura", "Michael", "Nina", "Oliver", "Paula",
        "Quinn", "Rachel", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xavier",
        "Yasmine", "Zachary", "Abel", "Kedar", "Sarah", "David", "Emma", "James"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
        "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
    ]
    domains = ["example.com", "company.org", "techcorp.net", "dataflow.io", "cloudscale.dev"]
    statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]
    
    # January 2024 timeframe as required by validate_cdc.sql (Jan 1 - Jan 31, 2024)
    start_date = datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime(2024, 1, 31, 23, 59, 59)
    total_seconds = int((end_date - start_date).total_seconds())

    # Generate records
    all_events = []
    
    # Event budget:
    # 1 event per active record (INSERT): target_active
    # 2 events per deleted record (INSERT + DELETE): target_deleted * 2
    # Remaining events: UPDATEs distributed across active (and deleted) records
    base_events_count = target_active + (target_deleted * 2)
    extra_updates = total_events - base_events_count
    if extra_updates < 0:
        raise ValueError("Total events must be at least target_active + target_deleted * 2")

    # Generate active records events
    # We assign extra updates across a subset of active records
    update_counts = [0] * target_active
    for _ in range(extra_updates):
        idx = random.randint(0, target_active - 1)
        update_counts[idx] += 1
        
    txn_counter = 1
    
    print(f"Generating {target_active} active records and {target_deleted} deleted records...")
    
    # 1. Active records
    for i in range(target_active):
        rec_id = f"user_{100000 + i}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        user_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{i % 997}@{random.choice(domains)}"
        age = random.randint(18, 75)
        status = random.choice(statuses)
        # Ensure varied balances including high-value (> 50,000)
        balance = round(random.uniform(500.0, 95000.0), 2)
        
        num_updates = update_counts[i]
        
        # Generate timestamps in chronological sequence for this record
        offsets = sorted(random.sample(range(total_seconds), num_updates + 1))
        
        # Event 1: INSERT
        t0 = start_date + timedelta(seconds=offsets[0])
        payload = {
            "user_name": user_name,
            "email": email,
            "age": age,
            "status": status,
            "balance": balance
        }
        all_events.append({
            "record_id": rec_id,
            "operation": "INSERT",
            "payload": payload,
            "timestamp": t0.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "transaction_id": f"txn_{txn_counter:07d}"
        })
        txn_counter += 1
        
        # Subsequent events: UPDATEs
        curr_payload = dict(payload)
        for u in range(num_updates):
            t_u = start_date + timedelta(seconds=offsets[u + 1])
            # Evolve payload
            if random.random() < 0.5:
                curr_payload["balance"] = round(curr_payload["balance"] + random.uniform(-1000.0, 5000.0), 2)
            if random.random() < 0.3:
                curr_payload["status"] = random.choice(statuses)
            if random.random() < 0.2:
                curr_payload["age"] += 1
                
            all_events.append({
                "record_id": rec_id,
                "operation": "UPDATE",
                "payload": dict(curr_payload),
                "timestamp": t_u.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "transaction_id": f"txn_{txn_counter:07d}"
            })
            txn_counter += 1
            
    # 2. Deleted records
    for j in range(target_deleted):
        rec_id = f"user_{900000 + j}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        user_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{j % 997}@{random.choice(domains)}"
        age = random.randint(20, 65)
        status = random.choice(statuses)
        balance = round(random.uniform(100.0, 20000.0), 2)
        
        offsets = sorted(random.sample(range(total_seconds), 2))
        t_insert = start_date + timedelta(seconds=offsets[0])
        t_delete = start_date + timedelta(seconds=offsets[1])
        
        # INSERT event
        all_events.append({
            "record_id": rec_id,
            "operation": "INSERT",
            "payload": {
                "user_name": user_name,
                "email": email,
                "age": age,
                "status": status,
                "balance": balance
            },
            "timestamp": t_insert.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "transaction_id": f"txn_{txn_counter:07d}"
        })
        txn_counter += 1
        
        # DELETE tombstone event (must be latest event for this record)
        all_events.append({
            "record_id": rec_id,
            "operation": "DELETE",
            "payload": {
                "user_name": user_name,
                "email": email,
                "age": age,
                "status": "INACTIVE",
                "balance": balance
            },
            "timestamp": t_delete.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "transaction_id": f"txn_{txn_counter:07d}"
        })
        txn_counter += 1
        
    assert len(all_events) == total_events, f"Expected {total_events} events, got {len(all_events)}"
    
    # Shuffle all events to simulate out-of-order, asynchronous delivery
    print(f"Shuffling {len(all_events)} events to simulate out-of-order delivery...")
    random.shuffle(all_events)
    
    # Write to output JSONL
    print(f"Writing to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        for event in all_events:
            f.write(json.dumps(event) + "\n")
            
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"? Generated {len(all_events)} JSONL logs ({file_size_mb:.2f} MB) in {output_file}")
    return output_file

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "data/cdc_events.jsonl"
    generate_cdc_events(out)
