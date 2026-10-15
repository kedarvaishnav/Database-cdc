#!/usr/bin/env python3
"""Fetch a small real Stack Overflow revision history as CDC-style JSONL.

The defaults (3758880 and 4888400) are real Stack Overflow answers with
published edit histories. The API's revision data is retained in ``payload``;
INSERT is assigned to the first revision and UPDATE to later revisions so the
existing CDC reducer can replay the version histories.

Usage:
    python3 src/fetch_stackoverflow_revisions.py
    python3 src/fetch_stackoverflow_revisions.py --post-id 3758880 --output data/sample.jsonl
"""

import argparse
from datetime import datetime, timezone
import json
import os
import random
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_POST_IDS = (3758880, 4888400)
API_ROOT = "https://api.stackexchange.com/2.3"
USER_AGENT = "database-cdc-course-demo/1.0 (Stack Exchange revision history importer)"


def fetch_post_revisions(post_id):
    """Return every public revision for one Stack Overflow post."""
    revisions = []
    page = 1

    while True:
        query = urlencode({
            "site": "stackoverflow",
            "pagesize": 100,
            "page": page,
            "filter": "withbody",
        })
        url = f"{API_ROOT}/posts/{post_id}/revisions?{query}"
        request = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urlopen(request, timeout=30) as response:
                result = json.load(response)
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Could not fetch revisions for post {post_id}: {exc}") from exc

        if "error_id" in result:
            raise RuntimeError(
                f"Stack Exchange API error {result['error_id']}: "
                f"{result.get('error_message', 'unknown error')}"
            )

        revisions.extend(result.get("items", []))
        if not result.get("has_more", False):
            break

        backoff = int(result.get("backoff", 0))
        if backoff:
            time.sleep(backoff)
        page += 1

    return revisions


def revision_to_event(revision):
    """Map a Stack Exchange revision to the project's CDC envelope."""
    post_id = int(revision["post_id"])
    revision_number = int(revision.get("revision_number", 0))
    created = datetime.fromtimestamp(
        int(revision["creation_date"]), tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    user = revision.get("user") or {}

    payload = {
        "post_type": revision.get("post_type"),
        "revision_type": revision.get("revision_type"),
        "revision_number": revision_number,
        "author_display_name": user.get("display_name"),
        "author_id": user.get("user_id"),
        "author_url": user.get("link"),
        "revision_comment": revision.get("comment"),
        "content_license": revision.get("content_license"),
        "is_rollback": revision.get("is_rollback", False),
        "source_url": f"https://stackoverflow.com/a/{post_id}",
    }
    # Keep only content that the API returned for this revision.
    for field in ("title", "body", "tags"):
        if revision.get(field) is not None:
            payload[field] = revision[field]

    return {
        "record_id": f"post_{post_id}",
        "operation": "INSERT" if revision_number == 1 else "UPDATE",
        "payload": payload,
        "timestamp": created,
        "transaction_id": (
            f"se_{post_id}_rev_{revision_number:010d}_"
            f"{revision.get('revision_guid', '')}"
        ),
    }


def write_events(post_ids, output_path):
    """Fetch, shuffle, and write revision events; return the event list."""
    events = []
    for post_id in post_ids:
        revisions = fetch_post_revisions(post_id)
        if not revisions:
            raise ValueError(f"No public revisions found for Stack Overflow post {post_id}")
        events.extend(revision_to_event(revision) for revision in revisions)

    # Simulate out-of-order delivery while preserving real source timestamps.
    random.Random(42).shuffle(events)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as output:
        for event in events:
            output.write(json.dumps(event, ensure_ascii=False) + "\n")
    return events


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--post-id", type=int, action="append", dest="post_ids",
        help=("Stack Overflow post ID; repeat to add more "
              f"(default: {', '.join(map(str, DEFAULT_POST_IDS))})"),
    )
    parser.add_argument(
        "--output", default=os.path.join(repo_root, "data", "stackoverflow_revisions.jsonl"),
        help="JSONL output path (default: data/stackoverflow_revisions.jsonl)",
    )
    args = parser.parse_args()
    post_ids = args.post_ids or list(DEFAULT_POST_IDS)

    events = write_events(post_ids, args.output)
    unique_posts = len({event["record_id"] for event in events})
    print(f"Fetched {len(events):,} real revision events across {unique_posts} post(s).")
    print(f"Wrote out-of-order CDC JSONL to: {os.path.abspath(args.output)}")
    print(f"Repeated post IDs in input: {len(events) - unique_posts:,}")
    print("Source values and per-revision content_license are retained in payload.")


if __name__ == "__main__":
    main()
