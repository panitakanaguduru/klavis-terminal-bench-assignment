#!/bin/sh
set -eu

cat > /app/processor.py <<'PY'
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA = Path("/app/data/events.jsonl")
CLEAN = Path("/app/output/clean_events.json")
METRICS = Path("/app/output/metrics.json")

REQUIRED = (
    "event_id",
    "user_id",
    "event_type",
    "event_time",
    "ingestion_time",
)


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("Timestamp must be a string")

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if dt.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return dt.astimezone(timezone.utc)


def process_events():
    latest = {}

    if DATA.exists():
        with DATA.open(encoding="utf-8") as f:
            for line in f:
                try:
                    event = json.loads(line)

                    if not isinstance(event, dict):
                        continue

                    if any(
                        not isinstance(event.get(field), str)
                        or not event[field].strip()
                        for field in REQUIRED
                    ):
                        continue

                    event_dt = parse_time(event["event_time"])
                    ingestion_dt = parse_time(event["ingestion_time"])

                except (ValueError, TypeError, json.JSONDecodeError):
                    continue

                event_id = event["event_id"]
                candidate = (
                    ingestion_dt,
                    event_dt,
                    json.dumps(event, sort_keys=True),
                )

                previous = latest.get(event_id)

                if previous is None or candidate > previous[0]:
                    latest[event_id] = (candidate, event)

    clean_events = [entry[1] for entry in latest.values()]

    clean_events.sort(
        key=lambda event: (
            parse_time(event["event_time"]),
            event["user_id"],
            event["event_id"],
        )
    )

    counts = defaultdict(int)
    users = defaultdict(set)

    for event in clean_events:
        kind = event["event_type"]
        counts[kind] += 1
        users[kind].add(event["user_id"])

    CLEAN.parent.mkdir(parents=True, exist_ok=True)

    CLEAN.write_text(
        json.dumps(clean_events, indent=2) + "\n",
        encoding="utf-8",
    )

    METRICS.write_text(
        json.dumps(
            {
                "event_counts": dict(sorted(counts.items())),
                "unique_users": {
                    kind: len(values)
                    for kind, values in sorted(users.items())
                },
            },
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    process_events()
PY

python /app/processor.py
