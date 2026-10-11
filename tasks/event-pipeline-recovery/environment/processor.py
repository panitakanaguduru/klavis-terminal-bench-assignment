import json
from collections import defaultdict
from pathlib import Path

DATA = Path("/app/data/events.jsonl")
CLEAN = Path("/app/output/clean_events.json")
METRICS = Path("/app/output/metrics.json")


def process_events():
    events = []

    with DATA.open() as f:
        for line in f:
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError:
                continue

    # BUG 1: Does not remove duplicate events
    clean_events = events

    # BUG 2: Does not validate required fields
    # BUG 3: Does not validate timestamps

    # BUG 4: Output depends on input ordering

    counts = defaultdict(int)
    users = defaultdict(set)

    for event in clean_events:
        event_type = event.get("event_type", "unknown")
        counts[event_type] += 1
        users[event_type].add(event.get("user_id"))

    CLEAN.parent.mkdir(parents=True, exist_ok=True)

    with CLEAN.open("w") as f:
        json.dump(clean_events, f, indent=2)

    with METRICS.open("w") as f:
        json.dump({
            "event_counts": dict(counts),
            "unique_users": {
                k: len(v) for k, v in users.items()
            }
        }, f, indent=2)


if __name__ == "__main__":
    process_events()
