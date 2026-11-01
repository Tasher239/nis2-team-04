import json
import os
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv


def validate(events, records):
    source = {event["event_id"]: event for event in events}
    if len(source) != len(events):
        raise ValueError("Input contains duplicate event_id values")
    expected = defaultdict(lambda: {"purchase_count": 0, "total_cents": 0})
    for event in events:
        totals = expected[event["user_id"]]
        totals["purchase_count"] += 1
        totals["total_cents"] += event["amount_cents"]

    seen = Counter(record["event_id"] for record in records)
    missing = sorted(source.keys() - seen.keys())
    unexpected = sorted(seen.keys() - source.keys())
    duplicates = sum(count - 1 for count in seen.values())
    event_errors = 0
    for record in records:
        source_event = source.get(record["event_id"])
        if source_event is None:
            continue
        if (
            record["user_id"] != source_event["user_id"]
            or record["amount_cents"] != source_event["amount_cents"]
        ):
            event_errors += 1
    final = {}
    for record in records:
        user = record["user_id"]
        if user not in final or record["purchase_count"] > final[user]["purchase_count"]:
            final[user] = {field: record[field] for field in ("purchase_count", "total_cents")}
    users = sorted(expected.keys() | final.keys())
    aggregate_errors = [user for user in users if expected.get(user) != final.get(user)]
    return {
        "ok": not (missing or unexpected or duplicates or event_errors or aggregate_errors),
        "missing_event_ids": missing,
        "unexpected_event_ids": unexpected,
        "duplicate_events": duplicates,
        "event_errors": event_errors,
        "aggregate_errors": aggregate_errors,
    }


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main():
    load_dotenv(Path(__file__).with_name(".env"))
    input_path = Path(os.environ["INPUT_PATH"])
    output_path = Path("output") / os.environ["RUN_NAME"]
    events = read_events(input_path)
    records = []
    for path in sorted(output_path.rglob("part-*")):
        if path.is_file():
            records.extend(read_events(path))
    report = validate(events, records)
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
