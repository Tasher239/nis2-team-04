"""Calculate metrics from a local processing journal."""
import argparse
import json
from pathlib import Path


def calculate(records: list[dict], commands: list[dict]) -> dict:
    times = sorted(record['processed_at_ns'] for record in records)
    if not times:
        raise ValueError('No processed events found')
    first = times[0]
    window_end = first + 10_000_000_000
    restarts = [item for item in commands if item['event'] == 'restore_requested']
    pause = recovery = None
    if restarts:
        if len(restarts) != 1:
            raise ValueError('Use one restoration per journal')
        restart = restarts[0]['time_ns']
        before = [t for t in times if t < restart]
        after = [t for t in times if t >= restart]
        if not before or not after:
            raise ValueError('Restoration needs events before and after the command')
        first = after[0]
        window_end = first + 10_000_000_000
        pause = (first - before[-1]) / 1e9
        recovery = (first - restart) / 1e9
    finished = [item['time_ns'] for item in commands if item['event'] == 'job_finished']
    complete = bool(finished and max(finished) >= window_end)
    return {
        'processed_records': len(times),
        'pause_s': pause,
        'recovery_s': recovery,
        'throughput_window_s': 10,
        'events_in_window': sum(first <= t < window_end for t in times),
        'window_complete': complete,
        'throughput_events_s': (
            sum(first <= t < window_end for t in times) / 10 if complete else None
        ),
    }


def read_journal(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    args = parser.parse_args()
    records = []
    for path in args.run.rglob('part-*'):
        if path.is_file():
            records.extend(read_journal(path))
    result = calculate(records, read_journal(args.run / 'commands.jsonl'))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
