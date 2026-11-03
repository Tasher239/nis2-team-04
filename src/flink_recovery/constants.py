NANOSECONDS_PER_SECOND: int = 1_000_000_000
THROUGHPUT_WINDOW_S: int = 10
THROUGHPUT_WINDOW_NS: int = THROUGHPUT_WINDOW_S * NANOSECONDS_PER_SECOND

DEFAULT_SEED: int = 0
MIN_AMOUNT_CENTS: int = 100
MAX_AMOUNT_CENTS: int = 10_000

COMMANDS_FILE: str = "commands.jsonl"
METADATA_FILE: str = "run.json"
SINK_FILE_GLOB: str = "part-*"
PROTECTED_INPUT: str = "data/purchases.jsonl"

JOB_NAME: str = "purchase-totals"
SOURCE_UID: str = "purchases-source"
PARSE_UID: str = "purchase-parse"
TOTALS_UID: str = "purchase-totals"
SINK_UID: str = "purchase-output"

COUNT_STATE: str = "count"
TOTAL_STATE: str = "total"
