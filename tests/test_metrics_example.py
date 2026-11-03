import json
from pathlib import Path

from flink_recovery.data.run_directory import RunDirectory
from flink_recovery.domain.metrics import JournalAnalyzer

EXAMPLE = Path(__file__).resolve().parents[1] / "nis" / "metrics-example"


def test_metrics_example_matches_expected() -> None:
    directory = RunDirectory(EXAMPLE)
    report = JournalAnalyzer().analyze(directory.journal())
    expected = json.loads((EXAMPLE / "expected.json").read_text())
    produced = json.loads(report.model_dump_json())
    assert produced == expected
