from pydantic import BaseModel, ConfigDict

from flink_recovery.constants import THROUGHPUT_WINDOW_S


class MetricDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    unit: str
    definition: str

    def as_row(self) -> str:
        return f"| `{self.name}` | {self.unit} | {self.definition} |"


METRIC_DEFINITIONS: tuple[MetricDefinition, ...] = (
    MetricDefinition(
        name="processed_records",
        unit="events",
        definition="Число записей, записанных приемником за весь запуск.",
    ),
    MetricDefinition(
        name="pause_s",
        unit="s",
        definition=(
            "От последнего события до восстановления до первого события после него; "
            "null, если восстановления не было."
        ),
    ),
    MetricDefinition(
        name="recovery_s",
        unit="s",
        definition=(
            "От команды восстановления до первого обработанного после нее события; "
            "null, если восстановления не было."
        ),
    ),
    MetricDefinition(
        name="throughput_events_s",
        unit="events/s",
        definition=(
            f"События за первые {THROUGHPUT_WINDOW_S} с после восстановления "
            f"(или после старта), деленные на {THROUGHPUT_WINDOW_S}; "
            "null, если окно не доработано до конца."
        ),
    ),
)
