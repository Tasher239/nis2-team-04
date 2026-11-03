from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    input_path: Path = Path("data/generated/purchases.jsonl")
    user_count: int = Field(default=1000, ge=1)
    purchases_per_user: int = Field(default=100, ge=1)
    run_name: str = Field(default="generated", min_length=1)
    parallelism: int = Field(default=2, ge=1)
    checkpoint_path: Path = Path("output/checkpoints")
    checkpoint_interval_ms: int = Field(default=1000, gt=0)
    output_root: Path = Path("output")

    @property
    def output_path(self) -> Path:
        return self.output_root / self.run_name
