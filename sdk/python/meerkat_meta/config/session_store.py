"""Session store configuration for Meta runtime."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class JsonSessionStoreConfig:
    path: str
    stale_lock_threshold_seconds: int = 30

    def to_dict(self) -> dict[str, Any]:
        return {
            "store": "json_file",
            "path": self.path,
            "stale_lock_threshold_seconds": self.stale_lock_threshold_seconds,
        }


@dataclass(frozen=True)
class BigQuerySessionStoreConfig:
    dataset: str
    table: str
    project_id: str | None = None
    gc_interval_hours: int = 6

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "store": "bigquery",
            "dataset": self.dataset,
            "table": self.table,
            "gc_interval_hours": self.gc_interval_hours,
        }
        if self.project_id:
            result["project_id"] = self.project_id
        return result


def json(path: str, **kwargs: Any) -> JsonSessionStoreConfig:
    return JsonSessionStoreConfig(path=path, **kwargs)


def bigquery(dataset: str, table: str, **kwargs: Any) -> BigQuerySessionStoreConfig:
    return BigQuerySessionStoreConfig(dataset=dataset, table=table, **kwargs)
