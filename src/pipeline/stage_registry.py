import json
import os
from dataclasses import dataclass
from typing import Dict, Literal, Optional


Stage = Literal["extract", "transform", "load"]
Status = Literal["done", "failed"]


@dataclass
class StageRegistry:
    path: str

    def _ensure_parent_dir(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def load(self) -> Dict[str, Dict[str, str]]:
        """
        Returns mapping:
          { "YYYY-MM": { "extract": "done", "transform": "done", "load": "failed" }, ... }
        """
        if not os.path.exists(self.path):
            return {}

        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)

        months = data.get("months", {})
        if not isinstance(months, dict):
            return {}

        # Normalize to str
        out: Dict[str, Dict[str, str]] = {}
        for month_key, stage_map in months.items():
            if isinstance(stage_map, dict):
                out[str(month_key)] = {str(k): str(v) for k, v in stage_map.items()}
        return out

    def save(self, months: Dict[str, Dict[str, str]]) -> None:
        self._ensure_parent_dir()
        tmp_path = self.path + ".tmp"
        payload = {"months": months}

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

        os.replace(tmp_path, self.path)

    def get_status(self, month_key: str, stage: Stage) -> Optional[str]:
        months = self.load()
        return months.get(month_key, {}).get(stage)

    def is_done(self, month_key: str, stage: Stage) -> bool:
        return self.get_status(month_key, stage) == "done"

    def mark(self, month_key: str, stage: Stage, status: Status) -> None:
        months = self.load()
        months.setdefault(month_key, {})
        months[month_key][stage] = status
        self.save(months)

    def mark_done(self, month_key: str, stage: Stage) -> None:
        self.mark(month_key, stage, "done")

    def mark_failed(self, month_key: str, stage: Stage) -> None:
        self.mark(month_key, stage, "failed")