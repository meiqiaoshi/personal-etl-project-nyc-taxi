import json
import os
from dataclasses import dataclass
from typing import Set


@dataclass
class ProcessingRegistry:
    path: str

    def _ensure_parent_dir(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def load(self) -> Set[str]:
        """
        Returns a set of processed month keys like '2023-01'.
        """
        if not os.path.exists(self.path):
            return set()

        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)

        processed = data.get("processed", [])
        if not isinstance(processed, list):
            return set()
        return set(str(x) for x in processed)

    def save(self, processed: Set[str]) -> None:
        """
        Atomic-ish write: write to temp then replace.
        """
        self._ensure_parent_dir()
        tmp_path = self.path + ".tmp"
        payload = {"processed": sorted(processed)}

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        os.replace(tmp_path, self.path)

    def is_processed(self, key: str) -> bool:
        processed = self.load()
        return key in processed

    def mark_processed(self, key: str) -> None:
        processed = self.load()
        processed.add(key)
        self.save(processed)