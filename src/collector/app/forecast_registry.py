from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ForecastModelRef:
    model_id: str
    version: str
    backend: str
    validation_mape: float | None
    file: str
    raw: dict[str, Any]


class ForecastRegistry:
    def __init__(self, model_dir: str):
        self._model_dir = Path(model_dir)

    def list_models(self) -> list[ForecastModelRef]:
        refs: list[ForecastModelRef] = []
        if not self._model_dir.exists():
            return refs
        for fp in sorted(self._model_dir.glob("*.json")):
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue
            model_id = str(raw.get("model_id") or "").strip()
            version = str(raw.get("version") or "").strip()
            if not model_id or not version:
                continue
            mape = raw.get("validation_mape")
            try:
                mape_val = float(mape) if mape is not None else None
            except Exception:
                mape_val = None
            refs.append(
                ForecastModelRef(
                    model_id=model_id,
                    version=version,
                    backend=str(raw.get("backend") or "unknown"),
                    validation_mape=mape_val,
                    file=str(fp),
                    raw=raw,
                )
            )
        refs.sort(key=lambda x: (x.model_id, x.version))
        return refs

    def resolve(self, model_id: str, version: str | None = None) -> ForecastModelRef | None:
        refs = [x for x in self.list_models() if x.model_id == model_id]
        if not refs:
            return None
        if version:
            for ref in refs:
                if ref.version == version:
                    return ref
            return None
        refs.sort(key=lambda x: x.version)
        return refs[-1]
