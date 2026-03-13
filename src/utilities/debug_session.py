"""Minimal NDJSON logger for the active debug session."""

from __future__ import annotations

import json
from pathlib import Path
from time import time
from uuid import uuid4

LOG_PATH = Path("/home/b/b382237/code/polarcap/python/polarcap_analysis/.cursor/debug-fd308a.log")
SESSION_ID = "fd308a"


def _json_safe(value):
    """Return a JSON-serializable copy with repr fallbacks."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return repr(value)


def write_debug_log(location: str, message: str, data: dict, hypothesis_id: str, run_id: str = "initial") -> None:
    """Append one NDJSON debug record for this Cursor debug session."""
    payload = {
        "sessionId": SESSION_ID,
        "id": f"log_{int(time() * 1000)}_{uuid4().hex[:8]}",
        "timestamp": int(time() * 1000),
        "location": location,
        "message": message,
        "data": _json_safe(data),
        "runId": run_id,
        "hypothesisId": hypothesis_id,
    }
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass
