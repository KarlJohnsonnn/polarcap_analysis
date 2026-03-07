"""Path resolution for COSMO-SPECS processing chain.

Resolves CS_RUNS_DIR and RUN_ERISWILL_*/ensemble_output without hardcoded absolutes.
"""

from __future__ import annotations

import os
from pathlib import Path


def get_runs_root(root: str | None = None) -> str | None:
    """Model data root (contains RUN_ERISWILL_* subdirs). Prefer --root, else $CS_RUNS_DIR."""
    if root and root.strip():
        return root.rstrip(os.sep)
    return os.environ.get("CS_RUNS_DIR") or None


def resolve_ensemble_output(runs_root: str, cs_run: str) -> str | None:
    """Find ensemble_output dir under runs_root. Prefer one containing cs_run. None if not found."""
    base = Path(runs_root)
    if not base.is_dir():
        return None
    best = None
    for cand in sorted(base.glob("RUN_ERISWILL_*x100")):
        ens = cand / "ensemble_output"
        if ens.is_dir():
            if (ens / cs_run).exists():
                return str(ens)
            if best is None:
                best = ens
    return str(best) if best else None
