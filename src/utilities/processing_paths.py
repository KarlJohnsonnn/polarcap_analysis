"""Path resolution for COSMO-SPECS processing chain.

Resolves CS_RUNS_DIR and output roots without scattering machine-specific logic
across the launch scripts.
"""

from __future__ import annotations

import os
from pathlib import Path

REMOTE_RUNS_ROOT = "/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs"


def default_local_processed_root() -> str:
    """Absolute path to ``scripts/data/registry/processed`` under the polarcap_analysis repo."""
    repo_root = Path(__file__).resolve().parents[2]
    return str((repo_root / "scripts" / "data" / "registry" / "processed").resolve())


def expand_path(path: str | None) -> str:
    """Expand shell-like path strings and trim whitespace."""
    if not path or not str(path).strip():
        return ""
    return os.path.expandvars(os.path.expanduser(str(path))).strip()


def get_runs_root(root: str | None = None) -> str | None:
    """Model data root (contains RUN_ERISWILL_* subdirs). Prefer --root, else $CS_RUNS_DIR."""
    chosen = expand_path(root)
    if chosen:
        return chosen.rstrip(os.sep)
    env_root = expand_path(os.environ.get("CS_RUNS_DIR"))
    return env_root.rstrip(os.sep) if env_root else None


def get_output_root(
    root: str | None = None,
    runs_root: str | None = None,
    cs_run: str | None = None,
) -> str:
    """Resolve output root for generated processing products.

    Priority:
    1. Explicit CLI/config value.
    2. ``$POLARCAP_OUTPUT_ROOT``.
    3. Matching ``RUN_ERISWILL_*x100/ensemble_output`` under the active runs root.
    4. Matching ``RUN_ERISWILL_*x100/ensemble_output`` under the known levante
       work tree.
    5. Local ``scripts/data/registry/processed`` under the repo (absolute path).
    """
    chosen = expand_path(root)
    if chosen:
        return chosen.rstrip(os.sep)

    env_root = expand_path(os.environ.get("POLARCAP_OUTPUT_ROOT"))
    if env_root:
        return env_root.rstrip(os.sep)

    for candidate in (runs_root, get_runs_root(), REMOTE_RUNS_ROOT):
        resolved = resolve_ensemble_output(candidate, cs_run=cs_run)
        if resolved:
            return resolved.rstrip(os.sep)

    return default_local_processed_root()


def resolve_ensemble_output(runs_root: str | None, cs_run: str | None = None) -> str | None:
    """Find an ``ensemble_output`` directory from a runs root or run directory.

    ``runs_root`` may be:
    - the parent containing multiple ``RUN_ERISWILL_*x100`` directories,
    - a specific ``RUN_ERISWILL_*x100`` directory,
    - an ``ensemble_output`` directory itself.
    """
    base_raw = expand_path(runs_root)
    if not base_raw:
        return None
    base = Path(base_raw)
    if not base.is_dir():
        return None

    if base.name == "ensemble_output":
        if cs_run is None or (base / cs_run).exists():
            return str(base)
        return None

    if base.name.startswith("RUN_ERISWILL_"):
        ens = base / "ensemble_output"
        if ens.is_dir() and (cs_run is None or (ens / cs_run).exists()):
            return str(ens)
        return None

    best = None
    for cand in sorted(base.glob("RUN_ERISWILL_*x100")):
        ens = cand / "ensemble_output"
        if ens.is_dir():
            if cs_run and (ens / cs_run).exists():
                return str(ens)
            if best is None:
                best = ens
    return str(best) if best else None


def find_ensemble_output_for_cs_run(
    cs_run: str,
    *,
    config_runs_root: str | None = None,
) -> tuple[str | None, list[str]]:
    """
    Locate ``.../ensemble_output`` that contains directory ``cs_run``.

    Order: ``config_runs_root`` (e.g. YAML ``paths.server_root``), ``$CS_RUNS_DIR``,
    ``$POLARCAP_OUTPUT_ROOT``, ``REMOTE_RUNS_ROOT``, ``default_local_processed_root()``.

    Returns ``(ensemble_output_path, list_of_roots_tried)``. The path is only returned if
    ``(ensemble_output / cs_run).is_dir()``.
    """
    tried: list[str] = []
    candidates = [
        config_runs_root,
        get_runs_root(),
        expand_path(os.environ.get("POLARCAP_OUTPUT_ROOT")),
        REMOTE_RUNS_ROOT,
        default_local_processed_root(),
    ]
    seen: set[str] = set()
    for raw in candidates:
        root = expand_path(raw)
        if not root or root in seen:
            continue
        seen.add(root)
        tried.append(root)
        ens = resolve_ensemble_output(root, cs_run=cs_run)
        if ens and (Path(ens) / cs_run).is_dir():
            return ens.rstrip(os.sep), tried
    return None, tried
