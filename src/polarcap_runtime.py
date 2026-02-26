from __future__ import annotations

import importlib
import os
import platform
import sys
from pathlib import Path


def is_server() -> bool:
    """Return True when running in server/HPC-like environments."""
    if os.getenv("JUPYTERHUB_API_URL") or os.getenv("JUPYTERHUB_USER"):
        return True
    if os.getenv("SLURM_JOB_ID"):
        return True
    return platform.system() != "Darwin"


def find_repo_root(start: Path | None = None) -> Path:
    """Find repo root by locating src/utilities/runtime_env.py."""
    cursor = (start or Path.cwd()).resolve()
    for candidate in (cursor, *cursor.parents):
        if (candidate / "src" / "utilities" / "runtime_env.py").is_file():
            return candidate
    raise FileNotFoundError("Could not locate repo root containing src/utilities/runtime_env.py")


def setup_notebook_path(start: Path | None = None) -> tuple[Path, Path]:
    """Ensure this repo's src path is first on sys.path."""
    repo_root = find_repo_root(start=start)
    src_dir = (repo_root / "src").resolve()
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    return repo_root, src_dir


def import_local_utilities(start: Path | None = None):
    """
    Import local utilities package even if a foreign package named 'utilities'
    was loaded earlier in the active kernel.
    """
    _, src_dir = setup_notebook_path(start=start)
    loaded_utilities = sys.modules.get("utilities")
    if loaded_utilities is not None:
        loaded_path = Path(getattr(loaded_utilities, "__file__", "")).resolve()
        if src_dir not in loaded_path.parents:
            del sys.modules["utilities"]
    return importlib.import_module("utilities")

