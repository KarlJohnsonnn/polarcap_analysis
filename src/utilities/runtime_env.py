from __future__ import annotations

import os
import platform


def is_server() -> bool:
    """Return True when running in server/HPC-like environments."""
    if os.getenv("JUPYTERHUB_API_URL") or os.getenv("JUPYTERHUB_USER"):
        return True
    if os.getenv("SLURM_JOB_ID"):
        return True
    return platform.system() != "Darwin"

