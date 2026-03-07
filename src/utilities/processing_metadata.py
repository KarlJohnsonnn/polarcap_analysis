"""
Provenance and metadata helpers for processing-chain NetCDF and Zarr outputs.

Provides: git commit id, creation timestamp, standardized global and variable
attrs, and Zarr-safe attr normalization (no ndarray/cmap in attrs).
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import xarray as xr


def find_repo_root(start: Optional[Path] = None) -> Optional[Path]:
    """Return repo root (directory containing .git) or None."""
    p = (start or Path.cwd()).resolve()
    for parent in (p, *p.parents):
        if (parent / ".git").exists():
            return parent
    return None


def git_head(repo_root: Optional[Path] = None) -> Optional[str]:
    """Return full SHA of HEAD; None if not a git repo or error."""
    rr = repo_root or find_repo_root()
    if rr is None:
        return None
    try:
        return subprocess.check_output(
            ["git", "-C", str(rr), "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return None


def provenance_attrs(
    *,
    stage: str,
    processing_level: str,
    title: str = "",
    summary: str = "",
    source_code_path: str = "",
    source_notebook_or_script: str = "",
    input_files: Optional[List[str]] = None,
    cs_run: str = "",
    exp_id: Optional[int] = None,
    exp_label: str = "",
    domain: str = "",
    history: str = "",
    conventions: str = "CF-1.8",
    repo_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Build a dict of global dataset attributes for provenance."""
    commit = git_head(repo_root)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    attrs: Dict[str, Any] = {
        "stage": stage,
        "processing_level": processing_level,
        "created_utc": now,
        "Conventions": conventions,
    }
    if title:
        attrs["title"] = title
    if summary:
        attrs["summary"] = summary
    if commit:
        attrs["git_commit"] = commit
        attrs["git_commit_short"] = commit[:12]
    if source_code_path:
        attrs["source_code_path"] = source_code_path
    if source_notebook_or_script:
        attrs["source_notebook_or_script"] = source_notebook_or_script
    if input_files is not None:
        attrs["input_files"] = input_files
    if cs_run:
        attrs["cs_run"] = cs_run
    if exp_id is not None:
        attrs["exp_id"] = exp_id
    if exp_label:
        attrs["exp_label"] = exp_label
    if domain:
        attrs["domain"] = domain
    if history:
        attrs["history"] = history
    return attrs


def normalize_attrs_for_zarr(attrs: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of attrs with only Zarr-serializable values (no ndarray, etc.)."""
    out: Dict[str, Any] = {}
    for k, v in attrs.items():
        if isinstance(v, np.ndarray):
            continue
        if hasattr(v, "__len__") and not isinstance(v, (str, list, tuple)):
            continue
        out[k] = v
    return out


def add_provenance_to_dataset(
    ds: xr.Dataset,
    **kwargs: Any,
) -> xr.Dataset:
    """Attach provenance global attrs to *ds* (in place). Merges with existing attrs."""
    extra = provenance_attrs(**kwargs)
    for k, v in extra.items():
        ds.attrs[k] = v
    return ds


def ensure_coord_attrs(
    ds: xr.Dataset,
    coord_meta: Optional[Dict[str, Dict[str, str]]] = None,
) -> xr.Dataset:
    """Ensure coordinates have long_name/description/units where provided."""
    if coord_meta is None:
        return ds
    for name, meta in coord_meta.items():
        if name not in ds.coords:
            continue
        for key in ("long_name", "description", "units"):
            if key in meta and key not in ds.coords[name].attrs:
                ds.coords[name].attrs[key] = meta[key]
    return ds
