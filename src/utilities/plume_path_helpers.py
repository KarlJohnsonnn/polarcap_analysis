"""Shared helpers for plume-path analysis (PSD waterfall, diagnostics).

Extracted from the former ``plume_lagrangian`` figure module so other utilities
do not depend on the removed legacy implementation.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import xarray as xr


def build_common_xlim(
    ds_by_run: dict[str, dict[str, xr.Dataset]],
    *, kind: str = "integrated", span_min: int = 5, anchor: np.datetime64 | None = None,
) -> list[np.datetime64]:
    starts = []
    for run in ds_by_run.values():
        ds = run.get(kind)
        if not isinstance(ds, xr.Dataset) or "time" not in ds.coords or ds.time.size == 0:
            continue
        starts.append(np.datetime64(ds.time.values.min(), "s"))
    if not starts:
        raise ValueError(f"Cannot infer xlim: no datasets with kind='{kind}' and valid time")
    t0 = (
        np.datetime64(anchor).astype("datetime64[s]")
        if anchor is not None
        else np.datetime64(str(min(starts))[:10] + "T12:29:00")
    )
    return [t0, t0 + np.timedelta64(int(span_min), "m")]


def diagnostics_table(
    ds_by_run: dict[str, dict[str, xr.Dataset]],
    *, kind: str = "integrated", variable: str = "nf", xlim: list[np.datetime64] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for label, run in ds_by_run.items():
        ds = run.get(kind)
        if ds is None:
            rows.append({"run": label, "status": "missing kind"})
            continue
        if not isinstance(ds, xr.Dataset):
            rows.append({"run": label, "status": f"invalid kind type: {type(ds).__name__}"})
            continue
        row = {
            "run": label, "status": "ok", "n_cells": int(ds.sizes.get("cell", 1)),
            "n_time": int(ds.sizes.get("time", 0)),
            "time_min": str(ds.time.values.min()).split(".")[0] if "time" in ds.coords and ds.time.size else "-",
            "time_max": str(ds.time.values.max()).split(".")[0] if "time" in ds.coords and ds.time.size else "-",
            "has_var": variable in ds,
        }
        if variable in ds and "time" in ds[variable].dims and xlim is not None:
            in_win = ds[variable].sel(time=slice(xlim[0], xlim[1]))
            row["n_time_in_xlim"] = int(in_win.sizes.get("time", 0))
            row["finite_in_xlim"] = int(np.isfinite(in_win.values).sum())
        rows.append(row)
    return pd.DataFrame(rows)
