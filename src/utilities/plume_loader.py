from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


def _cell_index(path: Path) -> int:
    match = re.search(r"_cell(\d+)\.nc$", path.name)
    return int(match.group(1)) if match else -1


def _flatten(items: list[list[Path]]) -> list[Path]:
    return [item for group in items for item in group]


def _preprocess_plume_ds(ds: xr.Dataset) -> xr.Dataset | None:
    if "rho" not in ds.data_vars:
        return None

    try:
        ds = ds.swap_dims({"path": "time"})
    except Exception:
        return None

    # Legacy COSMO-SPECS files can be shifted by -3h.
    if ds.time.values[0] < np.datetime64("2023-01-25T12:00:00"):
        ds = ds.assign_coords(time=ds.time + np.timedelta64(3, "h"))

    if "time" in ds.indexes:
        time_index = ds.indexes["time"]
        if time_index.has_duplicates:
            ds = ds.isel(time=~time_index.duplicated())

    return ds.sortby("time")


def _discover_runs_from_processed(processed_root: Path) -> list[dict]:
    pattern = re.compile(
        r"^data_(?P<cs>cs-.*?__\d{8}_\d{6})_(?P<exp>\d{14})_(?P<kind>integrated|extreme|vertical)_plume_path_.*_cell\d+\.nc$"
    )
    found: dict[tuple[str, str], dict] = {}
    for path in processed_root.glob("data_*_plume_path_*_cell*.nc"):
        match = pattern.match(path.name)
        if not match:
            continue
        cs_run, exp_id = match.group("cs"), match.group("exp")
        found[(cs_run, exp_id)] = {
            "label": f"{cs_run}:{exp_id}",
            "cs_run": cs_run,
            "exp_id": exp_id,
        }
    return [found[key] for key in sorted(found)]


def _build_cfg(runs: list[dict], processed_root: Path, kinds: tuple[str, ...]) -> dict:
    cfg = {}
    for run in runs:
        cs_run, exp_id = run["cs_run"], run["exp_id"]
        label = run.get("label", f"{cs_run}:{exp_id}")
        entry = {"label": label, "cs_run": cs_run, "exp_id": exp_id}
        for kind in kinds:
            files_by_tracer = []
            for tracer_var in ("qi", "qs"):
                files_by_tracer.append(
                    list(processed_root.glob(f"data_{cs_run}_{exp_id}_{kind}_plume_path_{tracer_var}_cell*.nc"))
                )
            entry[f"flist_{kind}"] = sorted(_flatten(files_by_tracer), key=_cell_index)
        cfg[label] = entry
    return cfg


def _run_time_grid(ds_list: list[xr.Dataset]) -> pd.DatetimeIndex:
    t_min = min(ds.time.values.min() for ds in ds_list)
    t_max = max(ds.time.values.max() for ds in ds_list)
    start = np.datetime64(t_min, "s") - np.timedelta64(5, "m")
    end = np.datetime64(t_max, "s") + np.timedelta64(5, "m")
    return pd.date_range(start, end, freq="10s")


def load_plume_path_runs(
    runs: list[dict] | None = None,
    *,
    processed_root: str | Path,
    kinds: tuple[str, ...] = ("integrated", "extreme", "vertical"),
) -> dict[str, dict[str, xr.Dataset]]:
    """
    Load processed plume-path files into a nested dict:
    {run_label: {kind: xr.Dataset}}.
    """
    root = Path(processed_root)
    run_list = _discover_runs_from_processed(root) if runs is None else runs
    cfg = _build_cfg(run_list, root, kinds=kinds)

    cs_run_datasets: dict[str, dict[str, xr.Dataset]] = {}
    for label, cfg_run in cfg.items():
        run_datasets = {kind: [] for kind in kinds}
        for kind in kinds:
            files = cfg_run.get(f"flist_{kind}", [])
            if not files:
                continue

            raw = []
            for file_path in files[::-1]:
                try:
                    ds = xr.open_dataset(file_path)
                except Exception:
                    continue
                ds = _preprocess_plume_ds(ds)
                if ds is not None:
                    raw.append(ds)

            if not raw:
                continue

            target_time = _run_time_grid(raw)
            aligned = [
                ds.reindex(time=target_time, method="nearest", tolerance="5s", fill_value=0)
                for ds in raw
            ]
            ds_kind = xr.concat(aligned, dim="cell")
            run_datasets[kind] = xr.where(ds_kind > 0, ds_kind, np.nan)

        if any(len(run_datasets[kind]) > 0 for kind in kinds):
            cs_run_datasets[label] = run_datasets

    return cs_run_datasets
