#!/usr/bin/env python3
"""Summarize early-window initiation process contributions from local LV3 rate files."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_CSV = REPO_ROOT / "data" / "registry" / "analysis_registry.csv"
PLAN_PC = REPO_ROOT / "data" / "plan_pc"
PROCESSED_ROOT = REPO_ROOT / "scripts" / "processing_chain" / "processed"
OUT_CSV = REPO_ROOT / "data" / "registry" / "initiation_process_fractions.csv"

H_LOW = 800.0
H_HIGH = 1400.0
WINDOW_MIN = 2.5


def _height_slice(da: xr.DataArray, h_low: float, h_high: float) -> xr.DataArray:
    z = np.asarray(da["height_level"].values, dtype=float)
    lo, hi = min(h_low, h_high), max(h_low, h_high)
    mask = (z >= lo) & (z <= hi)
    if not np.any(mask):
        return da
    return da.isel(height_level=np.where(mask)[0])


def _seed_start(cs_run: str, flare_expname: str, time0: np.datetime64) -> np.datetime64:
    meta = json.loads((PLAN_PC / f"{cs_run}.json").read_text(encoding="utf-8"))
    flare_sec = int(meta.get(flare_expname, {}).get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_starttime", 1800))
    return np.datetime64(time0, "s") + np.timedelta64(flare_sec, "s")


def _local_lv3(cs_run: str, exp_id: int) -> Path | None:
    path = PROCESSED_ROOT / cs_run / "lv3_rates" / f"process_rates_exp{exp_id}.nc"
    return path if path.is_file() else None


def _mean_diff(ds_flare: xr.Dataset, ds_ref: xr.Dataset, rate_name: str, label_name: str, proc_dim: str, station_idx: int, window: slice) -> list[dict[str, object]]:
    flare = ds_flare[rate_name].isel(station=station_idx).sel(time=window)
    ref = ds_ref[rate_name].isel(station=station_idx).sel(time=window)
    flare = _height_slice(flare, H_LOW, H_HIGH).mean(dim=("time", "height_level"))
    ref = _height_slice(ref, H_LOW, H_HIGH).mean(dim=("time", "height_level"))
    labels = [str(v) for v in ds_flare[label_name].values]
    flare_vals = np.asarray(flare.values, dtype=float)
    ref_vals = np.asarray(ref.values, dtype=float)
    mask = np.isfinite(flare_vals) & np.isfinite(ref_vals)
    diffs = np.full(flare_vals.shape, np.nan, dtype=float)
    np.subtract(flare_vals, ref_vals, out=diffs, where=mask)
    pos_total = float(np.nansum(np.maximum(np.nan_to_num(diffs, nan=0.0), 0.0)))
    rows = []
    for label, diff in zip(labels, diffs):
        diff = float(diff) if np.isfinite(diff) else np.nan
        pos_part = max(diff, 0.0) if np.isfinite(diff) else 0.0
        rows.append(
            {
                "phase_rate": rate_name,
                "process_label": label,
                "mean_rate_diff": diff,
                "positive_fraction": float(pos_part / pos_total) if pos_total > 0 else 0.0,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize early initiation process fractions from local LV3 rates.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_CSV, help="Merged analysis registry CSV.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output CSV path.")
    args = parser.parse_args()

    reg = pd.read_csv(args.registry, dtype=str)
    sel = reg[
        (reg["is_reference"].astype(str).str.upper() == "FALSE")
        & (reg["include_in_paper"].astype(str).str.upper() == "TRUE")
        & (reg["ref_exp_id"].astype(str) != "")
        & (reg["local_lv3_available"].astype(str).str.upper() == "TRUE")
    ]
    rows: list[dict[str, object]] = []
    for row in sel.itertuples(index=False):
        flare_path = _local_lv3(row.cs_run, int(row.exp_id))
        ref_path = _local_lv3(row.cs_run, int(row.ref_exp_id))
        if flare_path is None or ref_path is None:
            continue
        ds_flare = xr.open_dataset(flare_path)
        ds_ref = xr.open_dataset(ref_path)
        time0 = np.datetime64(ds_flare["time"].values[0], "s")
        seed_start = _seed_start(row.cs_run, row.expname, time0)
        window = slice(seed_start, seed_start + np.timedelta64(int(WINDOW_MIN * 60), "s"))
        for station_idx in range(int(ds_flare.sizes.get("station", 0))):
            proc_rows = []
            proc_rows.extend(_mean_diff(ds_flare, ds_ref, "rate_N_W", "process_label_N_W", "process_N_W", station_idx, window))
            proc_rows.extend(_mean_diff(ds_flare, ds_ref, "rate_N_F", "process_label_N_F", "process_N_F", station_idx, window))
            for proc in proc_rows:
                rows.append(
                    {
                        "cs_run": row.cs_run,
                        "exp_id": int(row.exp_id),
                        "expname": row.expname,
                        "ref_exp_id": int(row.ref_exp_id),
                        "ref_expname": row.ref_expname,
                        "pair_method": row.pair_method,
                        "station_idx": station_idx,
                        "window_start": str(seed_start),
                        "window_end": str(seed_start + np.timedelta64(int(WINDOW_MIN * 60), "s")),
                        **proc,
                    }
                )
    out = pd.DataFrame(rows).sort_values(["cs_run", "exp_id", "station_idx", "phase_rate", "process_label"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
