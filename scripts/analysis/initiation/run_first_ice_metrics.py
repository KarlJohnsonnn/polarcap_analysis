#!/usr/bin/env python3
"""Compute first excess-ice onset metrics from local processed LV2 meteogram Zarr files."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.processing_paths import default_local_processed_root  # noqa: E402
from utilities.table_paths import registry_output_paths, resolve_registry_input, sync_file  # noqa: E402

REGISTRY_CSV = resolve_registry_input("analysis_registry.csv", repo_root=REPO_ROOT)
PLAN_PC = REPO_ROOT / "data" / "plan_pc"
PROCESSED_ROOT = Path(default_local_processed_root())
OUT_PATHS = registry_output_paths("first_ice_onset_metrics.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]

THRESHOLD_M3 = 1.0


def _height_slice(da: xr.DataArray, h_low: float, h_high: float) -> xr.DataArray:
    z = np.asarray(da["height_level"].values)
    lo, hi = min(h_low, h_high), max(h_low, h_high)
    if z[-1] > z[0]:
        return da.sel(height_level=slice(lo, hi))
    return da.sel(height_level=slice(hi, lo))


def _seed_start(cs_run: str, flare_expname: str, time0: np.datetime64) -> np.datetime64:
    meta = json.loads((PLAN_PC / f"{cs_run}.json").read_text(encoding="utf-8"))
    flare_sec = int(meta.get(flare_expname, {}).get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_starttime", 1800))
    return np.datetime64(time0, "s") + np.timedelta64(flare_sec, "s")


def _local_zarr(processed_root: Path, cs_run: str) -> Path | None:
    paths = sorted((processed_root / cs_run / "lv2_meteogram").glob("Meteogram_*.zarr"))
    return paths[-1] if paths else None


def _number_abs(ds: xr.Dataset, var_name: str, exp_id: int, station_idx: int) -> xr.DataArray:
    rho = ds["RHO"].isel(expname=exp_id, station=station_idx)
    conc = ds[var_name].isel(expname=exp_id, station=station_idx) * rho
    conc = conc.mean("height_level")
    if "bins" in conc.dims:
        conc = conc.sum("bins")
    return conc


def _onset_metrics(excess: xr.DataArray, seed_start: np.datetime64) -> tuple[bool, str, float]:
    search = excess.sel(time=slice(seed_start, seed_start + np.timedelta64(15, "m")))
    exceed = np.where(np.asarray(search.values) > THRESHOLD_M3)[0]
    if exceed.size == 0:
        return False, "", np.nan
    onset_time = np.datetime64(search["time"].values[exceed[0]], "s")
    onset_min = float((onset_time - seed_start) / np.timedelta64(1, "m"))
    return True, str(onset_time), onset_min


def _peak(series: xr.DataArray) -> float:
    vals = np.asarray(series.values, dtype=float)
    if vals.size == 0 or np.all(~np.isfinite(vals)):
        return np.nan
    return float(np.nanmax(vals))


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute first excess-ice onset metrics from local LV2 outputs.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_CSV, help="Merged analysis registry CSV.")
    parser.add_argument("--processed-root", type=Path, default=PROCESSED_ROOT, help="Canonical processed output root.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output CSV path.")
    args = parser.parse_args()

    reg = pd.read_csv(args.registry, dtype=str)
    sel = reg[
        (reg["is_reference"].astype(str).str.upper() == "FALSE")
        & (reg["include_in_paper"].astype(str).str.upper() == "TRUE")
        & (reg["ref_exp_id"].astype(str) != "")
        & (reg["local_lv2_available"].astype(str).str.upper() == "TRUE")
    ]

    rows: list[dict[str, object]] = []
    for row in sel.itertuples(index=False):
        zarr_path = _local_zarr(args.processed_root, row.cs_run)
        if zarr_path is None:
            continue
        try:
            ds = xr.open_zarr(zarr_path)
        except Exception as exc:
            print(f"Skip unreadable LV2 zarr for {row.cs_run}: {exc}")
            continue
        for station_idx in range(int(ds.sizes.get("station", 0))):
            ice_flare = _number_abs(ds, "NF", int(row.exp_id), station_idx)
            ice_ref = _number_abs(ds, "NF", int(row.ref_exp_id), station_idx)
            inp_flare = _number_abs(ds, "NINP", int(row.exp_id), station_idx)
            inp_ref = _number_abs(ds, "NINP", int(row.ref_exp_id), station_idx)
            time0 = np.datetime64(ice_flare["time"].values[0], "s")
            seed_start = _seed_start(row.cs_run, row.expname, time0)
            ice_excess = ice_flare - ice_ref
            inp_excess = inp_flare - inp_ref
            ice_found, ice_time, ice_min = _onset_metrics(ice_excess, seed_start)
            inp_found, inp_time, inp_min = _onset_metrics(inp_excess, seed_start)
            rows.append(
                {
                    "cs_run": row.cs_run,
                    "exp_id": int(row.exp_id),
                    "expname": row.expname,
                    "ref_exp_id": int(row.ref_exp_id),
                    "ref_expname": row.ref_expname,
                    "pair_method": row.pair_method,
                    "station_idx": station_idx,
                    "seed_start": str(seed_start),
                    "threshold_number_m3": THRESHOLD_M3,
                    "ice_onset_found": "TRUE" if ice_found else "FALSE",
                    "ice_onset_time": ice_time,
                    "ice_onset_min_since_seed": ice_min,
                    "peak_excess_ice_number_m3": _peak(ice_excess),
                    "inp_onset_found": "TRUE" if inp_found else "FALSE",
                    "inp_onset_time": inp_time,
                    "inp_onset_min_since_seed": inp_min,
                    "peak_excess_inp_number_m3": _peak(inp_excess),
                }
            )
    out = pd.DataFrame(rows).sort_values(["cs_run", "exp_id", "station_idx"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    if args.output.resolve() == OUT_CSV.resolve():
        sync_file(args.output, [OUT_PATHS["legacy"]])
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
