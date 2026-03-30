#!/usr/bin/env python3
"""
LV3: Process-rate datasets from meteogram Zarr.

Reads Zarr from processed/<cs_run>/lv2_meteogram/ (or given path), builds
rate datasets per experiment, writes NetCDFs to processed/<cs_run>/lv3_rates/
with full provenance.

Usage:
  python run_lv3.py --cs-run cs-eriswil__20260304_110254
  python run_lv3.py --zarr /path/to/Meteogram_*.zarr --exp-ids 0 1 2
  POLARCAP_OUTPUT_ROOT=/work/.../RUN_ERISWILL_200x160x100/ensemble_output python run_lv3.py --cs-run cs-eriswil__20260304_110254
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

import numpy as np
import xarray as xr

from utilities.process_rates import build_rates_for_experiments
from utilities.processing_paths import get_output_root


def _sanitize_attr_value(value):
    """Convert NetCDF-incompatible metadata to strings before serialization."""
    if isinstance(value, np.ndarray):
        return repr(value.tolist()) if value.ndim > 1 else value
    if isinstance(value, (list, tuple)) and value and isinstance(value[0], (list, tuple, dict)):
        return repr(value)
    return value


def _sanitize_netcdf_attrs(ds: xr.Dataset) -> xr.Dataset:
    """Strip or serialize attrs that NetCDF4 cannot write."""
    clean = ds.copy(deep=False)
    clean.attrs = {key: _sanitize_attr_value(val) for key, val in clean.attrs.items()}
    for name in clean.variables:
        clean[name].attrs = {key: _sanitize_attr_value(val) for key, val in clean[name].attrs.items()}
    return clean


def parse_args():
    p = argparse.ArgumentParser(description="LV3: Process-rate datasets from meteogram Zarr")
    p.add_argument("--cs-run", default=None, help="Run ID; used to find Zarr under --out/<cs_run>/lv2_meteogram/")
    p.add_argument("--zarr", default=None, help="Direct path to Meteogram_*.zarr (overrides --cs-run lookup)")
    p.add_argument(
        "--out",
        default=None,
        help="Output root for lv3_rates and LV2 lookup (default: $POLARCAP_OUTPUT_ROOT, else matching RUN_ERISWILL_*x100/ensemble_output, else repo data/processed)",
    )
    p.add_argument("--exp-ids", type=int, nargs="*", default=None,
                   help="Experiment indices to process (default: all)")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing NetCDFs")
    return p.parse_args()


def main():
    args = parse_args()
    out = get_output_root(args.out, cs_run=args.cs_run)
    if args.zarr:
        zarr_path = Path(args.zarr)
        if not zarr_path.exists():
            print(f"Zarr not found: {zarr_path}")
            sys.exit(1)
        cs_run = args.cs_run or "unknown"
    else:
        if not args.cs_run:
            print("Provide --cs-run or --zarr", file=sys.stderr)
            sys.exit(1)
        cs_run = args.cs_run
        out_root = Path(out) / cs_run / "lv2_meteogram"
        if not out_root.is_dir():
            print(f"LV2 directory not found: {out_root}", file=sys.stderr)
            sys.exit(1)
        candidates = sorted(out_root.glob("Meteogram_*.zarr"))
        if not candidates:
            print(f"No Zarr under {out_root}", file=sys.stderr)
            sys.exit(1)
        # Prefer non-debug Zarr when both exist
        non_dbg = [c for c in candidates if "_dbg" not in c.name]
        zarr_path = non_dbg[-1] if non_dbg else candidates[-1]

    print(f"Opening Zarr: {zarr_path}")
    ds = xr.open_zarr(str(zarr_path))
    n_exp = ds.sizes.get("expname", 0)
    if n_exp == 0:
        print("Zarr has no experiments (expname size 0); nothing to process.", file=sys.stderr)
        sys.exit(1)
    exp_ids = args.exp_ids if args.exp_ids is not None else list(range(n_exp))
    if not exp_ids:
        print("No experiment indices to process (--exp-ids empty or none).", file=sys.stderr)
        sys.exit(1)
    try:
        config = __import__("utilities.namelist_metadata", fromlist=["metadata_manager"]).metadata_manager.config
    except Exception:
        config = None

    rates_by_exp, rates_ds_by_exp = build_rates_for_experiments(
        ds, exp_ids, config=config,
        LBB=slice(30, 50), CBB=slice(30, 50),
        repo_root=_script_dir.parent.parent,
    )
    out_dir = Path(out) / cs_run / "lv3_rates"
    out_dir.mkdir(parents=True, exist_ok=True)
    for eid, rates_ds in rates_ds_by_exp.items():
        nc_path = out_dir / f"process_rates_exp{eid}.nc"
        if not args.overwrite and nc_path.exists():
            print(f"  Skip (exists): {nc_path}")
            continue
        _sanitize_netcdf_attrs(rates_ds).to_netcdf(str(nc_path), mode="w")
        print(f"  Wrote {nc_path}")
    print(f"Done. LV3 rates under {out_dir}")


if __name__ == "__main__":
    main()
