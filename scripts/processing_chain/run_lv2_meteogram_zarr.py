#!/usr/bin/env python3
"""
LV2: Meteogram NetCDFs → single Zarr store.

Reuses meteogram_io.build_meteogram_zarr. Writes to processed/<cs_run>/lv2_meteogram/
with provenance attrs (git commit, stage, processing_level, cs_run, input_files).

Usage:
  python run_meteogram_zarr.py -r cs-eriswil__20250710_105311
  python run_meteogram_zarr.py -r cs-eriswil__20250710_105311 --debug
  POLARCAP_OUTPUT_ROOT=/work/.../RUN_ERISWILL_200x160x100/ensemble_output python run_meteogram_zarr.py -r cs-eriswil__20250710_105311
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from time import time as time_now

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
from utilities.meteogram_io import (
    discover_meteogram_files,
    get_max_timesteps,
    get_variable_names,
    build_meteogram_zarr,
)
from utilities.init_common import get_station_coords_from_cfg
from utilities.processing_metadata import provenance_attrs
from utilities.compute_fabric import allocate_resources, calculate_optimal_scaling
from utilities.processing_paths import get_output_root, get_runs_root, resolve_ensemble_output


def parse_args():
    p = argparse.ArgumentParser(description="LV2: Meteogram NetCDF → Zarr")
    p.add_argument("-r", "--cs-run", required=True,
                   help="Run ID (e.g. cs-eriswil__YYYYMMDD_HHMMSS)")
    p.add_argument("--root", default=get_runs_root(),
                   help="Root: either RUN_ERISWILL_*x100/ parent, or flat meteogram root with <cs_run>/ subdirs")
    p.add_argument(
        "--out",
        default=None,
        help="Output root; <out>/<cs_run>/lv2_meteogram/ (default: $POLARCAP_OUTPUT_ROOT, else matching RUN_ERISWILL_*x100/ensemble_output, else repo scripts/data/processed)",
    )
    p.add_argument("-d", "--debug", action="store_true", help="Small subset, no SLURM")
    p.add_argument("-s", "--slurm", action="store_true", help="Use SLURM/Dask cluster")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing Zarr")
    return p.parse_args()


def main():
    args = parse_args()
    if not re.match(r"cs-eriswil__\d{8}_\d{6}", args.cs_run):
        raise ValueError(f"Invalid cs_run format: {args.cs_run}")

    debug_mode = args.debug or getattr(sys, "gettrace", lambda: None)() is not None
    debug_flag = "_dbg" if debug_mode else ""

    runs_root = get_runs_root(args.root)
    if not runs_root:
        print("Set CS_RUNS_DIR or pass --root (model data root or meteogram root).", file=sys.stderr)
        sys.exit(1)
    root_dir = resolve_ensemble_output(runs_root, args.cs_run)
    if root_dir is not None:
        data_dir = f"{root_dir.rstrip(os.sep)}/{args.cs_run}/"
    else:
        # Fallback: flat meteogram root (run dirs directly under root, e.g. .../meteograms/<cs_run>/)
        flat_run = Path(runs_root) / args.cs_run
        if flat_run.is_dir() and (flat_run / f"{args.cs_run}.json").is_file():
            data_dir = f"{flat_run}{os.sep}"
        else:
            print(
                f"No RUN_ERISWILL_*x100/ensemble_output under {runs_root} and no {args.cs_run}/ with run JSON there.",
                file=sys.stderr,
            )
            sys.exit(1)

    meta_file = f"{data_dir}/{args.cs_run}.json"
    if not os.path.isfile(meta_file):
        print(f"Run metadata not found: {meta_file}", file=sys.stderr)
        sys.exit(1)
    file_dict = discover_meteogram_files(data_dir, dbg=debug_mode)
    if not file_dict:
        print(f"No meteogram files in {data_dir}; check --root and --cs-run", file=sys.stderr)
        sys.exit(1)
    max_time = get_max_timesteps(file_dict)
    sample_file = next(iter(file_dict.values()))[0]
    variables = get_variable_names(sample_file)
    station_coords = get_station_coords_from_cfg(meta_file)

    if debug_mode:
        variables = ["RGRENZ_left", "RGRENZ_right", "U", "V", "W", "T", "QV", "QC", "HMLd", "HHLd"]

    out = get_output_root(args.out, runs_root=runs_root, cs_run=args.cs_run)
    out_root = Path(out) / args.cs_run / "lv2_meteogram"
    out_root.mkdir(parents=True, exist_ok=True)
    zarr_path = str(out_root / f"Meteogram_{args.cs_run}_nVar{len(variables)}_nMet{len(station_coords)}_nExp{len(file_dict)}{debug_flag}.zarr")

    if not args.overwrite and os.path.isdir(zarr_path):
        print(f"Zarr exists, skip (use --overwrite to rebuild): {zarr_path}")
        return

    global_attrs = provenance_attrs(
        stage="lv2",
        processing_level="LV2",
        title="PolarCAP meteogram Zarr (multi-experiment, multi-station)",
        summary="Meteogram variables from COSMO-SPECS; concatenated along expname.",
        cs_run=args.cs_run,
        domain="",
        input_files=[meta_file] + [f for flist in file_dict.values() for f in flist[:1]],
    )
    # Keep attrs Zarr-serializable (no long lists of paths in input_files if huge)
    if isinstance(global_attrs.get("input_files"), list) and len(global_attrs["input_files"]) > 50:
        global_attrs["input_files"] = f"{len(global_attrs['input_files'])} files (see manifest)"

    cluster = client = None
    if args.slurm and not debug_mode:
        n_nodes, n_cpu, mem_gb, scale_workers, walltime = calculate_optimal_scaling(
            max_time, len(file_dict), len(station_coords), debug_mode,
        )
        cluster, client = allocate_resources(
            n_cpu=n_cpu, n_jobs=n_nodes, m=int(mem_gb),
            walltime=walltime, part="compute",
        )
        cluster.scale(scale_workers)

    t0 = time_now()
    build_meteogram_zarr(
        file_dict,
        zarr_path,
        variables=variables,
        max_time=max_time,
        max_height_level=20,
        station_coords=station_coords,
        meta_file=meta_file,
        debug_mode=debug_mode,
        global_attrs=global_attrs,
    )
    print(f"\nDone in {time_now() - t0:.1f} s  →  {zarr_path}")

    if cluster is not None:
        cluster.close()
    if client is not None:
        client.close()


if __name__ == "__main__":
    main()
