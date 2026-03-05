"""Fast meteogram-to-Zarr conversion.

Drop-in replacement for ``00-prepM.py`` with the same CLI interface.
All heavy lifting lives in ``utilities.meteogram_io``; this script is
just argument parsing, optional SLURM cluster setup, and one function call.

Usage
-----
    python 01-prepM-fast.py -r cs-eriswil__20250710_105311
    python 01-prepM-fast.py -r cs-eriswil__20250710_105311 -d   # debug (small subset)
    python 01-prepM-fast.py -r cs-eriswil__20250710_105311 -s   # SLURM cluster
"""

import argparse
import os
import re
import sys
from time import time as time_now

import xarray as xr

# ── project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))
from utilities.meteogram_io import (
    discover_meteogram_files,
    get_max_timesteps,
    get_variable_names,
    build_meteogram_zarr,
)
from utilities.init_common import get_station_coords_from_cfg
from utilities.compute_fabric import allocate_resources, calculate_optimal_scaling


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Fast meteogram NetCDF → Zarr")
    p.add_argument("-r", "--cs_run", default="cs-eriswil__20250710_105311",
                   help="Run ID  (format: cs-eriswil__YYYYMMDD_HHMMSS)")
    p.add_argument("-d", "--debug", action="store_true",
                   help="Debug mode – small subset, no SLURM")
    p.add_argument("-s", "--slurm", action="store_true",
                   help="Spin up a SLURM/Dask cluster")
    return p.parse_args()


def _is_debugger_active():
    return getattr(sys, "gettrace", lambda: None)() is not None


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # validate run ID
    if not re.match(r"cs-eriswil__\d{8}_\d{6}", args.cs_run):
        raise ValueError(f"Invalid cs_run format: {args.cs_run}")

    debug_mode = args.debug or _is_debugger_active()
    debug_flag = "_dbg" if debug_mode else ""
    if debug_mode:
        print(f"{'*' * 50}\nDEBUG MODE ACTIVE\n{'*' * 50}")

    # paths
    # Determine root_dir based on environment (server vs local)
    if os.path.exists("/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/"):
        root_dir = "/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/"
    else:
        root_dir = "/Users/schimmel/data/cosmo-specs/meteograms/"
    data_dir = f"{root_dir}/{args.cs_run}/"
    meta_file = f"{data_dir}/{args.cs_run}.json"

    # discover files
    file_dict = discover_meteogram_files(data_dir, dbg=debug_mode)
    max_time = get_max_timesteps(file_dict)
    sample_file = next(iter(file_dict.values()))[0]
    variables = get_variable_names(sample_file)
    station_coords = get_station_coords_from_cfg(meta_file)

    if debug_mode:
        variables = ["RGRENZ_left", "RGRENZ_right", "U", "V", "W",
                     "T", "QV", "QC", "HMLd", "HHLd"]

    print(f"\nExperiments : {len(file_dict)}")
    print(f"Stations    : {len(station_coords)}")
    print(f"Variables   : {len(variables)}")
    print(f"Max timesteps: {max_time}")

    # optional SLURM cluster
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

    # build zarr
    zarr_path = (
        f"{data_dir}/Meteogram_{args.cs_run}"
        f"_nVar{len(variables)}"
        f"_nMet{len(station_coords)}"
        f"_nExp{len(file_dict)}"
        f"{debug_flag}.zarr"
    )

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
    )
    elapsed = time_now() - t0
    print(f"\nDone in {elapsed:.1f} s  →  {zarr_path}")

    # teardown
    if cluster is not None:
        cluster.close()
    if client is not None:
        client.close()


if __name__ == "__main__":
    main()
