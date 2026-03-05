"""Fast meteogram NetCDF-to-Zarr pipeline.

Replaces the slow ``00-prepM.py`` workflow with:
* parallel ``ncdump`` header reads (ThreadPoolExecutor)
* auto-detected engine: h5netcdf for NetCDF-4 (GIL-free), netcdf4 for classic
* per-file open + immediate variable selection (no ``open_mfdataset``)
* single-level parallelism — no nested thread pools
* streaming region-writes to a pre-allocated Zarr store
* blosc-zstd compression on write
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import xarray as xr

from utilities.init_common import get_station_coords_from_cfg

# ---------------------------------------------------------------------------
# NetCDF engine auto-detection
# ---------------------------------------------------------------------------

_ENGINE_CACHE: Optional[str] = None


def _detect_nc_engine(path: str) -> str:
    """Choose the best xarray engine for *path* based on magic bytes.

    HDF5 files (NetCDF-4) → ``h5netcdf``  (releases GIL)
    Classic NetCDF-3       → ``netcdf4``   (handles both CDF-1 and CDF-2)
    """
    global _ENGINE_CACHE
    if _ENGINE_CACHE is not None:
        return _ENGINE_CACHE

    with open(path, "rb") as fh:
        magic = fh.read(8)

    if magic[:4] == b"\x89HDF":
        _ENGINE_CACHE = "h5netcdf"
    else:
        _ENGINE_CACHE = "netcdf4"

    print(f"Auto-detected NetCDF engine: {_ENGINE_CACHE}")
    return _ENGINE_CACHE


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def discover_meteogram_files(
    data_dir: str,
    *,
    exclude_stations: Sequence[str] = ("SE", "OB"),
    dbg: bool = False,
) -> Dict[str, List[str]]:
    """Return ``{expname: [sorted file paths]}`` for all meteogram NetCDFs.

    Files matching ``M_??_??_??????????????.nc`` are grouped by the 14-char
    experiment suffix.  Stations listed in *exclude_stations* are dropped.
    In debug mode only the first 2 experiments are kept (all stations retained).
    """
    all_files = sorted(glob.glob(f"{data_dir}/M_??_??_??????????????.nc"))
    expnames = sorted({f.split("/")[-1].split("_")[-1].split(".")[0] for f in all_files})

    station_id_of = lambda f: f.split("/")[-1].split("_")[1]

    files_per_exp: Dict[str, List[str]] = {}
    for exp in expnames:
        matched = [
            f for f in glob.glob(f"{data_dir}/M_??_??_{exp}.nc")
            if station_id_of(f) not in exclude_stations
        ]
        files_per_exp[exp] = sorted(matched)

    if dbg:
        max_exp = min(2, len(files_per_exp))
        files_per_exp = dict(list(files_per_exp.items())[:max_exp])
        for k, v in files_per_exp.items():
            print(f"DBG discover: {k}  ({len(v)} stations)")

    return files_per_exp


# ---------------------------------------------------------------------------
# Fast header inspection
# ---------------------------------------------------------------------------

def _ncdump_time_size(path: str) -> int:
    """Read the current time-dimension size from the NetCDF header."""
    try:
        result = subprocess.run(
            ["ncdump", "-h", path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if "time = " in line.lower() and ";" in line:
                    # e.g. "	time = UNLIMITED ; // (422 currently)"
                    size_str = line.split("; // (")[1].replace(" currently)", "")
                    return int(size_str)
    except Exception:
        pass
    return 0


def get_max_timesteps(file_dict: Dict[str, List[str]], *, max_workers: int = 32) -> int:
    """Return the maximum time-dimension length across all files.

    Runs ``ncdump -h`` in parallel via a thread pool (I/O-bound subprocess).
    """
    all_files = [f for files in file_dict.values() for f in files]
    print(f"Checking time steps in {len(all_files)} files (parallel ncdump)...")
    sizes = [_ncdump_time_size(f) for f in all_files]
    max_t = max(sizes) if sizes else 0
    print(f"Max time steps: {max_t}")
    return max_t


def get_variable_names(sample_file: str) -> List[str]:
    """Read variable names from one NetCDF file (header only)."""
    engine = _detect_nc_engine(sample_file)
    with xr.open_dataset(sample_file, engine=engine) as ds:
        return list(ds.data_vars)


# ---------------------------------------------------------------------------
# Per-station preprocessing (kept lazy)
# ---------------------------------------------------------------------------

def _preprocess_station(
    ds: xr.Dataset,
    max_time: int,
    max_height_level: int = 20,
) -> xr.Dataset:
    """Preprocess a single-station dataset: pad time, swap height dims, trim.

    All operations stay lazy (no ``.compute()`` / ``.values``).
    """
    # --- time padding ---
    ds_time = ds.time.values
    if max_time > 0 and ds_time.size < max_time:
        missing = max_time - ds_time.size
        if ds_time.size < 2:
            raise ValueError("Need >= 2 time steps to infer dt")
        dt = ds_time[1] - ds_time[0]
        new_times = np.concatenate([
            ds_time,
            [ds_time[-1] + (i + 1) * dt for i in range(missing)],
        ])
        ds = ds.reindex(time=new_times, method="pad", fill_value=np.nan)

    # --- swap HMLd / HHLd to generic height indices ---
    if "HMLd" in ds.sizes:
        n = ds.sizes["HMLd"]
        ds = ds.swap_dims({"HMLd": "height_level"})
        ds = ds.assign_coords(height_level=np.arange(n))
        ds["HMLd"] = ds.HMLd

    if "HHLd" in ds.sizes:
        n = ds.sizes["HHLd"]
        ds = ds.swap_dims({"HHLd": "height_level2"})
        ds = ds.assign_coords(height_level2=np.arange(n))
        ds["HHLd"] = ds.HHLd

    # --- trim to top *max_height_level* levels ---
    if "height_level" in ds.sizes:
        ds = ds.isel(height_level=slice(-max_height_level, None))
    if "height_level2" in ds.sizes:
        ds = ds.isel(height_level2=slice(-max_height_level - 1, None))

    return ds


# ---------------------------------------------------------------------------
# Load one experiment (flat open, no open_mfdataset)
# ---------------------------------------------------------------------------

def _open_experiment(
    sorted_files: List[str],
    variables: List[str],
    max_time: int,
    max_height_level: int,
    chunks: Dict[str, int],
) -> Tuple[xr.Dataset, np.ndarray]:
    """Load all station files for a single experiment and concat along *station*.

    Uses ``xr.open_dataset`` per file with auto-detected engine instead of
    ``open_mfdataset`` to avoid nested dask graphs and GIL contention.
    """
    station_id_of = lambda f: str(f.split("/")[-1].split("_")[1])
    station_ids = np.array([station_id_of(f) for f in sorted_files], dtype="i4")
    engine = _detect_nc_engine(sorted_files[0])

    datasets = []
    for path in sorted_files:
        ds = xr.open_dataset(path, engine=engine, chunks=chunks)
        ds = ds[variables]
        ds = _preprocess_station(ds, max_time, max_height_level)
        datasets.append(ds)

    ds_exp = xr.concat(datasets, dim="station", coords="minimal", compat="override")
    return ds_exp, station_ids


# ---------------------------------------------------------------------------
# Bin-boundary coordinate helper
# ---------------------------------------------------------------------------

def _compute_bin_coords(n_bins: int = 66, n_max: float = 2.0,
                        r_min: float = 1e-9, rhow: float = 1e3):
    """Mass/radius bin edges and centres (COSMO-SPECS defaults)."""
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min ** 3
    j0w = (n_max - 1.0) / np.log(2.0)
    m_edges = m0w * np.exp(np.arange(n_bins + 1) / j0w)
    r_edges = np.cbrt(m_edges / fact)
    m_cen = np.sqrt(m_edges[1:] * m_edges[:-1])   # geometric mean (log-spaced grid)
    r_cen = np.cbrt(m_cen / fact)
    return m_edges, m_cen, r_edges, r_cen


# ---------------------------------------------------------------------------
# Coordinate / metadata enrichment
# ---------------------------------------------------------------------------

def add_coords_and_metadata(
    ds: xr.Dataset,
    meta_file: Optional[str] = None,
    station_coords: Optional[Dict[str, Tuple[float, float]]] = None,
) -> xr.Dataset:
    """Add station lat/lon, bin boundaries/centres, and namelist metadata."""
    # --- station coordinates ---
    if station_coords is None and meta_file is not None:
        station_coords = get_station_coords_from_cfg(meta_file)
    if station_coords is not None and "station" in ds.sizes:
        coords_arr = np.array(list(station_coords.values()), dtype="f8")
        n = ds.sizes["station"]
        if "station_lat" not in ds.coords:
            ds = ds.assign_coords(station_lat=(["station"], coords_arr[:n, 0]))
        if "station_lon" not in ds.coords:
            ds = ds.assign_coords(station_lon=(["station"], coords_arr[:n, 1]))

    # --- bin coordinates ---
    if "bins" in ds.sizes:
        nb = ds.sizes["bins"]
        m_edges, m_cen, r_edges, r_cen = _compute_bin_coords(n_bins=nb)
        ds = ds.assign_coords(
            bins_boundaries=xr.DataArray(np.arange(nb + 1), dims="bins_boundaries"),
            mass_centers=xr.DataArray(m_cen, dims="bins",
                                     attrs={"units": "kg", "long_name": "Mass bin centers"}),
            mass_boundaries=xr.DataArray(m_edges, dims="bins_boundaries",
                                        attrs={"units": "kg", "long_name": "Mass bin boundaries"}),
            radius_centers=xr.DataArray(r_cen, dims="bins",
                                       attrs={"units": "m", "long_name": "Radius bin centers"}),
            radius_boundaries=xr.DataArray(r_edges, dims="bins_boundaries",
                                          attrs={"units": "m", "long_name": "Radius bin boundaries"}),
        )

    # --- namelist-derived variable metadata ---
    try:
        from utilities.namelist_metadata import update_dataset_metadata
        ds = update_dataset_metadata(ds)
        # strip attrs that are not zarr-serialisable (e.g. cmap arrays)
        for vname in ds.data_vars:
            for key in list(ds[vname].attrs):
                val = ds[vname].attrs[key]
                if isinstance(val, np.ndarray) or (hasattr(val, '__len__') and not isinstance(val, (str, list))):
                    ds[vname].attrs.pop(key)
    except Exception as exc:
        print(f"[meteogram_io] metadata enrichment skipped: {exc}")

    return ds


# ---------------------------------------------------------------------------
# Zarr encoding helper
# ---------------------------------------------------------------------------

def _zarr_encoding(ds: xr.Dataset, compression_level: int = 3) -> dict:
    """Build per-variable Zarr encoding with blosc-zstd compression (v2 format)."""
    import numcodecs
    compressor = numcodecs.Blosc(cname="zstd", clevel=compression_level,
                                 shuffle=numcodecs.Blosc.BITSHUFFLE)
    return {var: {"compressor": compressor} for var in ds.data_vars}


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def build_meteogram_zarr(
    file_dict: Dict[str, List[str]],
    zarr_path: str,
    *,
    variables: List[str],
    max_time: int,
    max_height_level: int = 20,
    station_coords: Optional[Dict[str, Tuple[float, float]]] = None,
    meta_file: Optional[str] = None,
    target_time_chunk: int = 500,
    target_station_chunk: int = 5,
    compression_level: int = 3,
    debug_mode: bool = False,
) -> str:
    """Build a Zarr store from per-experiment meteogram NetCDF files.

    Strategy
    --------
    1. Open every experiment lazily (flat per-file open, no open_mfdataset).
    2. Concatenate along *expname* with a single ``xr.concat``.
    3. Assign coordinates and metadata.
    4. Write to Zarr in one pass with blosc-zstd compression.

    Returns the *zarr_path*.
    """
    import dask
    from dask.diagnostics import ProgressBar

    if station_coords is None and meta_file is not None:
        station_coords = get_station_coords_from_cfg(meta_file)

    expnames = list(file_dict.keys())
    n_experiments = len(expnames)

    # --- target chunks (set once — no rechunking later) ---
    target_chunks: Dict[str, int] = {
        "time": min(target_time_chunk, max_time) if max_time > 0 else target_time_chunk,
        "height_level": -1,
        "height_level2": -1,
        "bins": -1,
    }

    # -----------------------------------------------------------
    # 1. Open all experiments lazily
    # -----------------------------------------------------------
    exp_datasets: List[xr.Dataset] = []
    station_ids = None

    for i, exp in enumerate(expnames):
        files = sorted(file_dict[exp])
        print(f"  Opening [{i + 1}/{n_experiments}] {exp}  ({len(files)} stations)")
        ds_exp, sids = _open_experiment(
            files, variables, max_time, max_height_level, target_chunks,
        )
        exp_datasets.append(ds_exp)
        if station_ids is None:
            station_ids = sids

    n_stations = exp_datasets[0].sizes["station"]
    target_chunks["station"] = min(target_station_chunk, n_stations)

    # -----------------------------------------------------------
    # 2. Concatenate along expname (single concat, lazy)
    # -----------------------------------------------------------
    print("Concatenating experiments...")
    ds_all = xr.concat(exp_datasets, dim="expname", coords="minimal", compat="override")

    # apply target chunks once
    active_chunks = {k: v for k, v in target_chunks.items() if k in ds_all.sizes}
    active_chunks["expname"] = 1
    ds_all = ds_all.chunk(active_chunks)

    # -----------------------------------------------------------
    # 3. Assign coordinates and metadata
    # -----------------------------------------------------------
    experiments_da = xr.DataArray(
        np.array(expnames, dtype="S"), dims="expname",
        attrs={"long_name": "Experiment name"},
    )
    station_ids_da = xr.DataArray(
        station_ids, dims="station",
        attrs={"long_name": "Station ID"},
    )
    ds_all = ds_all.assign_coords(expname=experiments_da, station=station_ids_da)

    if station_coords is not None:
        coords_arr = np.array(list(station_coords.values()), dtype="f8")
        ds_all = ds_all.assign_coords(
            station_lat=xr.DataArray(coords_arr[:n_stations, 0], dims="station",
                                     attrs={"units": "deg", "long_name": "Latitude"}),
            station_lon=xr.DataArray(coords_arr[:n_stations, 1], dims="station",
                                     attrs={"units": "deg", "long_name": "Longitude"}),
        )

    if "dim" in ds_all.coords:
        ds_all = ds_all.drop_vars("dim")

    ds_all = add_coords_and_metadata(ds_all, meta_file=meta_file,
                                     station_coords=station_coords)

    encoding = _zarr_encoding(ds_all, compression_level)

    # -----------------------------------------------------------
    # 4. Write to Zarr
    # -----------------------------------------------------------
    if os.path.exists(zarr_path):
        print(f"Removing existing Zarr store: {zarr_path}")
        shutil.rmtree(zarr_path)

    print(f"Writing Zarr store: {zarr_path}")
    print(f"  shape : {dict(ds_all.sizes)}")
    print(f"  chunks: {dict(ds_all.chunks)}")

    delayed = ds_all.to_zarr(zarr_path, mode="w", compute=False,
                             encoding=encoding, zarr_format=2)

    with ProgressBar(minimum=2):
        dask.compute(delayed)

    print(f"\nZarr store complete: {zarr_path}")
    return zarr_path
