"""Fast meteogram NetCDF-to-Zarr pipeline.

Replaces the slow ``00-prepM.py`` workflow with:
* ``ncdump -h`` header reads for time-dimension size
* auto-detected engine: h5netcdf for NetCDF-4 (GIL-free), netcdf4 for classic
* per-file open + immediate variable selection (no ``open_mfdataset``)
* single-level parallelism — no nested thread pools
* streaming region-writes to a pre-allocated Zarr store
* blosc-zstd compression on write
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
from time import perf_counter
from typing import Dict, List, Optional, Sequence, Tuple, Any

import numpy as np
import xarray as xr

from utilities.init_common import get_station_coords_from_cfg

# ---------------------------------------------------------------------------
# NetCDF engine auto-detection
# ---------------------------------------------------------------------------

_ENGINE_CACHE: Optional[str] = None
_DEBUG_LOG_PATH = "/home/b/b382237/code/polarcap/python/polarcap_analysis/.cursor/debug-00bfbe.log"


def _debug_log(hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    try:
        import time

        payload = {
            "sessionId": "00bfbe",
            "runId": os.environ.get("SLURM_JOB_ID", "local"),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass


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


def _resolve_nc_engine(path: str, nc_engine: Optional[str]) -> str:
    """Return xarray backend: explicit ``h5netcdf`` / ``netcdf4``, or auto-detect."""
    if nc_engine is None or str(nc_engine).lower() == "auto":
        return _detect_nc_engine(path)
    eng = str(nc_engine).lower()
    if eng not in ("h5netcdf", "netcdf4"):
        raise ValueError(f"nc_engine must be 'auto', 'h5netcdf', or 'netcdf4', got {nc_engine!r}")
    return eng


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

    Runs ``ncdump -h`` once per file (subprocess I/O).
    """
    all_files = [f for files in file_dict.values() for f in files]
    print(f"Checking time steps in {len(all_files)} files (ncdump)...")
    sizes = [_ncdump_time_size(f) for f in all_files]
    max_t = max(sizes) if sizes else 0
    print(f"Max time steps: {max_t}")
    return max_t


def get_variable_names(sample_file: str) -> List[str]:
    """Read variable names from one NetCDF file (header only)."""
    engine = _detect_nc_engine(sample_file)
    with xr.open_dataset(sample_file, engine=engine) as ds:
        return [str(name) for name in ds.data_vars.keys()]


# ---------------------------------------------------------------------------
# Time alignment (experiment-level target) + per-station height preprocessing
# ---------------------------------------------------------------------------

def _build_target_time_values(ds_time: np.ndarray, max_time: int) -> np.ndarray:
    """Extend ``time`` coordinate to ``max_time`` steps using fixed Δt from the file."""
    if max_time <= 0 or ds_time.size >= max_time:
        return ds_time
    if ds_time.size < 2:
        raise ValueError("Need >= 2 time steps to infer dt")
    missing = max_time - ds_time.size
    dt = ds_time[1] - ds_time[0]
    tail = np.array([ds_time[-1] + (i + 1) * dt for i in range(missing)], dtype=ds_time.dtype)
    return np.concatenate([ds_time, tail])


def _align_station_time(ds: xr.Dataset, target_times: np.ndarray) -> xr.Dataset:
    """Reindex to shared ``target_times`` so stations can be concatenated."""
    if "time" not in ds.sizes:
        return ds
    cur = ds.time.values
    if cur.shape == target_times.shape and np.array_equal(cur, target_times):
        return ds
    if "time" not in ds.xindexes and "time" in ds.coords:
        ds = ds.set_xindex("time")
    return ds.reindex(time=target_times, method="pad", fill_value=np.nan)


def _preprocess_station_heights(ds: xr.Dataset, max_height_level: int = 20) -> xr.Dataset:
    """Swap HMLd/HHLd dims, trim vertical levels (lazy on dask-backed data)."""
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
    *,
    profile_io: bool = False,
    open_fast: bool = False,
    nc_engine: Optional[str] = None,
) -> Tuple[xr.Dataset, np.ndarray]:
    """Load all station files for a single experiment and concat along *station*.

    Time grid is computed once from the first station file, then each file is
    aligned to that target before height preprocessing and concat.
    """
    station_id_of = lambda f: str(f.split("/")[-1].split("_")[1])
    station_ids = np.array([station_id_of(f) for f in sorted_files], dtype="i4")
    engine = _resolve_nc_engine(sorted_files[0], nc_engine)

    open_kw: Dict[str, Any] = {"engine": engine, "chunks": chunks}
    if open_fast:
        open_kw["create_default_indexes"] = False
        open_kw["cache"] = False

    t_open = t_sel = t_align = t_heights = 0.0
    t_exp0 = perf_counter()
    target_times: Optional[np.ndarray] = None
    datasets: List[xr.Dataset] = []

    # region agent log
    _debug_log(
        "H1",
        "meteogram_io.py:_open_experiment:start",
        "open_experiment_options",
        {
            "n_files": len(sorted_files),
            "engine": engine,
            "open_fast": open_fast,
            "max_time": int(max_time),
            "chunk_keys": sorted(str(k) for k in chunks.keys()),
        },
    )
    # endregion

    for path in sorted_files:
        t0 = perf_counter()
        ds = xr.open_dataset(path, **open_kw)
        t_open += perf_counter() - t0

        t0 = perf_counter()
        ds = ds[variables]
        t_sel += perf_counter() - t0

        t0 = perf_counter()
        if target_times is None:
            target_times = _build_target_time_values(ds.time.values, max_time)
            # region agent log
            _debug_log(
                "H2",
                "meteogram_io.py:_open_experiment:target_time",
                "target_time_seeded",
                {
                    "path": os.path.basename(path),
                    "seed_time_size": int(ds.sizes.get("time", -1)),
                    "target_time_size": int(target_times.size),
                    "has_time_coord": "time" in ds.coords,
                    "has_time_xindex": "time" in ds.xindexes,
                    "xindexes": [str(k) for k in ds.xindexes.keys()],
                },
            )
            # endregion
        if "time" in ds.sizes and (
            ds.sizes["time"] != target_times.size or "time" not in ds.xindexes
        ):
            # region agent log
            _debug_log(
                "H3",
                "meteogram_io.py:_open_experiment:before_align",
                "align_candidate",
                {
                    "path": os.path.basename(path),
                    "sizes": {str(k): int(v) for k, v in ds.sizes.items()},
                    "target_time_size": int(target_times.size),
                    "has_time_coord": "time" in ds.coords,
                    "time_coord_dims": list(ds.coords["time"].dims) if "time" in ds.coords else [],
                    "time_coord_size": int(ds.coords["time"].size) if "time" in ds.coords else None,
                    "has_time_xindex": "time" in ds.xindexes,
                    "xindexes": [str(k) for k in ds.xindexes.keys()],
                },
            )
            # endregion
        try:
            ds = _align_station_time(ds, target_times)
            if "time" in ds.sizes and ds.sizes["time"] == target_times.size:
                # region agent log
                _debug_log(
                    "H5",
                    "meteogram_io.py:_open_experiment:after_align",
                    "align_succeeded",
                    {
                        "path": os.path.basename(path),
                        "time_size": int(ds.sizes["time"]),
                        "target_time_size": int(target_times.size),
                        "has_time_xindex": "time" in ds.xindexes,
                        "xindexes": [str(k) for k in ds.xindexes.keys()],
                    },
                )
                # endregion
        except Exception as exc:
            # region agent log
            _debug_log(
                "H4",
                "meteogram_io.py:_open_experiment:align_exception",
                "align_failed",
                {
                    "path": os.path.basename(path),
                    "engine": engine,
                    "open_fast": open_fast,
                    "sizes": {str(k): int(v) for k, v in ds.sizes.items()},
                    "target_time_size": int(target_times.size),
                    "has_time_coord": "time" in ds.coords,
                    "time_coord_dims": list(ds.coords["time"].dims) if "time" in ds.coords else [],
                    "time_coord_size": int(ds.coords["time"].size) if "time" in ds.coords else None,
                    "has_time_xindex": "time" in ds.xindexes,
                    "xindexes": [str(k) for k in ds.xindexes.keys()],
                    "exception_type": type(exc).__name__,
                    "exception_message": str(exc),
                },
            )
            # endregion
            raise
        t_align += perf_counter() - t0

        t0 = perf_counter()
        ds = _preprocess_station_heights(ds, max_height_level)
        t_heights += perf_counter() - t0

        datasets.append(ds)

    t0 = perf_counter()
    ds_exp = xr.concat(datasets, dim="station", coords="minimal", compat="override")
    t_concat = perf_counter() - t0

    ds_exp = ds_exp.assign_coords(station=station_ids)

    if profile_io:
        n = max(len(sorted_files), 1)
        t_total = perf_counter() - t_exp0
        print(
            f"    [io] open={t_open:.2f}s sel={t_sel:.2f}s align={t_align:.2f}s "
            f"heights={t_heights:.2f}s concat={t_concat:.2f}s total={t_total:.2f}s "
            f"({n} files, engine={engine}, open_fast={open_fast})"
        )

    return ds_exp, station_ids


def _coerce_station_id(value: Any) -> int | str:
    """Return numeric station ids when possible, otherwise preserve the string."""
    try:
        return int(value)
    except Exception:
        return str(value)


def _station_id_array(values: Sequence[Any]) -> np.ndarray:
    """Return a compact station-id array with numeric dtype when possible."""
    vals = [_coerce_station_id(v) for v in values]
    if vals and all(isinstance(v, int) for v in vals):
        return np.asarray(vals, dtype="i4")
    return np.asarray([str(v) for v in vals], dtype="U")


# ---------------------------------------------------------------------------
# Bin-boundary coordinate helper
# ---------------------------------------------------------------------------

def _compute_bin_coords(n_bins: int = 66, n_max: float = 2.0,
                        r_min: float = 1e-9, rhow: float = 1e3):
    """Mass/radius bin edges and centres (COSMO-SPECS defaults)."""
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min ** 3
    j0w = (n_max - 1.0) / np.log(2.0)  # natural logarithm
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
    target_time_chunk: int = 1024,
    target_station_chunk: int = -1,
    target_bins_chunk: int = -1,
    compression_level: int = 3,
    debug_mode: bool = False,
    global_attrs: Optional[Dict[str, Any]] = None,
    profile_io: bool = False,
    open_fast: bool = False,
    nc_engine: Optional[str] = None,
) -> str:
    """Build a Zarr store from per-experiment meteogram NetCDF files.

    Chunk defaults favour larger writes on shared HPC filesystems while keeping
    ``expname`` chunked by one experiment for region-based incremental writes.

    Strategy
    --------
    1. Open every experiment lazily (flat per-file open, no open_mfdataset).
    2. Pre-create the Zarr store from a template dataset with final coordinates.
    3. Write coordinate payloads once.
    4. Stream one experiment at a time into its ``expname`` region.

    Returns the *zarr_path*.
    """
    from dask.base import compute
    from dask.diagnostics.progress import ProgressBar

    if station_coords is None and meta_file is not None:
        station_coords = get_station_coords_from_cfg(meta_file)

    expnames = list(file_dict.keys())
    n_experiments = len(expnames)

    if profile_io or open_fast or (nc_engine and str(nc_engine).lower() != "auto"):
        sample = next(iter(file_dict.values()))[0]
        print(
            f"  NetCDF read options: engine={_resolve_nc_engine(sample, nc_engine)} "
            f"open_fast={open_fast} profile_io={profile_io}"
        )

    # --- target chunks (set once and reused for every region write) ---
    target_chunks: Dict[str, int] = {
        "time": min(target_time_chunk, max_time) if max_time > 0 else target_time_chunk,
        "height_level": -1,
        "height_level2": -1,
        "bins": target_bins_chunk,
    }

    # -----------------------------------------------------------
    # 1. Open all experiments lazily
    # -----------------------------------------------------------
    exp_datasets: List[xr.Dataset] = []
    station_ids_seen: List[Any] = []

    for i, exp in enumerate(expnames):
        files = sorted(file_dict[exp])
        print(f"  Opening [{i + 1}/{n_experiments}] {exp}  ({len(files)} stations)")
        ds_exp, sids = _open_experiment(
            files,
            variables,
            max_time,
            max_height_level,
            target_chunks,
            profile_io=profile_io,
            open_fast=open_fast,
            nc_engine=nc_engine,
        )
        station_ids_seen.extend([_coerce_station_id(s) for s in sids.tolist()])
        exp_datasets.append(ds_exp)

    if station_coords is not None:
        station_ids = _station_id_array(sorted({_coerce_station_id(k) for k in station_coords.keys()}))
    else:
        station_ids = _station_id_array(sorted(set(station_ids_seen)))

    if not exp_datasets:
        raise ValueError("No experiment datasets available for Zarr export.")

    n_stations = len(station_ids)
    target_chunks["station"] = -1 if target_station_chunk == -1 else min(target_station_chunk, n_stations)

    # -----------------------------------------------------------
    # 2. Resolve final chunk plan and build the template dataset
    # -----------------------------------------------------------
    sample_sizes = dict(exp_datasets[0].sizes)
    sample_sizes["station"] = n_stations
    sample_sizes["expname"] = n_experiments
    active_chunks = {}
    for k, v in target_chunks.items():
        if k not in sample_sizes:
            continue
        active_chunks[k] = sample_sizes[k] if v == -1 else min(v, sample_sizes[k])
    active_chunks["expname"] = 1

    write_chunks = {k: v for k, v in active_chunks.items() if k != "expname"}
    exp_datasets = [ds_exp.reindex(station=station_ids).chunk(write_chunks) for ds_exp in exp_datasets]

    # -----------------------------------------------------------
    # 3. Assign coordinates and metadata to the template dataset
    # -----------------------------------------------------------
    experiments_da = xr.DataArray(
        np.asarray(expnames, dtype="U"), dims="expname",
        attrs={"long_name": "Experiment name"},
    )
    station_ids_da = xr.DataArray(station_ids, dims="station", attrs={"long_name": "Station ID"})
    ds_template = exp_datasets[0].expand_dims(expname=experiments_da)
    ds_template = ds_template.assign_coords(expname=experiments_da, station=station_ids_da)

    if station_coords is not None:
        station_lat = []
        station_lon = []
        for sid in station_ids:
            lat_lon = station_coords.get(str(sid))
            if lat_lon is None:
                station_lat.append(np.nan)
                station_lon.append(np.nan)
            else:
                station_lat.append(float(lat_lon[0]))
                station_lon.append(float(lat_lon[1]))
        ds_template = ds_template.assign_coords(
            station_lat=xr.DataArray(np.asarray(station_lat, dtype="f8"), dims="station",
                                     attrs={"units": "deg", "long_name": "Latitude"}),
            station_lon=xr.DataArray(np.asarray(station_lon, dtype="f8"), dims="station",
                                     attrs={"units": "deg", "long_name": "Longitude"}),
        )

    if "dim" in ds_template.coords:
        ds_template = ds_template.drop_vars("dim")

    ds_template = add_coords_and_metadata(ds_template, meta_file=meta_file,
                                          station_coords=station_coords)
    ds_template = ds_template.chunk(active_chunks)

    if global_attrs:
        for k, v in global_attrs.items():
            ds_template.attrs[k] = v

    encoding = _zarr_encoding(ds_template, compression_level)

    # -----------------------------------------------------------
    # 4. Write to Zarr
    # -----------------------------------------------------------
    if os.path.exists(zarr_path):
        print(f"Removing existing Zarr store: {zarr_path}")
        shutil.rmtree(zarr_path)

    print(f"Initializing Zarr store: {zarr_path}")
    print(f"  shape : {dict(ds_template.sizes)}")
    print(f"  chunks: {dict(ds_template.chunks)}")

    ds_template.to_zarr(
        zarr_path,
        mode="w",
        compute=False,
        encoding=encoding,
        zarr_format=2,
    )
    ds_template.coords.to_dataset().to_zarr(zarr_path, mode="a", zarr_format=2)

    print("Writing experiment regions...")
    for i, (exp, ds_exp) in enumerate(zip(expnames, exp_datasets)):
        print(f"  Writing [{i + 1}/{n_experiments}] {exp}")
        ds_region = xr.Dataset(
            data_vars={
                name: da.expand_dims(expname=[exp])
                for name, da in ds_exp.data_vars.items()
            }
        ).chunk(active_chunks)
        non_region_dim_vars = {
            str(name): list(da.dims)
            for name, da in ds_region.variables.items()
            if "expname" not in da.dims
        }
        ds_region = ds_region.drop_vars(list(non_region_dim_vars))
        delayed = ds_region.to_zarr(
            zarr_path,
            mode="r+",
            region={"expname": slice(i, i + 1)},
            compute=False,
            zarr_format=2,
        )
        with ProgressBar(minimum=2):
            compute(delayed)

    print(f"\nZarr store complete: {zarr_path}")
    return zarr_path
