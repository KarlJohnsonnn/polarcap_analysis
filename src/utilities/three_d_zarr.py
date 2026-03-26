"""Build normalized multi-experiment Zarr stores from 3D COSMO-SPECS NetCDFs."""
# pyright: reportMissingImports=false

from __future__ import annotations

import glob
import json
import shutil
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import xarray as xr

try:
    from compute_fabric import auto_chunk_dataset, describe_chunk_plan
    from model_helpers import (
        define_bin_boundaries,
        get_model_datetime_from_meta,
        harmonize_experiment_time_to_finest,
    )
    from processing_metadata import normalize_attrs_for_zarr
except ImportError:  # pragma: no cover - package-style import fallback
    from utilities.compute_fabric import auto_chunk_dataset, describe_chunk_plan
    from utilities.model_helpers import (
        define_bin_boundaries,
        get_model_datetime_from_meta,
        harmonize_experiment_time_to_finest,
    )
    from utilities.processing_metadata import normalize_attrs_for_zarr


_METEO_3D_VARS = (
    "t",
    "p0",
    "pp",
    "qv",
    "rho0",
    "rho",
    "hhl",
    "ut",
    "vt",
    "wt",
    "qc",
    "qr",
    "qi",
    "qs",
    "dz",
    "tsbm",
    "blocks",
    "weight",
)
_SPECTRAL_3D_VARS = ("nf", "nw", "qw", "qf", "qfw")
_BULK_3D_VARS = ("icnc", "cdnc", "qwtot", "qftot", "nc", "nr", "ni", "ns")

def discover_3d_files(data_dir: str) -> dict[str, str]:
    """Return ``{expname: nc_path}`` for all ``3D_*.nc`` files in *data_dir*."""
    files = sorted(glob.glob(f"{data_dir.rstrip('/')}/3D_*.nc"))
    file_dict: dict[str, str] = {}
    for path in files:
        expname = Path(path).stem.split("_")[-1]
        if not expname:
            continue
        if expname in file_dict:
            raise ValueError(f"Duplicate 3D experiment id detected: {expname}")
        file_dict[expname] = path
    return file_dict


def load_run_metadata(meta_file: str) -> dict[str, Any]:
    """Load run metadata JSON."""
    with open(meta_file, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_domain_xy(meta: dict[str, Any]) -> str:
    """Infer ``<nx>x<ny>`` from run metadata."""
    for entry in meta.values():
        if not isinstance(entry, dict) or "INPUT_ORG" not in entry:
            continue
        domain_full = str(entry.get("domain", "")).strip()
        if not domain_full:
            continue
        parts = domain_full.split("x")
        if len(parts) >= 2:
            return "x".join(parts[:2])
    raise ValueError("Could not infer domain from run metadata.")


def _load_extpar_grid(extpar_file: str) -> dict[str, np.ndarray]:
    with xr.open_dataset(extpar_file) as ds_extpar:
        lat2d = np.asarray(ds_extpar["lat"].values[7:-7, 7:-7])
        lon2d = np.asarray(ds_extpar["lon"].values[7:-7, 7:-7])
    return {
        "latitude2D": lat2d,
        "longitude2D": lon2d,
        "latitude": np.linspace(float(lat2d.min()), float(lat2d.max()), lat2d.shape[0]),
        "longitude": np.linspace(float(lon2d.min()), float(lon2d.max()), lon2d.shape[1]),
    }


def _maybe_add_bulk_variables(ds: xr.Dataset) -> xr.Dataset:
    if "nf" in ds:
        ds["icnc"] = ds["nf"].sum(dim="diameter")
        ds["ni"] = ds["nf"].isel(diameter=slice(30, 50)).sum(dim="diameter")
        ds["ns"] = ds["nf"].isel(diameter=slice(50, None)).sum(dim="diameter")
    if "nw" in ds:
        ds["cdnc"] = ds["nw"].sum(dim="diameter")
        ds["nc"] = ds["nw"].isel(diameter=slice(30, 50)).sum(dim="diameter")
        ds["nr"] = ds["nw"].isel(diameter=slice(50, None)).sum(dim="diameter")
    if "qw" in ds:
        ds["qwtot"] = ds["qw"].sum(dim="diameter")
    if "qfw" in ds:
        ds["qftot"] = ds["qfw"].sum(dim="diameter")
    return ds


def _requested_variable_names(var_sets: Sequence[str] | None) -> set[str] | None:
    if not var_sets:
        return None
    requested: set[str] = set()
    if "meteo" in var_sets:
        requested.update(_METEO_3D_VARS)
    if "spec" in var_sets:
        requested.update(_SPECTRAL_3D_VARS)
    if "bulk" in var_sets:
        requested.update(_BULK_3D_VARS)
    return requested


def _open_3d_dataset(
    ncfile_3d: str,
    extpar_grid: dict[str, np.ndarray],
    nml_input_org: dict[str, Any],
    *,
    var_sets: Sequence[str] | None = None,
    chunks: dict[str, int] | None = None,
) -> xr.Dataset:
    ds = xr.open_dataset(ncfile_3d, chunks=chunks or {})

    rename_dims = {dim: new for dim, new in {"x": "longitude", "y": "latitude", "z": "altitude"}.items() if dim in ds.dims}
    if rename_dims:
        ds = ds.rename(rename_dims)

    if "bin" in ds.dims:
        diameter_um = define_bin_boundaries() * 1.0e6 * 2.0
        ds = ds.rename(bin="diameter")
        ds = ds.assign_coords(
            diameter=xr.DataArray(
                (diameter_um[1:] + diameter_um[:-1]) / 2.0,
                dims="diameter",
            )
        )

    time_coord = get_model_datetime_from_meta(nml_input_org, ds.time.values)
    ds = ds.assign_coords(
        time=xr.DataArray(time_coord, dims="time"),
        longitude=xr.DataArray(extpar_grid["longitude"], dims="longitude"),
        latitude=xr.DataArray(extpar_grid["latitude"], dims="latitude"),
        latitude2D=xr.DataArray(extpar_grid["latitude2D"], dims=["latitude", "longitude"]),
        longitude2D=xr.DataArray(extpar_grid["longitude2D"], dims=["latitude", "longitude"]),
    )

    if "hhl" in ds:
        altitude3d = ds["hhl"].isel(time=0)
        ds = ds.assign_coords(
            altitude=xr.DataArray(
                altitude3d.mean(dim=("longitude", "latitude")),
                dims="altitude",
            ),
            altitude3D=xr.DataArray(
                altitude3d,
                dims=["altitude", "latitude", "longitude"],
            ),
        )

    ds.time.attrs = {"units": "UTC"}
    ds.latitude.attrs = {"units": "deg"}
    ds.longitude.attrs = {"units": "deg"}
    if "altitude" in ds.coords:
        ds.altitude.attrs = {"units": "m"}

    if "bulk" in set(var_sets or ()):
        ds = _maybe_add_bulk_variables(ds)

    requested = _requested_variable_names(var_sets)
    if requested is not None:
        keep = [name for name in ds.data_vars if name in requested]
        ds = ds[keep]

    ds.attrs["ncfile"] = ncfile_3d
    ds.attrs["run_id"] = Path(ncfile_3d).stem.split("_")[-1]
    return ds


def _median_time_step_seconds(time_values: np.ndarray) -> float:
    tv = np.asarray(time_values, dtype="datetime64[ns]")
    if tv.size < 2:
        raise ValueError("Need at least two time steps to infer dt.")
    deltas = np.diff(tv.astype(np.int64))
    return float(np.median(deltas)) / 1e9


def _build_common_time_axis(
    ds_list: Sequence[xr.Dataset],
    exp_names: Sequence[str],
    target_step_seconds: float,
) -> xr.DataArray:
    if target_step_seconds <= 0:
        raise ValueError("target_step_seconds must be positive.")

    times_list = [np.asarray(ds.time.values, dtype="datetime64[ns]") for ds in ds_list]
    dts = [_median_time_step_seconds(t) for t in times_list]

    t0 = max(t[0] for t in times_list)
    t1 = min(t[-1] for t in times_list)
    if t1 < t0:
        raise ValueError("Experiments have no overlapping time range.")

    step_ns = int(round(float(target_step_seconds) * 1e9))
    t0_i = t0.astype(np.int64)
    t1_i = t1.astype(np.int64)
    n = int(np.floor((t1_i - t0_i) / step_ns)) + 1
    common = (t0_i + np.arange(n, dtype=np.int64) * step_ns).astype("datetime64[ns]")
    common = common[common <= t1]
    if common.size < 2:
        raise ValueError("Common time axis is too short after overlap trimming.")

    print(
        "Time harmonization: median native dt (s) -> target grid dt=%s s, n_time=%s, overlap [%s ... %s]"
        % (
            target_step_seconds,
            common.size,
            np.datetime_as_string(common[0], unit="s"),
            np.datetime_as_string(common[-1], unit="s"),
        )
    )
    print("  per experiment:", dict(zip(exp_names, dts)))

    return xr.DataArray(common, dims=("time",))


def harmonize_experiment_time(
    ds_list: Sequence[xr.Dataset],
    *,
    exp_names: Sequence[str],
    target_step_seconds: float | None = None,
    method: str = "linear",
) -> list[xr.Dataset]:
    """Interpolate each dataset to one shared time grid."""
    if not ds_list:
        return []
    if target_step_seconds is None:
        return list(harmonize_experiment_time_to_finest(ds_list, exp_names=exp_names, method=method))

    common_time = _build_common_time_axis(ds_list, exp_names, float(target_step_seconds))
    return [ds.interp(time=common_time, method=method) for ds in ds_list]


def _ordered_dims(dims: Sequence[str], preferred: Sequence[str]) -> tuple[str, ...]:
    preferred_dims = [dim for dim in preferred if dim in dims]
    remaining_dims = [dim for dim in dims if dim not in set(preferred_dims)]
    return tuple(preferred_dims + remaining_dims)


def _transpose_dataset(ds: xr.Dataset, preferred: Sequence[str]) -> xr.Dataset:
    dims = _ordered_dims(tuple(ds.dims), preferred)
    return ds.transpose(*dims)


def _normalize_dataset_attrs(ds: xr.Dataset) -> xr.Dataset:
    ds.attrs = {}
    for name in ds.variables:
        ds[name].attrs = normalize_attrs_for_zarr(dict(ds[name].attrs))
    return ds


def _coord_values_match(left: xr.DataArray, right: xr.DataArray) -> bool:
    a = np.asarray(left.values)
    b = np.asarray(right.values)
    if a.dtype.kind in {"f", "c"} or b.dtype.kind in {"f", "c"}:
        return np.allclose(a, b, equal_nan=True)
    return np.array_equal(a, b)


def _validate_shared_grid(ds_list: Sequence[xr.Dataset], exp_names: Sequence[str]) -> None:
    if not ds_list:
        raise ValueError("No datasets loaded for concat.")

    ref = ds_list[0]
    ref_dims = {dim: size for dim, size in ref.sizes.items() if dim != "time"}
    for expname, ds in zip(exp_names[1:], ds_list[1:]):
        dims = {dim: size for dim, size in ds.sizes.items() if dim != "time"}
        if dims != ref_dims:
            raise ValueError(
                f"Non-time dimensions differ for {expname}: expected {ref_dims}, found {dims}"
            )

        for coord_name in ("altitude", "longitude", "latitude", "diameter"):
            if coord_name not in ref.coords or coord_name not in ds.coords:
                continue
            if not _coord_values_match(ref.coords[coord_name], ds.coords[coord_name]):
                raise ValueError(f"Coordinate mismatch for {coord_name} in experiment {expname}")


def _zarr_encoding(ds: xr.Dataset, compression_level: int = 3) -> dict[str, dict[str, Any]]:
    """Build per-variable Zarr encoding with blosc-zstd compression."""
    import numcodecs

    compressor = numcodecs.Blosc(
        cname="zstd",
        clevel=compression_level,
        shuffle=numcodecs.Blosc.BITSHUFFLE,
    )
    return {var: {"compressor": compressor} for var in ds.data_vars}


def _chunk_summary(ds: xr.Dataset) -> dict[str, list[int]]:
    if not ds.chunks:
        return {}
    return {dim: [int(chunk) for chunk in chunks] for dim, chunks in dict(ds.chunks).items()}


def _write_manifest(
    manifest_path: Path,
    *,
    zarr_path: str,
    meta_file: str,
    extpar_file: str,
    expnames: Sequence[str],
    file_dict: dict[str, str],
    var_sets: Sequence[str] | None,
    target_time_step_seconds: float | None,
    time_interp_method: str,
    ds: xr.Dataset,
    validation: dict[str, Any],
) -> str:
    manifest = {
        "zarr_path": zarr_path,
        "meta_file": meta_file,
        "extpar_file": extpar_file,
        "expnames": list(expnames),
        "source_files": [{"expname": expname, "path": file_dict[expname]} for expname in expnames],
        "var_sets": list(var_sets) if var_sets else None,
        "data_vars": list(ds.data_vars),
        "target_time_step_seconds": target_time_step_seconds,
        "time_interp_method": time_interp_method,
        "sizes": {dim: int(size) for dim, size in ds.sizes.items()},
        "chunks": _chunk_summary(ds),
        "cleanup_candidates": list(file_dict.values()),
        "rebuild_required_for_new_experiments": True,
        "validation": validation,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return str(manifest_path)


def validate_3d_zarr(
    zarr_path: str,
    *,
    expected_expnames: Sequence[str] | None = None,
    expected_time_step_seconds: float | None = None,
    require_diameter: bool = True,
) -> dict[str, Any]:
    """Open the Zarr store and validate key dimensions and metadata."""
    ds = xr.open_zarr(zarr_path)

    required_dims = {"expname", "time", "altitude", "longitude", "latitude"}
    missing = required_dims.difference(ds.dims)
    if missing:
        raise ValueError(f"Missing required dimensions in Zarr: {sorted(missing)}")
    if require_diameter and "diameter" not in ds.dims:
        raise ValueError("Expected a diameter dimension in the Zarr store.")

    expnames = [str(value) for value in np.asarray(ds.expname.values).tolist()]
    if expected_expnames is not None and expnames != list(expected_expnames):
        raise ValueError(f"expname mismatch: expected {list(expected_expnames)}, found {expnames}")

    time_step_seconds = None
    if ds.sizes.get("time", 0) > 1:
        time_step_seconds = _median_time_step_seconds(ds.time.values)
        if expected_time_step_seconds is not None and not np.isclose(
            time_step_seconds,
            float(expected_time_step_seconds),
        ):
            raise ValueError(
                f"Unexpected time step: expected {expected_time_step_seconds}s, found {time_step_seconds}s"
            )

    return {
        "sizes": {dim: int(size) for dim, size in ds.sizes.items()},
        "expnames": expnames,
        "time_step_seconds": time_step_seconds,
    }


def build_3d_zarr(
    file_dict: dict[str, str],
    zarr_path: str,
    *,
    meta_file: str,
    extpar_file: str,
    var_sets: Sequence[str] | None = None,
    target_time_step_seconds: float | None = 10.0,
    time_interp_method: str = "linear",
    target_chunk_mb: int | None = None,
    min_chunk_mb: int = 64,
    max_chunk_mb: int = 512,
    memory_fraction: float = 0.12,
    compression_level: int = 3,
    global_attrs: dict[str, Any] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Build one normalized, multi-experiment 3D Zarr store."""
    import dask
    from dask.diagnostics import ProgressBar

    if not file_dict:
        raise ValueError("No 3D NetCDF files found.")

    meta = load_run_metadata(meta_file)
    expnames = list(file_dict.keys())
    ds_list: list[xr.Dataset] = []
    preferred_dims = ("time", "altitude", "longitude", "latitude", "diameter")
    extpar_grid = _load_extpar_grid(extpar_file)
    var_sets_tuple = tuple(dict.fromkeys(var_sets)) if var_sets else None

    for idx, expname in enumerate(expnames, start=1):
        if expname not in meta:
            raise KeyError(f"Experiment {expname} is missing from {meta_file}")
        nml_input_org = meta[expname].get("INPUT_ORG")
        if not isinstance(nml_input_org, dict):
            raise KeyError(f"Experiment {expname} has no INPUT_ORG block in {meta_file}")

        print(f"  Opening [{idx}/{len(expnames)}] {expname}")
        ds = _open_3d_dataset(
            file_dict[expname],
            extpar_grid,
            nml_input_org,
            var_sets=var_sets_tuple,
            chunks={},
        )
        ds = _transpose_dataset(ds, preferred_dims)
        ds = _normalize_dataset_attrs(ds)
        ds_list.append(ds)

    _validate_shared_grid(ds_list, expnames)
    ds_list = harmonize_experiment_time(
        ds_list,
        exp_names=expnames,
        target_step_seconds=target_time_step_seconds,
        method=time_interp_method,
    )

    print("Concatenating experiments...")
    ds_all = xr.concat(ds_list, dim="expname", coords="minimal", compat="override")
    ds_all = ds_all.assign_coords(
        expname=xr.DataArray(
            np.asarray(expnames, dtype="U"),
            dims="expname",
            attrs={"long_name": "Experiment name"},
        )
    )
    ds_all = _transpose_dataset(ds_all, ("expname", *preferred_dims))

    ds_all, chunk_dict = auto_chunk_dataset(
        ds_all,
        target_chunk_mb=target_chunk_mb,
        min_chunk_mb=min_chunk_mb,
        max_chunk_mb=max_chunk_mb,
        memory_fraction=memory_fraction,
        prefer_dims=("time", "altitude", "longitude", "latitude", "diameter", "expname"),
    )
    if "expname" in ds_all.sizes:
        chunk_dict["expname"] = 1
    ds_all = ds_all.chunk({dim: size for dim, size in chunk_dict.items() if dim in ds_all.sizes})
    chunk_plan = describe_chunk_plan(ds_all, chunk_dict)

    if global_attrs:
        ds_all.attrs.update(normalize_attrs_for_zarr(dict(global_attrs)))

    zarr_target = Path(zarr_path)
    zarr_target.parent.mkdir(parents=True, exist_ok=True)
    if zarr_target.exists():
        if not overwrite:
            raise FileExistsError(f"Zarr exists: {zarr_target}")
        if zarr_target.is_dir():
            shutil.rmtree(zarr_target)
        else:
            zarr_target.unlink()

    print(f"Writing Zarr store: {zarr_target}")
    print(f"  shape : {dict(ds_all.sizes)}")
    print(f"  chunks: {_chunk_summary(ds_all)}")
    print(f"  {chunk_plan}")

    delayed = ds_all.to_zarr(
        zarr_target,
        mode="w",
        compute=False,
        encoding=_zarr_encoding(ds_all, compression_level),
        zarr_format=2,
    )
    with ProgressBar(minimum=2):
        dask.compute(delayed)

    validation = validate_3d_zarr(
        str(zarr_target),
        expected_expnames=expnames,
        expected_time_step_seconds=target_time_step_seconds,
        require_diameter="diameter" in ds_all.dims,
    )
    manifest_path = _write_manifest(
        zarr_target.with_suffix(".manifest.json"),
        zarr_path=str(zarr_target),
        meta_file=meta_file,
        extpar_file=extpar_file,
        expnames=expnames,
        file_dict=file_dict,
        var_sets=var_sets_tuple,
        target_time_step_seconds=target_time_step_seconds,
        time_interp_method=time_interp_method,
        ds=ds_all,
        validation=validation,
    )

    return {
        "zarr_path": str(zarr_target),
        "manifest_path": manifest_path,
        "chunk_plan": chunk_plan,
        "validation": validation,
    }
