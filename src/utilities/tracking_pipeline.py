"""
LV1a/LV1b pipeline: run discovery, tobac tracking, plume-path extraction and writing.

Reusable functions for scripts/processing_chain/run_tracking.py.
Uses model_helpers (make_3d_preprocessor, convert_units_3d, define_bin_boundaries)
and namelist_metadata (update_dataset_metadata). Writes to staged dirs with provenance.
"""

from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import xarray as xr

from utilities.model_helpers import (
    convert_units_3d,
    define_bin_boundaries,
    make_3d_preprocessor,
)
from utilities.namelist_metadata import update_dataset_metadata
from utilities.processing_metadata import add_provenance_to_dataset, git_head, provenance_attrs


# Paper/notebook: n_i^feature = n_f^flare - n_f^ref, threshold ≥ 1 L⁻¹ (ice crystal number conc)
DEFAULT_TRACER_SPECS: List[Tuple[str, Tuple[int, Optional[int]]]] = [
    ("nf", (0, None)),  # size-integrated ice number concentration (all diameter bins)
]
DEFAULT_THRESHOLD = 1.0  # 1 per liter (L⁻¹), Omanovic et al. 2024
DEFAULT_EXTRACTION_TYPES = ("integrated", "extreme", "vertical")
DEG_KM_LON = 111.13295254925466  # 1 deg longitude ≈ 111 km at mid-lat
DEFAULT_FLARE_LAT, DEFAULT_FLARE_LON = 47.07425, 7.90522
DEFAULT_MAX_ALTITUDE = 1500  # m

# Keys that differ between flare and reference (emission-related only)
FLARE_REF_DIFF_KEYS = frozenset({
    "lflare",  # sbm_par
    "flare_emission", "lflare_inp", "lflare_ccn", "flare_dn", "flare_dp", "flare_sig",  # flare_sbm
})


def prep_tobac_input(
    ds_flare: xr.DataArray,
    ds_ref: xr.DataArray,
    standard_name: Optional[str] = None,
    mask_threshold: float = 1e-12,
) -> xr.DataArray:
    """Build tobac input field: flare − ref, masked below threshold. Units 1/L (e.g. 1/dm³)."""
    tobac_input = ds_flare - ds_ref
    tobac_input.attrs["units"] = "1/dm3"
    if "standard_name" in tobac_input.attrs and standard_name is None:
        tobac_input.attrs.pop("standard_name")
    elif standard_name is not None:
        tobac_input.attrs["standard_name"] = standard_name
    return xr.where(tobac_input < mask_threshold, mask_threshold, tobac_input)


@dataclass
class RunContext:
    """Minimal context for one (cs_run, flare_idx, ref_idx) combination."""

    cs_run: str
    domain_xy: str  # e.g. "50x40", "200x160"
    model_data_path: str
    extpar_file: str
    meta: Dict[str, Any]
    flare_exp_name: str
    ref_exp_name: str
    flare_nc_file: str
    ref_nc_file: str
    flare_idx: int
    ref_idx: int
    threshold: float = DEFAULT_THRESHOLD
    flare_lat: float = DEFAULT_FLARE_LAT
    flare_lon: float = DEFAULT_FLARE_LON
    flare_alt_idx: int = -1
    max_altitude: float = DEFAULT_MAX_ALTITUDE
    resolution_deg: float = 0.004
    resolution: str = "400m"

    def reduced_domain(self) -> Dict[str, slice]:
        return {
            "latitude": slice(None, self.flare_lat + 2.0 * self.resolution_deg),
            "longitude": slice(None, self.flare_lon + 2.0 * self.resolution_deg),
            "altitude": slice(self.max_altitude, None),
        }


def _is_flare(meta: Dict[str, Any], exp_name: str) -> bool:
    """True if experiment is marked as flare in run JSON; False if missing or not flare."""
    try:
        return bool(meta.get(exp_name, {}).get("INPUT_ORG", {}).get("sbm_par", {}).get("lflare", False))
    except Exception:
        return False


def _non_emission_signature(meta: Dict[str, Any], exp_name: str) -> Tuple[Any, ...]:
    """Signature of all INPUT_ORG params that should match between flare and reference (excl. emission)."""
    io = meta.get(exp_name, {}).get("INPUT_ORG", {})
    sbm = io.get("sbm_par", {})
    flare = io.get("flare_sbm", {})
    runctl = io.get("runctl", {})
    # Flare-only keys we ignore; everything else must match
    sbm_sub = {k: v for k, v in sbm.items() if k not in FLARE_REF_DIFF_KEYS}
    flare_sub = {k: v for k, v in flare.items() if k not in FLARE_REF_DIFF_KEYS}
    return (
        io.get("domain"),
        tuple(sorted(sbm_sub.items())),
        tuple(sorted(flare_sub.items())),
        tuple(sorted(runctl.items())),
    )


def find_matching_reference(
    meta: Dict[str, Any],
    flare_exp_name: str,
    ref_names: List[str],
) -> Optional[str]:
    """
    Return the reference experiment that matches the flare in all non-emission parameters.
    Ref and flare must differ only in lflare, flare_emission, lflare_inp, lflare_ccn (and flare_dn/dp/sig).
    Returns None if no matching ref found.
    """
    sig_flare = _non_emission_signature(meta, flare_exp_name)
    for ref_name in ref_names:
        if _non_emission_signature(meta, ref_name) == sig_flare:
            return ref_name
    return None


def discover_3d_runs(
    model_data_root: str,
    domain_xy: str,
    cs_run: str,
    flare_idx: int = 0,
    ref_idx: int = -1,
    threshold: float = DEFAULT_THRESHOLD,
) -> Optional[RunContext]:
    """
    Build RunContext for one (cs_run, flare_idx, ref_idx).
    ref_idx < 0: auto-select reference that matches flare in non-emission params only.
    Returns None if no flare/ref or no matching ref when ref_idx < 0.
    """
    model_data_path = f"{model_data_root}/RUN_ERISWILL_{domain_xy}x100/ensemble_output/{cs_run}/"
    extpar_file = f"{model_data_root}/RUN_ERISWILL_{domain_xy}x100/COS_in/extPar_Eriswil_{domain_xy}.nc"
    json_files = glob.glob(f"{model_data_path}*.json")
    filelist_3d = sorted(glob.glob(f"{model_data_path}3D_??????????????.nc"))
    if not json_files or not filelist_3d:
        return None
    with open(json_files[0]) as f:
        meta = json.load(f)
    exp_names = [p.split("/")[-1].split("_")[-1].split(".")[0] for p in filelist_3d]
    flare_names = [e for e in exp_names if _is_flare(meta, e)]
    ref_names = [e for e in exp_names if not _is_flare(meta, e)]
    if not flare_names or not ref_names:
        return None
    if flare_idx >= len(flare_names):
        return None
    flare_exp_name = flare_names[flare_idx]
    if ref_idx < 0:
        ref_exp_name = find_matching_reference(meta, flare_exp_name, ref_names)
        if ref_exp_name is None:
            return None
        ref_idx = ref_names.index(ref_exp_name)
    elif ref_idx >= len(ref_names):
        return None
    else:
        ref_exp_name = ref_names[ref_idx]
    flare_nc = next(p for p in filelist_3d if flare_exp_name in p)
    ref_nc = next(p for p in filelist_3d if ref_exp_name in p)
    domain_str = meta[flare_exp_name].get("domain", "")
    resolution = "400m" if "50x40" in domain_str else "100m"
    resolution_deg = 0.004 if "50x40" in domain_str else 0.001
    flare_hight = meta.get(flare_exp_name, {}).get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_hight", 1)
    return RunContext(
        cs_run=cs_run,
        domain_xy=domain_xy,
        model_data_path=model_data_path,
        extpar_file=extpar_file,
        meta=meta,
        flare_exp_name=flare_exp_name,
        ref_exp_name=ref_exp_name,
        flare_nc_file=flare_nc,
        ref_nc_file=ref_nc,
        flare_idx=flare_idx,
        ref_idx=ref_idx,
        threshold=threshold,
        resolution=resolution,
        resolution_deg=resolution_deg,
        flare_alt_idx=-int(flare_hight),
    )


def _load_3d_for_tracking(
    nc_file: str,
    extpar_file: str,
    nml: Dict[str, Any],
    reduced_domain: Dict[str, slice],
    variables: List[str],
    chunks: Optional[Dict[str, int]] = None,
) -> xr.Dataset:
    """Load 3D dataset, restrict domain, convert units, keep only *variables*."""
    preprocess = make_3d_preprocessor(nc_file, extpar_file, nml)
    ds = xr.open_mfdataset(
        nc_file, preprocess=preprocess, chunks=chunks or {"time": 4}, parallel=True
    )
    run_id = nc_file.split("/")[-1].split("_")[1].split(".")[0]
    ds.attrs["ncfile"] = nc_file
    ds.attrs["run_id"] = run_id
    ds = update_dataset_metadata(ds)
    ds = ds.sel(reduced_domain)
    ds = convert_units_3d(ds, ds["rho"])
    return ds[variables]


def run_tobac_tracking(
    ctx: RunContext,
    output_dir: Optional[Union[str, Path]] = None,
    tracer_specs: Optional[List[Tuple[str, Tuple[int, Optional[int]]]]] = None,
    overwrite: bool = False,
    persist: bool = True,
) -> List[Tuple[str, str, str, str]]:
    """
    Run tobac feature detection, linking, segmentation for each tracer.
    Writes features CSV, tracks CSV, features_mask CSV, segmentation_mask NetCDF under output_dir.
    Returns list of (tracer_var, features_path, tracks_path, segm_path).
    """
    import tobac
    import iris
    from dask.diagnostics import ProgressBar

    iris.FUTURE.date_microseconds = True
    out = Path(output_dir or ctx.model_data_path)
    out.mkdir(parents=True, exist_ok=True)
    tracer_specs = tracer_specs or DEFAULT_TRACER_SPECS
    reduced = ctx.reduced_domain()

    nml_flare = ctx.meta[ctx.flare_exp_name]["INPUT_ORG"]
    nml_ref = ctx.meta[ctx.ref_exp_name]["INPUT_ORG"]
    ds_flare = _load_3d_for_tracking(
        ctx.flare_nc_file, ctx.extpar_file, nml_flare, reduced, ["nf", "t"]
    )
    ds_ref = _load_3d_for_tracking(
        ctx.ref_nc_file, ctx.extpar_file, nml_ref, reduced, ["nf", "t"]
    )
    flare_alt_idx = getattr(ctx, "flare_alt_idx", -nml_flare["flare_sbm"]["flare_hight"])
    ds_flare.attrs["flare_alt"] = float(ds_flare.altitude.values[100 - flare_alt_idx])

    parameter_features = {
        "position_threshold": "extreme",
        "sigma_threshold": 1.0,
        "n_min_threshold": 0,
        "target": "maximum",
        "vertical_coord": "altitude",
        "threshold": [ctx.threshold, ctx.threshold * 1e1, ctx.threshold * 1e2],
    }

    written: List[Tuple[str, str, str, str]] = []
    for var, (lo, hi) in tracer_specs:
        q_flare = ds_flare["nf"].isel(diameter=slice(lo, hi)).sum("diameter")
        q_ref = ds_ref["nf"].isel(diameter=slice(lo, hi)).sum("diameter")
        tobac_input = prep_tobac_input(q_flare, q_ref, mask_threshold=1e-12)
        if persist:
            with ProgressBar():
                tobac_input = tobac_input.persist()
        delta_x = 1e3 * np.mean(np.diff(tobac_input.longitude.values)) * DEG_KM_LON
        delta_y = 1e3 * np.mean(np.diff(tobac_input.latitude.values)) * DEG_KM_LON
        delta_t = np.mean(
            np.diff(tobac_input.time.astype("datetime64[s]")).astype(float)
        )
        tobac_iris = tobac_input.to_iris()
        dxy, dt = tobac.get_spacings(
            tobac_iris, grid_spacing=np.max([delta_x, delta_y]), time_spacing=delta_t
        )
        features_file = out / f"{ctx.flare_exp_name}_{var}_tobac_features.csv"
        tracks_file = out / f"{ctx.flare_exp_name}_{var}_tobac_track.csv"
        features_mask_file = out / f"{ctx.flare_exp_name}_{var}_tobac_features_mask.csv"
        segm_mask_file = out / f"{ctx.flare_exp_name}_{var}_tobac_mask_xarray.nc"
        if not overwrite and segm_mask_file.exists():
            written.append((var, str(features_file), str(tracks_file), str(segm_mask_file)))
            continue
        features = tobac.feature_detection_multithreshold(
            tobac_iris, dxy, **parameter_features
        )
        if features is None or len(features) == 0:
            continue
        features.to_csv(str(features_file))
        tracks = tobac.linking_trackpy(
            features, tobac_iris, vertical_coord=parameter_features["vertical_coord"],
            dt=dt, dxy=dxy, v_max=100,
        )
        tracks.to_csv(str(tracks_file))
        iris_mask, features_mask = tobac.segmentation.segmentation(
            features, tobac_iris, dxy,
            threshold=parameter_features["threshold"][0],
            vertical_coord=parameter_features["vertical_coord"],
        )
        features_mask.to_csv(str(features_mask_file))
        xr_mask = xr.DataArray.from_iris(iris_mask)
        segm_ds = xr_mask.to_dataset(name="segmentation_mask")
        add_provenance_to_dataset(
            segm_ds,
            stage="lv1a",
            processing_level="LV1a",
            title="Tobac segmentation mask",
            cs_run=ctx.cs_run,
            exp_label=ctx.flare_exp_name,
            domain=ctx.domain_xy,
            input_files=[ctx.flare_nc_file, ctx.ref_nc_file],
        )
        if segm_mask_file.exists():
            segm_mask_file.unlink()
        segm_ds.to_netcdf(str(segm_mask_file), mode="w")
        written.append((var, str(features_file), str(tracks_file), str(segm_mask_file)))
    return written


def extract_segmented_tracks_paths(
    ds: xr.Dataset,
    tracks: xr.Dataset,
    segm_mask: xr.DataArray,
    *,
    pre_track_minutes: int = 5,
    delta_t: int = 10,
    mode: str = "integrated",
) -> Dict[int, xr.Dataset]:
    """
    Extract per-cell path datasets (time × diameter or time × altitude × diameter).
    mode: 'integrated' | 'extreme' | 'vertical'.
    """
    cells = {}
    path_times = None
    path_lat = None
    path_lon = None
    path_alt = None
    for cell_id in np.unique(tracks["cell"].values):
        track = tracks.where(tracks["cell"] == cell_id, drop=True)
        t0 = track["time"].astype("datetime64[ns]").values[0]
        t0_earlier = t0 - np.timedelta64(pre_track_minutes, "m")
        pre_times = pd.date_range(
            t0_earlier, t0 - np.timedelta64(delta_t, "s"), freq=f"{delta_t}s"
        )
        n_pre = len(pre_times)
        path_times = np.concatenate([
            pre_times.values.astype("datetime64[ns]"),
            track["time"].values.astype("datetime64[ns]"),
        ])
        flare_lat = ds.attrs.get("flare_lat", DEFAULT_FLARE_LAT)
        flare_lon = ds.attrs.get("flare_lon", DEFAULT_FLARE_LON)
        flare_alt = ds.attrs.get("flare_alt", ds.altitude.values[0])
        path_lat = np.concatenate([[flare_lat] * n_pre, track["latitude"].values])
        path_lon = np.concatenate([[flare_lon] * n_pre, track["longitude"].values])
        path_alt = np.concatenate([[flare_alt] * n_pre, track["altitude"].values])
        path_coords = {
            "time": xr.DataArray(path_times, dims="path"),
            "latitude": xr.DataArray(path_lat, dims="path"),
            "longitude": xr.DataArray(path_lon, dims="path"),
            "altitude": xr.DataArray(path_alt, dims="path"),
        }
        if mode == "extreme":
            ds_sub = ds.sel(path_coords, method="nearest")
            cell_ds = ds_sub.copy(deep=True)
            cell_ds["temperature"] = ds_sub["t"]
            cell_ds.attrs["kind"] = "extreme"
        elif mode == "vertical":
            ds_sub = ds.sel({
                "time": path_coords["time"],
                "latitude": path_coords["latitude"],
                "longitude": path_coords["longitude"],
            }, method="nearest")
            cell_ds = ds_sub.copy(deep=True)
            cell_ds["temperature"] = xr.DataArray(ds_sub["t"].values, dims=["path", "altitude"])
            cell_ds.attrs["kind"] = "vertical"
        elif mode == "integrated":
            ds_masked = xr.where(segm_mask > 0.0, ds, 0.0)
            ds_sub = ds_masked.sel(time=xr.DataArray(path_times, dims="path"), method="nearest")
            n_plume = (segm_mask > 0).sum(["altitude", "latitude", "longitude"])
            n_plume_path = n_plume.sel(
                time=xr.DataArray(path_times, dims="path"), method="nearest"
            )
            cell_ds = ds_sub.sum(["altitude", "latitude", "longitude"]) / np.maximum(
                n_plume_path.values, 1
            )
            cell_ds["temperature"] = xr.DataArray(
                ds_sub["t"].mean(["altitude", "latitude", "longitude"]).values, dims="path"
            )
            cell_ds.attrs["kind"] = "integrated"
        else:
            raise ValueError(f"Invalid mode: {mode}")
        cells[cell_id] = cell_ds
    return cells


def run_plume_path_extraction(
    ctx: RunContext,
    lv1_tracking_dir: Union[str, Path],
    output_dir: Union[str, Path],
    tracer_specs: Optional[List[Tuple[str, Tuple[int, Optional[int]]]]] = None,
    extraction_types: Tuple[str, ...] = DEFAULT_EXTRACTION_TYPES,
    overwrite: bool = False,
    chunks: Optional[Dict[str, int]] = None,
) -> List[Path]:
    """
    Load 3D (full spec), load tracks/segm per tracer, run extract_segmented_tracks_paths
    for each mode, write per-cell NetCDFs to output_dir. Returns list of written paths.
    """
    from dask.diagnostics import ProgressBar

    tracer_specs = tracer_specs or DEFAULT_TRACER_SPECS
    lv1 = Path(lv1_tracking_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    reduced = ctx.reduced_domain()
    nml = ctx.meta[ctx.flare_exp_name]["INPUT_ORG"]
    preprocess = make_3d_preprocessor(ctx.flare_nc_file, ctx.extpar_file, nml)
    ds = xr.open_mfdataset(
        ctx.flare_nc_file,
        preprocess=preprocess,
        chunks=chunks or {"time": 4},
        parallel=True,
    )
    ds = ds.sel(reduced)
    ds = update_dataset_metadata(ds)
    ds.attrs["flare_lat"] = ctx.flare_lat
    ds.attrs["flare_lon"] = ctx.flare_lon
    flare_alt_idx = getattr(ctx, "flare_alt_idx", -nml["flare_sbm"]["flare_hight"])
    ds.attrs["flare_alt"] = float(ds.altitude.values[100 - flare_alt_idx])
    ds.attrs["delta_x"] = 1e3 * np.mean(np.diff(ds.longitude.values)) * DEG_KM_LON
    ds.attrs["delta_y"] = 1e3 * np.mean(np.diff(ds.latitude.values)) * DEG_KM_LON
    ds = convert_units_3d(ds, ds["rho"])
    d_um = define_bin_boundaries() * 1e6 * 2.0
    ds = ds.assign_coords(diameter_edges=xr.DataArray(d_um, dims="diameter_edges"))

    written: List[Path] = []
    for var, _ in tracer_specs:
        tracks_file = lv1 / f"{ctx.flare_exp_name}_{var}_tobac_track.csv"
        segm_file = lv1 / f"{ctx.flare_exp_name}_{var}_tobac_mask_xarray.nc"
        if not tracks_file.exists() or not segm_file.exists():
            continue
        tracks = pd.read_csv(tracks_file).to_xarray()
        ds_segm = xr.open_dataset(segm_file).sel(reduced)
        ds_segm, ds_aligned = xr.align(
            ds_segm, ds, join="inner", exclude=["altitude", "latitude", "longitude", "diameter_edges"]
        )
        segm_mask = ds_segm["segmentation_mask"]
        for mode in extraction_types:
            cells = extract_segmented_tracks_paths(
                ds_aligned, tracks, segm_mask, mode=mode
            )
            for cell_id, cell_ds in cells.items():
                nc_path = out / f"data_{ctx.cs_run}_{ctx.flare_exp_name}_{mode}_plume_path_{var}_cell{cell_id}.nc"
                if not overwrite and nc_path.exists():
                    written.append(nc_path)
                    continue
                out_ds = cell_ds.copy(deep=True)
                if "time" in out_ds.coords:
                    out_ds["time"].attrs.pop("units", None)
                    out_ds["time"].attrs.pop("calendar", None)
                add_provenance_to_dataset(
                    out_ds,
                    stage="lv1b",
                    processing_level="LV1b",
                    title=f"Plume path {mode}",
                    cs_run=ctx.cs_run,
                    exp_label=ctx.flare_exp_name,
                    domain=ctx.domain_xy,
                    input_files=[str(tracks_file), str(segm_file)],
                )
                delayed = out_ds.to_netcdf(str(nc_path), compute=False)
                with ProgressBar():
                    delayed.compute()
                written.append(nc_path)
        ds_segm.close()
    return written
