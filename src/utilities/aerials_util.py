"""Utilities for compact aerial COSMO-SPECS notebooks."""
from __future__ import annotations

import glob
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from dask.base import compute as dask_compute
from dask.diagnostics.progress import ProgressBar
from matplotlib import colors as mcolors
from matplotlib.colors import SymLogNorm
from matplotlib.ticker import FormatStrFormatter

from utilities.compute_fabric import (
    DaskProfiler,
    allocate_resources,
    auto_chunk_dataset_priority_split,
    describe_chunk_plan,
    is_server,
)
from utilities.convert_to_video import convert_to_video
from utilities.ensemble_config_diff import create_ensemble_diff_table
from utilities.model_helpers import (
    calculate_supersaturation_ice,
    calculate_supersaturation_water,
    convert_units_3d,
    fetch_3d_data,
    harmonize_experiment_time_to_finest,
)
from utilities.namelist_metadata import update_dataset_metadata
from utilities.plotting import add_ruler, create_fade_cmap, create_new_jet3, make_pastel
from utilities.processing_paths import find_ensemble_output_for_cs_run
from utilities.style_profiles import (
    apply_publication_axis_grid,
    apply_publication_axis_tick_geometry,
    apply_publication_style,
)

xr.set_options(keep_attrs=True)

PUBLICATION_CONSTRAINED_PAD = {"w_pad": 7.5 / 72.0, "h_pad": 7.5 / 72.0, "wspace": 0.05, "hspace": 0.05}
STATS_DROP_COORDS = ("latitude2D", "longitude2D", "altitude3D")
STAT_VARS = (
    "icnc_prod_domain_sum",
    "cdnc_reduction_3x3_sum",
    "supersat_water_flare_pct",
    "supersat_water_ref_pct",
    "supersat_water_delta_pct",
    "supersat_ice_flare_pct",
    "supersat_ice_ref_pct",
    "supersat_ice_delta_pct",
    "ice_mass_produced_kg",
    "ice_mass_evaporating_kg",
    "ice_mass_lowest_level_kg",
    "cum_ice_mass_produced_kg",
    "cum_ice_mass_evaporating_kg",
    "cum_ice_mass_lowest_level_kg_s",
)

# On-disk schema version for all_flare_statistics *.nc (invalidate cache when bumped).
# 2: cdnc_reduction_3x3_sum stacked on track_var (ni/ns tobac tracks).
ALL_FLARE_STATISTICS_NC_FORMAT = 2


@dataclass(frozen=True)
class AerialsRunConfig:
    cs_run: str
    cs_runs_dir: Path | None
    flare_idx_list: tuple[int, ...]
    ref_idx_list: tuple[int, ...]
    time_debug_stride: int = 0
    altitude_slice: tuple[int | None, int | None] = (80, None)
    domain_xy: str = "50x42"
    seed_start: str = "2023-01-25T12:29:50"
    plot_window: tuple[str, str] | None = None
    plot_all_frames: bool = True
    render_video: bool = True
    rechunk_on_load: bool = True
    dask_open_chunks: dict[str, int] | None = None
    dask_open_time_chunk: int = 4
    dask_profile: bool = False
    dask_profile_dir: Path | None = None
    dask_profile_memory_interval: float = 0.5


@dataclass(frozen=True)
class AerialsPlotConfig:
    pixel_size_latlon: tuple[float, float] = (1920, 1080 / 1.75)
    lat_lon_frames_parallel: bool = True
    lat_lon_worker_cap_50x40: int = 4
    lat_lon_worker_cap_default: int = 2
    lat_lon_draw_contours: bool = False
    lat_lon_draw_contour_labels: bool = False
    lat_lon_savefig_tight: bool = False
    lat_lon_profile_frames: bool = True
    dask_profile: bool = False
    dask_profile_dir: Path | None = None
    dask_profile_memory_interval: float = 0.5
    flare_lat: float = 47.07425
    flare_lon: float = 7.90522
    origin_lat: float = 47.070522
    origin_lon: float = 7.872991
    plot_xlim: tuple[float, float] = (7.773, 7.95)
    plot_ylim: tuple[float, float] = (47.02, 47.1)
    v_lims_anom_nf: tuple[float, float] = (-1e5, 1e5)
    v_lims_anom_nw: tuple[float, float] = (-1e5, 1e5)
    anom_symlog_linthresh_nf: float = 100.0
    anom_symlog_linthresh_nw: float = 100.0
    plan_anom_cmap: str = "RdBu_r"
    plan_anom_cmap_n_nf: int = 8
    plan_anom_cmap_n_nw: int = 8
    plan_anom_cmap_n_extra: int = 3
    nf_anom_contour_nw: tuple[float, ...] = (100, 10000)
    gaussian_sigma: float = 3.0
    poolsize: int = field(default_factory=lambda: min(4, os.cpu_count() or 4))


@dataclass(frozen=True)
class AerialsContext:
    repo_root: Path
    model_data_path: Path
    extpar_file: Path
    gfx_png: Path
    gfx_mp4: Path
    cs_run: str
    domain_xy: str
    seed_start: np.datetime64
    plot_window: tuple[str, str] | None
    flist_3d: tuple[Path, ...]
    meta: dict[str, Any]
    pair_rows: tuple[tuple[str, str, Any, Any, Any], ...]
    flare_candidates: tuple[str, ...]
    ref_candidates: tuple[str, ...]
    load_candidates: tuple[str, ...]

    @property
    def diff_table(self) -> pd.DataFrame | None:
        return create_ensemble_diff_table(self.meta, self.load_candidates, pair_rows=self.pair_rows)


@dataclass(frozen=True)
class AerialsData:
    ds_3d: xr.Dataset
    stats_3d: xr.Dataset
    chunk_plan: dict[str, int]


@dataclass(frozen=True)
class PlanViewProducts:
    cfg: dict[str, Any]
    ds_time: np.ndarray
    nf_by_exp: dict[str, xr.DataArray]
    nw_by_exp: dict[str, xr.DataArray]
    plan_lon_edges: np.ndarray
    plan_lat_edges: np.ndarray
    plan_lon: np.ndarray
    plan_lat: np.ndarray
    extpar_lon_edges: np.ndarray
    extpar_lat_edges: np.ndarray
    height: np.ndarray


def find_repo_root(start: Path | None = None) -> Path:
    start = (start or Path.cwd()).resolve()
    for candidate in (start, *start.parents):
        if (candidate / "src" / "utilities" / "model_helpers.py").is_file():
            return candidate
    raise FileNotFoundError("Run inside polarcap_analysis repository.")


def _unique(seq):
    return tuple(dict.fromkeys(seq))


def _dask_profile_dir(ctx: AerialsContext, configured: Path | None) -> Path:
    return configured or (ctx.gfx_png / "dask_profile")


def _aerials_profiler(
    ctx: AerialsContext,
    label: str,
    *,
    enabled: bool,
    output_dir: Path | None,
    memory_interval: float,
) -> DaskProfiler:
    return DaskProfiler(
        label=label,
        output_dir=_dask_profile_dir(ctx, output_dir),
        enabled=enabled,
        memory_interval=memory_interval,
    )


def _aerials_open_chunks(config: AerialsRunConfig) -> dict[str, int]:
    if config.dask_open_chunks is not None:
        return dict(config.dask_open_chunks)
    return {"time": max(1, int(config.dask_open_time_chunk))}


def _exp_meta(meta: dict[str, Any], exp_name: str, *keys: str) -> Any:
    obj: Any = meta[exp_name]
    for key in keys:
        obj = obj[key]
    return obj


def _pick_extpar(ens_out: Path, domain_xy: str) -> Path:
    runs_root = ens_out.parent
    name = "extPar_Eriswil_200x160.nc" if domain_xy == "200x160" else "extPar_Eriswil_50x40.nc"
    return runs_root.parent / name


def prepare_aerials_context(config: AerialsRunConfig) -> AerialsContext:
    repo_root = find_repo_root()
    ens_out, tried = find_ensemble_output_for_cs_run(
        config.cs_run,
        config_runs_root=str(config.cs_runs_dir) if config.cs_runs_dir else None,
    )
    if not ens_out:
        raise FileNotFoundError(
            "Could not find ensemble_output/"
            + config.cs_run
            + ". Tried:\n  "
            + "\n  ".join(tried)
        )

    model_data_path = Path(ens_out) / config.cs_run
    flist_3d = tuple(sorted(model_data_path.glob("3D_??????????????.nc")))
    if not flist_3d:
        raise FileNotFoundError(f"No 3D_*.nc under {model_data_path}")

    json_files = sorted(model_data_path.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No ensemble json next to 3D files in {model_data_path}")
    meta = json.loads(json_files[0].read_text())

    exp_names = [path.name.split("_")[-1].split(".")[0] for path in flist_3d]
    flares = [e for e in exp_names if _exp_meta(meta, e, "INPUT_ORG", "sbm_par")["lflare"]]
    refs = [e for e in exp_names if not _exp_meta(meta, e, "INPUT_ORG", "sbm_par")["lflare"]]
    if len(config.flare_idx_list) != len(config.ref_idx_list):
        raise ValueError("flare_idx_list and ref_idx_list must have same length.")

    rows: list[tuple[str, str, Any, Any, Any]] = []
    for j, (i_flare, i_ref) in enumerate(zip(config.flare_idx_list, config.ref_idx_list, strict=True)):
        if not (0 <= i_flare < len(flares)):
            raise IndexError(f"flare_idx_list[{j}]={i_flare} invalid for {len(flares)} flares.")
        if not (0 <= i_ref < len(refs)):
            raise IndexError(f"ref_idx_list[{j}]={i_ref} invalid for {len(refs)} refs.")
        fexp, rexp = flares[i_flare], refs[i_ref]
        rows.append(
            (
                fexp,
                rexp,
                _exp_meta(meta, fexp, "INPUT_ORG", "sbm_par").get("ishape"),
                _exp_meta(meta, fexp, "INPUT_ORG", "flare_sbm").get("flare_emission"),
                _exp_meta(meta, fexp, "INPUT_ORG", "flare_sbm").get("flare_dn"),
            )
        )
    if not rows:
        raise ValueError("No flare/reference pairs built from config.")

    flare_candidates = _unique(row[0] for row in rows)
    ref_candidates = _unique(row[1] for row in rows)
    load_candidates = _unique((*flare_candidates, *ref_candidates))
    gfx_png = repo_root / "output" / "gfx" / "png" / "aerials" / config.cs_run.replace("/", "_")
    gfx_mp4 = repo_root / "output" / "gfx" / "mp4"
    gfx_png.mkdir(parents=True, exist_ok=True)
    gfx_mp4.mkdir(parents=True, exist_ok=True)

    ctx = AerialsContext(
        repo_root=repo_root,
        model_data_path=model_data_path,
        extpar_file=_pick_extpar(Path(ens_out), config.domain_xy),
        gfx_png=gfx_png,
        gfx_mp4=gfx_mp4,
        cs_run=config.cs_run,
        domain_xy=config.domain_xy,
        seed_start=np.datetime64(config.seed_start),
        plot_window=config.plot_window,
        flist_3d=flist_3d,
        meta=meta,
        pair_rows=tuple(rows),
        flare_candidates=flare_candidates,
        ref_candidates=ref_candidates,
        load_candidates=load_candidates,
    )
    print("cs_run:", ctx.cs_run, "domain:", ctx.domain_xy)
    print("model_data_path:", ctx.model_data_path)
    print("extpar_file:", ctx.extpar_file)
    if ctx.plot_window:
        print("plot window:", *ctx.plot_window)
    return ctx


def _maybe_allocate_dask(ctx: AerialsContext) -> None:
    if os.environ.get("SLURM_ARRAY_TASK_ID"):
        print("Detected SLURM array execution; skipping notebook-side Dask allocation.")
        return
    if "200x160" not in ctx.domain_xy and "50x4" not in ctx.domain_xy:
        return
    for name in ("dask_client", "dask_cluster"):
        obj = globals().get(name)
        if obj is not None:
            try:
                obj.close()
            except Exception:
                pass
            globals()[name] = None

    try:
        import psutil

        ram_gb = max(8, int(psutil.virtual_memory().total / (1024**3)))
    except Exception:
        ram_gb = 64
    nworker_min = 8
    frac = 0.33 if is_server() else 0.32
    member_scale = min(1.0, max(0.5, len(ctx.load_candidates) / 6))
    mem_cluster_gb = float(max(32, min(192, int(ram_gb * frac * member_scale))))
    n_cpu = os.cpu_count() or nworker_min
    n_workers = max(2, min(nworker_min, len(ctx.load_candidates), max(1, n_cpu // nworker_min)))
    cluster, client = allocate_resources(
        n_cpu=n_workers,
        n_jobs=1,
        m=int(round(mem_cluster_gb)),
        n_threads_per_process=1,
        port="8787",
    )
    globals()["dask_cluster"] = cluster
    globals()["dask_client"] = client
    print(f"Dask: {n_workers} workers for {len(ctx.load_candidates)} loaded members, ~{mem_cluster_gb:.0f} GB.")


def load_aerials_dataset(ctx: AerialsContext, config: AerialsRunConfig) -> AerialsData:
    with _aerials_profiler(
        ctx,
        "load_aerials_dataset",
        enabled=config.dask_profile,
        output_dir=config.dask_profile_dir,
        memory_interval=config.dask_profile_memory_interval,
    ) as prof:
        with prof.phase("allocate_dask"):
            _maybe_allocate_dask(ctx)

        with prof.phase("open_prepare_members"):
            nc_by_exp = {p.name.split("_")[-1].split(".")[0]: p for p in ctx.flist_3d}
            open_chunks = _aerials_open_chunks(config)
            print(f"3D open chunks: {open_chunks}")
            blocks, dtimes = [], []
            for exp_name in ctx.load_candidates:
                ds_i = fetch_3d_data(
                    str(nc_by_exp[exp_name]),
                    str(ctx.extpar_file),
                    ctx.meta[exp_name]["INPUT_ORG"],
                    var_sets=["meteo", "bulk", "spec"],
                    chunks=open_chunks,
                )
                ds_i = update_dataset_metadata(ds_i)
                alt = ds_i.altitude.values
                print(f"{exp_name}: altitude slice {config.altitude_slice} spans {alt[slice(*config.altitude_slice)][0]:.2f}..{alt[slice(*config.altitude_slice)][-1]:.2f} m")
                ds_i = ds_i.isel(altitude=slice(*config.altitude_slice))
                ds_i = convert_units_3d(ds_i, ds_i["rho"])
                dtimes.append(ds_i.time.values[1] - ds_i.time.values[0])
                blocks.append(ds_i)

        with prof.phase("concat_rechunk"):
            print(
                "OUTPUT cadence:",
                {e: ctx.meta[e]["INPUT_ORG"]["sbm_par"].get("nc_output_hcomb") for e in ctx.load_candidates},
            )
            ds_3d = cast(xr.Dataset, xr.concat(blocks, dim=pd.Index(ctx.load_candidates, name="expname")))
            if config.rechunk_on_load:
                ds_3d, chunk_plan = auto_chunk_dataset_priority_split(
                    ds_3d,
                    min_chunk_mb=32,
                    max_chunk_mb=256,
                    memory_fraction=0.08,
                    split_dims=("expname", "time"),
                )
                print("ds_3d chunk plan:", describe_chunk_plan(ds_3d, chunk_plan))
            else:
                chunk_plan = {}
                print("ds_3d rechunk skipped (config.rechunk_on_load=False)")

        with prof.phase("time_stride"):
            if config.time_debug_stride > 1:
                ds_3d = ds_3d.reindex(time=blocks[int(np.argmax(dtimes))].time)
                ds_3d = ds_3d.isel(time=slice(None, None, config.time_debug_stride))

        return AerialsData(
            ds_3d=ds_3d,
            stats_3d=ds_3d.drop_vars(STATS_DROP_COORDS, errors="ignore"),
            chunk_plan=chunk_plan,
        )


def _curvilinear_cell_edges(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.shape != y.shape or x.ndim != 2:
        raise ValueError("x and y must be same-shape 2D cell-center coordinates.")

    def _edge_from_centers(a):
        ny, nx = a.shape
        ap = np.empty((ny + 2, nx + 2), dtype=float)
        ap[1:-1, 1:-1] = a
        ap[0, 1:-1] = 2.0 * a[0, :] - a[1, :]
        ap[-1, 1:-1] = 2.0 * a[-1, :] - a[-2, :]
        ap[1:-1, 0] = 2.0 * a[:, 0] - a[:, 1]
        ap[1:-1, -1] = 2.0 * a[:, -1] - a[:, -2]
        ap[0, 0] = 2.0 * ap[1, 0] - ap[2, 0]
        ap[0, -1] = 2.0 * ap[1, -1] - ap[2, -1]
        ap[-1, 0] = 2.0 * ap[-2, 0] - ap[-3, 0]
        ap[-1, -1] = 2.0 * ap[-2, -1] - ap[-3, -1]
        return 0.25 * (ap[:-1, :-1] + ap[1:, :-1] + ap[:-1, 1:] + ap[1:, 1:])

    return _edge_from_centers(x), _edge_from_centers(y)


def _plot_cfg(ds_3d: xr.Dataset, ctx: AerialsContext, plot: AerialsPlotConfig) -> dict[str, Any]:
    apply_publication_style()
    ds_lon = ds_3d.longitude
    ds_lat = ds_3d.latitude
    ds_time = ds_3d.time
    return {
        "cs_run": ctx.cs_run,
        "resolution": "400m" if "50x4" in ctx.domain_xy else "100m",
        "resolution_deg": 0.004 if "50x4" in ctx.domain_xy else 0.001,
        "dpi": int(float(plt.rcParams["savefig.dpi"])),
        "pixel_size_latlon": plot.pixel_size_latlon,
        "poolsize": plot.poolsize,
        "lat_lon_frames_parallel": plot.lat_lon_frames_parallel,
        "lat_lon_worker_cap_50x40": plot.lat_lon_worker_cap_50x40,
        "lat_lon_worker_cap_default": plot.lat_lon_worker_cap_default,
        "lat_lon_draw_contours": plot.lat_lon_draw_contours,
        "lat_lon_draw_contour_labels": plot.lat_lon_draw_contour_labels,
        "lat_lon_savefig_tight": plot.lat_lon_savefig_tight,
        "lat_lon_profile_frames": plot.lat_lon_profile_frames,
        "flare_lat": plot.flare_lat,
        "flare_lon": plot.flare_lon,
        "origin_lat": plot.origin_lat,
        "origin_lon": plot.origin_lon,
        "plot_xlim": plot.plot_xlim,
        "plot_ylim": plot.plot_ylim,
        "delta_x": float(1e3 * ds_lon.diff("longitude").mean().item() * 111.13295254925466),
        "delta_y": float(1e3 * ds_lat.diff("latitude").mean().item() * 111.13295254925466),
        "delta_t": float(ds_time.diff("time").mean().astype(float).item()),
        "n_lon": ds_3d.sizes["longitude"],
        "n_lat": ds_3d.sizes["latitude"],
        "n_time": ds_3d.sizes["time"],
        "v_lims_anom_nf": plot.v_lims_anom_nf,
        "v_lims_anom_nw": plot.v_lims_anom_nw,
        "anom_symlog_linthresh_nf": plot.anom_symlog_linthresh_nf,
        "anom_symlog_linthresh_nw": plot.anom_symlog_linthresh_nw,
        "plan_anom_cmap": plot.plan_anom_cmap,
        "plan_anom_cmap_n_nf": plot.plan_anom_cmap_n_nf,
        "plan_anom_cmap_n_nw": plot.plan_anom_cmap_n_nw,
        "plan_anom_cmap_n_extra": plot.plan_anom_cmap_n_extra,
        "nf_anom_contour_nw": plot.nf_anom_contour_nw,
        "gaussian_sigma": plot.gaussian_sigma,
        "tick_size": int(plt.rcParams["xtick.labelsize"]),
        "axis_size": int(plt.rcParams["axes.labelsize"]),
        "timer_size": int(plt.rcParams["figure.titlesize"]) + 2,
    }


def build_plan_view_products(
    ctx: AerialsContext,
    ds_3d: xr.Dataset,
    plot: AerialsPlotConfig | None = None,
    *,
    compute_plan_arrays: bool = True,
) -> PlanViewProducts:
    """Terrain overlay + plot cfg; set compute_plan_arrays=False to skip integrated Δnf/Δnw (video only)."""
    plot = plot or AerialsPlotConfig()
    with _aerials_profiler(
        ctx,
        "build_plan_view_products",
        enabled=plot.dask_profile,
        output_dir=plot.dask_profile_dir,
        memory_interval=plot.dask_profile_memory_interval,
    ) as prof:
        with prof.phase("load_extpar"):
            cfg = _plot_cfg(ds_3d, ctx, plot)
            extpar = xr.open_mfdataset(str(ctx.extpar_file), chunks="auto")
            lat2d = extpar["lat"].isel(rlat=slice(7, -7), rlon=slice(7, -7)).load().values
            lon2d = extpar["lon"].isel(rlat=slice(7, -7), rlon=slice(7, -7)).load().values
            height = extpar["HSURF"].isel(rlat=slice(7, -7), rlon=slice(7, -7)).load().values
            ext_lon_edges, ext_lat_edges = _curvilinear_cell_edges(lon2d, lat2d)

        if not compute_plan_arrays:
            empty2d = np.empty((0, 0), dtype=np.float64)
            print("build_plan_view_products: skipped harmonized Δnf/Δnw (facette+stats path only).")
            return PlanViewProducts(
                cfg=cfg,
                ds_time=ds_3d.time.values,
                nf_by_exp={},
                nw_by_exp={},
                plan_lon_edges=empty2d,
                plan_lat_edges=empty2d,
                plan_lon=empty2d,
                plan_lat=empty2d,
                extpar_lon_edges=ext_lon_edges,
                extpar_lat_edges=ext_lat_edges,
                height=height,
            )

        with prof.phase("build_plan_graph"):
            blocks, labels = [], []
            for fexp, rexp, *_ in ctx.pair_rows:
                blocks.append(ds_3d[["nf", "nw", "dz"]].sel(expname=fexp))
                labels.append(f"{fexp}__flare")
                blocks.append(ds_3d[["nf", "nw", "dz"]].sel(expname=rexp))
                labels.append(f"{rexp}__ref")
            h_cubes = harmonize_experiment_time_to_finest(blocks, exp_names=labels)

            plan_nf_by_exp, plan_nw_by_exp = {}, {}
            for j, (fexp, _, *_) in enumerate(ctx.pair_rows):
                flare = h_cubes[2 * j]
                ref = h_cubes[2 * j + 1]
                delta = xr.Dataset({"nf": flare["nf"] - ref["nf"], 
                                    "nw": flare["nw"] - ref["nw"], 
                                    "dz": flare["dz"]})
                integ = (delta[["nf", "nw"]].isel(diameter=slice(30, None)).sum("diameter") * delta["dz"]).sum("altitude")
                plan_nf_by_exp[fexp] = integ["nf"]
                plan_nw_by_exp[fexp] = integ["nw"]

            keys, arrays = [], []
            for exp in ctx.flare_candidates:
                keys.extend([(exp, "nf"), (exp, "nw")])
                arrays.extend([plan_nf_by_exp[exp], plan_nw_by_exp[exp]])
            print(f"Plan-view reduced arrays queued: {len(arrays)}, approx final size={sum(a.nbytes for a in arrays) / 2**20:.1f} MiB")

        with prof.phase("compute_plan_arrays"):
            with ProgressBar():
                loaded = dask_compute(*arrays)

        with prof.phase("assemble_plan_products"):
            nf_loaded, nw_loaded = {}, {}
            for (exp, field), da in zip(keys, loaded, strict=True):
                print(f"loaded {exp} {field}: shape={da.shape}, approx={da.nbytes / 2**20:.1f} MiB")
                (nf_loaded if field == "nf" else nw_loaded)[exp] = da

            plan_lon = ds_3d["longitude2D"].load().values
            plan_lat = ds_3d["latitude2D"].load().values
            plan_lon_edges, plan_lat_edges = _curvilinear_cell_edges(plan_lon, plan_lat)
            return PlanViewProducts(
                cfg=cfg,
                ds_time=ds_3d.time.values,
                nf_by_exp=nf_loaded,
                nw_by_exp=nw_loaded,
                plan_lon_edges=plan_lon_edges,
                plan_lat_edges=plan_lat_edges,
                plan_lon=plan_lon,
                plan_lat=plan_lat,
                extpar_lon_edges=ext_lon_edges,
                extpar_lat_edges=ext_lat_edges,
                height=height,
            )


def _plan_anom_cbar_tick_mpl(x: float, cfg: dict[str, Any] | None = None) -> str:
    """Mathtext for exact signed powers of ten."""
    if not np.isfinite(x):
        return ""
    sgn, ax = ("-", abs(float(x))) if x < 0 else ("", float(x))
    linthresh = 10.0 if cfg is None else float(cfg.get("anom_symlog_linthresh_nf", 10.0))
    if ax <= linthresh - 1:
        return ""
    n = int(np.rint(np.log10(ax)))
    return f"${sgn}10^{{{n}}}$"


def _plan_anom_tick_min_abs(cfg: dict[str, Any] | None = None, fallback: float = 1.0) -> float:
    if cfg is not None:
        fallback = float(cfg.get("plan_anom_cbar_tick_min_abs", cfg.get("plan_anom_mid_white_abs", fallback)))
    return max(abs(float(fallback)), float(np.finfo(float).tiny))


def _plan_anom_cbar_decade_ticks(v_lo: float, v_hi: float, min_abs: float) -> list[float]:
    if not (np.isfinite(v_lo) and np.isfinite(v_hi)) or v_hi <= v_lo:
        return []
    max_abs = max(abs(v_lo), abs(v_hi))
    if max_abs <= 0:
        return []
    min_abs = min(max(abs(float(min_abs)), float(np.finfo(float).tiny)), max_abs)
    e_min = int(np.ceil(np.log10(min_abs) - 1e-12))
    e_max = int(np.floor(np.log10(max_abs) + 1e-12))
    decades = [10.0**e for e in range(e_min, e_max + 1)]
    ticks = [-d for d in reversed(decades) if v_lo <= -d <= v_hi]
    ticks.extend(d for d in decades if v_lo <= d <= v_hi)
    return ticks


def plan_anom_cbar_discrete(cbar, norm, n_discrete: int, cfg: dict[str, Any] | None = None) -> None:
    """Place exact decade ticks on color transitions."""
    if int(n_discrete) < 2:
        return
    min_abs = _plan_anom_tick_min_abs(cfg)
    ticks = _plan_anom_cbar_decade_ticks(float(norm.vmin), float(norm.vmax), min_abs)
    cbar.set_ticks(ticks)
    cbar.set_ticklabels([_plan_anom_cbar_tick_mpl(float(x), cfg) for x in ticks])
    cbar.ax.minorticks_off()


def publication_icnc_cmap():
    return create_fade_cmap(
        make_pastel(create_new_jet3(), desaturation=0.35, darken=0.80),
        n_fade=2,
    )


def add_map_annotations(ax, cfg: dict[str, Any]) -> None:
    ax.scatter(cfg["origin_lon"], cfg["origin_lat"], s=70, marker="x", color="red", zorder=2)
    ax.scatter(cfg["flare_lon"], cfg["flare_lat"], s=50, marker="o", facecolor="none", edgecolor="white", lw=2.5, zorder=2)
    ax.scatter(cfg["flare_lon"], cfg["flare_lat"], s=50, marker="o", facecolor="none", edgecolor="red", lw=1.0, zorder=2)
    add_ruler(ax, 47.05, 7.804, cfg["flare_lat"], cfg["flare_lon"])


def _read_tobac_tracks_if_exists(ctx: AerialsContext, exp_name: str) -> pd.DataFrame | None:
    track_dir = ctx.model_data_path / "lv1_tracking"
    if not any((track_dir / f"{exp_name}_{track_var}_tobac_track.csv").exists() for track_var in ("ni", "ns")):
        return None
    return _read_tobac_tracks(ctx, exp_name)


def _add_track_overlay(ax, tracks: pd.DataFrame, *, label_first_axis: bool = False) -> None:
    if "cell" not in tracks or "track_var" not in tracks:
        return
    colors = {"ni": "black", "ns": "tab:blue"}
    for key, track in tracks.groupby(["track_var", "cell"], sort=True):
        track_var, cell_id = cast(tuple[str, Any], key)
        color = colors.get(str(track_var), "0.2")
        label = f"{track_var} cell {cell_id}" if label_first_axis else None
        ax.plot(
            track["longitude"],
            track["latitude"],
            linewidth=1.2,
            alpha=0.7,
            color=color,
            zorder=2,
            label=label,
        )
        ax.scatter(
            track["longitude"].iloc[::20],
            track["latitude"].iloc[::20],
            s=10,
            marker="x",
            alpha=0.8,
            c=color,
            linewidths=0.2,
            zorder=3,
        )


def render_facette_overview(
    ctx: AerialsContext,
    ds_3d: xr.Dataset,
    products: PlanViewProducts,
    *,
    varname: str = "nf",
    diameters: tuple[int | None, int | None] = (30, None),
    time_window: tuple[str | np.datetime64, str | np.datetime64] | None = None,
    col_wrap: int = 5,
    max_panels: int | None = None,
) -> list[Path]:
    """Render 5x5 static overview facets for configured flare candidates."""
    cfg = products.cfg
    max_panels = max_panels or col_wrap * col_wrap
    time_sel = slice(*time_window) if time_window is not None else slice(None)
    n_altitude = int(ds_3d.sizes["altitude"])
    da_full = (
        ds_3d[varname]
        .isel(diameter=slice(*diameters))
        .sel(latitude=slice(*cfg["plot_ylim"]))
        .sel(longitude=slice(*cfg["plot_xlim"]))
        .sel(time=time_sel)
        .sum("diameter")
        .sum("altitude")
        / n_altitude
    )

    out_paths: list[Path] = []
    for flare_exp in ctx.flare_candidates:
        da_exp = da_full.sel(expname=flare_exp)
        n_time = int(da_exp.sizes["time"])
        if n_time <= max_panels:
            time_idx = np.arange(n_time, dtype=int)
        else:
            time_idx = np.unique(np.round(np.linspace(0, n_time - 1, num=max_panels)).astype(int))
        da_facet = da_exp.isel(time=time_idx).load()

        side_in = float(cfg["pixel_size_latlon"][0]) / float(cfg["dpi"])
        font_scale = float(cfg.get("facette_font_scale", 1.25))
        tick_fs = max(6, int(round(cfg["tick_size"] * font_scale)))
        axis_fs = max(8, int(round(cfg["axis_size"] * font_scale)))
        timer_fs = max(7, int(round((cfg["timer_size"] - 3) * font_scale)))
        cbar_tick_fs = max(7, int(round(tick_fs * 1.15)))
        cbar_label_fs = max(9, int(round(axis_fs * 1.1)))

        grid = da_facet.plot(  # pyright: ignore[reportCallIssue]
            x="longitude",
            y="latitude",
            col="time",
            col_wrap=col_wrap,
            figsize=(side_in * 1.35, side_in * 1.35),
            sharex=True,
            sharey=True,
            add_colorbar=True,
            robust=True,
            cmap=publication_icnc_cmap(),
            norm=mcolors.LogNorm(1, 1e3),
            cbar_kwargs={
                "orientation": "horizontal",
                "location": "bottom",
                "shrink": 0.9,
                "pad": 0.075,
                "aspect": 40,
                "label": r"$\frac{1}{\Delta z}\int_{z_{base}}^{z_{top}} N_f\,dz$  / (L$^{-1}$)",
            },
            zorder=4,
        )
        fig, axes = grid.fig, grid.axs

        from mpl_toolkits.axes_grid1.inset_locator import inset_axes

        terrain = None
        tracks = _read_tobac_tracks_if_exists(ctx, flare_exp)
        n_plot = int(da_facet.sizes["time"])
        legend_axis_idx = 1 if n_plot > 1 else 0
        for i, ax in enumerate(axes.flat):
            terrain = ax.pcolormesh(
                products.extpar_lon_edges,
                products.extpar_lat_edges,
                products.height,
                cmap="terrain",
                vmin=300,
                alpha=0.55,
                shading="flat",
                zorder=1,
            )
            if tracks is not None and i < n_plot:
                _add_track_overlay(ax, tracks, label_first_axis=(i == legend_axis_idx))
            apply_publication_axis_tick_geometry(ax)
            apply_publication_axis_grid(ax)
            ax.tick_params(axis="both", which="major", labelsize=tick_fs)
            ax.yaxis.set_major_formatter(FormatStrFormatter("%.2f"))

        if terrain is not None:
            cax = inset_axes(axes.flat[0], width="10%", height="33%", loc="lower right", borderpad=0.6)
            cb_terrain = fig.colorbar(terrain, cax=cax, orientation="vertical")
            cb_terrain.ax.yaxis.set_ticks_position("left")
            cb_terrain.ax.yaxis.set_label_position("left")
            cb_terrain.ax.yaxis.set_major_formatter(FormatStrFormatter("%.2f"))
            cb_terrain.ax.tick_params(
                labelsize=max(7, cbar_tick_fs - 3),
                pad=0.05,
                left=True,
                right=False,
                labelleft=True,
                labelright=False,
            )
            for tick_label in cb_terrain.ax.get_yticklabels():
                tick_label.set_bbox(
                    dict(facecolor="white", alpha=0.6, edgecolor="none", boxstyle="round,pad=0.1")
                )

        if grid.cbar is not None:
            grid.cbar.ax.tick_params(labelsize=cbar_tick_fs)
            grid.cbar.set_label(grid.cbar.ax.get_xlabel(), fontsize=cbar_label_fs)

        for i, ax in enumerate(axes.flat):
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.set_title("")
            ax.minorticks_off()
            if i >= n_plot:
                continue
            timestamp: Any = pd.Timestamp(np.asarray(da_facet.time.values[i]).item())
            hhmmss = "" if pd.isna(timestamp) else timestamp.strftime("%H:%M:%S")
            ax.text(
                0.98,
                0.98,
                hhmmss,
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontweight="semibold",
                fontsize=timer_fs,
            )
        if tracks is not None:
            axes.flat[legend_axis_idx].legend(loc="lower right")
        fig.text(0.5, 0.225, "longitude / (°)", ha="center", va="top", fontsize=axis_fs)
        fig.text(0.01, 0.5, "latitude / (°)", ha="right", va="center", fontsize=axis_fs, rotation=90)

        out = ctx.gfx_png / f"facette_{ctx.cs_run}_{ctx.domain_xy}_{flare_exp}_area.png"
        fig.savefig(out, dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(out.resolve().as_uri())
        out_paths.append(out)
    return out_paths


def _elapsed_since_seed_label(ds_time: np.ndarray, seed_start: np.datetime64, iframe: int) -> str:
    elapsed_s = int((np.datetime64(ds_time[iframe], "s") - np.datetime64(seed_start, "s")) / np.timedelta64(1, "s"))
    sign = "-" if elapsed_s < 0 else ""
    hours, rem = divmod(abs(elapsed_s), 3600)
    minutes, seconds = divmod(rem, 60)
    return f"t = {sign}{hours:02d}:{minutes:02d}:{seconds:02d} since seed"


def _write_lat_lon_frame(ctx: AerialsContext, products: PlanViewProducts, iframe: int) -> int:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    from scipy.ndimage import gaussian_filter

    cfg = products.cfg
    t_frame = time.perf_counter()
    timings = {"setup": 0.0, "data": 0.0, "mesh": 0.0, "colorbar": 0.0, "savefig": 0.0}
    n_f = len(ctx.flare_candidates)
    fig_w = cfg["pixel_size_latlon"][0] / cfg["dpi"]
    fig_h = cfg["pixel_size_latlon"][1] / cfg["dpi"] * max(1, n_f)
    fig, axes = plt.subplots(n_f, 2, figsize=(fig_w, fig_h), sharex=True, sharey=True, constrained_layout=True)
    getattr(fig, "set_constrained_layout_pads")(**PUBLICATION_CONSTRAINED_PAD)
    if n_f == 1:
        axes = np.array([axes])
    axes = np.atleast_2d(axes)
    pm_nf, pm_nw = [], []

    norm_nf = SymLogNorm(
        linthresh=float(cfg["anom_symlog_linthresh_nf"]),
        linscale=0.5,
        vmin=float(cfg["v_lims_anom_nf"][0]),
        vmax=float(cfg["v_lims_anom_nf"][1]),
        base=10,
    )
    norm_nw = SymLogNorm(
        linthresh=float(cfg["anom_symlog_linthresh_nw"]),
        linscale=0.5,
        vmin=float(cfg["v_lims_anom_nw"][0]),
        vmax=float(cfg["v_lims_anom_nw"][1]),
        base=10,
    )

    for axs, exp in zip(axes, ctx.flare_candidates, strict=True):
        t0 = time.perf_counter()
        for ax in axs:
            add_map_annotations(ax, cfg)
            ax.pcolormesh(
                products.extpar_lon_edges,
                products.extpar_lat_edges,
                products.height,
                cmap="terrain",
                vmin=300,
                alpha=0.55,
                shading="flat",
                zorder=1,
            )
            apply_publication_axis_tick_geometry(ax)
            apply_publication_axis_grid(ax)
            ax.xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
            ax.yaxis.set_major_formatter(FormatStrFormatter("%.2f"))
            ax.tick_params(axis="both", which="major", labelsize=cfg["tick_size"])
        timings["setup"] += time.perf_counter() - t0

        t0 = time.perf_counter()
        nf_plot = gaussian_filter(np.asarray(products.nf_by_exp[exp].isel(time=iframe), dtype=float), sigma=cfg["gaussian_sigma"])
        nw_plot = gaussian_filter(np.asarray(products.nw_by_exp[exp].isel(time=iframe), dtype=float), sigma=cfg["gaussian_sigma"])
        timings["data"] += time.perf_counter() - t0

        t0 = time.perf_counter()
        pm_nf.append(
            axs[0].pcolormesh(products.plan_lon_edges, products.plan_lat_edges, nf_plot, cmap=cfg["plan_anom_cmap"], norm=norm_nf, shading="flat", zorder=50)
        )
        pm_nw.append(
            axs[1].pcolormesh(products.plan_lon_edges, products.plan_lat_edges, nw_plot, cmap=cfg["plan_anom_cmap"], norm=norm_nw, shading="flat", zorder=50)
        )
        timings["mesh"] += time.perf_counter() - t0

        for ax in axs:
            ax.set_xlim(*cfg["plot_xlim"])
            ax.set_ylim(*cfg["plot_ylim"])
            for side in ("top", "right", "bottom", "left"):
                ax.spines[side].set_visible(False)

    t0 = time.perf_counter()
    fig.text(0.5, 0.0, "longitude / (°)", ha="center", va="top", fontsize=cfg["axis_size"])
    fig.text(0.0, 0.5, "latitude / (°)", ha="right", va="center", fontsize=cfg["axis_size"], rotation=90)
    cb_nf = fig.colorbar(pm_nf[0], ax=axes[0, 0], extend="both", shrink=0.9, aspect=30, pad=0.075, orientation="horizontal", location="top")
    cb_nw = fig.colorbar(pm_nw[0], ax=axes[0, 1], extend="both", shrink=0.9, aspect=30, pad=0.075, orientation="horizontal", location="top")
    plan_anom_cbar_discrete(cb_nf, norm_nf, int(cfg["plan_anom_cmap_n_nf"]) + int(cfg["plan_anom_cmap_n_extra"]), cfg)
    plan_anom_cbar_discrete(cb_nw, norm_nw, int(cfg["plan_anom_cmap_n_nw"]) + int(cfg["plan_anom_cmap_n_extra"]), cfg)
    cb_nf.set_label(r"$\frac{1}{\Delta z}\int_{z_{base}}^{z_{top}} N\,dz$", fontsize=cfg["axis_size"])
    cb_nw.set_label(r"$\frac{1}{\Delta z}\int_{z_{base}}^{z_{top}} N\,dz$", fontsize=cfg["axis_size"])
    cb_nf.ax.tick_params(labelsize=cfg["tick_size"] - 1, pad=0.05)
    cb_nw.ax.tick_params(labelsize=cfg["tick_size"] - 1, pad=0.05)
    timings["colorbar"] += time.perf_counter() - t0

    fig.text(
        0.985,
        0.0,
        _elapsed_since_seed_label(products.ds_time, ctx.seed_start, iframe),
        ha="right",
        va="bottom",
        fontweight="semibold",
        fontsize=max(6, cfg["timer_size"] - 4),
    )
    experiments = "_".join(ctx.flare_candidates)
    out = ctx.gfx_png / f"lat_lon_frame_{iframe:03d}_{ctx.domain_xy}_{experiments}_area.png"
    savefig_kwargs = {"dpi": cfg["dpi"]}
    if bool(cfg.get("lat_lon_savefig_tight", True)):
        savefig_kwargs["bbox_inches"] = "tight"
    t0 = time.perf_counter()
    fig.savefig(out, **savefig_kwargs)
    timings["savefig"] += time.perf_counter() - t0
    plt.close(fig)

    profile = cfg.get("lat_lon_profile_frames", False)
    if (isinstance(profile, bool) and profile and iframe == 0) or (isinstance(profile, int) and profile > 0 and iframe % profile == 0):
        parts = ", ".join(f"{name}={value:.2f}s" for name, value in timings.items())
        print(f"frame {iframe:03d}: total={time.perf_counter() - t_frame:.2f}s ({parts})")
    return iframe


def render_plan_view_animation(
    ctx: AerialsContext,
    products: PlanViewProducts,
    *,
    plot_all_frames: bool = True,
    render_video: bool = True,
) -> Path | None:
    for path in ctx.gfx_png.glob("lat_lon_frame_*.png"):
        path.unlink()
    print("Cleaned:", ctx.gfx_png)

    n_time = int(products.cfg["n_time"])
    if plot_all_frames:
        missing = [
            e
            for e in ctx.flare_candidates
            if e not in products.nf_by_exp or e not in products.nw_by_exp
        ]
        if missing:
            raise ValueError(
                "lat-lon frames require integrated Δnf/Δnw per flare exp; missing "
                f"{missing}. Use build_plan_view_products(..., compute_plan_arrays=True)."
            )
        n_workers = min(int(products.cfg.get("poolsize", 8)), n_time, int(os.cpu_count() or 8))
        worker_cap_key = "lat_lon_worker_cap_50x40" if ctx.domain_xy == "50x40" else "lat_lon_worker_cap_default"
        worker_cap = int(products.cfg.get(worker_cap_key, 2))
        n_workers = max(1, min(n_workers, worker_cap))
        use_pool = (
            bool(products.cfg.get("lat_lon_frames_parallel", True))
            and sys.platform == "linux"
            and n_workers > 1
            and n_time > 1
        )
        print(f"lat-lon frames: workers={n_workers}, use_pool={use_pool}, worker_cap={worker_cap_key}:{worker_cap}")
        if use_pool:
            from concurrent.futures import ProcessPoolExecutor
            from itertools import repeat

            chunksize = max(1, n_time // max(1, 8 * n_workers))
            with ProcessPoolExecutor(max_workers=n_workers) as ex:
                list(ex.map(_write_lat_lon_frame, repeat(ctx), repeat(products), range(n_time), chunksize=chunksize))
        else:
            for iframe in range(n_time):
                _write_lat_lon_frame(ctx, products, iframe)
        print("All multi-column plan-view frames saved")

    if not render_video:
        return None
    experiments = "_".join(ctx.flare_candidates)
    input_pattern = ctx.gfx_png / f"lat_lon_frame_%03d_{ctx.domain_xy}_{experiments}_area.png"
    out_mp4 = ctx.gfx_mp4 / f"lat_lon_multi_flare_{ctx.domain_xy}_{ctx.cs_run.replace('__', '_')}_{experiments}_.mp4"
    convert_to_video(str(input_pattern), str(out_mp4), resolution="1920:1080", loop_count=2, framerate=15)
    print(out_mp4.resolve().as_uri())
    return out_mp4


def _read_tobac_tracks(ctx: AerialsContext, exp_name: str, track_vars: tuple[str, ...] = ("ni", "ns")) -> pd.DataFrame:
    track_dir = ctx.model_data_path / "lv1_tracking"
    parts = []
    missing = []
    for track_var in track_vars:
        track_file = track_dir / f"{exp_name}_{track_var}_tobac_track.csv"
        if not track_file.exists():
            missing.append(track_file)
            continue
        df = pd.read_csv(track_file).dropna(subset=["frame", "hdim_1", "hdim_2"]).copy()
        df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
        df[["frame", "hdim_1", "hdim_2"]] = df[["frame", "hdim_1", "hdim_2"]].astype(int)
        df["time"] = pd.to_datetime(df["time"])
        df["expname"] = exp_name
        df["track_var"] = track_var
        df["source_path"] = str(track_file)
        if {"cell", "threshold_value"}.issubset(df.columns):
            df = (
                df.sort_values("threshold_value", ascending=False)
                .drop_duplicates(subset=["track_var", "cell", "time"], keep="first")
                .reset_index(drop=True)
            )
        parts.append(df)

    if not parts:
        missing_txt = "\n  ".join(str(p) for p in missing)
        raise FileNotFoundError(f"Missing tobac track files for {exp_name}; tried:\n  {missing_txt}")
    return pd.concat(parts, ignore_index=True)


def _cdnc_track_reduction_series(cdnc_roll: xr.DataArray, tracks_df: pd.DataFrame, time_coord: xr.DataArray) -> xr.DataArray:
    n_time = int(time_coord.sizes["time"])
    need = ["time", "hdim_1", "hdim_2"]
    points = tracks_df.dropna(subset=need).loc[:, need].copy()
    if points.empty:
        return xr.DataArray(np.full(n_time, np.nan), dims=("time",), coords={"time": time_coord}, name="cdnc_reduction_3x3_sum")

    model_times = pd.DatetimeIndex(pd.to_datetime(time_coord.values))
    track_times = pd.to_datetime(points["time"])
    in_range = track_times.between(model_times.min(), model_times.max())
    points = points.loc[in_range].copy()
    track_times = track_times.loc[in_range]
    if points.empty:
        return xr.DataArray(np.full(n_time, np.nan), dims=("time",), coords={"time": time_coord}, name="cdnc_reduction_3x3_sum")

    frame_points = model_times.get_indexer(track_times, method="nearest")
    keep = frame_points >= 0
    points = points.iloc[np.flatnonzero(keep)]
    frame_points = frame_points[keep]
    if len(frame_points) == 0:
        return xr.DataArray(np.full(n_time, np.nan), dims=("time",), coords={"time": time_coord}, name="cdnc_reduction_3x3_sum")

    values = cdnc_roll.isel(
        time=xr.DataArray(frame_points, dims="track_point"),
        latitude=xr.DataArray(points["hdim_1"].to_numpy(dtype=int), dims="track_point"),
        longitude=xr.DataArray(points["hdim_2"].to_numpy(dtype=int), dims="track_point"),
    )
    by_frame = values.reset_coords(drop=True).assign_coords(frame=("track_point", frame_points)).groupby("frame").sum(skipna=False)
    return by_frame.reindex(frame=np.arange(n_time)).rename({"frame": "time"}).assign_coords(time=time_coord).rename("cdnc_reduction_3x3_sum")


def _drop_scalar_expname_coord(da: xr.DataArray) -> xr.DataArray:
    if "expname" in da.coords and "expname" not in da.dims:
        return da.reset_coords("expname", drop=True)
    return da


def _pair_stats_tasks(ctx: AerialsContext, stats_3d: xr.Dataset, fexp: str, rexp: str, cell_area: float):
    tracks_df = _read_tobac_tracks(ctx, fexp)
    flare = stats_3d.sel(expname=fexp)
    ref = stats_3d.sel(expname=rexp)
    nf_delta_col = (flare["nf"] - ref["nf"]).isel(diameter=slice(30, None)).sum("diameter").sum("altitude")
    nw_delta_col = (flare["nw"] - ref["nw"]).isel(diameter=slice(30, None)).sum("diameter").sum("altitude")
    icnc_prod = nf_delta_col.sum(("latitude", "longitude")).rename("icnc_prod_domain_sum")
    cdnc_roll = (-nw_delta_col).clip(min=0).rolling(latitude=3, longitude=3, center=True).sum()
    cdnc_by_track_var = []
    track_vars = []
    for track_var, group in tracks_df.groupby("track_var", sort=True):
        cdnc_by_track_var.append(_cdnc_track_reduction_series(cdnc_roll, group, nw_delta_col["time"]))
        track_vars.append(str(track_var))
    cdnc_reduction = xr.concat(cdnc_by_track_var, dim=pd.Index(track_vars, name="track_var"))

    qv_units = str(flare["qv"].attrs.get("units", "")).lower()
    qv_factor = 1e-3 if qv_units in {"gm-3", "g/m3", "g m-3"} else 1.0
    sw_flare = calculate_supersaturation_water(flare["t"], flare["qv"] * qv_factor)
    sw_ref = calculate_supersaturation_water(ref["t"], ref["qv"] * qv_factor)
    si_flare = calculate_supersaturation_ice(flare["t"], flare["qv"] * qv_factor)
    si_ref = calculate_supersaturation_ice(ref["t"], ref["qv"] * qv_factor)
    supersat = xr.Dataset(
        {
            "supersat_water_flare_pct": _drop_scalar_expname_coord(sw_flare.mean(("altitude", "latitude", "longitude"))),
            "supersat_water_ref_pct": _drop_scalar_expname_coord(sw_ref.mean(("altitude", "latitude", "longitude"))),
            "supersat_water_delta_pct": _drop_scalar_expname_coord((sw_flare - sw_ref).mean(("altitude", "latitude", "longitude"))),
            "supersat_ice_flare_pct": _drop_scalar_expname_coord(si_flare.mean(("altitude", "latitude", "longitude"))),
            "supersat_ice_ref_pct": _drop_scalar_expname_coord(si_ref.mean(("altitude", "latitude", "longitude"))),
            "supersat_ice_delta_pct": _drop_scalar_expname_coord((si_flare - si_ref).mean(("altitude", "latitude", "longitude"))),
        }
    )
    qf_delta = (flare["qf"] - ref["qf"]).isel(diameter=slice(30, None)).sum("diameter")
    ice_budget = xr.Dataset(
        {
            "ice_mass_total_kg": (qf_delta * flare["dz"] * cell_area).sum(("altitude", "latitude", "longitude")),
            "ice_mass_lowest_level_kg": (qf_delta.isel(altitude=0) * flare["dz"].isel(altitude=0) * cell_area).sum(("latitude", "longitude")),
        }
    )
    return (icnc_prod, cdnc_reduction, supersat, ice_budget), len(tracks_df)


def _finish_ice_budget(ice_budget: xr.Dataset) -> xr.Dataset:
    total = ice_budget["ice_mass_total_kg"]
    time_index = pd.to_datetime(total.time.values)
    total_s = pd.Series(np.asarray(total, dtype=float), index=time_index)
    dt_s = float(np.median(np.diff(total.time.values.astype("datetime64[s]")).astype(float)))
    dmass = total_s.diff().fillna(0.0)
    lowest = pd.Series(np.asarray(ice_budget["ice_mass_lowest_level_kg"], dtype=float), index=time_index).clip(lower=0.0)
    step = pd.DataFrame(
        {
            "ice_mass_produced_kg": dmass.clip(lower=0.0).values,
            "ice_mass_evaporating_kg": (-dmass).clip(lower=0.0).values,
            "ice_mass_lowest_level_kg": lowest.values,
        },
        index=time_index,
    )
    cum = pd.DataFrame(
        {
            "cum_ice_mass_produced_kg": step["ice_mass_produced_kg"].cumsum(),
            "cum_ice_mass_evaporating_kg": step["ice_mass_evaporating_kg"].cumsum(),
            "cum_ice_mass_lowest_level_kg_s": (step["ice_mass_lowest_level_kg"] * dt_s).cumsum(),
        },
        index=time_index,
    )
    out = pd.concat([step, cum], axis=1)
    out.index.name = "time"
    return out.to_xarray()


def write_all_flare_statistics(
    ctx: AerialsContext,
    stats_3d: xr.Dataset,
    cfg: dict[str, Any] | None = None,
    *,
    dask_profile: bool = False,
    dask_profile_dir: Path | None = None,
    dask_profile_memory_interval: float = 0.5,
) -> xr.Dataset:
    stats_dir = ctx.gfx_png / "intermediate_statistics"
    stats_dir.mkdir(parents=True, exist_ok=True)
    out_file = stats_dir / f"all_flare_statistics_{ctx.domain_xy}_{ctx.cs_run.replace('__', '_')}.nc"
    expected = ",".join(map(str, ctx.flare_candidates))
    if out_file.is_file():
        cached = xr.open_dataset(out_file)
        if (
            cached.attrs.get("cs_run") == ctx.cs_run
            and cached.attrs.get("domain_xy") == ctx.domain_xy
            and cached.attrs.get("flare_candidates") == expected
            and int(cached.attrs.get("nc_format", 0)) == ALL_FLARE_STATISTICS_NC_FORMAT
        ):
            out = cached.load()
            cached.close()
            print(f"loaded all-flare statistics from {out_file.resolve().as_uri()}")
            return out
        cached.close()

    with _aerials_profiler(
        ctx,
        "write_all_flare_statistics",
        enabled=dask_profile,
        output_dir=dask_profile_dir,
        memory_interval=dask_profile_memory_interval,
    ) as prof:
        with prof.phase("build_stats_graph"):
            cell_area = float((cfg or {}).get("delta_x", 100.0)) ** 2
            tasks, task_meta, n_tracks = [], [], {}
            ref_for_flare = {f: r for f, r, *_ in ctx.pair_rows}
            for fexp in ctx.flare_candidates:
                rexp = ref_for_flare[fexp]
                pair_tasks, n_track = _pair_stats_tasks(ctx, stats_3d, fexp, rexp, cell_area)
                tasks.extend(pair_tasks)
                task_meta.append((fexp, rexp))
                n_tracks[fexp] = n_track

            print(f"All-flare statistics queued: {len(tasks)} arrays ({len(ctx.flare_candidates)} flare candidates)")

        with prof.phase("compute_stats_arrays"):
            with ProgressBar():
                loaded = dask_compute(*tasks)

        with prof.phase("assemble_write_stats"):
            by_exp: list[xr.Dataset] = []
            for i, (fexp, rexp) in enumerate(task_meta):
                icnc_prod = cast(xr.DataArray, loaded[4 * i])
                cdnc_reduction = cast(xr.DataArray, loaded[4 * i + 1])
                supersat = cast(xr.Dataset, loaded[4 * i + 2])
                ice_budget = cast(xr.Dataset, loaded[4 * i + 3])
                exp_stats = xr.merge([xr.merge([icnc_prod, cdnc_reduction]), supersat, _finish_ice_budget(ice_budget)])
                exp_stats = exp_stats.expand_dims(expname=[fexp]).assign_coords(ref_expname=("expname", [rexp]))
                exp_stats.attrs["n_tobac_track_rows"] = int(n_tracks[fexp])
                by_exp.append(exp_stats)

            all_stats = cast(xr.Dataset, xr.concat(by_exp, dim="expname"))
            all_stats.attrs.update(
                {
                    "cs_run": ctx.cs_run,
                    "domain_xy": ctx.domain_xy,
                    "flare_candidates": expected,
                    "nc_format": ALL_FLARE_STATISTICS_NC_FORMAT,
                    "description": "All flare-candidate bulk, ice-budget, and supersaturation statistics.",
                }
            )
            for name, var in all_stats.variables.items():
                if np.issubdtype(var.dtype, np.datetime64):
                    all_stats[str(name)].attrs.pop("units", None)
            all_stats.to_netcdf(out_file)
            print(f"wrote all-flare statistics: {out_file.resolve().as_uri()}")
            return all_stats

