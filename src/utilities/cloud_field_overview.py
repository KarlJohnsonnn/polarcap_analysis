"""
Cloud-field overview helpers promoted from notebook 01.

The figure combines:
- low/high-resolution extpar orography maps
- bulk QW and QFW time-height panels per station
- liquid and ice process-tendency profile panels
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.ticker import FixedLocator, FuncFormatter

from polarcap_runtime import is_server

from utilities.plotting import get_extpar_data
from utilities.process_budget_data import load_process_budget_data, select_rates_for_range, stn_label
from utilities.process_rates import PROCESS_PLOT_ORDER
from utilities.style_profiles import FULL_COL_IN, MAX_H_IN, MM, proc_color

DEFAULT_PLOT_START_MIN = 12 * 60 + 25
DEFAULT_PLOT_END_MIN = 13 * 60 + 5
DEFAULT_ZLIM_TH_W = (1.0e-5, 1.0e0)
DEFAULT_ZLIM_TH_I = (1.0e-5, 1.0e0)
DEFAULT_XLIM_PROC = (-1.0e1, 1.0e1)
DEFAULT_YLIM_GLOB = (700.0, 1750.0)
DEFAULT_LINTHRESH_PROC = 1.0e-8
DEFAULT_LINSCALE_PROC = 0.5
DEFAULT_SYMLOG_MAJOR_NTICKS = 3
DEFAULT_SYMLOG_LABEL_MIN_ABS = 1.0e-8
DEFAULT_TERRAIN_LIMS = (100.0, 1750.0)
DEFAULT_N_QLEVELS = 5
DEFAULT_OUTPUT_DIR = Path("output") / "gfx" / "png" / "01"

DEFAULT_WINDOW_SPECS_MIN: list[tuple[str, str, float, float, str]] = [
    ("seeding", "Early ice", 2.0, 12.0, "#4C78A8"),
    ("obs_site", "Ice growth", 5.0, 22.0, "#F58518"),
    ("precip_site", "Ice precip.", 14.0, 28.0, "#54A24B"),
]


def _overview_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    plotting = cfg.get("plotting", {})
    if not isinstance(plotting, dict):
        return {}
    overview = plotting.get("cloud_field_overview", {})
    return overview if isinstance(overview, dict) else {}


def _as_datetime64(value: str | np.datetime64 | None) -> np.datetime64 | None:
    if value is None:
        return None
    return np.datetime64(value)


def _as_tuple(value: Any, default: tuple[float, float]) -> tuple[float, float]:
    if value is None:
        return default
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return float(value[0]), float(value[1])
    raise ValueError(f"Expected a 2-item list/tuple, got {value!r}")


def _clip_window(
    start: np.datetime64,
    end: np.datetime64,
    time_values: np.ndarray,
) -> tuple[np.datetime64, np.datetime64]:
    t0 = np.datetime64(time_values[0])
    t1 = np.datetime64(time_values[-1])
    return (start if start > t0 else t0), (end if end < t1 else t1)


def _discrete_log_cmap_norm(
    cmap_name: str,
    vmin: float,
    vmax: float,
    n_levels: int,
) -> dict[str, Any]:
    boundaries = np.logspace(np.log10(vmin), np.log10(vmax), int(n_levels) + 1)
    cmap = mcolors.ListedColormap(plt.get_cmap(cmap_name)(np.linspace(0, 1, int(n_levels))))
    norm = mcolors.BoundaryNorm(boundaries, int(n_levels))
    return {"cmap": cmap, "norm": norm}


def default_active_vars(
    zlim_th_w: tuple[float, float] = DEFAULT_ZLIM_TH_W,
    zlim_th_i: tuple[float, float] = DEFAULT_ZLIM_TH_I,
    n_qlevels: int = DEFAULT_N_QLEVELS,
) -> dict[str, tuple[str, slice, bool, bool, str, dict[str, Any]]]:
    return {
        "QW": (
            "QW_bulk",
            slice(None, None),
            True,
            True,
            "QW - liquid water mass",
            _discrete_log_cmap_norm("viridis", zlim_th_w[0], zlim_th_w[1], n_qlevels),
        ),
        "QFW": (
            "QFW_bulk",
            slice(None, None),
            True,
            True,
            "QFW - ice plus liquid shell mass",
            _discrete_log_cmap_norm("viridis", zlim_th_i[0], zlim_th_i[1], n_qlevels),
        ),
    }


def _resolve_exp_id(cfg: dict[str, Any], exp_idx: int) -> int:
    plot_exp_ids = list(cfg.get("plot_exp_ids", []))
    if 0 <= int(exp_idx) < len(plot_exp_ids):
        return int(plot_exp_ids[int(exp_idx)])
    return int(exp_idx)


def _resolve_plot_bounds(
    seed_start: np.datetime64,
    plot_start: str | np.datetime64 | None,
    plot_end: str | np.datetime64 | None,
) -> tuple[np.datetime64, np.datetime64]:
    plot_start_dt = _as_datetime64(plot_start)
    plot_end_dt = _as_datetime64(plot_end)
    day = np.datetime64(seed_start, "D")
    if plot_start_dt is None:
        plot_start_dt = day + np.timedelta64(DEFAULT_PLOT_START_MIN, "m")
    if plot_end_dt is None:
        plot_end_dt = day + np.timedelta64(DEFAULT_PLOT_END_MIN, "m")
    return plot_start_dt, plot_end_dt


def _resolve_extpar_paths(
    cfg: dict[str, Any],
    extpar_low: str | Path | None,
    extpar_high: str | Path | None,
) -> tuple[Path, Path]:
    if extpar_low is not None and extpar_high is not None:
        return Path(extpar_low).expanduser(), Path(extpar_high).expanduser()

    local_root = Path.home() / "data" / "cosmo-specs" / "meteograms"
    low_path = local_root / "extPar_Eriswil_50x40.nc"
    high_path = local_root / "extPar_Eriswil_200x160.nc"

    if is_server():
        server_root_raw = cfg.get("paths", {}).get("server_root")
        if server_root_raw:
            server_root = Path(server_root_raw).expanduser()
            low_path = server_root / "COS_in" / "extPar_Eriswil_50x40.nc"
            high_path = (
                server_root.parent
                / "RUN_ERISWILL_200x160x100"
                / "COS_in"
                / "extPar_Eriswil_200x160.nc"
            )

    if extpar_low is not None:
        low_path = Path(extpar_low).expanduser()
    if extpar_high is not None:
        high_path = Path(extpar_high).expanduser()
    return low_path, high_path


def _bbox_index_slices(
    lat2d: np.ndarray,
    lon2d: np.ndarray,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
) -> tuple[slice, slice]:
    in_bbox = (
        (lat2d >= lat_min)
        & (lat2d <= lat_max)
        & (lon2d >= lon_min)
        & (lon2d <= lon_max)
    )
    rows = np.where(np.any(in_bbox, axis=1))[0]
    cols = np.where(np.any(in_bbox, axis=0))[0]
    if rows.size == 0 or cols.size == 0:
        return slice(None), slice(None)
    return slice(int(rows.min()), int(rows.max()) + 1), slice(int(cols.min()), int(cols.max()) + 1)


def crop_extpar_to_shared_bbox(
    lat_low: np.ndarray,
    lon_low: np.ndarray,
    hsurf_low: np.ndarray,
    lat_high: np.ndarray,
    lon_high: np.ndarray,
    hsurf_high: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    lon_min = max(float(np.amin(lon_low)), float(np.amin(lon_high)))
    lon_max = min(float(np.amax(lon_low)), float(np.amax(lon_high)))
    lat_min = max(float(np.amin(lat_low)), float(np.amin(lat_high)))
    lat_max = min(float(np.amax(lat_low)), float(np.amax(lat_high)))
    s_low_lat, s_low_lon = _bbox_index_slices(lat_low, lon_low, lat_min, lat_max, lon_min, lon_max)
    s_high_lat, s_high_lon = _bbox_index_slices(lat_high, lon_high, lat_min, lat_max, lon_min, lon_max)
    return (
        lat_low[s_low_lat, s_low_lon],
        lon_low[s_low_lat, s_low_lon],
        hsurf_low[s_low_lat, s_low_lon],
        lat_high[s_high_lat, s_high_lon],
        lon_high[s_high_lat, s_high_lon],
        hsurf_high[s_high_lat, s_high_lon],
    )


def build_phase_windows(
    seed_start: np.datetime64,
    time_values: np.ndarray,
    window_specs_min: list[tuple[str, str, float, float, str]],
) -> list[dict[str, Any]]:
    time_windows: list[dict[str, Any]] = []
    for key, label, start_min, end_min, color in window_specs_min:
        start = seed_start + np.timedelta64(int(float(start_min) * 60), "s")
        end = seed_start + np.timedelta64(int(float(end_min) * 60), "s")
        start, end = _clip_window(start, end, time_values)
        if start < end:
            time_windows.append(
                {"key": key, "label": label, "start": start, "end": end, "color": color}
            )
    return time_windows


def build_bulk_mass_dataset(
    ds_exp: xr.Dataset,
    rho: xr.DataArray | None,
    active_vars: dict[str, tuple[str, slice, bool, bool, str, dict[str, Any]]],
) -> xr.Dataset:
    mass_unit = r"g m$^{-3}$" if rho is not None else r"kg kg$^{-1}$"
    bulk: dict[str, xr.DataArray] = {}
    for src_name, (out_name, bin_slice, pos_only, unit_convert, label, _) in active_vars.items():
        data = ds_exp[src_name]
        if "bins" in data.dims:
            data = data.isel(bins=bin_slice).sum(dim="bins")
        if unit_convert and rho is not None and src_name.startswith("Q"):
            data = data * rho * 1.0e3
            data.attrs["units"] = mass_unit
        if pos_only:
            data = xr.where(data > 0, data, np.nan)
        data.attrs["long_name"] = label
        bulk[out_name] = data
    return xr.Dataset(bulk)


def _y_coord_name(da: xr.DataArray) -> str:
    return "HMLd" if "HMLd" in da.coords else "height_level"


def _y_dim_name(da: xr.DataArray) -> str:
    if "height_level" in da.dims:
        return "height_level"
    if "HMLd" in da.dims:
        return "HMLd"
    raise ValueError(f"No supported vertical dimension found in {da.dims!r}")


def _height_centers_and_layer_thickness(sample_da: xr.DataArray) -> tuple[np.ndarray, np.ndarray]:
    """Return height centres and native layer thicknesses."""
    height = np.asarray(sample_da.coords["height_level"].values, dtype=float)
    if height.size == 1:
        return height, np.array([20.0])
    edges = np.empty(height.size + 1, dtype=float)
    edges[1:-1] = 0.5 * (height[1:] + height[:-1])
    edges[0] = height[0] - 0.5 * (height[1] - height[0])
    edges[-1] = height[-1] + 0.5 * (height[-1] - height[-2])
    return height, np.diff(edges)


def _height_centers_and_bar_heights(
    sample_da: xr.DataArray,
    bar_fill: float = 0.92,
) -> tuple[np.ndarray, np.ndarray]:
    """Return plotting heights and slightly shrunken bar heights."""
    height, layer_thickness = _height_centers_and_layer_thickness(sample_da)
    return height, layer_thickness * bar_fill


def _time_integrated_profiles(
    rates_dict: dict[str, xr.DataArray],
    station_idx: int,
    window: dict[str, Any],
) -> dict[str, np.ndarray]:
    """Integrate signed process rates over one phase window."""
    profiles: dict[str, np.ndarray] = {}
    for proc, da in rates_dict.items():
        sel = da.isel(station=station_idx).sel(time=slice(window["start"], window["end"]))
        n_time = int(sel.sizes.get("time", 0))
        if n_time < 2:
            continue
        dt_s = xr.DataArray(
            sel["time"].diff("time").astype("timedelta64[s]").astype(float).values,
            dims=("time",),
            coords={"time": sel["time"].isel(time=slice(0, -1)).values},
        )
        values = (sel.isel(time=slice(0, -1)) * dt_s).sum(dim="time").compute().values.astype(float)
        profiles[proc] = np.nan_to_num(values)
    return profiles


def build_qfw_plume_ridge(
    qfw_da: xr.DataArray,
    window: dict[str, Any],
    *,
    floor: float,
) -> dict[str, np.ndarray]:
    """Trace the ice-plume ridge from QFW as the per-time height maximum."""
    y_name = _y_coord_name(qfw_da)
    y_dim = _y_dim_name(qfw_da)
    da = xr.where(qfw_da > floor, qfw_da, np.nan).sel(time=slice(window["start"], window["end"]))
    if int(da.sizes.get("time", 0)) == 0:
        return {"time": np.array([], dtype="datetime64[ns]"), "height": np.array([]), "height_idx": np.array([], dtype=int)}

    vals = np.asarray(da.transpose("time", y_dim).values, dtype=float)
    heights = np.asarray(da.coords[y_name].values, dtype=float)
    times = np.asarray(da.time.values)
    ridge_time: list[np.datetime64] = []
    ridge_height: list[float] = []
    ridge_idx: list[int] = []

    for time_idx, row in enumerate(vals):
        if not np.isfinite(row).any():
            continue
        idx = int(np.nanargmax(row))
        ridge_time.append(np.datetime64(times[time_idx]))
        ridge_height.append(float(heights[idx]))
        ridge_idx.append(idx)

    return {
        "time": np.asarray(ridge_time),
        "height": np.asarray(ridge_height, dtype=float),
        "height_idx": np.asarray(ridge_idx, dtype=int),
    }


def _ridge_sampled_profiles(
    rates_dict: dict[str, xr.DataArray],
    station_idx: int,
    ridge: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    """Average process tendencies sampled along the QFW plume ridge."""
    if ridge["height_idx"].size == 0:
        return {}

    sample_da = next(iter(rates_dict.values())).isel(station=station_idx)
    y_dim = _y_dim_name(sample_da)
    n_height = int(sample_da.sizes.get(y_dim, 0))
    profiles: dict[str, np.ndarray] = {}

    for proc, da in rates_dict.items():
        station_da = da.isel(station=station_idx)
        sampled = station_da.sel(time=ridge["time"]).compute()
        vals_2d = np.asarray(sampled.transpose("time", y_dim).values, dtype=float)
        bins: list[list[float]] = [[] for _ in range(n_height)]
        for row_idx, height_idx in enumerate(ridge["height_idx"]):
            bins[int(height_idx)].append(float(vals_2d[row_idx, int(height_idx)]))
        profile = np.zeros(n_height, dtype=float)
        for idx, vals in enumerate(bins):
            if vals:
                profile[idx] = float(np.nanmean(vals))
        profiles[proc] = np.nan_to_num(profile)
    return profiles


def draw_qfw_plume_ridge(ax: plt.Axes, ridge: dict[str, np.ndarray]) -> None:
    """Overlay the diagnosed QFW ridge on the time-height panel."""
    if ridge["time"].size < 2:
        return
    ax.plot(
        ridge["time"],
        ridge["height"],
        color="#D62728",
        linewidth=2.0,
        alpha=0.95,
        solid_capstyle="round",
        zorder=6,
    )


def _phase_minutes(seed_start: np.datetime64, window: dict[str, Any]) -> tuple[float, float]:
    """Return phase-window bounds in minutes since seeding start."""
    t_lo = float((window["start"] - seed_start) / np.timedelta64(1, "m"))
    t_hi = float((window["end"] - seed_start) / np.timedelta64(1, "m"))
    return t_lo, t_hi


def default_cloud_phase_budget_outputs(
    repo_root: Path,
    exp_label: str,
    active_range_key: str,
) -> dict[str, Path]:
    """Return default output paths for phase-integrated budget summaries."""
    gfx = repo_root / "output" / "gfx"
    stem = f"cloud_phase_budget_summary_{exp_label}_{active_range_key}"
    return {
        "long_csv": gfx / "csv" / "01" / f"{stem}_long.csv",
        "summary_csv": gfx / "csv" / "01" / f"{stem}.csv",
        "summary_tex": gfx / "tex" / "01" / f"{stem}.tex",
    }


def build_cloud_phase_budget_tables(context: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build long-form and compact phase-integrated budget summary tables."""
    long_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    seed_start = context["seed_start"]

    for row_idx in range(int(context["n_stations"])):
        station_id = f"S{row_idx + 1}"
        station_name = stn_label(row_idx, context["station_labels"])
        window = context["time_windows"][row_idx]
        t_lo_min, t_hi_min = _phase_minutes(seed_start, window)

        dominant: dict[str, dict[str, tuple[str, float] | None]] = {
            "liquid": {"source": None, "sink": None},
            "ice": {"source": None, "sink": None},
        }

        for reservoir, rates_dict in [("liquid", context["rates_q_liq"]), ("ice", context["rates_q_ice"])]:
            sample_da = next(iter(rates_dict.values())).isel(station=row_idx)
            _, layer_thickness = _height_centers_and_layer_thickness(sample_da)
            profiles = _time_integrated_profiles(rates_dict, row_idx, window)

            for proc in PROCESS_PLOT_ORDER:
                values = profiles.get(proc)
                if values is None:
                    continue
                column_net = float(np.nansum(np.asarray(values, dtype=float) * layer_thickness))
                long_rows.append(
                    {
                        "station": station_id,
                        "station_label": station_name,
                        "phase_key": window["key"],
                        "phase_label": window["label"],
                        "t_lo_min": t_lo_min,
                        "t_hi_min": t_hi_min,
                        "reservoir": reservoir,
                        "process": proc,
                        "column_net_g_m2": column_net,
                    }
                )
                if column_net > 0:
                    best = dominant[reservoir]["source"]
                    if best is None or column_net > best[1]:
                        dominant[reservoir]["source"] = (proc, column_net)
                elif column_net < 0:
                    best = dominant[reservoir]["sink"]
                    if best is None or column_net < best[1]:
                        dominant[reservoir]["sink"] = (proc, column_net)

        summary_rows.append(
            {
                "station": station_id,
                "phase_label": window["label"],
                "time_frame_min": f"{t_lo_min:.1f}-{t_hi_min:.1f}",
                "liq_source": "" if dominant["liquid"]["source"] is None else dominant["liquid"]["source"][0],
                "liq_source_g_m2": np.nan if dominant["liquid"]["source"] is None else dominant["liquid"]["source"][1],
                "liq_sink": "" if dominant["liquid"]["sink"] is None else dominant["liquid"]["sink"][0],
                "liq_sink_g_m2": np.nan if dominant["liquid"]["sink"] is None else dominant["liquid"]["sink"][1],
                "ice_source": "" if dominant["ice"]["source"] is None else dominant["ice"]["source"][0],
                "ice_source_g_m2": np.nan if dominant["ice"]["source"] is None else dominant["ice"]["source"][1],
                "ice_sink": "" if dominant["ice"]["sink"] is None else dominant["ice"]["sink"][0],
                "ice_sink_g_m2": np.nan if dominant["ice"]["sink"] is None else dominant["ice"]["sink"][1],
            }
        )

    return pd.DataFrame(long_rows), pd.DataFrame(summary_rows)


def save_cloud_phase_budget_tables(
    long_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    out_paths: dict[str, Path],
) -> dict[str, Path]:
    """Write phase-integrated budget summaries to CSV and LaTeX."""
    for path in out_paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(out_paths["long_csv"], index=False)
    summary_df.to_csv(out_paths["summary_csv"], index=False)
    latex_df = summary_df.copy()
    for col in ["liq_source", "liq_sink", "ice_source", "ice_sink"]:
        latex_df[col] = latex_df[col].map(
            lambda v: "" if not isinstance(v, str) or not v else v.replace("_", " ").title()
        )
    for col in ["liq_source_g_m2", "liq_sink_g_m2", "ice_source_g_m2", "ice_sink_g_m2"]:
        latex_df[col] = latex_df[col].map(lambda v: "" if pd.isna(v) else f"{float(v):.2e}")
    latex_df = latex_df.rename(
        columns={
            "station": "Station",
            "phase_label": "Phase",
            "time_frame_min": "Window [min]",
            "liq_source": "Liquid source",
            "liq_source_g_m2": "Liquid src. [g m$^{-2}$]",
            "liq_sink": "Liquid sink",
            "liq_sink_g_m2": "Liquid sink [g m$^{-2}$]",
            "ice_source": "Ice source",
            "ice_source_g_m2": "Ice src. [g m$^{-2}$]",
            "ice_sink": "Ice sink",
            "ice_sink_g_m2": "Ice sink [g m$^{-2}$]",
        }
    )
    latex_df.to_latex(out_paths["summary_tex"], index=False)
    return out_paths


def _stack_extent(profiles: dict[str, np.ndarray]) -> float:
    if not profiles:
        return 0.0
    arrs = list(profiles.values())
    pos_sum = np.sum([np.clip(arr, 0.0, None) for arr in arrs], axis=0)
    neg_sum = np.sum([np.clip(arr, None, 0.0) for arr in arrs], axis=0)
    return float(max(np.nanmax(pos_sum), np.nanmax(np.abs(neg_sum))))


def collect_scale_params(
    rates_dict: dict[str, xr.DataArray],
    station_count: int,
    ridges: list[dict[str, np.ndarray]],
    xlim: float | None = None,
    linthresh: float | None = None,
) -> tuple[float, float]:
    if xlim is not None and linthresh is not None:
        return float(xlim), float(linthresh)

    vmax = 0.0
    nonzero: list[np.ndarray] = []
    for station_idx in range(int(station_count)):
        profiles = _ridge_sampled_profiles(rates_dict, station_idx, ridges[station_idx])
        vmax = max(vmax, _stack_extent(profiles))
        for values in profiles.values():
            arr = np.abs(np.asarray(values, dtype=float))
            arr = arr[np.isfinite(arr) & (arr > 0)]
            if arr.size:
                nonzero.append(arr)
    computed_xlim = 1.05 * vmax if vmax > 0 else 1.0e-12
    if not nonzero:
        computed_linthresh = 1.0e-10
    else:
        all_nonzero = np.concatenate(nonzero)
        computed_linthresh = max(
            float(np.quantile(all_nonzero, 0.10)),
            computed_xlim * 1.0e-8,
            1.0e-8,
        )
    return (
        float(xlim) if xlim is not None else computed_xlim,
        float(linthresh) if linthresh is not None else computed_linthresh,
    )


def plot_net_profiles(
    ax: plt.Axes,
    rates_dict: dict[str, xr.DataArray],
    station_idx: int,
    ridge: dict[str, np.ndarray],
    xlim: tuple[float, float],
    linthresh: float,
    *,
    linscale: float = DEFAULT_LINSCALE_PROC,
    lw_white: float = 2.25,
    lw_step: float = 0.75,
    lw_black: float = 0.35,
    fill_alpha: float = 0.5,
) -> list[str]:
    sample_da = next(iter(rates_dict.values())).isel(station=station_idx)
    height, _ = _height_centers_and_bar_heights(sample_da)
    profiles = _ridge_sampled_profiles(rates_dict, station_idx, ridge)
    active: list[str] = []
    top_order = ("DROP_COLLISION", "REFREEZING", "IMMERSION_FREEZING")
    draw_order = [p for p in PROCESS_PLOT_ORDER if p not in top_order] + [
        p for p in top_order if p in PROCESS_PLOT_ORDER
    ]
    for proc in draw_order:
        values = profiles.get(proc)
        if values is None or not np.any(np.abs(values) > 0):
            continue
        active.append(proc)
        zorder = 30 if proc in top_order else (1 if proc == "CONDENSATION" else 20)
        ax.step(values, height, color="white", linewidth=lw_white, alpha=0.5, zorder=zorder)
        ax.fill_betweenx(
            height,
            0,
            values,
            color=proc_color(proc),
            alpha=fill_alpha,
            linewidth=lw_step,
            step="post",
            zorder=zorder + 0.05,
        )
        ax.step(
            values,
            height,
            color="black",
            linewidth=lw_black,
            alpha=1.0,
            zorder=zorder + 0.1,
        )
    ax.set_xscale("symlog", linthresh=linthresh, linscale=linscale, base=10)
    ax.axvline(0.0, color="0.15", linewidth=0.5, zorder=3)
    ax.grid(axis="x", which="major", alpha=0.18, linewidth=0.35)
    ax.grid(axis="x", which="minor", alpha=0.10, linewidth=0.20)
    ax.set_xlim(*xlim)
    return active


def set_symlog_ticks(
    ax: plt.Axes,
    xlim: tuple[float, float],
    linthresh: float,
    *,
    n_decades: int = DEFAULT_SYMLOG_MAJOR_NTICKS,
    label_min_abs: float = DEFAULT_SYMLOG_LABEL_MIN_ABS,
) -> None:
    lo, hi = float(xlim[0]), float(xlim[1])
    major = [0.0]
    for exp in range(-12, 5):
        if exp % int(n_decades) != 0:
            continue
        for sign in (1.0, -1.0):
            tick = sign * (10.0 ** exp)
            if tick != 0 and lo <= tick <= hi:
                major.append(tick)
    minor = []
    for exp in range(-11, 5):
        if exp % int(n_decades) == 0:
            continue
        for sign in (1.0, -1.0):
            tick = sign * (10.0 ** exp)
            if lo <= tick <= hi:
                minor.append(tick)
    ax.xaxis.set_major_locator(FixedLocator(sorted(set(major))))
    ax.xaxis.set_minor_locator(FixedLocator(sorted(set(minor))))

    def _fmt(x: float, _pos: int) -> str:
        if np.isclose(x, 0.0):
            return "0"
        if abs(x) < float(label_min_abs):
            return ""
        power = int(round(np.log10(abs(x))))
        if power < 0:
            prefix = "-" if x < 0 else ""
            return prefix + r"$10^{\text{-}" + str(abs(power)) + "}$"
        return rf"$-10^{{{power}}}$" if x < 0 else rf"$10^{{{power}}}$"

    ax.xaxis.set_major_formatter(FuncFormatter(_fmt))
    ax.tick_params(axis="x", which="major", length=4.5)
    ax.tick_params(axis="x", which="minor", length=2.25)


def _log_cbar_fmt(x: float, _pos: int) -> str:
    if x <= 0:
        return ""
    power = int(round(np.log10(x)))
    if power < 0:
        return r"$10^{\text{-}" + str(abs(power)) + "}$"
    return rf"$10^{{{power}}}$"


def default_cloud_field_overview_output(
    repo_root: Path,
    exp_label: str,
    active_range_key: str,
) -> Path:
    stem = f"cloud_field_overview_mass_profiles_steps_symlog_{exp_label}_{active_range_key}.png"
    return repo_root / DEFAULT_OUTPUT_DIR / stem


def load_cloud_field_overview_context(
    repo_root: Path,
    config_path: str | Path | None,
    *,
    exp_idx: int = 0,
    active_range_key: str | None = None,
    plot_start: str | np.datetime64 | None = None,
    plot_end: str | np.datetime64 | None = None,
    extpar_low: str | Path | None = None,
    extpar_high: str | Path | None = None,
) -> dict[str, Any]:
    cfg = load_process_budget_data(repo_root, config_path=config_path)
    overview_cfg = _overview_cfg(cfg)
    exp_id = _resolve_exp_id(cfg, exp_idx)
    seed_start = np.datetime64(cfg["seed_start"])
    plot_start_dt, plot_end_dt = _resolve_plot_bounds(seed_start, plot_start, plot_end)
    zlim_th_w = _as_tuple(overview_cfg.get("zlim_th_w"), DEFAULT_ZLIM_TH_W)
    zlim_th_i = _as_tuple(overview_cfg.get("zlim_th_i"), DEFAULT_ZLIM_TH_I)
    xlim_proc = _as_tuple(overview_cfg.get("xlim_proc"), DEFAULT_XLIM_PROC)
    ylim_glob = _as_tuple(overview_cfg.get("ylim_glob"), DEFAULT_YLIM_GLOB)
    terrain_lims = _as_tuple(overview_cfg.get("terrain_lims"), DEFAULT_TERRAIN_LIMS)
    linthresh_proc = float(overview_cfg.get("linthresh_proc", DEFAULT_LINTHRESH_PROC))
    linscale_proc = float(overview_cfg.get("linscale_proc", DEFAULT_LINSCALE_PROC))
    n_qlevels = int(overview_cfg.get("n_qlevels", DEFAULT_N_QLEVELS))
    symlog_major_nticks = int(
        overview_cfg.get("symlog_major_nticks", DEFAULT_SYMLOG_MAJOR_NTICKS)
    )
    symlog_label_min_abs = float(
        overview_cfg.get("symlog_label_min_abs", DEFAULT_SYMLOG_LABEL_MIN_ABS)
    )
    window_specs = overview_cfg.get("window_specs_min", DEFAULT_WINDOW_SPECS_MIN)
    active_range = active_range_key or cfg.get("active_range_key", "ALLBB")

    ds_exp = cfg["ds"].isel(expname=exp_id).sel(time=slice(plot_start_dt, plot_end_dt))
    if ds_exp.sizes.get("time", 0) == 0:
        raise ValueError("Selected plot range does not overlap the experiment time axis.")

    rho = ds_exp["RHO"] if "RHO" in ds_exp.data_vars else None
    exp_raw = cfg["ds"].expname.values[exp_id]
    exp_label = exp_raw.decode() if isinstance(exp_raw, bytes) else str(exp_raw)
    station_labels = {int(k): v for k, v in cfg["station_labels"].items()}
    rates = cfg["rates_by_exp"][exp_id]
    selected_rates = select_rates_for_range(rates, active_range)
    rates_q_liq = selected_rates["rates_Q_liq"]
    rates_q_ice = selected_rates["rates_Q_ice"]
    active_vars = default_active_vars(zlim_th_w=zlim_th_w, zlim_th_i=zlim_th_i, n_qlevels=n_qlevels)
    bulk = build_bulk_mass_dataset(ds_exp, rho, active_vars)
    time_windows = build_phase_windows(seed_start, ds_exp.time.values, window_specs)
    n_stations = int(ds_exp.sizes.get("station", 0))
    if len(time_windows) < n_stations:
        raise ValueError(
            f"Need at least {n_stations} valid phase windows, got {len(time_windows)}."
        )

    extpar_low_path, extpar_high_path = _resolve_extpar_paths(cfg, extpar_low, extpar_high)
    lat_low, lon_low, hsurf_low = get_extpar_data(str(extpar_low_path))
    lat_high, lon_high, hsurf_high = get_extpar_data(str(extpar_high_path))
    (
        lat_low,
        lon_low,
        hsurf_low,
        lat_high,
        lon_high,
        hsurf_high,
    ) = crop_extpar_to_shared_bbox(lat_low, lon_low, hsurf_low, lat_high, lon_high, hsurf_high)

    qfw_ridges = [
        build_qfw_plume_ridge(bulk["QFW_bulk"].isel(station=station_idx), time_windows[station_idx], floor=zlim_th_i[0])
        for station_idx in range(n_stations)
    ]
    total_xlim, total_linthresh = collect_scale_params(rates_q_liq, n_stations, qfw_ridges)
    plot_settings = {spec[0]: {"label": spec[4], **spec[5]} for spec in active_vars.values()}

    return {
        "cfg": cfg,
        "repo_root": repo_root,
        "exp_id": exp_id,
        "exp_label": exp_label,
        "seed_start": seed_start,
        "active_range_key": active_range,
        "plot_start": plot_start_dt,
        "plot_end": plot_end_dt,
        "time_windows": time_windows,
        "qfw_ridges": qfw_ridges,
        "station_labels": station_labels,
        "n_stations": n_stations,
        "bulk": bulk,
        "ds_exp": ds_exp,
        "rates_q_liq": rates_q_liq,
        "rates_q_ice": rates_q_ice,
        "plot_settings": plot_settings,
        "zlim_th_w": zlim_th_w,
        "zlim_th_i": zlim_th_i,
        "xlim_proc": xlim_proc,
        "ylim_glob": ylim_glob,
        "terrain_lims": terrain_lims,
        "linthresh_proc": linthresh_proc,
        "linscale_proc": linscale_proc,
        "symlog_major_nticks": symlog_major_nticks,
        "symlog_label_min_abs": symlog_label_min_abs,
        "lat_low": lat_low,
        "lon_low": lon_low,
        "hsurf_low": hsurf_low,
        "lat_high": lat_high,
        "lon_high": lon_high,
        "hsurf_high": hsurf_high,
        "extpar_low_path": extpar_low_path,
        "extpar_high_path": extpar_high_path,
        "total_xlim": total_xlim,
        "total_linthresh": total_linthresh,
        "output_path": default_cloud_field_overview_output(repo_root, exp_label, active_range),
        "phase_budget_paths": default_cloud_phase_budget_outputs(repo_root, exp_label, active_range),
    }


def render_cloud_field_overview(context: dict[str, Any]) -> plt.Figure:
    n_stations = int(context["n_stations"])
    nrows, ncols = 2 + 1 + n_stations, 4
    fig_width = FULL_COL_IN * 1.3
    fig_height = min((96 + 25 * n_stations) * MM, MAX_H_IN)
    fig = plt.figure(figsize=(fig_width, fig_height), constrained_layout=True)
    fig.set_constrained_layout_pads(w_pad=3.0 / 72.0, h_pad=3.0 / 72.0, wspace=0.045, hspace=0.045)
    grid = gridspec.GridSpec(
        nrows,
        ncols,
        figure=fig,
        width_ratios=[1.0, 1.0, 1.0, 1.0],
        height_ratios=[1.3, 1.3, 0.14] + [1.0] * n_stations,
        hspace=0.08,
        wspace=0.14,
    )

    map_grid = grid[:2, :].subgridspec(2, 2, width_ratios=[1, 1], wspace=0.04)
    ax_low = fig.add_subplot(map_grid[:, 0])
    ax_high = fig.add_subplot(map_grid[:, 1])
    terrain_vmin, terrain_vmax = context["terrain_lims"]
    map_artist = None
    for ax, lon, lat, hsurf, title in [
        (ax_low, context["lon_low"], context["lat_low"], context["hsurf_low"], "Low-res extpar orography (400 m)"),
        (ax_high, context["lon_high"], context["lat_high"], context["hsurf_high"], "High-res extpar orography (100 m)"),
    ]:
        map_artist = ax.pcolormesh(
            lon,
            lat,
            hsurf,
            cmap="terrain",
            vmin=terrain_vmin,
            vmax=terrain_vmax,
            shading="auto",
        )
        ax.set_title(title)
        ax.set_xlabel("Longitude [deg E]")
        ax.set_ylabel("Latitude [deg N]")
        ax.set_aspect("equal")
        for station_idx in range(n_stations):
            slat = float(context["ds_exp"].station_lat.values[station_idx])
            slon = float(context["ds_exp"].station_lon.values[station_idx])
            ax.plot(slon, slat, "o", ms=5.5, mfc="none", mec="black", mew=0.7, zorder=5)
            ax.annotate(
                f"S{station_idx + 1}",
                (slon, slat),
                textcoords="offset points",
                xytext=(4, 4),
                fontsize=6.5,
                fontweight="bold",
            )
        ax.tick_params(top=True, right=True, labeltop=False, labelright=False)

    if map_artist is not None:
        fig.colorbar(
            map_artist,
            ax=[ax_low, ax_high],
            label="Surface height [m]",
            shrink=0.5,
            pad=0.025,
        )

    qw_axes: list[plt.Axes] = []
    qfw_axes: list[plt.Axes] = []
    budget_axes_top: list[plt.Axes] = []
    active_processes: set[str] = set()
    last_qw_artist = None

    for row_idx in range(n_stations):
        ds_stn = context["bulk"].isel(station=row_idx)
        station_name = stn_label(row_idx, context["station_labels"])
        window_stn = context["time_windows"][row_idx]
        ridge_stn = context["qfw_ridges"][row_idx]
        ax_qw = fig.add_subplot(grid[row_idx + 3, 0])
        ax_qfw = fig.add_subplot(grid[row_idx + 3, 1], sharey=ax_qw)
        qw_axes.append(ax_qw)
        qfw_axes.append(ax_qfw)

        for ax, name, zlim in [
            (ax_qw, "QW_bulk", context["zlim_th_w"]),
            (ax_qfw, "QFW_bulk", context["zlim_th_i"]),
        ]:
            da = xr.where(ds_stn[name] > zlim[0], ds_stn[name], np.nan)
            style = context["plot_settings"][name]
            artist = da.plot.pcolormesh(
                ax=ax,
                x="time",
                y=_y_coord_name(da),
                cmap=style["cmap"],
                norm=style["norm"],
                add_colorbar=False,
                add_labels=False,
                rasterized=True,
            )
            if name == "QW_bulk":
                last_qw_artist = artist
            if ax is ax_qfw:
                draw_qfw_plume_ridge(ax_qfw, ridge_stn)
            ax.axvline(window_stn["start"], linestyle="--", color="k", linewidth=0.7, zorder=4)
            ax.axvline(window_stn["end"], linestyle="--", color="k", linewidth=0.7, zorder=4)
            ax.set_xlim(context["plot_start"], context["plot_end"])
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
            ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=5))
            ax.tick_params(axis="x", top=False, bottom=True, labelsize=7)
            if row_idx < n_stations - 1:
                ax.set_xlabel("")
                ax.set_xticklabels([])
            else:
                ax.set_xlabel("Time [UTC]")
                ax.xaxis.set_label_position("bottom")
            if ax is ax_qw:
                ax.set_ylabel(station_name)
            else:
                ax.set_ylabel("")
                ax.tick_params(axis="y", labelleft=False)
            ax.tick_params(right=True)
            ax.set_ylim(*context["ylim_glob"])

        for col, rates_dict in [(2, context["rates_q_liq"]), (3, context["rates_q_ice"])]:
            ax = fig.add_subplot(grid[row_idx + 3, col], sharey=ax_qw)
            if row_idx == 0:
                budget_axes_top.append(ax)
            active_processes.update(
                plot_net_profiles(
                    ax,
                    rates_dict,
                    row_idx,
                    ridge_stn,
                    context["xlim_proc"],
                    context["linthresh_proc"],
                    linscale=context["linscale_proc"],
                )
            )
            ax.tick_params(axis="y", labelleft=False, right=True)
            for spine in ("top", "bottom"):
                ax.spines[spine].set_visible(False)
            if row_idx < n_stations - 1:
                ax.tick_params(axis="x", labelbottom=False)
            else:
                ax.set_xlabel(r"process tendency along ice-plume ridge (g m$^{-3}$ s$^{-1}$)")
            set_symlog_ticks(
                ax,
                context["xlim_proc"],
                context["linthresh_proc"],
                n_decades=context["symlog_major_nticks"],
                label_min_abs=context["symlog_label_min_abs"],
            )
            ax.set_ylim(*context["ylim_glob"])

    cax_qw = fig.add_subplot(grid[2, :2])
    cax_proc = fig.add_subplot(grid[2, 2:4])
    if last_qw_artist is not None:
        fig.colorbar(
            last_qw_artist,
            cax=cax_qw,
            orientation="horizontal",
            label=r"Liquid and ice water content / (g m$^{-3}$)",
        )
        cax_qw.xaxis.set_major_formatter(FuncFormatter(_log_cbar_fmt))
        cax_qw.xaxis.set_ticks_position("top")
        cax_qw.xaxis.set_label_position("top")
        cax_qw.tick_params(axis="x", top=True, bottom=False, labeltop=True, labelbottom=False)

    qw_axes[0].set_title("Liquid", color="royalblue", ha="left", x=0.0, weight="bold")
    qfw_axes[0].set_title("Ice", color="darkorange", ha="right", x=1.0, weight="bold")
    budget_axes_top[0].set_title("Liquid", color="royalblue", ha="left", x=0.0, weight="bold")
    budget_axes_top[1].set_title("Ice", color="darkorange", ha="right", x=1.0, weight="bold")

    legend_order = [proc for proc in PROCESS_PLOT_ORDER if proc in active_processes]
    if legend_order:
        proc_cmap = mcolors.ListedColormap([proc_color(proc) for proc in legend_order], name="proc")
        proc_norm = mcolors.BoundaryNorm(np.arange(len(legend_order) + 1), proc_cmap.N)
        sm = plt.cm.ScalarMappable(norm=proc_norm, cmap=proc_cmap)
        sm.set_array([])
        cbar_proc = fig.colorbar(
            sm,
            cax=cax_proc,
            orientation="horizontal",
            ticks=np.arange(len(legend_order)) + 0.5,
        )
        cbar_proc.ax.set_xticklabels([proc.replace("_", "\n") for proc in legend_order], fontsize=5.5)
        cbar_proc.ax.xaxis.set_ticks_position("top")
        cbar_proc.ax.xaxis.set_label_position("top")
        cbar_proc.outline.set_linewidth(0.5)

    fig.canvas.draw()
    for cax, width_scale, height_scale in ((cax_qw, 0.78, 0.62), (cax_proc, 0.92, 0.62)):
        box = cax.get_position()
        width_new = box.width * width_scale
        height_new = box.height * height_scale
        cax.set_position(
            [
                box.x0 + (box.width - width_new) / 2,
                box.y0 + (box.height - height_new) / 2,
                width_new,
                height_new,
            ]
        )

    fig.suptitle(
        f"Cloud field overview — Exp {context['exp_label']}: orography, QW/QWF, liquid/ice ridge-sampled "
        f"process tendencies ({context['active_range_key']})",
        y=1.01,
    )
    return fig


def save_cloud_field_overview(
    fig: plt.Figure,
    output_path: str | Path,
    *,
    dpi: int = 300,
) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, bbox_inches="tight")
    print(f"saved -> {out}")
    return out
