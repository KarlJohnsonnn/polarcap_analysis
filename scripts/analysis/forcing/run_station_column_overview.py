#!/usr/bin/env python3
"""Publication layout: dual orography maps (each 3×3 GridSpec cells) + per-station columns (6 wide) from LV2 zarr.

Panels are lettered in reading order: **a)** and **b)** are the coarse- and fine-grid orography; each subsequent
station row adds six panels (profiles left to right) in alphabetical sequence (**c)**–**h)** for the first row, etc.).

Suggested figure caption (fill bracketed items for the manuscript):

    Figure X. **(a,b)** Model surface height (m a.m.s.l.; colour scale as inset colour bar) for the **coarse-** and
    **fine-resolution** outer-domain meshes over the Eriswil area on a **common map extent** (ExtPar). Open circles
    mark meteogram column locations (S1–Sn); red and blue symbols mark the **seeding** site and the **Eriswil**
    observatory. **(c–…)** Time-mean vertical columns from the level-2 meteogram archive: for each station row,
    mass-based full-level height *H* (m a.m.s.l.) versus **liquid** and **mixed-phase** hydrometeor mass density
    (g m⁻³; sum over bins from the selected lower bin index times air density), **air temperature** (°C), **relative
    humidity with respect to liquid water**, **turbulent kinetic energy**, and **vertical velocity** (half-level fields
    linearly interpolated to full levels). **TKE** is plotted as **2 TKE²** in **m² s⁻²**, where the archive field is a
    **m s⁻¹** turbulent-velocity scale. Within each column, member curves, the ensemble mean, and the shaded spread share
    a **distinct colour** (liquid, mixed-phase, T, RH, TKE, W). Time averaging is over **[UTC range or full meteogram
    period]**. If an **expname** dimension is present, **faint** member profiles are overlaid, a **shaded band** shows the
    ensemble spread (default **min–max**; optional **p10–p90** or **p25–p75** when building the figure), and a **thicker**
    line shows the **ensemble mean**; the key between **(a,b)** and **(c–…)** explains members, spread, and mean.

Example:
--zarr /work/bb1178/schimmel/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/cs-eriswil__20260328_205320/lv2_meteogram/Meteogram_cs-eriswil__20260328_205320_nVar136_nMet3_nExp15.zarr \

    python scripts/analysis/forcing/run_station_column_overview.py \
        --zarr /work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/cs-eriswil__20260304_110254/Meteogram_cs-eriswil__20260304_110254_nVar136_nMet3_nExp5.zarr/  \
        --extpar-low "$CS_RUNS_DIR/extPar_Eriswil_50x40.nc" \
        --extpar-high "$CS_RUNS_DIR/extPar_Eriswil_200x160.nc" \
        --plot-start "2023-01-25T12:25:00" \
        --plot-end "2023-01-25T13:05:00" \
        --spread-mode minmax \
        --ylim-glob 700 1600
"""
from __future__ import annotations

import argparse
import os
import string
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import yaml
from matplotlib.gridspec import GridSpec
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.path import Path as MplPath

REPO_ROOT = Path(__file__).resolve().parents[3]
POLARCAP_ROOT = Path(os.environ.get("POLARCAP_ROOT", REPO_ROOT))
DEFAULT_GFX_PNG_01 = POLARCAP_ROOT / "output" / "gfx" / "png" / "01"
# Each orography panel spans this many rows × 3 columns in the master 6-column GridSpec.
MAP_SPEC_ROWS = 4
MAP_SPEC_COLS_EACH = 3
MAP_PROFILE_GAP_RATIO = 0.35 # increase for more space between maps and profiles
# Inset colorbar in ax_hi coords (left, bottom, width, height); left≈1 gaps from map edge. constrained_layout ignores colorbar `pad`.
MAP_CBAR_INSET_AXES = (1.05, 0.075, 0.02, 0.85)
# Single GridSpec: use the same relative spacing and pad for rows and columns (uniform gutters).
CL_REL_SPACE = 0.028
CL_PAD_IN = 1.25 / 72.0
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.cloud_field_overview import (  # noqa: E402
    DEFAULT_TERRAIN_LIMS,
    DEFAULT_YLIM_GLOB,
    crop_extpar_to_shared_bbox,
    _resolve_extpar_paths,
)
from utilities.model_helpers import (  # noqa: E402
    COORDINATES_OF_ERISWIL,
    relative_humidity_wrt_water_percent,
)
from utilities.plotting import get_extpar_data  # noqa: E402
from utilities.style_profiles import (  # noqa: E402
    FULL_COL_IN,
    MAX_H_IN,
    MM,
    apply_publication_style,
)


def _load_paths_cfg(config_path: Path | None) -> dict:
    if config_path is None or not config_path.is_file():
        return {}
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _temperature_celsius(T: xr.DataArray) -> xr.DataArray:
    units = str(T.attrs.get("units", "")).lower()
    if "celsius" in units or "℃" in units or "°c" in units:
        return T
    tmax = float(np.nanmax(np.asarray(T.values)))
    if tmax > 120.0:
        return T - 273.15
    return T


def _circle_with_x_marker_path() -> MplPath:
    """Closed unit-ish circle plus an '×' for plot/legend markers (stroke only)."""
    n = 36
    t = np.linspace(0, 2 * np.pi, n, endpoint=False)
    verts: list[tuple[float, float]] = [(float(np.cos(ti)), float(np.sin(ti))) for ti in t]
    codes = [MplPath.MOVETO] + [MplPath.LINETO] * (n - 2) + [MplPath.CLOSEPOLY]
    d = 0.48
    verts.extend([(-d, -d), (d, d), (-d, d), (d, -d)])
    codes.extend([MplPath.MOVETO, MplPath.LINETO, MplPath.MOVETO, MplPath.LINETO])
    return MplPath(verts, codes)  # type: ignore[arg-type]


def _plot_circle_x(
    ax: plt.Axes,
    lon: float,
    lat: float,
    *,
    edgecolor: str,
    markersize: float = 6.5,
    markeredgewidth: float = 0.85,
    zorder: int = 6,
) -> None:
    ax.plot(
        lon,
        lat,
        linestyle="None",
        marker=_circle_with_x_marker_path(),
        markersize=markersize,
        markerfacecolor="none",
        markeredgecolor=edgecolor,
        markeredgewidth=markeredgewidth,
        clip_on=True,
        zorder=zorder,
    )


def _panel_label_for_index(i: int) -> str:
    """Return 'a)', 'b)', … 'z)', then 'aa)', 'ab)', … for large composite figures."""
    abc = string.ascii_lowercase
    if i < 26:
        return f"{abc[i]})"
    i -= 26
    return f"{abc[i // 26]}{abc[i % 26]})"


def _draw_panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(
        0.035,
        0.91,
        label,
        transform=ax.transAxes,
        fontsize=8,
        fontweight="bold",
        va="top",
        ha="left",
        clip_on=False,
        zorder=30,
        bbox=dict(
            boxstyle="square,pad=0.28",
            facecolor="white",
            edgecolor="none",
            alpha=0.5,
        ),
    )


def _vertical_arrays_per_exp(
    da: xr.DataArray,
    *,
    n_stations: int,
    n_exp: int,
    has_exp: bool,
) -> list[list[np.ndarray]]:
    """Per-station list of length n_exp: full- or half-level heights (or one field) per experiment."""
    rows: list[list[np.ndarray]] = []
    for i in range(n_stations):
        per_e: list[np.ndarray] = []
        for e in range(n_exp):
            if has_exp and "expname" in da.dims:
                per_e.append(np.asarray(da.isel(station=i, expname=e).values, dtype=float))
            elif "station" in da.dims:
                z = np.asarray(da.isel(station=i).values, dtype=float)
                per_e.append(z)
            else:
                per_e.append(np.asarray(da.values, dtype=float))
        if not (has_exp and "expname" in da.dims) and n_exp > 1:
            z0 = per_e[0]
            per_e = [z0.copy() for _ in range(n_exp)]
        rows.append(per_e)
    return rows


def _interp_half_to_full_levels(
    values_half: np.ndarray,
    z_half: np.ndarray,
    z_full: np.ndarray,
) -> np.ndarray:
    zh = np.asarray(z_half, dtype=float)
    vh = np.asarray(values_half, dtype=float)
    zf = np.asarray(z_full, dtype=float)
    order = np.argsort(zh)
    zh, vh = zh[order], vh[order]
    return np.interp(zf, zh, vh, left=np.nan, right=np.nan)


def _interp_profile_to_reference(
    z_src: np.ndarray,
    x_src: np.ndarray,
    z_ref: np.ndarray,
) -> np.ndarray:
    zs = np.asarray(z_src, dtype=float)
    xs = np.asarray(x_src, dtype=float)
    zr = np.asarray(z_ref, dtype=float)
    good = np.isfinite(zs) & np.isfinite(xs)
    if np.count_nonzero(good) < 2:
        return np.full_like(zr, np.nan, dtype=float)
    zs = zs[good]
    xs = xs[good]
    order = np.argsort(zs)
    zs = zs[order]
    xs = xs[order]
    return np.interp(zr, zs, xs, left=np.nan, right=np.nan)


def _spread_bounds(
    stack: np.ndarray,
    mode: str,
) -> tuple[np.ndarray, np.ndarray, str]:
    label = _spread_label(mode)
    valid = np.any(np.isfinite(stack), axis=0)
    lo = np.full(stack.shape[1], np.nan, dtype=float)
    hi = np.full(stack.shape[1], np.nan, dtype=float)
    if np.any(valid):
        data = stack[:, valid]
        if mode == "minmax":
            lo[valid] = np.nanmin(data, axis=0)
            hi[valid] = np.nanmax(data, axis=0)
        elif mode == "p10-p90":
            lo[valid] = np.nanpercentile(data, 10.0, axis=0)
            hi[valid] = np.nanpercentile(data, 90.0, axis=0)
            label = "ensemble p10-p90"
        else:
            lo[valid] = np.nanpercentile(data, 25.0, axis=0)
            hi[valid] = np.nanpercentile(data, 75.0, axis=0)
            label = "ensemble p25-p75"
    return lo, hi, label


def _spread_label(mode: str) -> str:
    if mode == "minmax":
        return "ensemble min-max"
    return f"ensemble {mode}"


def _draw_orography_maps(
    fig,
    gs_maps_low,
    gs_maps_high,
    *,
    lon_low,
    lat_low,
    hsurf_low,
    lon_high,
    lat_high,
    hsurf_high,
    station_lat,
    station_lon,
    terrain_vmin: float,
    terrain_vmax: float,
) -> tuple[plt.Axes, plt.Axes, plt.cm.ScalarMappable]:
    ax_lo = fig.add_subplot(gs_maps_low)
    ax_hi = fig.add_subplot(gs_maps_high)
    mappable = None
    lon_lo, lon_hi = np.min(lon_low), np.max(lon_high)
    lat_lo, lat_hi = np.min(lat_low), np.max(lat_high)
    for ax, lon, lat, hsurf, ylabel in [
        (ax_lo, lon_low, lat_low, hsurf_low, "Latitude / (° N)"),
        (ax_hi, lon_high, lat_high, hsurf_high, ""),
    ]:
        mappable = ax.pcolormesh(
            lon,
            lat,
            hsurf,
            cmap="terrain",
            vmin=terrain_vmin,
            vmax=terrain_vmax,
            shading="auto",
            rasterized=True,
        )
        ax.set_aspect("equal")
        ax.set_xlabel("Longitude / (° E)")
        ax.set_ylabel(ylabel)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.tick_params(top=True, right=True, labeltop=False, labelright=False)
        ax.set_xlim(lon_lo, lon_hi)
        ax.set_ylim(lat_lo, lat_hi)

    n_st = len(station_lat)
    for i in range(n_st):
        slat = float(station_lat[i])
        slon = float(station_lon[i])
        for ax in (ax_lo, ax_hi):
            ax.plot(slon, slat, "o", ms=5.5, mfc="none", mec="black", mew=0.7, zorder=5)
            ax.annotate(
                f"S{i + 1}",
                (slon, slat),
                textcoords="offset points",
                xytext=(4, 4),
                fontsize=6.5,
                fontweight="bold",
            )

    seed_lat, seed_lon = COORDINATES_OF_ERISWIL["seeding"]
    obs_lat, obs_lon = COORDINATES_OF_ERISWIL["eriswil"]
    for ax in (ax_lo, ax_hi):
        _plot_circle_x(ax, seed_lon, seed_lat, edgecolor="red")
        _plot_circle_x(ax, obs_lon, obs_lat, edgecolor="blue")

    cx_mark = _circle_with_x_marker_path()
    handles = [
        Line2D(
            [],
            [],
            linestyle="None",
            marker="o",
            markersize=5.5,
            markerfacecolor="none",
            markeredgecolor="black",
            markeredgewidth=0.7,
            label="meteogram columns",
        ),
        Line2D(
            [],
            [],
            linestyle="None",
            marker=cx_mark,
            markersize=6.5,
            markerfacecolor="none",
            markeredgecolor="red",
            markeredgewidth=0.85,
            label="seeding",
        ),
        Line2D(
            [],
            [],
            linestyle="None",
            marker=cx_mark,
            markersize=6.5,
            markerfacecolor="none",
            markeredgecolor="blue",
            markeredgewidth=0.85,
            label="Eriswil obs.",
        ),
    ]
    ax_hi.legend(handles=handles, loc="upper right", frameon=False, fontsize=6)

    return ax_lo, ax_hi, mappable


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zarr",
        type=Path,
        required=True,
        help="Path to LV2 meteogram zarr store.",
    )
    parser.add_argument(
        "--plot-start",
        type=str,
        default=None,
        help="Start time (UTC), e.g. 2023-01-25T12:00:00. Mean over [start,end].",
    )
    parser.add_argument("--plot-end", type=str, default=None, help="End time (UTC), inclusive slice end.")
    parser.add_argument("--bin-start", type=int, default=30, help="First bin index for QW/QFW sum (inclusive).")
    parser.add_argument(
        "--spread-mode",
        choices=("minmax", "p10-p90", "p25-p75"),
        default="minmax",
        help="Spread statistic for the ensemble band in the lower profile panels.",
    )
    parser.add_argument("--ylim-glob", type=float, nargs=2, default=list(DEFAULT_YLIM_GLOB), help="Height axis limits (m a.m.s.l.).")
    parser.add_argument(
        "--terrain-lims",
        type=float,
        nargs=2,
        default=list(DEFAULT_TERRAIN_LIMS),
        help="Surface height colour scale limits for maps.",
    )
    parser.add_argument("--extpar-low", type=Path, default=None)
    parser.add_argument("--extpar-high", type=Path, default=None)
    parser.add_argument(
        "--paths-config",
        type=Path,
        default=REPO_ROOT / "config" / "publication_figures.yaml",
        help="YAML with paths.server_root for ExtPar search on servers (optional).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=f"Output PNG path. Default: {DEFAULT_GFX_PNG_01}/station_column_overview_<zarr_stem>.png",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=None,
        help="Raster DPI for PNG (default: rcParams['savefig.dpi'] after publication style).",
    )
    args = parser.parse_args()

    apply_publication_style()
    save_dpi = int(args.dpi if args.dpi is not None else mpl.rcParams["savefig.dpi"])

    cfg_paths = _load_paths_cfg(args.paths_config.expanduser().resolve())
    extpar_low, extpar_high = _resolve_extpar_paths(
        cfg_paths,
        args.extpar_low,
        args.extpar_high,
    )
    lat_low, lon_low, hsurf_low = get_extpar_data(str(extpar_low))
    lat_high, lon_high, hsurf_high = get_extpar_data(str(extpar_high))
    lat_low, lon_low, hsurf_low, lat_high, lon_high, hsurf_high = crop_extpar_to_shared_bbox(
        lat_low, lon_low, hsurf_low, lat_high, lon_high, hsurf_high
    )

    ds = xr.open_zarr(args.zarr.expanduser().resolve())

    plot_start = np.datetime64(args.plot_start) if args.plot_start else None
    plot_end = np.datetime64(args.plot_end) if args.plot_end else None
    if plot_start is not None or plot_end is not None:
        ds_t = ds.sel(time=slice(plot_start, plot_end))
    else:
        ds_t = ds
    if ds_t.sizes.get("time", 0) == 0:
        raise ValueError("No time steps in selected range.")
    ds_m = ds_t.mean(dim="time", skipna=True)

    has_exp = "expname" in ds_m.dims
    n_exp = int(ds_m.sizes["expname"]) if has_exp else 1

    slat_da = ds_m["station_lat"]
    slon_da = ds_m["station_lon"]
    if has_exp and "expname" in slat_da.dims:
        station_lat = slat_da.isel(expname=0).values
        station_lon = slon_da.isel(expname=0).values
    else:
        station_lat = slat_da.values
        station_lon = slon_da.values
    n_stations = int(ds_m.sizes["station"])

    qw_b = ds_m["QW"].isel(bins=slice(args.bin_start, None)).sum(dim="bins")
    qfw_b = ds_m["QFW"].isel(bins=slice(args.bin_start, None)).sum(dim="bins")
    rho = ds_m["RHO"]
    qw_gm3 = (qw_b * rho * 1.0e3).load()
    qfw_gm3 = (qfw_b * rho * 1.0e3).load()
    T_c = _temperature_celsius(ds_m["T"]).load()
    rh = relative_humidity_wrt_water_percent(ds_m["QV"], ds_m["T"], ds_m["PML"]).load()
    hml_da = ds_m["HMLd"].load()
    hhl_da = ds_m["HHLd"].load()
    hml_st_exp = _vertical_arrays_per_exp(hml_da, n_stations=n_stations, n_exp=n_exp, has_exp=has_exp)
    hhl_st_exp = _vertical_arrays_per_exp(hhl_da, n_stations=n_stations, n_exp=n_exp, has_exp=has_exp)

    # Archive TKE is a velocity scale (m/s); turbulent kinetic energy as usually reported: TKE_energy = 2 * TKE^2 (m²/s²).
    tke = (2.0 * (ds_m["TKE"] ** 2)).load()
    w_field = ds_m["W"].load()

    y0, y1 = float(args.ylim_glob[0]), float(args.ylim_glob[1])
    profile_row0 = MAP_SPEC_ROWS + 1
    nrows = profile_row0 + n_stations
    height_ratios = [1.0] * MAP_SPEC_ROWS + [MAP_PROFILE_GAP_RATIO] + [1.0] * n_stations
    fig_h = min((54 + 20 * n_stations) * MM, MAX_H_IN)
    # figsize in inches × save_dpi ≈ pixel canvas; keep figure.dpi in sync for Agg rasterization.
    mpl.rcParams["figure.dpi"] = float(save_dpi)
    fig = plt.figure(figsize=(FULL_COL_IN * 1.25, fig_h), constrained_layout=True, dpi=save_dpi)
    fig.set_constrained_layout_pads(
        w_pad=CL_PAD_IN,
        h_pad=CL_PAD_IN,
        wspace=CL_REL_SPACE,
        hspace=CL_REL_SPACE,
    )
    gs = GridSpec(nrows, 6, figure=fig, height_ratios=height_ratios)

    ax_lo, ax_hi, terr_mappable = _draw_orography_maps(
        fig,
        gs[0:MAP_SPEC_ROWS, 0:MAP_SPEC_COLS_EACH],
        gs[0:MAP_SPEC_ROWS, MAP_SPEC_COLS_EACH : MAP_SPEC_COLS_EACH * 2],
        lon_low=lon_low,
        lat_low=lat_low,
        hsurf_low=hsurf_low,
        lon_high=lon_high,
        lat_high=lat_high,
        hsurf_high=hsurf_high,
        station_lat=station_lat,
        station_lon=station_lon,
        terrain_vmin=float(args.terrain_lims[0]),
        terrain_vmax=float(args.terrain_lims[1]),
    )
    cbar_ax = ax_hi.inset_axes(
        MAP_CBAR_INSET_AXES,
        transform=ax_hi.transAxes,
        clip_on=False,
    )
    fig.colorbar(terr_mappable, cax=cbar_ax, label="Surface height / (m a.m.s.l.)", pad=0.12)

    prof_axes: list[list[plt.Axes]] = []
    legend_handles: list[Line2D | Patch] | None = None
    for row, st in enumerate(range(n_stations)):
        r = profile_row0 + row

        row_axes: list[plt.Axes] = []
        for col in range(6):
            if row == 0:
                ax = fig.add_subplot(gs[r, col])
            else:
                ax = fig.add_subplot(gs[r, col], sharey=prof_axes[0][col])
            row_axes.append(ax)
        prof_axes.append(row_axes)

        ax0, ax1, ax_t, ax_rh, ax_tke, ax_w = row_axes
        prof_data: list[list[np.ndarray]] = [[] for _ in range(6)]
        z_ml_ref = np.asarray(hml_st_exp[st][0], dtype=float)
        profile_colors = ("#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#17becf")
        for ei in range(n_exp):
            z_ml = np.asarray(hml_st_exp[st][ei], dtype=float)
            z_hl = np.asarray(hhl_st_exp[st][ei], dtype=float)
            if has_exp:
                tke_1d = tke.isel(station=st, expname=ei).values
                w_1d = w_field.isel(station=st, expname=ei).values
                x_qw = np.asarray(qw_gm3.isel(station=st, expname=ei).values, dtype=float)
                x_qfw = np.asarray(qfw_gm3.isel(station=st, expname=ei).values, dtype=float)
                x_t = np.asarray(T_c.isel(station=st, expname=ei).values, dtype=float)
                x_rh = np.asarray(rh.isel(station=st, expname=ei).values, dtype=float)
            else:
                tke_1d = tke.isel(station=st).values
                w_1d = w_field.isel(station=st).values
                x_qw = np.asarray(qw_gm3.isel(station=st).values, dtype=float)
                x_qfw = np.asarray(qfw_gm3.isel(station=st).values, dtype=float)
                x_t = np.asarray(T_c.isel(station=st).values, dtype=float)
                x_rh = np.asarray(rh.isel(station=st).values, dtype=float)
            tke_ml = np.asarray(_interp_half_to_full_levels(tke_1d, z_hl, z_ml), dtype=float).copy()
            if tke_ml.size > 1:
                tke_ml[0] = np.nan
            if tke_ml.size > 2:
                tke_ml[-1] = np.nan
            w_ml = _interp_half_to_full_levels(w_1d, z_hl, z_ml)
            prof_data[0].append(_interp_profile_to_reference(z_ml, x_qw, z_ml_ref))
            prof_data[1].append(_interp_profile_to_reference(z_ml, x_qfw, z_ml_ref))
            prof_data[2].append(_interp_profile_to_reference(z_ml, x_t, z_ml_ref))
            prof_data[3].append(_interp_profile_to_reference(z_ml, x_rh, z_ml_ref))
            prof_data[4].append(_interp_profile_to_reference(z_ml, tke_ml, z_ml_ref))
            prof_data[5].append(_interp_profile_to_reference(z_ml, w_ml, z_ml_ref))

            if has_exp:
                c0, c1, ct, crh, ctke, cw = profile_colors
                ax0.plot(x_qw, z_ml, color=c0, linewidth=0.7, alpha=0.45, zorder=2)
                ax1.plot(x_qfw, z_ml, color=c1, linewidth=0.7, alpha=0.45, zorder=2)
                ax_t.plot(x_t, z_ml, color=ct, linewidth=0.7, alpha=0.45, zorder=2)
                ax_rh.plot(x_rh, z_ml, color=crh, linewidth=0.7, alpha=0.45, zorder=2)
                ax_tke.plot(np.asarray(tke_ml, dtype=float), z_ml, color=ctke, linewidth=0.7, alpha=0.45, zorder=2)
                ax_w.plot(np.asarray(w_ml, dtype=float), z_ml, color=cw, linewidth=0.7, alpha=0.45, zorder=2)

        profile_axes = (ax0, ax1, ax_t, ax_rh, ax_tke, ax_w)
        for ax, profiles, color in zip(profile_axes, prof_data, profile_colors, strict=True):
            stack = np.asarray(profiles, dtype=float)
            if n_exp > 1:
                mean_prof = np.full(z_ml_ref.shape, np.nan, dtype=float)
                valid = np.any(np.isfinite(stack), axis=0)
                if np.any(valid):
                    mean_prof[valid] = np.nanmean(stack[:, valid], axis=0)
                spread_lo, spread_hi, _ = _spread_bounds(stack, args.spread_mode)
                ax.fill_betweenx(z_ml_ref, spread_lo, spread_hi, facecolor=color, alpha=0.35, zorder=3)
                ax.plot(mean_prof, z_ml_ref, color=color, linewidth=1.15, zorder=4)
            else:
                ax.plot(stack[0], z_ml_ref, color=color, linewidth=1.0, zorder=4)

        if row == 0 and n_exp > 1:
            leg_c = profile_colors[0]
            legend_handles = [
                Line2D([], [], color=leg_c, linewidth=0.7, alpha=0.45, label="members"),
                Patch(facecolor=leg_c, edgecolor="none", alpha=0.35, label=_spread_label(args.spread_mode)),
                Line2D([], [], color=leg_c, linewidth=1.15, label="ensemble mean"),
            ]

        if row == 0:
            for ax in row_axes:
                ax.set_ylim(y0, y1)
        for ax in row_axes:
            ax.grid(True, alpha=0.22, linewidth=0.5)
            ax.axhline(y0, color="0.5", linewidth=0.35)
            ax.axhline(y1, color="0.5", linewidth=0.35)
            ax.tick_params(axis="x", top=True, bottom=True, labelsize=6)
        if st < n_stations - 1:
            for ax in row_axes:
                ax.set_xticklabels([])

        if st == n_stations - 1:
            ax0.set_xlabel(r"liquid mass / (g m$^{-3}$)")
            ax1.set_xlabel(r"ice mass / (g m$^{-3}$)")
            ax_t.set_xlabel(r"air temperature / ($^\circ$C)")
            ax_rh.set_xlabel(r"relative humidity / (\%)")
            ax_tke.set_xlabel(r"TKE / (m$^2$ s$^{-2}$)")
            ax_w.set_xlabel(r"W / (m s$^{-1}$)")

    row_y_label = min(n_stations - 1, n_stations // 2)
    prof_axes[row_y_label][0].set_ylabel("height / (m a.m.s.l.)", fontsize=6)
    last_col = 2*MAP_SPEC_COLS_EACH - 1
    for st in range(n_stations):
        for col, ax in enumerate(prof_axes[st]):
            if col == 0:
                ax.tick_params( axis="y", left=True, right=True, labelleft=True, labelright=False, labelsize=6 )
            elif col == last_col:
                ax.tick_params( axis="y", left=True, right=True, labelleft=False, labelright=False, labelsize=6 )
                ax.yaxis.set_label_position("right")
                ax.set_ylabel("S" + str(st + 1), fontsize=8, rotation=0, ha="left", va="center", fontweight="bold")
            else:
                ax.tick_params( axis="y", left=True, right=True, labelleft=False, labelright=False, labelsize=6 )

    if legend_handles is not None:
        fig.canvas.draw()
        map_bottom = min(ax_lo.get_position().y0, ax_hi.get_position().y0)
        prof_top = max(ax.get_position().y1 for ax in prof_axes[0])
        legend_y = 0.47 * (map_bottom + prof_top)
        fig.legend(
            handles=legend_handles,
            loc="center",
            bbox_to_anchor=(0.5, legend_y),
            ncol=3,
            frameon=False,
            fontsize=6,
            handlelength=1.8,
            columnspacing=1.4,
        )

    panel_i = 0
    _draw_panel_label(ax_lo, _panel_label_for_index(panel_i))
    panel_i += 1
    _draw_panel_label(ax_hi, _panel_label_for_index(panel_i))
    panel_i += 1
    for st in range(n_stations):
        for col in range(6):
            _draw_panel_label(prof_axes[st][col], _panel_label_for_index(panel_i))
            panel_i += 1

    out = args.output
    if out is None:
        stem = args.zarr.resolve().stem
        out = DEFAULT_GFX_PNG_01 / f"station_column_overview_{stem}.png"
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    save_kw: dict = {
        "dpi": save_dpi,
        "bbox_inches": "tight",
        "facecolor": fig.get_facecolor(),
        "edgecolor": "none",
    }
    if out.suffix.lower() in (".png", ".jpg", ".jpeg", ".tif", ".tiff"):
        save_kw["pil_kwargs"] = {"compress_level": 3}
    fig.savefig(out, **save_kw)
    w_in, h_in = fig.get_size_inches()
    print(
        f"saved -> {out.resolve().as_uri()} "
        f"({save_dpi} dpi, ~{w_in * save_dpi:.0f}×{h_in * save_dpi:.0f} px raster)"
    )


if __name__ == "__main__":
    main()
