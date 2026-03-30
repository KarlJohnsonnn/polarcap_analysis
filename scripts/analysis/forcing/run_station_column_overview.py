#!/usr/bin/env python3
"""Publication layout: dual orography maps (each 3×3 GridSpec cells) + per-station columns (6 wide) from LV2 zarr.

Panels are lettered in reading order: **a)** and **b)** are the coarse- and fine-grid orography; each subsequent
station row adds six panels (profiles left to right) in alphabetical sequence (**c)**–**h)** for the first row, etc.).

Suggested figure caption (fill bracketed items for the manuscript):

    Figure X. **(a,b)** Model surface height (m above mean sea level; colour scale as inset colour bar) for the
    **coarse-** and **fine-resolution** outer-domain meshes over the Eriswil area on a **common map extent** (ExtPar).
    Open circles mark meteogram column locations (S1–Sn); red and blue symbols mark the **seeding** site and the
    **Eriswil** observatory. **(c–…)** Time-mean vertical columns from the level-2 meteogram archive: for each row,
    mass-based full-level height *H* (m a.m.s.l.) versus **liquid** and **mixed-phase** hydrometeor mass density
    (g m⁻³; sum over bins from the selected lower bin index times air density), **air temperature** (°C), **relative
    humidity with respect to liquid water**, **turbulent kinetic energy**, and **vertical velocity** (half-level fields
    linearly interpolated to full levels). **TKE** is plotted as **2 TKE²** in **m² s⁻²**, where the archive field is a
    **m s⁻¹** turbulent-velocity scale. Time averaging is over **[UTC range or full meteogram period]**; if an
    **expname** dimension is present, **member [k]** is shown.

Example:
--zarr /work/bb1178/schimmel/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/cs-eriswil__20260328_205320/lv2_meteogram/Meteogram_cs-eriswil__20260328_205320_nVar136_nMet3_nExp15.zarr \

    python scripts/analysis/forcing/run_station_column_overview.py \
        --zarr /work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output/cs-eriswil__20260304_110254/Meteogram_cs-eriswil__20260304_110254_nVar136_nMet3_nExp5.zarr/ \
        --exp-idx 0 \
        --extpar-low "$CS_RUNS_DIR/extPar_Eriswil_50x40.nc" \
        --extpar-high "$CS_RUNS_DIR/extPar_Eriswil_200x160.nc" \
        --plot-start "2023-01-25T12:25:00" \
        --plot-end "2023-01-25T13:05:00"
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
from matplotlib.path import Path as MplPath

REPO_ROOT = Path(__file__).resolve().parents[3]
POLARCAP_ROOT = Path(os.environ.get("POLARCAP_ROOT", REPO_ROOT))
DEFAULT_GFX_PNG_01 = POLARCAP_ROOT / "output" / "gfx" / "png" / "01"
# Each orography panel spans this many rows × 3 columns in the master 6-column GridSpec.
MAP_SPEC_ROWS = 4
MAP_SPEC_COLS_EACH = 3
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
        ),
    )


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
    parser.add_argument("--exp-idx", type=int, default=0, help="Index along expname dimension.")
    parser.add_argument(
        "--plot-start",
        type=str,
        default=None,
        help="Start time (UTC), e.g. 2023-01-25T12:00:00. Mean over [start,end].",
    )
    parser.add_argument("--plot-end", type=str, default=None, help="End time (UTC), inclusive slice end.")
    parser.add_argument("--bin-start", type=int, default=30, help="First bin index for QW/QFW sum (inclusive).")
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
    if "expname" in ds.dims:
        ds = ds.isel(expname=args.exp_idx)

    plot_start = np.datetime64(args.plot_start) if args.plot_start else None
    plot_end = np.datetime64(args.plot_end) if args.plot_end else None
    if plot_start is not None or plot_end is not None:
        ds_t = ds.sel(time=slice(plot_start, plot_end))
    else:
        ds_t = ds
    if ds_t.sizes.get("time", 0) == 0:
        raise ValueError("No time steps in selected range.")
    ds_m = ds_t.mean(dim="time", skipna=True)

    station_lat = ds_m["station_lat"].values
    station_lon = ds_m["station_lon"].values
    n_stations = int(ds_m.sizes["station"])

    qw_b = ds_m["QW"].isel(bins=slice(args.bin_start, None)).sum(dim="bins")
    qfw_b = ds_m["QFW"].isel(bins=slice(args.bin_start, None)).sum(dim="bins")
    rho = ds_m["RHO"]
    qw_gm3 = (qw_b * rho * 1.0e3).load()
    qfw_gm3 = (qfw_b * rho * 1.0e3).load()
    T_c = _temperature_celsius(ds_m["T"]).load()
    rh = relative_humidity_wrt_water_percent(ds_m["QV"], ds_m["T"], ds_m["PML"]).load()
    hml_da = ds_m["HMLd"].load()
    if "station" in hml_da.dims:
        hml_st = [np.asarray(hml_da.isel(station=i).values, dtype=float) for i in range(n_stations)]
    else:
        z_ml = np.asarray(hml_da.values, dtype=float)
        hml_st = [z_ml] * n_stations

    # Archive TKE is a velocity scale (m/s); turbulent kinetic energy as usually reported: TKE_energy = 2 * TKE^2 (m²/s²).
    tke = (2.0 * (ds_m["TKE"] ** 2)).load()
    w_field = ds_m["W"].load()
    hhl_da = ds_m["HHLd"].load()
    if "station" in hhl_da.dims:
        hhl_st = [np.asarray(hhl_da.isel(station=i).values, dtype=float) for i in range(n_stations)]
    else:
        z_hl = np.asarray(hhl_da.values, dtype=float)
        hhl_st = [z_hl] * n_stations

    tke_lbl = r"TKE / (m$^2$ s$^{-2}$)"
    w_lbl = "W"
    if "units" in ds_m["W"].attrs:
        w_lbl = f"W / ({ds_m['W'].attrs['units']})"

    y0, y1 = float(args.ylim_glob[0]), float(args.ylim_glob[1])
    nrows = MAP_SPEC_ROWS + n_stations
    fig_h = min((48 + 20 * n_stations) * MM, MAX_H_IN)
    # figsize in inches × save_dpi ≈ pixel canvas; keep figure.dpi in sync for Agg rasterization.
    mpl.rcParams["figure.dpi"] = float(save_dpi)
    fig = plt.figure(figsize=(FULL_COL_IN * 1.25, fig_h), constrained_layout=True, dpi=save_dpi)
    fig.set_constrained_layout_pads(
        w_pad=CL_PAD_IN,
        h_pad=CL_PAD_IN,
        wspace=CL_REL_SPACE,
        hspace=CL_REL_SPACE,
    )
    gs = GridSpec(nrows, 6, figure=fig)

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
    for row, st in enumerate(range(n_stations)):
        r = MAP_SPEC_ROWS + row
        z_ml = np.asarray(hml_st[st], dtype=float)
        z_hl = np.asarray(hhl_st[st], dtype=float)
        tke_1d = tke.isel(station=st).values
        w_1d = w_field.isel(station=st).values
        tke_ml = _interp_half_to_full_levels(tke_1d, z_hl, z_ml)
        w_ml = _interp_half_to_full_levels(w_1d, z_hl, z_ml)

        row_axes: list[plt.Axes] = []
        for col in range(6):
            if row == 0:
                ax = fig.add_subplot(gs[r, col])
            else:
                ax = fig.add_subplot(gs[r, col], sharey=prof_axes[0][col])
            row_axes.append(ax)
        prof_axes.append(row_axes)

        ax0, ax1, ax_t, ax_rh, ax_tke, ax_w = row_axes
        x_qw = np.asarray(qw_gm3.isel(station=st).values, dtype=float)
        x_qfw = np.asarray(qfw_gm3.isel(station=st).values, dtype=float)
        ax0.plot(x_qw, z_ml, color="#1f77b4", linewidth=0.9)
        ax1.plot(x_qfw, z_ml, color="#ff7f0e", linewidth=0.9)
        ax_t.plot(np.asarray(T_c.isel(station=st).values, dtype=float), z_ml, color="#2ca02c", linewidth=0.9)
        ax_rh.plot(np.asarray(rh.isel(station=st).values, dtype=float), z_ml, color="#d62728", linewidth=0.9)
        ax_tke.plot(np.asarray(tke_ml, dtype=float), z_ml, color="#9467bd", linewidth=0.9)
        ax_w.plot(np.asarray(w_ml, dtype=float), z_ml, color="#17becf", linewidth=0.9)

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
            ax1.set_xlabel(r"mixed-phase mass / (g m$^{-3}$)")
            ax_t.set_xlabel(r"air temperature / ($^\circ$C)")
            ax_rh.set_xlabel("relative humidity / (%)")
            ax_tke.set_xlabel(tke_lbl)
            ax_w.set_xlabel(w_lbl)

    row_y_label = min(n_stations - 1, n_stations // 2)
    prof_axes[row_y_label][0].set_ylabel("height / (m a.m.s.l.)", fontsize=6)
    last_col = 2*MAP_SPEC_COLS_EACH - 1
    for st in range(n_stations):
        for col, ax in enumerate(prof_axes[st]):
            if col == 0:
                ax.tick_params( axis="y", left=True, right=True, labelleft=True, labelright=False, labelsize=6 )
            elif col == last_col:
                ax.tick_params( axis="y", left=True, right=True, labelleft=False, labelright=True, labelsize=6 )
            else:
                ax.tick_params( axis="y", left=True, right=True, labelleft=False, labelright=False, labelsize=6 )

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
