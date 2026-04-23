"""Plume-lagrangian figure (notebook 03).

xarray-first heatmaps, HOLIMO overlay, growth fits, histogram, and optional progressive
presenter PNGs (``render_figure(..., presenter_mode=True)``). Replaces the removed legacy
``plume_lagrangian`` module for this view.

Units reminder
--------------
Model ``nf`` is already per litre. Heatmaps show ``<nf> / dln D`` after the
ensemble mean. HOLIMO PSDs come in cm^-3; we multiply by ``HOLIMO_CM3_TO_L1``
(1000) for overlay unless the variant has unknown units.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib import patheffects as pe
from matplotlib.axes import Axes
from matplotlib.cm import ScalarMappable
from matplotlib.colors import LogNorm
from matplotlib.figure import Figure
from matplotlib.legend_handler import HandlerTuple
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
from matplotlib.ticker import AutoMinorLocator, FixedLocator, FuncFormatter
from matplotlib.transforms import Bbox

from utilities.holimo_helpers import prepare_holimo_for_overlay
from utilities.plotting import create_fade_cmap, create_new_jet3, make_pastel
from utilities.plume_loader import load_plume_path_runs
from utilities.style_profiles import (
    FULL_COL_IN,
    MAX_H_IN,
    apply_publication_axis_tick_geometry,
    apply_publication_panel_axis,
)

xr.set_options(keep_attrs=True)

HOLIMO_CM3_TO_L1 = 1000.0

# Unknown-unit HOLIMO variants must NOT be rescaled (legacy convention).
_HOLIMO_UNKNOWN = {"Ice_PSDMnoNormMajsiz", "Ice_PSDlinNormMajsiz", "Ice_PSDlogNormMajsiz"}

# Panel C step spectra: same outer/inner treatment as ``_draw_fit`` (growth curves).
MODEL_SPECTRUM_COLOR = "#FFB343"
HOLIMO_SPECTRUM_COLOR = "royalblue"
_HIST_STEP_LW_OUT = 2.75
_HIST_STEP_LW_IN = 0.4

# ----- config ---------------------------------------------------------------
def _default_runs() -> list[dict[str, str]]:
    return [
        {"label": "400m, inp 1e6, ccn 0 (spherical)",      "cs_run": "cs-eriswil__20251125_114053", "exp_id": "20251125114238"},
        {"label": "400m, inp 1e6, ccn 400 (analytic)",     "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211431"},
        {"label": "400m, inp 1e6, ccn 400 (planar)",       "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211551"},
        {"label": "400m, inp 1e6, ccn 400 (spherical)",    "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131550"},
        {"label": "400m, inp 1e6, ccn 400 (columnar 2)",   "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131632"},
    ]


@dataclass
class FigCfg:
    processed_root: Path = Path("data/processed")
    holimo_file: Path = Path("data/observations/holimo_data/CL_20230125_1000_1140_SM058_SM060_ts1.nc")
    output: Path = Path("output/gfx/png/03/figure12_ensemble_mean_plume_path.png")

    runs: list[dict[str, str]] = field(default_factory=_default_runs)
    kinds: tuple[str, ...] = ("integrated", "vertical", "extreme")
    kind: str = "extreme"
    variable: str = "nf"

    model_seed: np.datetime64 = np.datetime64("2023-01-25T12:29:50")
    xlim: tuple[np.datetime64, np.datetime64] = (
        np.datetime64("2023-01-25T12:31:00"), np.datetime64("2023-01-25T13:14:00"),
    )
    time_window_holimo: tuple[np.datetime64, np.datetime64] = (
        np.datetime64("2023-01-25T10:10:00"), np.datetime64("2023-01-25T12:00:00"),
    )
    time_frames_plume: list[tuple[np.datetime64, np.datetime64]] = field(default_factory=lambda: [
        (np.datetime64("2023-01-25T10:56:00"), np.datetime64("2023-01-25T11:04:00")),
        (np.datetime64("2023-01-25T10:35:00"), np.datetime64("2023-01-25T10:42:00")),
        (np.datetime64("2023-01-25T11:24:00"), np.datetime64("2023-01-25T11:29:00")),
    ])
    seeding_start_times: list[np.datetime64] = field(default_factory=lambda: [
        np.datetime64("2023-01-25T10:50:00"),
        np.datetime64("2023-01-25T10:28:00"),
        np.datetime64("2023-01-25T11:15:00"),
    ])
    obs_ids: tuple[str, ...] = ("SM059", "SM058", "SM060")

    # Panel ranges: (xlim_min, ylim_um, zlim).
    top_xlim: tuple[float, float] = (0.01, 35.0)
    top_ylim: tuple[float, float] = (5.0, 600.0)
    zoom_xlim: tuple[float, float] = (5.0, 16.0)
    zoom_ylim: tuple[float, float] = (30.0, 325.0)   # also fit cutoff (diameter_max).
    hist_xlim: tuple[float, float] = (20.0, 280.0)
    hist_ylim: tuple[float, float] = (1.0, 1500.0)
    zlim: tuple[float, float] = (1.0, 3000.0)

    holimo_var: str = "Ice_PSDlogNorm"
    threshold: float = 1.0e-10
    model_diameter_smoothing_bins: int = 0

    mission_markers: tuple[str, ...] = ("o", "^", "*")
    mission_msizes: tuple[float, ...] = (30.0, 30.0, 30.0)
    mission_colors: tuple[str, ...] = ("#0072B2", "#D55E00", "#009E73")
    envelope_pct: tuple[float, float] = (10.0, 90.0)
    sublinear_alphas: tuple[float, ...] = (0.25, 0.5, 0.75, 1.0, 1.25)
    all_alpha: float = 0.75
    zoom_alpha: float = 0.35
    n_timeline: int = 200


# Initialization of the figure configuration.
DEFAULT_CFG = FigCfg()


# ----- small utilities ------------------------------------------------------
def holimo_scale(var: str) -> float:
    return 1.0 if var in _HOLIMO_UNKNOWN else HOLIMO_CM3_TO_L1


def _ensemble_label_parts(unit_m: str) -> tuple[str, str]:
    """Build colorbar / histogram text from ensemble label parts (legacy-compatible)."""
    dnd = r"$\mathrm{d}n_f/\mathrm{d}\ln D$"
    u = str(unit_m).strip()
    if u == "L-1":
        return dnd, r"$L^{-1}$"
    if "L-1" in u:
        return dnd, "$" + u.replace("L-1", r"L^{-1}") + "$"
    return dnd, u


def _log_edges(centers: np.ndarray) -> np.ndarray:
    c = np.unique(np.sort(centers[np.isfinite(centers) & (centers > 0)]))
    lc = np.log(c)
    e = np.empty(c.size + 1)
    e[1:-1] = 0.5 * (lc[:-1] + lc[1:])
    e[0]    = lc[0]  - 0.5 * (lc[1] - lc[0])
    e[-1]   = lc[-1] + 0.5 * (lc[-1] - lc[-2])
    return np.exp(e)


def _elapsed(da: xr.DataArray, t0: np.datetime64) -> xr.DataArray:
    mins = (da.time - t0) / np.timedelta64(1, "m")
    return da.assign_coords(time_elapsed=("time", mins.values))


def _float_fmt(x: float, _pos: int = 0) -> str:
    return f"{x:.3f}".rstrip("0").rstrip(".") if x < 1 else f"{x:.0f}"


def _weighted_mean_D(da: xr.DataArray) -> tuple[np.ndarray, np.ndarray]:
    """Concentration-weighted geometric mean diameter per time step."""
    extra = [d for d in da.dims if d not in ("time", "diameter")]
    if extra:
        da = da.mean(dim=extra, skipna=True)
    w = da.where(np.isfinite(da) & (da > 0), 0.0)
    log_d = np.log(da["diameter"])
    den = w.sum("diameter")
    D_mean = np.exp((w * log_d).sum("diameter") / den.where(den > 0))
    t = np.asarray(da["time_elapsed"].values, dtype=float)
    Dm = np.asarray(D_mean, dtype=float)
    ok = np.isfinite(t) & np.isfinite(Dm) & (Dm > 0)
    return t[ok], Dm[ok]


# ----- ensemble assembly ----------------------------------------------------
def build_ensemble(
    datasets: dict[str, dict[str, xr.Dataset]], *, variable: str,
    smooth_bins: int, reindex_freq: str = "10s",
) -> dict[str, dict[str, xr.Dataset]]:
    """Mean across runs on a common grid, divided by dln D; optional rolling smoothing."""
    kinds = {k for run in datasets.values() for k in run}
    ens: dict[str, xr.Dataset] = {}
    for kind in kinds:
        runs = [r[kind] for r in datasets.values()
                if isinstance(r.get(kind), xr.Dataset) and variable in r[kind]]
        if not runs:
            continue
        t_min = min(r[variable].time.values.min() for r in runs)
        t_max = max(r[variable].time.values.max() for r in runs)
        common_t = pd.date_range(t_min, t_max, freq=reindex_freq)

        das = []
        for ds in runs:
            da = ds[variable]
            if "cell" in da.dims:
                da = da.sum("cell", skipna=True)
            if kind == "vertical" and "altitude" in da.dims:
                da = da.mean("altitude", skipna=True)
            das.append(da.reindex(time=common_t, method="nearest", tolerance="5s").fillna(0.0))

        da_mean = xr.concat(das, dim="run").mean("run", keep_attrs=True)
        first = runs[0]
        edges = (first["diameter_edges"].values if "diameter_edges" in first
                 else _log_edges(np.asarray(da_mean.diameter.values, dtype=float)))
        dlnD = xr.DataArray(np.log(edges[1:]) - np.log(edges[:-1]), dims="diameter")
        da_mean = da_mean / dlnD
        if smooth_bins > 1:
            da_mean = da_mean.rolling(diameter=smooth_bins, center=True, min_periods=1).mean()
        da_mean.attrs = das[0].attrs.copy()
        ens[kind] = xr.Dataset(
            {variable: da_mean,
             "diameter_edges": xr.DataArray(edges, dims="diameter_edge")},
            attrs=first.attrs,
        )
    return {"Ensemble Mean": ens}


# ----- HOLIMO flatten for scatter overlay -----------------------------------
def _holimo_scatter_points(
    da: xr.DataArray, cfg: FigCfg,
) -> pd.DataFrame:
    """Flatten all missions to (mission, elapsed_min, diameter, value), dedupe by max."""
    frames = []
    scale = holimo_scale(cfg.holimo_var)
    for m, ((t_lo, t_hi), seed) in enumerate(zip(cfg.time_frames_plume, cfg.seeding_start_times)):
        sel = da.sel(time=slice(t_lo, t_hi))
        if sel.time.size == 0:
            continue
        vals = sel.values.astype(float) * scale
        diam = sel["diameter"].values.astype(float)
        elapsed = ((sel["time"].values - np.datetime64(seed)) / np.timedelta64(1, "m")).astype(float)
        X, Y = np.meshgrid(elapsed, diam, indexing="ij")
        ok = np.isfinite(vals) & (vals > cfg.threshold) & (X >= 0)
        if ok.any():
            frames.append(pd.DataFrame({"x": X[ok], "y": Y[ok], "c": vals[ok], "m": m}))
    if not frames:
        return pd.DataFrame(columns=["x", "y", "c", "m"])
    df = pd.concat(frames, ignore_index=True).sort_values("c")
    return df.drop_duplicates(subset=["x", "y"], keep="last")


def holimo_scatter_elapsed_extent_in_zoom(df_pts: pd.DataFrame, cfg: FigCfg) -> tuple[float, float]:
    """First/last HOLIMO scatter x (elapsed min) inside zoom xlim; panel-B guide lines for hist window."""
    if df_pts is None or getattr(df_pts, "empty", True):
        return cfg.zoom_xlim[0], cfg.zoom_xlim[1]
    sub = df_pts[(df_pts["x"] >= cfg.zoom_xlim[0]) & (df_pts["x"] <= cfg.zoom_xlim[1])]
    if sub.empty:
        return cfg.zoom_xlim[0], cfg.zoom_xlim[1]
    return float(sub["x"].min()), float(sub["x"].max())


# ----- fit helpers ----------------------------------------------------------
def _draw_fit(
    ax: Axes, x, y, *, color: str, zorder: int, label: str | None = None,
    lw_outer: float = 2.1, lw_inner: float = 0.35,
) -> tuple[Line2D, Line2D]:
    """Composite line: outer colored band + inner white dashed, each haloed."""
    halo_out = pe.withStroke(linewidth=lw_outer*1.125, foreground="white", alpha=0.75)
    halo_in  = pe.withStroke(linewidth=lw_inner*1.65, foreground="white", alpha=0.75)
    (lo,) = ax.plot(x, y, color=color, lw=lw_outer, alpha=0.7, zorder=zorder,
                    solid_capstyle="round", path_effects=[halo_out])
    (li,) = ax.plot(x, y, color="black", lw=lw_inner, alpha=1.0, zorder=zorder + 1,
                    dash_capstyle="round", path_effects=[halo_in], ls=(0, (8, 4)))
    if label is not None:
        ax.__dict__.setdefault("_fit_handles", []).append((label, (lo, li)))
    return lo, li


def _per_run_D_grid(
    datasets: dict[str, dict[str, xr.Dataset]], cfg: FigCfg, t_grid: np.ndarray,
) -> np.ndarray:
    rows = []
    for run in datasets.values():
        ds = run.get(cfg.kind)
        if not isinstance(ds, xr.Dataset) or cfg.variable not in ds:
            continue
        da = ds[cfg.variable]
        if "cell" in da.dims:
            da = da.sum("cell", skipna=True)
        da = da.sel(time=slice(*cfg.xlim))
        if da.sizes.get("time", 0) == 0:
            continue
        da = _elapsed(da, cfg.model_seed).sel(diameter=slice(*cfg.zoom_ylim))
        t_r, D_r = _weighted_mean_D(da)
        if t_r.size < 2:
            continue
        order = np.argsort(t_r)
        rows.append(np.exp(np.interp(t_grid, t_r[order], np.log(D_r[order]),
                                     left=np.nan, right=np.nan)))
    if not rows:
        raise ValueError("No per-run trajectories for envelope.")
    return np.vstack(rows)


# ----- panels (match ``plot_plume_path_sum`` elapsed axis + ``_style_panels``) ---
def _plume_path_intervals_dt(t0: np.datetime64, t1: np.datetime64) -> tuple[int, int]:
    dt = t1 - t0
    if dt < np.timedelta64(5, "m"):
        return 1, 30
    if dt < np.timedelta64(30, "m"):
        return 5, 10
    if dt < np.timedelta64(60, "m"):
        return 5, 30
    return 30, 10


def _format_elapsed_time_ticks(
    ax: Axes,
    t0: np.datetime64,
    t_end: np.datetime64,
    *,
    major_interval: int,
    minor_interval: int,
    max_major_ticks: int = 14,
    max_minor_ticks: int = 180,
) -> None:
    """Elapsed-time tick labels for the lagrangian time axis."""
    duration_min = float((t_end - t0) / np.timedelta64(1, "m"))
    if not np.isfinite(duration_min) or duration_min <= 0:
        return
    major_step = max(int(major_interval), int(np.ceil(duration_min / max_major_ticks)))
    minor_step = max(int(minor_interval), int(np.ceil(duration_min / max_minor_ticks)))
    major_times = np.arange(0, duration_min + major_step, major_step)
    minor_times = np.arange(0, duration_min + minor_step, minor_step)
    ax.xaxis.set_major_locator(FixedLocator(major_times.tolist()))
    ax.xaxis.set_minor_locator(FixedLocator(minor_times.tolist()))
    ax.set_xticklabels([f"+{int(t):02d}" for t in major_times])


def _apply_panel_x_elapsed(
    ax: Axes, cfg: FigCfg, *, display_xlim: tuple[float, float],
) -> None:
    """``_apply_x_axis_elapsed`` then clip to ``display_xlim`` (panel lims), like the original figure."""
    t0, t1 = cfg.model_seed, cfg.xlim[1]
    duration_min = float((t1 - t0) / np.timedelta64(1, "m"))
    ax.set_xscale("linear")
    ax.set_xlim(0.0, duration_min)
    mi, Mi = _plume_path_intervals_dt(t0, t1)
    _format_elapsed_time_ticks(ax, t0, t1, major_interval=mi, minor_interval=Mi)
    ax.set_xlim(*display_xlim)


def _axis_ticks_grid_pub(ax: Axes) -> None:
    """Publication panel grid + tick geometry (``style_profiles.apply_publication_panel_axis``)."""
    apply_publication_panel_axis(ax)


def setup_lagrangian_panel_axes(
    ax: Axes, cfg: FigCfg, *, display_xlim: tuple[float, float], ylim: tuple[float, float],
) -> None:
    """Axes limits, ticks, and grid for lagrangian heatmap panels (no data)."""
    _apply_panel_x_elapsed(ax, cfg, display_xlim=display_xlim)
    ax.set_yscale("log")
    ax.set_ylim(*ylim)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_title("")
    ax.yaxis.set_major_formatter(FuncFormatter(_float_fmt))
    _axis_ticks_grid_pub(ax)


def _draw_heatmap(
    ax: Axes, ens_ds: xr.Dataset, cfg: FigCfg, *,
    display_xlim: tuple[float, float], ylim: tuple[float, float], alpha: float,
    time_slice: tuple[np.datetime64, np.datetime64],
) -> Any:
    t0, t1 = time_slice
    da = ens_ds[cfg.variable].sel(time=slice(t0, t1))
    da = _elapsed(da, cfg.model_seed)
    norm = LogNorm(vmin=cfg.zlim[0], vmax=cfg.zlim[1])
    mesh = da.plot.pcolormesh(
        x="time_elapsed", y="diameter", ax=ax,
        cmap=_cmap_model_psd(), alpha=alpha, add_colorbar=False, norm=norm,
    )
    setup_lagrangian_panel_axes(ax, cfg, display_xlim=display_xlim, ylim=ylim)
    return mesh, da


def _cmap_model_psd():
    """Colormap for ensemble model PSD pcolormesh (panels A, B)."""
    return create_fade_cmap(
        make_pastel(create_new_jet3(), desaturation=0.35, darken=0.80),
        n_fade=2,
    )


def _cmap_holimo_scatter():
    """Colormap for HOLIMO concentration scatter (panels A, B); same as model PSD heatmap."""
    return _cmap_model_psd()


def _overlay_holimo_scatter(
    axes: list[Axes], df_pts: pd.DataFrame, cfg: FigCfg, alphas: tuple[float, float],
) -> None:
    if df_pts.empty:
        return
    norm = LogNorm(vmin=cfg.zlim[0], vmax=cfg.zlim[1])
    boost = [2.0, 15.0]
    for m in range(len(cfg.obs_ids)):
        sub = df_pts[df_pts["m"] == m]
        if sub.empty:
            continue
        for ax_idx, (ax, alpha) in enumerate(zip(axes, alphas)):
            y_lo, y_hi = ax.get_ylim()
            s = sub[(sub["y"] >= y_lo) & (sub["y"] <= y_hi)]
            if s.empty:
                continue
            ax.scatter(
                s["x"], s["y"], c=s["c"],
                marker=cfg.mission_markers[m],
                s=cfg.mission_msizes[m] + boost[min(ax_idx, len(boost) - 1)],
                cmap=_cmap_holimo_scatter(), norm=norm, alpha=alpha,
                edgecolors="white", linewidths=0.2, zorder=120,
            )


def _holimo_elapsed_and_combined_linear(
    da_holimo_raw: xr.DataArray, cfg: FigCfg,
) -> tuple[dict[str, xr.DataArray], list[tuple[float, float]], tuple[float, float] | None]:
    """HOLIMO per-mission elapsed fields plus pooled linear fit in (t, D) (same as figure overlay)."""
    obs_elapsed: dict[str, xr.DataArray] = {}
    elapsed_wins: list[tuple[float, float]] = []
    for obs_id, (t_lo, t_hi), seed in zip(cfg.obs_ids, cfg.time_frames_plume, cfg.seeding_start_times):
        lo = float((np.datetime64(t_lo) - seed) / np.timedelta64(1, "m"))
        hi = float((np.datetime64(t_hi) - seed) / np.timedelta64(1, "m"))
        elapsed_wins.append((min(lo, hi), max(lo, hi)))
        da_o = da_holimo_raw.sel(time=slice(t_lo, t_hi))
        if da_o.sizes.get("time", 0):
            obs_elapsed[obs_id] = _elapsed(da_o, seed)
    combined_t, combined_D = [], []
    for mi, obs_id in enumerate(cfg.obs_ids):
        da_o = obs_elapsed.get(obs_id)
        if da_o is None:
            continue
        lo, hi = elapsed_wins[mi]
        sub = da_o.where((da_o.time_elapsed >= lo) & (da_o.time_elapsed <= hi), drop=True) \
                  .sel(diameter=slice(*cfg.zoom_ylim))
        t_o, D_o = _weighted_mean_D(sub)
        if t_o.size >= 2:
            combined_t.append(t_o)
            combined_D.append(D_o)
    if not combined_t:
        return obs_elapsed, elapsed_wins, None
    tc, Dc = np.concatenate(combined_t), np.concatenate(combined_D)
    if tc.size < 2:
        return obs_elapsed, elapsed_wins, None
    a_c, b_c = np.polyfit(tc, Dc, 1)
    return obs_elapsed, elapsed_wins, (float(a_c), float(b_c))


@dataclass
class TrajectoryFitPack:
    """Precomputed trajectories and growth fits for panels A/B (also used by presenter mode)."""

    t_grid: np.ndarray
    p_lo: np.ndarray
    p_hi: np.ndarray
    obs_elapsed: dict[str, xr.DataArray]
    elapsed_wins: list[tuple[float, float]]
    hol_lin: tuple[float, float] | None
    t_m: np.ndarray
    D_m: np.ndarray
    a_mu: float
    b_mu: float
    alpha_star: float
    C_star: float
    alpha_C: dict[float, float]
    has_model: bool


def _linefit_width_pairs(ax_top: Axes, ax_zoom: Axes | None) -> list[tuple[float, float, Axes]]:
    if ax_zoom is None:
        return [(3.5, 0.5, ax_top)]
    return [
        (3.5, 0.5, ax_top),
        (6.0, 0.85, ax_zoom),
    ]


def compute_trajectory_fit_pack(
    datasets: dict[str, dict[str, xr.Dataset]],
    da_holimo_raw: xr.DataArray,
    ens_ds: xr.Dataset, cfg: FigCfg,
) -> TrajectoryFitPack:
    """Envelope percentiles, HOLIMO windows, and model growth-fit coefficients."""
    t_grid = np.linspace(cfg.zoom_xlim[0], cfg.zoom_xlim[1], 400)
    runs_D = _per_run_D_grid(datasets, cfg, t_grid)
    with np.errstate(invalid="ignore"):
        p_lo = np.nanpercentile(runs_D, cfg.envelope_pct[0], axis=0)
        p_hi = np.nanpercentile(runs_D, cfg.envelope_pct[1], axis=0)

    obs_elapsed, elapsed_wins, hol_lin = _holimo_elapsed_and_combined_linear(da_holimo_raw, cfg)

    da_m = (_elapsed(ens_ds[cfg.variable].sel(time=slice(*cfg.xlim)), cfg.model_seed)
            .sel(diameter=slice(*cfg.zoom_ylim)))
    da_m = da_m.where((da_m.time_elapsed >= cfg.zoom_xlim[0])
                      & (da_m.time_elapsed <= cfg.zoom_xlim[1]), drop=True)
    t_m, D_m = _weighted_mean_D(da_m)

    if t_m.size < 2:
        return TrajectoryFitPack(
            t_grid=t_grid, p_lo=p_lo, p_hi=p_hi,
            obs_elapsed=obs_elapsed, elapsed_wins=elapsed_wins, hol_lin=hol_lin,
            t_m=t_m, D_m=D_m,
            a_mu=float("nan"), b_mu=float("nan"), alpha_star=float("nan"), C_star=float("nan"),
            alpha_C={}, has_model=False,
        )

    a_mu, b_mu = np.polyfit(t_m, D_m, 1)
    log_t, log_D = np.log(t_m), np.log(D_m)
    alpha_star, log_C_star = np.polyfit(log_t, log_D, 1)
    C_star = float(np.exp(log_C_star))
    alpha_C = {
        float(a): float(np.exp(np.mean(log_D - float(a) * log_t)))
        for a in cfg.sublinear_alphas
    }
    return TrajectoryFitPack(
        t_grid=t_grid, p_lo=p_lo, p_hi=p_hi,
        obs_elapsed=obs_elapsed, elapsed_wins=elapsed_wins, hol_lin=hol_lin,
        t_m=t_m, D_m=D_m,
        a_mu=float(a_mu), b_mu=float(b_mu), alpha_star=float(alpha_star), C_star=C_star,
        alpha_C=alpha_C, has_model=True,
    )


def _draw_envelope(ax_zoom: Axes, pack: TrajectoryFitPack) -> None:
    ok = np.isfinite(pack.p_lo) & np.isfinite(pack.p_hi)
    ax_zoom.fill_between(
        pack.t_grid[ok], pack.p_lo[ok], pack.p_hi[ok], color="#6e6e6e", alpha=0.35, lw=0, zorder=205,
    )


def _draw_sublinear_curves(
    ax_top: Axes, ax_zoom: Axes | None, pack: TrajectoryFitPack, cfg: FigCfg, *, annotate_zoom: bool,
) -> None:
    if not pack.has_model:
        return
    a_lo, a_hi = min(cfg.sublinear_alphas), max(cfg.sublinear_alphas)
    span = (a_hi - a_lo) or 1.0
    c_lin, c_pow = "#CD1C18", "#FFB343"
    for idx, (lw_o, lw_i, ax) in enumerate(_linefit_width_pairs(ax_top, ax_zoom)):
        t_fit = np.linspace(max(ax.get_xlim()[0], 1e-3), ax.get_xlim()[1], cfg.n_timeline)
        for a_fix in cfg.sublinear_alphas:
            seg = 8.0 - 6.0 * (a_fix - a_lo) / span
            a_alpha = 0.95 if float(a_fix) == 0.75 else 0.75
            ax.plot(
                t_fit, pack.alpha_C[float(a_fix)] * t_fit ** a_fix, color="black",
                ls=(0, (seg, seg)), lw=0.65, alpha=a_alpha, zorder=206,
            )
        annotate_here = annotate_zoom and (
            (ax_zoom is not None and idx == 1)
            or (ax_zoom is None and idx == 0)
        )
        if annotate_here:
            t_end = ax.get_xlim()[1]
            for a_fix in cfg.sublinear_alphas:
                if float(a_fix) == 0.75:
                    continue
                y = pack.alpha_C[float(a_fix)] * t_end ** a_fix
                if np.isfinite(y):
                    ax.annotate(f"{a_fix:g}", xy=(t_end, y), xytext=(4, 0),
                                textcoords="offset points", fontsize=4.4,
                                ha="left", va="center", clip_on=False, zorder=207)
            y_opt = pack.C_star * t_end ** pack.alpha_star
            if np.isfinite(y_opt):
                ax.annotate(f"{pack.alpha_star:.2f}", xy=(t_end, y_opt), xytext=(4, 0),
                            textcoords="offset points", fontsize=4.4, color=c_pow,
                            ha="left", va="center", clip_on=False, zorder=208)


def _draw_model_linear_power(
    ax_top: Axes, ax_zoom: Axes | None, pack: TrajectoryFitPack, cfg: FigCfg, *, include_linear: bool, include_power: bool,
) -> None:
    if not pack.has_model:
        return
    c_lin, c_pow = "#CD1C18", "#FFB343"
    for lw_o, lw_i, ax in _linefit_width_pairs(ax_top, ax_zoom):
        t_fit = np.linspace(max(ax.get_xlim()[0], 1e-3), ax.get_xlim()[1], cfg.n_timeline)
        if include_linear:
            _draw_fit(ax, t_fit, pack.a_mu * t_fit + pack.b_mu, color=c_lin, zorder=211,
                      label=rf"COSMO-SPECS $D_\mathrm{{lin}}(t)={pack.a_mu:.2f} + {pack.b_mu:.2f}\cdot t$",
                      lw_outer=lw_o, lw_inner=lw_i)
        if include_power:
            _draw_fit(ax, t_fit, pack.C_star * t_fit ** pack.alpha_star, color=c_pow, zorder=212,
                      label=rf"COSMO-SPECS $D_\mathrm{{pow}}(t)={pack.C_star:.2f}\cdot t^{{{pack.alpha_star:.2f}}}$",
                      lw_outer=lw_o, lw_inner=lw_i)


def draw_zoom_growth_fit_overlay(ax_zoom: Axes, pack: TrajectoryFitPack, cfg: FigCfg) -> None:
    """Envelope + growth fits on zoom panel only (presenter step 11). Order: HOLIMO, sublinear, COSMO linear+power."""
    lw_o, lw_i = 6.0, 0.85
    _draw_envelope(ax_zoom, pack)
    if pack.hol_lin is not None:
        a_c, b_c = pack.hol_lin
        hol_t0, hol_t1 = min(w[0] for w in pack.elapsed_wins), max(w[1] for w in pack.elapsed_wins)
        ax_lo, ax_hi = ax_zoom.get_xlim()
        t_lo = max(ax_lo, hol_t0, 1e-3)
        t_hi = min(ax_hi, hol_t1)
        if t_hi > t_lo:
            t_c = np.linspace(t_lo, t_hi, cfg.n_timeline)
            _draw_fit(ax_zoom, t_c, a_c * t_c + b_c, color="royalblue", zorder=209,
                      label="HOLIMO linear fit", lw_outer=lw_o, lw_inner=lw_i)
    if pack.has_model:
        t_fit = np.linspace(max(ax_zoom.get_xlim()[0], 1e-3), ax_zoom.get_xlim()[1], cfg.n_timeline)
        a_lo, a_hi = min(cfg.sublinear_alphas), max(cfg.sublinear_alphas)
        span = (a_hi - a_lo) or 1.0
        for a_fix in cfg.sublinear_alphas:
            seg = 8.0 - 6.0 * (a_fix - a_lo) / span
            a_alpha = 0.95 if float(a_fix) == 0.75 else 0.75
            ax_zoom.plot(
                t_fit, pack.alpha_C[float(a_fix)] * t_fit ** a_fix, color="black",
                ls=(0, (seg, seg)), lw=0.65, alpha=a_alpha, zorder=206,
            )
        t_end = ax_zoom.get_xlim()[1]
        c_pow = "#FFB343"
        for a_fix in cfg.sublinear_alphas:
            if float(a_fix) == 0.75:
                continue
            y = pack.alpha_C[float(a_fix)] * t_end ** a_fix
            if np.isfinite(y):
                ax_zoom.annotate(f"{a_fix:g}", xy=(t_end, y), xytext=(4, 0),
                                 textcoords="offset points", fontsize=4.4,
                                 ha="left", va="center", clip_on=False, zorder=207)
        y_opt = pack.C_star * t_end ** pack.alpha_star
        if np.isfinite(y_opt):
            ax_zoom.annotate(f"{pack.alpha_star:.2f}", xy=(t_end, y_opt), xytext=(4, 0),
                             textcoords="offset points", fontsize=4.4, color=c_pow,
                             ha="left", va="center", clip_on=False, zorder=208)
        _draw_fit(ax_zoom, t_fit, pack.a_mu * t_fit + pack.b_mu, color="#CD1C18", zorder=211,
                  label=rf"COSMO-SPECS $D_\mathrm{{lin}}(t)={pack.a_mu:.2f} + {pack.b_mu:.2f}\cdot t$",
                  lw_outer=lw_o, lw_inner=lw_i)
        _draw_fit(ax_zoom, t_fit, pack.C_star * t_fit ** pack.alpha_star, color="#FFB343", zorder=212,
                  label=rf"COSMO-SPECS $D_\mathrm{{pow}}(t)={pack.C_star:.2f}\cdot t^{{{pack.alpha_star:.2f}}}$",
                  lw_outer=lw_o, lw_inner=lw_i)


def _draw_holimo_linear_fit(
    ax_top: Axes, ax_zoom: Axes | None, pack: TrajectoryFitPack, cfg: FigCfg,
) -> None:
    if pack.hol_lin is None:
        return
    a_c, b_c = pack.hol_lin
    hol_t0, hol_t1 = min(w[0] for w in pack.elapsed_wins), max(w[1] for w in pack.elapsed_wins)
    for idx, (lw_o, lw_i, ax) in enumerate(_linefit_width_pairs(ax_top, ax_zoom)):
        ax_lo, ax_hi = ax.get_xlim()
        use_zoom_clip = ax_zoom is not None and ax is ax_zoom
        t_lo = max(ax_lo, hol_t0, 1e-3) if use_zoom_clip else max(ax_lo, 1e-3)
        t_hi = min(ax_hi, hol_t1) if use_zoom_clip else ax_hi
        if t_hi > t_lo:
            t_c = np.linspace(t_lo, t_hi, cfg.n_timeline)
            _draw_fit(ax, t_c, a_c * t_c + b_c, color="royalblue", zorder=209,
                      label="HOLIMO linear fit", lw_outer=lw_o, lw_inner=lw_i)


def _add_trajectory_and_fits(
    ax_top: Axes, ax_zoom: Axes,
    datasets: dict[str, dict[str, xr.Dataset]],
    da_holimo_raw: xr.DataArray,
    ens_ds: xr.Dataset, cfg: FigCfg,
) -> tuple[dict[str, xr.DataArray], list[tuple[float, float]]]:
    pack = compute_trajectory_fit_pack(datasets, da_holimo_raw, ens_ds, cfg)
    _draw_envelope(ax_zoom, pack)
    if pack.has_model:
        _draw_model_linear_power(
            ax_top, ax_zoom, pack, cfg, include_linear=True, include_power=True,
        )
        _draw_sublinear_curves(ax_top, ax_zoom, pack, cfg, annotate_zoom=True)
    _draw_holimo_linear_fit(ax_top, ax_zoom, pack, cfg)
    return pack.obs_elapsed, pack.elapsed_wins


def _add_fit_legend(ax: Axes) -> None:
    raw: list[tuple[str, Any]] = list(ax.__dict__.get("_fit_handles", []))
    if not raw:
        return
    holimo = [(l, h) for l, h in raw if "HOLIMO" in l]
    rest = [(l, h) for l, h in raw if "HOLIMO" not in l]
    has_model_fits = any("model" in l for l, _ in rest)
    extra: list[tuple[str, Any]] = []
    if has_model_fits:
        extra.append((
            r"COSMO-SPECS fixed $D \propto t^{\alpha}$",
            Line2D([0], [0], color="black", ls=(0, (6, 6)), lw=0.65, alpha=0.75),
        ))
    composite = holimo + rest + extra
    prev = ax.get_legend()
    kw = dict(
        labels=[l for l, _ in composite],
        handles=[h for _, h in composite],
        handler_map={tuple: HandlerTuple(ndivide=1, pad=0.0)},
        fontsize=7, framealpha=0.8, borderaxespad=0.3, handlelength=2.6, labelspacing=0.3,
    )
    if prev is not None:
        ax.add_artist(prev)
        ax.figure.canvas.draw()
        bb = prev.get_window_extent().transformed(ax.transAxes.inverted())
        leg = ax.legend(**kw, loc="lower right",
                        bbox_to_anchor=(bb.x0 - 0.05 * bb.x0, bb.y0), bbox_transform=ax.transAxes)
    else:
        leg = ax.legend(**kw, loc="lower right")
    leg.set_zorder(500)


def _hist_profile(da: xr.DataArray, edges: np.ndarray, *, threshold: float = 0.0) -> np.ndarray:
    """Time-integrated count per diameter bin (trapz over elapsed minutes)."""
    extra = [d for d in da.dims if d not in ("time", "diameter")]
    if extra:
        da = da.mean(dim=extra, skipna=True)
    vals = np.asarray(da.values, dtype=float)
    vals = np.where(np.isfinite(vals) & (vals > threshold), vals, 0.0)
    t_min = ((da["time"].values - da["time"].values[0]) / np.timedelta64(1, "m")).astype(float)
    weights = np.trapezoid(vals, x=t_min, axis=0)
    hist, _ = np.histogram(np.asarray(da["diameter"].values, dtype=float),
                           bins=edges, weights=weights)
    return hist.astype(float)


def _plot_hist(ax: Axes, diam: np.ndarray, vals: np.ndarray, color: str, *, zorder: int) -> None:
    """Filled step histogram plus composite outline (``_draw_fit``-style halos and inner dash)."""
    ax.fill_between(diam, vals, step="pre", color=color, alpha=0.2, zorder=zorder)
    lw_o, lw_i = _HIST_STEP_LW_OUT, _HIST_STEP_LW_IN
    halo_out = pe.withStroke(linewidth=lw_o * 1.125, foreground="white", alpha=0.75)
    halo_in = pe.withStroke(linewidth=lw_i * 1.65, foreground="white", alpha=0.75)
    ax.step(
        diam, vals, where="pre", color=color, lw=lw_o, alpha=0.7, zorder=zorder + 1,
        solid_capstyle="round", path_effects=[halo_out],
    )
    ax.step(
        diam, vals, where="pre", color="black", lw=lw_i, alpha=1.0, zorder=zorder + 2,
        dash_capstyle="round", path_effects=[halo_in], ls=(0, (8, 4)),
    )


def _hist_step_legend_handle(color: str) -> tuple[Line2D, Line2D]:
    """Match ``_plot_hist`` / ``_draw_fit`` for panel C legend (same lw as subplot steps)."""
    lw_o, lw_i = _HIST_STEP_LW_OUT, _HIST_STEP_LW_IN
    halo_out = pe.withStroke(linewidth=lw_o * 1.125, foreground="white", alpha=0.75)
    halo_in = pe.withStroke(linewidth=lw_i * 1.65, foreground="white", alpha=0.75)
    lo = Line2D(
        [0], [0], color=color, lw=lw_o, alpha=0.7, solid_capstyle="round", path_effects=[halo_out],
    )
    li = Line2D(
        [0], [0], color="black", lw=lw_i, alpha=1.0, dash_capstyle="round",
        path_effects=[halo_in], ls=(0, (8, 4)),
    )
    return (lo, li)


def _span_min(da: xr.DataArray) -> float:
    t = np.asarray(da["time"].values)
    return max(float((t[-1] - t[0]) / np.timedelta64(1, "m")), 0.0) if t.size >= 2 else 0.0


def _peak_idx(v: np.ndarray, n: int = 2) -> list[int]:
    if v.size < 3:
        return [int(np.nanargmax(v))] if v.size and np.isfinite(v).any() else []
    idx = [i for i in range(1, v.size - 1)
           if np.isfinite(v[i]) and v[i] >= v[i - 1] and v[i] >= v[i + 1]]
    return sorted(idx or [int(np.nanargmax(v))], key=lambda i: v[i], reverse=True)[:n]


def _median_diam(diam: np.ndarray, w: np.ndarray) -> float:
    cdf = np.nancumsum(w)
    return float(np.interp(0.5, cdf / cdf[-1], diam)) if cdf[-1] > 0 else np.nan


# ----- top-level ------------------------------------------------------------
def _growth_fit_table_caption(cfg: FigCfg) -> str:
    """Short figure-note style caption: processing and metrics (matches printed table)."""
    sm = cfg.model_diameter_smoothing_bins
    sm_phrase = f", {sm}-bin rolling mean on diameter" if sm > 1 else ""
    return (
        "Quantity is concentration-weighted geometric mean equivalent diameter (μm) versus plume elapsed "
        "time, limited to the zoom panel’s time and diameter bounds. "
        f"Model: ensemble mean of {cfg.variable!r} per logarithmic diameter bin for kind {cfg.kind!r}, after 10 s "
        "run alignment, spatial reductions (cell sum, vertical mean where applicable), mean over runs, "
        f"division by Δln D{sm_phrase}, and windowing. "
        "HOLIMO: prepared in situ PSDs (resampled means, coverage gate, short temporal smoothing), same "
        "diameter band, legs pooled within mission elapsed windows; one ordinary least-squares line in "
        "time to pooled (t, D) is the reference (no L⁻¹ rescaling on this table). "
        "rmse_logD is RMS error in ln D on model samples; r, MAE, MSE, and SD compare each fitted model "
        "curve to that reference in μm on uniform time over the zoom–flight overlap."
    )


def _growth_fit_table_caption_latex(cfg: FigCfg) -> str:
    """LaTeX ``\\caption{...}`` for the growth-fit table (requires ``siunitx``, ``amsmath``)."""
    sm = cfg.model_diameter_smoothing_bins
    sm_tex = f", {sm}-bin rolling mean on diameter" if sm > 1 else ""
    var_esc = cfg.variable.replace("_", r"\_")
    kind_esc = cfg.kind.replace("_", r"\_")
    return (
        r"\caption{The reported quantity is the concentration-weighted geometric mean equivalent "
        r"diameter (\si{\micro\meter}) versus plume elapsed time, restricted to the zoom panel's time "
        r"and diameter bounds. \textbf{Model:} ensemble mean of \texttt{"
        + var_esc
        + r"} per logarithmic diameter bin for kind \texttt{"
        + kind_esc
        + r"}, after \SI{10}{\second} run alignment, spatial reductions (cell sum, vertical mean "
        r"where applicable), mean over runs, division by $\Delta \ln D$"
        + sm_tex
        + r", and windowing. \textbf{HOLIMO:} prepared in situ PSDs (resampled means, coverage gate, "
        r"short temporal smoothing), same diameter band, legs pooled within mission elapsed windows; "
        r"one ordinary least-squares line in time to pooled $(t,D)$ is the reference (no L$^{-1}$ "
        r"rescaling on this table). \textsc{rmse}$_{\ln D}$ is the RMS error in $\ln D$ on model "
        r"samples; $r$, MAE, MSE, and SD compare each fitted model curve to that reference in "
        r"\si{\micro\meter} on uniform time over the zoom--flight overlap.}"
    )


def _latex_escape_fit_text(s: str) -> str:
    """Map Unicode α to math; minimal escaping for tabular (avoids breaking ``$...$``)."""
    t = (
        s.replace("α*", r"\ensuremath{\alpha^{\ast}}")
        .replace("α", r"\ensuremath{\alpha}")
    )
    return t.replace("&", r"\&").replace("%", r"\%").replace("_", r"\_")


def _latex_num(x: float) -> str:
    return f"{x:.4f}" if np.isfinite(x) else "---"


def format_model_growth_fit_table_latex(
    *,
    caption_latex: str,
    rows: list[tuple[str, str, float, float, float, float, float]],
    meta_comment: str,
) -> str:
    """Full ``table*`` fragment: caption, ``booktabs`` tabular, and numeric rows."""
    lines = [
        "% " + meta_comment.replace("\n", "\n% "),
        r"%\usepackage{amsmath} % for \text in column headers if extended",
        r"%\usepackage{siunitx} % matches manuscript preamble",
        r"%\usepackage{booktabs}",
        r"\begin{table*}[t]",
        r"\centering",
        caption_latex,
        r"\label{tab:plume_evolution_fits}",
        r"\begin{tabular}{@{}llrrrrrr@{}}",
        r"\toprule",
        r"Config & Coeffs.\ & "
        r"\textsc{rmse}$_{\ln D}$ & $r$ & MAE & MSE & SD(res) \\",  # μm / μm^2 / μm; see caption
        r"\midrule",
    ]
    for name, poly, rlog, r, mae, mse, sd in rows:
        lines.append(
            f"{_latex_escape_fit_text(name)} & {_latex_escape_fit_text(poly)} & "
            f"{_latex_num(rlog)} & {_latex_num(r)} & {_latex_num(mae)} & {_latex_num(mse)} & {_latex_num(sd)} \\\\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table*}",
            "",
        ]
    )
    return "\n".join(lines)


def print_model_growth_fit_table(context: dict[str, Any]) -> str | None:
    """Log growth-fit table; return LaTeX ``table*`` fragment for the same data, or ``None`` if skipped."""
    cfg: FigCfg = context["cfg"]
    ens_ds: xr.Dataset = context["ensemble"]["Ensemble Mean"][cfg.kind]
    da_m = (_elapsed(ens_ds[cfg.variable].sel(time=slice(*cfg.xlim)), cfg.model_seed)
            .sel(diameter=slice(*cfg.zoom_ylim)))
    da_m = da_m.where((da_m.time_elapsed >= cfg.zoom_xlim[0])
                      & (da_m.time_elapsed <= cfg.zoom_xlim[1]), drop=True)
    t_m, D_m = _weighted_mean_D(da_m)
    alphas = cfg.sublinear_alphas
    if t_m.size < 2:
        print("[plume_lagrangian] model growth fits: n<2, skip table")
        return None
    a_mu, b_mu = np.polyfit(t_m, D_m, 1)
    log_t_m, log_D_m = np.log(t_m), np.log(D_m)
    alpha_C = {
        float(a): float(np.exp(np.mean(log_D_m - float(a) * log_t_m))) for a in alphas
    }
    alpha_star, log_C_star = np.polyfit(log_t_m, log_D_m, 1)
    alpha_star = float(alpha_star)
    C_star = float(np.exp(log_C_star))
    rmse_opt = float(np.sqrt(np.mean((log_D_m - (alpha_star * log_t_m + log_C_star)) ** 2)))
    rmse_fixed = {
        float(a): float(np.sqrt(np.mean((log_D_m - (a * log_t_m + np.log(alpha_C[float(a)]))) ** 2)))
        for a in alphas
    }
    rmse_lin = float(np.sqrt(np.mean((log_D_m - np.log(a_mu * t_m + b_mu)) ** 2)))

    da_holimo = context["ds_holimo"][cfg.holimo_var]
    _, elapsed_wins, hol_lin = _holimo_elapsed_and_combined_linear(da_holimo, cfg)
    t_grid: np.ndarray | None = None
    y_hol: np.ndarray | None = None
    if hol_lin is not None:
        hol_t0 = min(w[0] for w in elapsed_wins)
        hol_t1 = max(w[1] for w in elapsed_wins)
        tg0 = max(cfg.zoom_xlim[0], hol_t0)
        tg1 = min(cfg.zoom_xlim[1], hol_t1)
        if tg1 > tg0:
            n_grid = max(200, cfg.n_timeline)
            t_grid = np.linspace(tg0, tg1, n_grid)
            a_h, b_h = hol_lin
            y_hol = a_h * t_grid + b_h

    def _vs_hol(y_m: np.ndarray) -> tuple[float, float, float, float]:
        if y_hol is None:
            return (float("nan"),) * 4
        res = y_m - y_hol
        mae = float(np.mean(np.abs(res)))
        mse = float(np.mean(res ** 2))
        sd = float(np.std(res, ddof=1)) if res.size > 1 else float("nan")
        if np.nanstd(y_m) > 0 and np.nanstd(y_hol) > 0:
            r = float(np.corrcoef(y_m, y_hol)[0, 1])
        else:
            r = float("nan")
        return r, mae, mse, sd

    def _fmt_metric(x: float) -> str:
        return f"{x:9.4f}" if np.isfinite(x) else "      n/a"

    hol_note = (
        f"vs HOLIMO linear on D (μm), t=[{t_grid[0]:.2f},{t_grid[-1]:.2f}] min, n={t_grid.size}"
        if t_grid is not None
        else "vs HOLIMO linear: (no overlap or no pooled HOLIMO fit)"
    )
    cap = _growth_fit_table_caption(cfg)
    cap_tex = _growth_fit_table_caption_latex(cfg)
    print(f"[plume_lagrangian] Table caption: {cap}")
    print(f"[plume_lagrangian] Table caption (LaTeX): {cap_tex}")
    print(
        f"[plume_lagrangian] growth fits: model n={t_m.size} "
        f"t=[{t_m.min():.2f},{t_m.max():.2f}] min; {hol_note}"
    )
    print(
        "  r=Pearson(model D, HOLIMO linear D) on grid; "
        "MAE/MSE/SD(res) in μm / μm² / μm for D_model(t)−D_HOLIMO(t)."
    )
    print(
        f"  {'config':<14} {'polyfit':<30} {'rmse_logD':>9} {'r':>9} {'MAE':>9} "
        f"{'MSE':>9} {'SD(res)':>9}"
    )
    rows: list[tuple[str, str, float, np.ndarray | None]] = [
        ("linear", f"a={a_mu:+.4f} b={b_mu:+.2f}", rmse_lin,
         a_mu * t_grid + b_mu if t_grid is not None else None),
        ("power α*", f"α*={alpha_star:.4f} C*={C_star:.4f}", rmse_opt,
         C_star * t_grid ** alpha_star if t_grid is not None else None),
    ]
    for a in alphas:
        af = float(a)
        rows.append((
            f"power α={a:g}",
            f"α={a:g} C={alpha_C[af]:.4f}",
            rmse_fixed[af],
            alpha_C[af] * t_grid ** af if t_grid is not None else None,
        ))
    latex_rows: list[tuple[str, str, float, float, float, float, float]] = []
    for name, poly_txt, rlog, y_curve in rows:
        if y_curve is None:
            r, mae, mse, sd = (float("nan"),) * 4
        else:
            r, mae, mse, sd = _vs_hol(np.asarray(y_curve, dtype=float))
        print(
            f"  {name:<14} {poly_txt:<30} {rlog:9.4f} {_fmt_metric(r)} {_fmt_metric(mae)} "
            f"{_fmt_metric(mse)} {_fmt_metric(sd)}"
        )
        latex_rows.append((name, poly_txt, rlog, r, mae, mse, sd))

    gen_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    meta = (
        f"Auto-generated by run_plume_lagrangian_evolution.py at {gen_utc}. "
        f"Model n={t_m.size}, t=[{t_m.min():.2f},{t_m.max():.2f}] min; {hol_note}. "
        "MAE and SD(res) in micrometres, MSE in micrometres squared; r is Pearson correlation "
        "of model vs HOLIMO linear D on the comparison time grid (see caption)."
    )
    return format_model_growth_fit_table_latex(
        caption_latex=cap_tex, rows=latex_rows, meta_comment=meta,
    )


def load_context(repo_root: Path, cfg: FigCfg = DEFAULT_CFG) -> dict[str, Any]:
    processed_root = repo_root / cfg.processed_root
    holimo_file = repo_root / cfg.holimo_file
    datasets = load_plume_path_runs(cfg.runs, processed_root=processed_root, kinds=cfg.kinds)
    ensemble = build_ensemble(datasets, variable=cfg.variable,
                              smooth_bins=cfg.model_diameter_smoothing_bins)
    ds_holimo = prepare_holimo_for_overlay(
        str(holimo_file), cfg.time_window_holimo,
        resample_s=10, smoothing_time_bins=3, min_coverage_frac=0.01,
    )
    return {"datasets": datasets, "ensemble": ensemble, "ds_holimo": ds_holimo,
            "output_path": repo_root / cfg.output, "cfg": cfg}


def _add_zoom_dashed_box(ax: Axes, cfg: FigCfg) -> None:
    """Dashed rectangle for zoom region (panel A); shared by full figure and presenter frames."""
    ax.add_patch(
        Rectangle(
            (cfg.zoom_xlim[0], cfg.zoom_ylim[0]),
            cfg.zoom_xlim[1] - cfg.zoom_xlim[0], cfg.zoom_ylim[1] - cfg.zoom_ylim[0],
            fill=False, ec="black", ls="--", lw=0.5, alpha=0.75, zorder=300,
        )
    )


def render_figure(
    context: dict[str, Any],
    *,
    presenter_mode: bool = False,
) -> tuple[Figure | None, Path | list[Path]]:
    """Render full figure, or (``presenter_mode=True``) progressive PNG series; last PNG matches normal output."""
    if presenter_mode:
        return render_presenter_frame_series(context)

    cfg: FigCfg = context["cfg"]
    ens_ds: xr.Dataset = context["ensemble"]["Ensemble Mean"][cfg.kind]
    ds_holimo: xr.Dataset = context["ds_holimo"]
    da_holimo = ds_holimo[cfg.holimo_var]
    hol_sc = holimo_scale(cfg.holimo_var)

    fig_w = FULL_COL_IN
    fig = plt.figure(figsize=(fig_w, min(fig_w / (9.4 / 6.0), MAX_H_IN)), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[0.65, 0.35], wspace=0.1, hspace=0.1)
    ax_top = fig.add_subplot(gs[0, 0:2])
    ax_zoom = fig.add_subplot(gs[1, 0])
    ax_hist = fig.add_subplot(gs[1, 1])

    heatmap_tslice = (cfg.model_seed, cfg.xlim[1])
    mesh_top, _ = _draw_heatmap(
        ax_top, ens_ds, cfg,
        display_xlim=cfg.top_xlim, ylim=cfg.top_ylim,
        alpha=cfg.all_alpha,
        time_slice=heatmap_tslice,
    )
    _draw_heatmap(
        ax_zoom, ens_ds, cfg,
        display_xlim=cfg.zoom_xlim, ylim=cfg.zoom_ylim,
        alpha=cfg.zoom_alpha,
        time_slice=heatmap_tslice,
    )
    # Match ``_style_panels``: drop 0 tick on top, minor ticks on both.
    ax_top.set_xticks([t for t in ax_top.get_xticks() if not np.isclose(t, 0.0)])
    for ax in (ax_top, ax_zoom):
        ax.xaxis.set_minor_locator(AutoMinorLocator(5))
    # Panel A: x tick marks on top and bottom (labels stay default: bottom only).
    ax_top.tick_params(axis="x", which="both", top=True, bottom=True)

    _add_zoom_dashed_box(ax_top, cfg)
    for sp in ax_zoom.spines.values():
        sp.set_color("black"); sp.set_linestyle("--")
    for ax in (ax_top, ax_zoom):
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_visible(True)
        ax.spines["right"].set_visible(True)
    for side in ("left", "right"):
        ax_top.spines[side].set_linestyle("-")
        ax_top.spines[side].set_color("black")

    # Colorbar before scatter/fits (panel assembly order).
    unit_m = ens_ds[cfg.variable].attrs.get("units", "-")
    dnd, vol = _ensemble_label_parts(unit_m)
    fig.colorbar(mesh_top, ax=ax_top, shrink=0.8, aspect=15, pad=0.0,
                 extend="both").set_label(f"ensemble-avg. {dnd} / ({vol})")

    # HOLIMO scatter overlay on both heatmaps.
    df_pts = _holimo_scatter_points(da_holimo, cfg)
    _overlay_holimo_scatter([ax_top, ax_zoom], df_pts, cfg, (cfg.all_alpha, cfg.zoom_alpha))

    # Mission legend on top panel.
    mission_leg_handles = [
        Line2D([0], [0], marker=cfg.mission_markers[i], ls="None", mfc="grey",
               mec="black", mew=0.4, ms=5, alpha=0.8, label=oid)
        for i, oid in enumerate(cfg.obs_ids)
    ]
    ax_top.legend(handles=mission_leg_handles, title="HOLIMO missions",
                  fontsize=7, title_fontsize=7, loc="lower right", framealpha=0.8)

    # Trajectories + fits.
    obs_elapsed, elapsed_wins = _add_trajectory_and_fits(
        ax_top, ax_zoom, context["datasets"], da_holimo, ens_ds, cfg,
    )
    _add_fit_legend(ax_top)

    # Histogram panel: model + combined HOLIMO.
    da_model = _elapsed(ens_ds[cfg.variable].sel(time=slice(*cfg.xlim)), cfg.model_seed)
    el_lo = min(w[0] for w in elapsed_wins); el_hi = max(w[1] for w in elapsed_wins)
    da_mod_win = da_model.where(
        (da_model.time_elapsed >= el_lo) & (da_model.time_elapsed <= el_hi), drop=True,
    )
    be_mod = _log_edges(np.asarray(
        da_model["diameter"].sel(diameter=slice(*cfg.zoom_ylim)).values, dtype=float))
    be_hol = _log_edges(np.asarray(
        da_holimo["diameter"].sel(diameter=slice(*cfg.zoom_ylim)).values, dtype=float))
    d_mod = np.sqrt(be_mod[:-1] * be_mod[1:])
    h_mod = _hist_profile(da_mod_win, be_mod)
    dur_mod = _span_min(da_mod_win)
    f_mod = h_mod / dur_mod if dur_mod > 0 else h_mod
    _plot_hist(ax_hist, d_mod, f_mod, MODEL_SPECTRUM_COLOR, zorder=13)

    # Combined HOLIMO histogram: sum across missions, normalize by total window duration.
    d_obs = np.sqrt(be_hol[:-1] * be_hol[1:])
    h_obs = np.zeros_like(d_obs, dtype=float)
    dur_obs = 0.0
    for mi, obs_id in enumerate(cfg.obs_ids):
        da_o = obs_elapsed.get(obs_id)
        if da_o is None:
            continue
        lo, hi = elapsed_wins[mi]
        sub = da_o.where((da_o.time_elapsed >= lo) & (da_o.time_elapsed <= hi), drop=True)
        if sub.sizes.get("time", 0) == 0:
            continue
        h_cur = _hist_profile(sub, be_hol, threshold=cfg.threshold)
        if h_cur.shape == h_obs.shape:
            h_obs += h_cur
        dur_obs += _span_min(sub)
    if dur_obs <= 0 or not np.isfinite(h_obs).any():
        raise ValueError("No valid HOLIMO histogram data.")
    f_obs = h_obs / dur_obs * hol_sc
    _plot_hist(ax_hist, d_obs, f_obs, HOLIMO_SPECTRUM_COLOR, zorder=5)

    dnd_h, vol_h = _ensemble_label_parts(unit_m)
    ax_hist.set(xscale="log", yscale="log", xlim=cfg.hist_xlim, ylim=cfg.hist_ylim,
                ylabel=f"time-avg. {dnd_h} / ({vol_h})", xlabel=r"D$_{\mathrm{eq}}$ / ($\mu$m)")
    ax_hist.grid(True, which="major", ls="--", lw=0.25, alpha=0.6)
    ax_hist.grid(True, which="minor", ls=":",  lw=0.15, alpha=0.35)
    ax_hist.yaxis.tick_right(); ax_hist.yaxis.set_label_position("right")
    ax_hist.spines["top"].set_visible(False); ax_hist.spines["left"].set_visible(False)
    fmt = FuncFormatter(_float_fmt)
    ax_hist.xaxis.set_major_formatter(fmt); ax_hist.yaxis.set_major_formatter(fmt)

    # Peak/median markers on histogram edge.
    x_edge = cfg.hist_xlim[1] * 0.92
    y_bot = 1.2 * ax_hist.get_ylim()[0]
    peak_kw = dict(marker=">", s=50, ec="black", lw=0.6, zorder=100, clip_on=False)
    med_kw  = dict(marker="v", s=50, ec="black", lw=0.6, zorder=100, clip_on=False)
    for i in _peak_idx(f_mod, 2):
        ax_hist.scatter(x_edge, f_mod[i], color=MODEL_SPECTRUM_COLOR, **peak_kw)
    for i in _peak_idx(f_obs, 1):
        ax_hist.scatter(x_edge, f_obs[i], color=HOLIMO_SPECTRUM_COLOR, **peak_kw)
    ax_hist.scatter(_median_diam(d_mod, f_mod), y_bot, color=MODEL_SPECTRUM_COLOR, **med_kw)
    ax_hist.scatter(_median_diam(d_obs, f_obs), y_bot, color=HOLIMO_SPECTRUM_COLOR, **med_kw)
    ax_hist.legend(
        [_hist_step_legend_handle(HOLIMO_SPECTRUM_COLOR), _hist_step_legend_handle(MODEL_SPECTRUM_COLOR)],
        ["HOLIMO", "COSMO-SPECS"],
        loc="upper right",
        frameon=False,
        fontsize=7,
        handler_map={tuple: HandlerTuple(ndivide=1, pad=0.0)},
    )

    # Axis labels + D_lim annotation.
    ax_top.set_xlabel(r"time elapsed / (min)")
    ax_zoom.set_xlabel(r"time elapsed / (min)")
    ax_zoom.yaxis.set_major_formatter(fmt)
    fig.supylabel(r"equivalent diameter D$_{\mathrm{eq}}$ / ($\mu$m)")
    ax_top.set_xlim(cfg.top_xlim)
    ax_zoom.set_xlim(cfg.zoom_xlim)
    ax_top.axhline(cfg.zoom_ylim[1], color="black", ls=":", lw=0.6, alpha=0.5, zorder=50)
    ax_zoom.annotate(
        rf"$D_{{\lim}}={cfg.zoom_ylim[0]:.0f}-{cfg.zoom_ylim[1]:.0f}\,\mu$m (fit cutoff)",
        xy=(cfg.zoom_xlim[1], cfg.zoom_ylim[0]), xytext=(-4, 3),
        textcoords="offset points", ha="right", va="bottom", fontsize=5, alpha=0.7,
    )

    x0_h, x1_h = holimo_scatter_elapsed_extent_in_zoom(df_pts, cfg)
    ax_zoom.axvline(x0_h, color="0.35", ls="--", lw=0.7, zorder=400)
    ax_zoom.axvline(x1_h, color="0.35", ls="--", lw=0.7, zorder=400)

    for ax in (ax_top, ax_zoom, ax_hist):
        apply_publication_axis_tick_geometry(ax)
    ax_hist.tick_params(axis="x", which="both", top=False)
    ax_hist.tick_params(axis="y", which="both", left=False)

    for ax, letter in ((ax_top, "A"), (ax_zoom, "B"), (ax_hist, "C")):
        ax.text(0.012, 0.97, f"({letter})", transform=ax.transAxes,
                ha="left", va="top", fontweight="semibold", fontsize=12,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=1.5),
                zorder=1000)

    return fig, context["output_path"]


# White border on saved figure (mm → inches inside save_figure); top/bottom larger for labels.
SAVEFIG_WHITE_BORDER_LR_MM = 2.0
SAVEFIG_WHITE_BORDER_TB_MM = 3.0


def save_figure(
    fig: Figure,
    out: Path,
    *,
    dpi: int = 500,
    tight_crop: bool = True,
) -> Path:
    """Save figure. Default ``tight_crop`` trims to visible ink (publication PNGs).

    Set ``tight_crop=False`` for presenter frame series so every PNG uses the same
    full-figure bbox; otherwise ``get_tightbbox`` differs per frame (legends, panels)
    and slides appear to jump or squash.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    if not tight_crop:
        bbox_inches = fig.bbox_inches
    else:
        renderer = fig.canvas.get_renderer()  # type: ignore[union-attr]
        tight = fig.get_tightbbox(renderer)
        # ``get_tightbbox`` can be in figure inches (coords comparable to fig width) or in
        # display pixels (~width * dpi). Applying ``dpi_scale_trans.inverted()`` is only
        # valid for the pixel case; otherwise it shrinks the bbox by ~dpi and yields a
        # near-empty PNG.
        if tight.x1 <= float(fig.get_figwidth()) * 1.5:
            bb_in = tight
        else:
            bb_in = tight.transformed(fig.dpi_scale_trans.inverted())
        pad_l = SAVEFIG_WHITE_BORDER_LR_MM / 25.4
        pad_r = SAVEFIG_WHITE_BORDER_LR_MM / 25.4
        pad_b = SAVEFIG_WHITE_BORDER_TB_MM / 25.4
        pad_t = SAVEFIG_WHITE_BORDER_TB_MM / 25.4
        bbox_inches = Bbox.from_extents(
            bb_in.x0 - pad_l,
            bb_in.y0 - pad_b,
            bb_in.x1 + pad_r,
            bb_in.y1 + pad_t,
        )
    fig.savefig(
        out,
        dpi=dpi,
        bbox_inches=bbox_inches,
        facecolor="white",
        edgecolor="none",
    )
    print(f"saved -> {out.resolve().as_uri()}")
    return out


# --- Progressive presenter frames (one module; avoids circular imports) ------------

N_PRESENTER_FRAMES = 15


def _presenter_ghost_axes_canvas_white(ax: Axes) -> None:
    """Keep tick/label/spine geometry for constrained_layout; paint axis ink white (invisible)."""
    ax.tick_params(axis="both", which="both", colors="white", labelcolor="white")
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    for sp in ax.spines.values():
        sp.set_edgecolor("white")
    ax.grid(False)
    ax.xaxis.get_offset_text().set_color("white")
    ax.yaxis.get_offset_text().set_color("white")


def _presenter_reserve_hist_axes_placeholder(ax_hist: Axes, cfg: FigCfg, unit_m: str) -> None:
    """Panel C geometry (hidden) so constrained_layout matches the final figure."""
    dnd_h, vol_h = _ensemble_label_parts(unit_m)
    ax_hist.set(
        xscale="log", yscale="log", xlim=cfg.hist_xlim, ylim=cfg.hist_ylim,
        ylabel=f"time-avg. {dnd_h} / ({vol_h})", xlabel=r"D$_{\mathrm{eq}}$ / ($\mu$m)",
    )
    ax_hist.grid(True, which="major", ls="--", lw=0.25, alpha=0.6)
    ax_hist.grid(True, which="minor", ls=":", lw=0.15, alpha=0.35)
    ax_hist.yaxis.tick_right()
    ax_hist.yaxis.set_label_position("right")
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["left"].set_visible(False)
    fmt = FuncFormatter(_float_fmt)
    ax_hist.xaxis.set_major_formatter(fmt)
    ax_hist.yaxis.set_major_formatter(fmt)


def _presenter_scatter_missions_cumulative(
    axes: list[Axes], df_pts: Any, cfg: FigCfg, alphas: tuple[float, ...], n_missions: int,
) -> None:
    """HOLIMO scatter for missions ``0 .. n_missions-1`` (cumulative) on given axes."""
    if df_pts is None or df_pts.empty or n_missions <= 0:
        return
    norm = LogNorm(vmin=cfg.zlim[0], vmax=cfg.zlim[1])
    boost = [2.0, 15.0]
    alpha_list = list(alphas) + [alphas[-1]] * max(0, len(axes) - len(alphas))
    for m in range(min(n_missions, len(cfg.obs_ids))):
        sub = df_pts[df_pts["m"] == m]
        if sub.empty:
            continue
        for ax_idx, ax in enumerate(axes):
            alpha = alpha_list[ax_idx]
            y_lo, y_hi = ax.get_ylim()
            s = sub[(sub["y"] >= y_lo) & (sub["y"] <= y_hi)]
            if s.empty:
                continue
            ax.scatter(
                s["x"], s["y"], c=s["c"],
                marker=cfg.mission_markers[m],
                s=cfg.mission_msizes[m] + boost[min(ax_idx, len(boost) - 1)],
                cmap=_cmap_holimo_scatter(), norm=norm, alpha=alpha,
                edgecolors="white", linewidths=0.2, zorder=120,
            )


def _presenter_mission_legend(ax: Axes, cfg: FigCfg, n_missions: int) -> None:
    if n_missions <= 0:
        return
    handles = [
        Line2D([0], [0], marker=cfg.mission_markers[i], ls="None", mfc="grey",
               mec="black", mew=0.4, ms=5, alpha=0.8, label=oid)
        for i, oid in enumerate(cfg.obs_ids[:n_missions])
    ]
    ax.legend(
        handles=handles, title="HOLIMO missions",
        fontsize=7, title_fontsize=7, loc="lower right", framealpha=0.8,
    )


def _presenter_dummy_colorbar(fig: Figure, ax: Axes, cfg: FigCfg, unit_m: str) -> None:
    dnd, vol = _ensemble_label_parts(unit_m)
    sm = ScalarMappable(norm=LogNorm(vmin=cfg.zlim[0], vmax=cfg.zlim[1]), cmap=_cmap_model_psd())
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, shrink=0.8, aspect=15, pad=0.0, extend="both")
    cb.set_label(f"ensemble-avg. {dnd} / ({vol})")


def _presenter_decorate_top_panel_lines(
    ax_top: Axes, pack: TrajectoryFitPack, cfg: FigCfg, frame_idx: int,
) -> None:
    """Fits on panel A only (frames 5–8): HOLIMO, linear, sublinear, power."""
    if frame_idx < 5:
        return
    if frame_idx >= 5:
        _draw_holimo_linear_fit(ax_top, None, pack, cfg)
    if frame_idx >= 6:
        _draw_model_linear_power(ax_top, None, pack, cfg, include_linear=True, include_power=False)
    if frame_idx >= 7:
        _draw_sublinear_curves(ax_top, None, pack, cfg, annotate_zoom=False)
    if frame_idx >= 8:
        _draw_model_linear_power(ax_top, None, pack, cfg, include_linear=False, include_power=True)
    if frame_idx >= 5:
        _add_fit_legend(ax_top)


def _presenter_build_staged_frame(
    context: dict[str, Any],
    frame_idx: int,
    pack: TrajectoryFitPack,
    df_pts: Any,
    x0_h: float,
    x1_h: float,
) -> Figure:
    cfg: FigCfg = context["cfg"]
    ens_ds: xr.Dataset = context["ensemble"]["Ensemble Mean"][cfg.kind]

    fig_w = FULL_COL_IN
    fig = plt.figure(figsize=(fig_w, min(fig_w / (9.4 / 6.0), MAX_H_IN)), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[0.65, 0.35], wspace=0.1, hspace=0.1)
    ax_top = fig.add_subplot(gs[0, 0:2])
    ax_zoom = fig.add_subplot(gs[1, 0])
    ax_hist = fig.add_subplot(gs[1, 1])

    heatmap_tslice = (cfg.model_seed, cfg.xlim[1])
    unit_m = ens_ds[cfg.variable].attrs.get("units", "-")

    show_b = frame_idx >= 10
    _presenter_reserve_hist_axes_placeholder(ax_hist, cfg, unit_m)
    if not show_b:
        setup_lagrangian_panel_axes(ax_zoom, cfg, display_xlim=cfg.zoom_xlim, ylim=cfg.zoom_ylim)
        apply_publication_axis_tick_geometry(ax_zoom)
        ax_zoom.set_xticks([t for t in ax_zoom.get_xticks() if not np.isclose(t, 0.0)])
        ax_zoom.xaxis.set_minor_locator(AutoMinorLocator(5))

    if frame_idx >= 1:
        mesh_top, _ = _draw_heatmap(
            ax_top, ens_ds, cfg,
            display_xlim=cfg.top_xlim, ylim=cfg.top_ylim,
            alpha=cfg.all_alpha,
            time_slice=heatmap_tslice,
        )
        ax_top.set_xticks([t for t in ax_top.get_xticks() if not np.isclose(t, 0.0)])
        ax_top.xaxis.set_minor_locator(AutoMinorLocator(5))
        ax_top.tick_params(axis="x", which="both", top=True, bottom=True)
        dnd, vol = _ensemble_label_parts(unit_m)
        fig.colorbar(mesh_top, ax=ax_top, shrink=0.8, aspect=15, pad=0.0,
                     extend="both").set_label(f"ensemble-avg. {dnd} / ({vol})")
    else:
        setup_lagrangian_panel_axes(ax_top, cfg, display_xlim=cfg.top_xlim, ylim=cfg.top_ylim)
        ax_top.set_xticks([t for t in ax_top.get_xticks() if not np.isclose(t, 0.0)])
        ax_top.xaxis.set_minor_locator(AutoMinorLocator(5))
        ax_top.tick_params(axis="x", which="both", top=True, bottom=True)
        _presenter_dummy_colorbar(fig, ax_top, cfg, unit_m)

    if frame_idx in (2, 3, 4):
        n_m = {2: 1, 3: 2, 4: 3}[frame_idx]
        _presenter_scatter_missions_cumulative(
            [ax_top], df_pts, cfg, (cfg.all_alpha, cfg.all_alpha), n_m,
        )
        _presenter_mission_legend(ax_top, cfg, n_m)
    elif frame_idx >= 5:
        _presenter_scatter_missions_cumulative(
            [ax_top], df_pts, cfg, (cfg.all_alpha, cfg.all_alpha), 3,
        )
        _presenter_mission_legend(ax_top, cfg, 3)

    if frame_idx >= 5:
        _presenter_decorate_top_panel_lines(ax_top, pack, cfg, frame_idx)

    if frame_idx >= 9:
        _add_zoom_dashed_box(ax_top, cfg)

    if show_b:
        alpha_b = cfg.all_alpha if frame_idx == 10 else cfg.zoom_alpha
        _draw_heatmap(
            ax_zoom, ens_ds, cfg,
            display_xlim=cfg.zoom_xlim, ylim=cfg.zoom_ylim,
            alpha=alpha_b,
            time_slice=heatmap_tslice,
        )
        ax_zoom.set_xticks([t for t in ax_zoom.get_xticks() if not np.isclose(t, 0.0)])
        ax_zoom.xaxis.set_minor_locator(AutoMinorLocator(5))
        _presenter_scatter_missions_cumulative([ax_zoom], df_pts, cfg, (alpha_b, alpha_b), 3)
        for sp in ax_zoom.spines.values():
            sp.set_color("black")
            sp.set_linestyle("--")

    if show_b and frame_idx >= 12:
        draw_zoom_growth_fit_overlay(ax_zoom, pack, cfg)

    if show_b and frame_idx >= 13:
        ax_zoom.axvline(x0_h, color="0.35", ls="--", lw=0.7, zorder=400)
        ax_zoom.axvline(x1_h, color="0.35", ls="--", lw=0.7, zorder=400)

    for ax in (ax_top, ax_zoom):
        if not show_b and ax is ax_zoom:
            continue
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_visible(True)
        ax.spines["right"].set_visible(True)
    for side in ("left", "right"):
        ax_top.spines[side].set_linestyle("-")
        ax_top.spines[side].set_color("black")

    fmt = FuncFormatter(_float_fmt)
    ax_top.set_xlabel(r"time elapsed / (min)")
    ax_zoom.set_xlabel(r"time elapsed / (min)")
    ax_zoom.yaxis.set_major_formatter(fmt)
    fig.supylabel(r"equivalent diameter D$_{\mathrm{eq}}$ / ($\mu$m)")
    ax_top.set_xlim(cfg.top_xlim)
    if show_b:
        ax_zoom.set_xlim(cfg.zoom_xlim)
        ax_top.axhline(cfg.zoom_ylim[1], color="black", ls=":", lw=0.6, alpha=0.5, zorder=50)
        ax_zoom.annotate(
            rf"$D_{{\lim}}={cfg.zoom_ylim[0]:.0f}-{cfg.zoom_ylim[1]:.0f}\,\mu$m (fit cutoff)",
            xy=(cfg.zoom_xlim[1], cfg.zoom_ylim[0]), xytext=(-4, 3),
            textcoords="offset points", ha="right", va="bottom", fontsize=5, alpha=0.7,
        )

    for ax in (ax_top, ax_zoom, ax_hist):
        apply_publication_axis_tick_geometry(ax)
    ax_hist.tick_params(axis="x", which="both", top=False)
    ax_hist.tick_params(axis="y", which="both", left=False)
    if not show_b:
        _presenter_ghost_axes_canvas_white(ax_zoom)
    _presenter_ghost_axes_canvas_white(ax_hist)

    for ax, letter, show in (
        (ax_top, "A", True),
        (ax_zoom, "B", show_b),
        (ax_hist, "C", False),
    ):
        if not show:
            continue
        ax.text(0.012, 0.97, f"({letter})", transform=ax.transAxes,
                ha="left", va="top", fontweight="semibold", fontsize=12,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=1.5),
                zorder=1000)

    return fig


def render_presenter_frame_series(
    context: dict[str, Any],
    *,
    dpi: int = 500,
) -> tuple[None, list[Path]]:
    """Build 15 progressive PNGs under ``<stem>_presenter/``; last frame is ``render_figure`` output."""
    cfg: FigCfg = context["cfg"]
    ens_ds: xr.Dataset = context["ensemble"]["Ensemble Mean"][cfg.kind]
    ds_holimo: xr.Dataset = context["ds_holimo"]
    da_holimo = ds_holimo[cfg.holimo_var]
    out_path: Path = context["output_path"]
    stem = out_path.stem
    parent = out_path.parent
    presenter_dir = parent / f"{stem}_presenter"
    presenter_dir.mkdir(parents=True, exist_ok=True)

    pack = compute_trajectory_fit_pack(
        context["datasets"], da_holimo, ens_ds, cfg,
    )
    df_pts = _holimo_scatter_points(da_holimo, cfg)
    x0_h, x1_h = holimo_scatter_elapsed_extent_in_zoom(df_pts, cfg)

    paths: list[Path] = []
    n_staged = N_PRESENTER_FRAMES - 1

    for frame_idx in range(n_staged):
        fig = _presenter_build_staged_frame(
            context, frame_idx, pack, df_pts, x0_h, x1_h,
        )
        p = presenter_dir / f"{stem}_presenter_{frame_idx:02d}.png"
        save_figure(fig, p, dpi=dpi, tight_crop=False)
        paths.append(p)
        plt.close(fig)

    last_idx = N_PRESENTER_FRAMES - 1
    last_fig, _ = render_figure(context, presenter_mode=False)
    assert isinstance(last_fig, Figure)
    final_out = presenter_dir / f"{stem}_presenter_{last_idx:02d}.png"
    save_figure(last_fig, final_out, dpi=dpi, tight_crop=False)
    paths.append(final_out)
    plt.close(last_fig)

    return None, paths
