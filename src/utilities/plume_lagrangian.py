"""Plume-lagrangian figure helpers promoted from notebook 03.

HOLIMO ice PSD variables differ by normalization (see ``HOLIMO_ICE_PSD_META``):
noNorm (per bin, cm^-3), linNorm (cm^-3 um^-1), logNorm (cm^-3 per log bin).
Model plume-path ``nf`` is already per litre; only HOLIMO is scaled by
``HOLIMO_CM3_TO_L1`` (cm^-3 -> L^-1) via ``holimo_scale_cm3_to_litres``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib import patheffects as pe
from matplotlib.ticker import AutoMinorLocator
from matplotlib.colors import LogNorm
from matplotlib.legend_handler import HandlerTuple
from matplotlib.lines import Line2D

from utilities.holimo_helpers import prepare_holimo_for_overlay
from utilities.plotting import create_fade_cmap, create_new_jet3, make_pastel
from utilities.plume_loader import load_plume_path_runs
from utilities.plume_path_plot import (
    _assign_elapsed_time,
    _prepare_da,
    build_common_xlim,
    diagnostics_table,
    plot_plume_path_sum,
)
from utilities.style_profiles import FULL_COL_IN, MAX_H_IN

xr.set_options(keep_attrs=True)

# ----- defaults ---------------------------------------------------------------
DEFAULT_PROCESSED_ROOT = Path("data") / "processed"
DEFAULT_HOLIMO_FILE = (
    Path("data") / "observations" / "holimo_data" / "CL_20230125_1000_1140_SM058_SM060_ts1.nc"
)
DEFAULT_OUTPUT = Path("output") / "gfx" / "png" / "03" / "figure12_ensemble_mean_plume_path_foo.png"

DEFAULT_RUNS: list[dict[str, str]] = [
    {"label": "400m, inp 1e6, ccn 0 (spherical)", "cs_run": "cs-eriswil__20251125_114053", "exp_id": "20251125114238"},
    {"label": "400m, inp 1e6, ccn 400 (analytic)", "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211431"},
    {"label": "400m, inp 1e6, ccn 400 (planar)", "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211551"},
    {"label": "400m, inp 1e6, ccn 400 (spherical)", "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131550"},
    {"label": "400m, inp 1e6, ccn 400 (columnar 2)", "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131632"},
]

DEFAULT_KINDS = ("integrated", "vertical", "extreme")
DEFAULT_TIME_WINDOW_HOLIMO = (np.datetime64("2023-01-25T10:10:00"), np.datetime64("2023-01-25T12:00:00"))
DEFAULT_TIME_FRAMES_PLUME = [
    [np.datetime64("2023-01-25T10:56:00"), np.datetime64("2023-01-25T11:04:00")],
    [np.datetime64("2023-01-25T10:35:00"), np.datetime64("2023-01-25T10:42:00")],
    [np.datetime64("2023-01-25T11:24:00"), np.datetime64("2023-01-25T11:29:00")],
]
DEFAULT_OBS_IDS = ["SM059", "SM058", "SM060"]
DEFAULT_SEEDING_START_TIMES = [
    np.datetime64("2023-01-25T10:50:00"),
    np.datetime64("2023-01-25T10:28:00"),
    np.datetime64("2023-01-25T11:15:00"),
]
DEFAULT_MODEL_SEED = np.datetime64("2023-01-25T12:29:50")
DEFAULT_KIND = "extreme"
DEFAULT_VARIABLE = "nf"
DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS = 0
DEFAULT_HOLIMO_VAR = "Ice_PSDlogNorm"
HOLIMO_CM3_TO_L1 = 1000.0
DEFAULT_UNIT_CONVERSION = HOLIMO_CM3_TO_L1

HOLIMO_ICE_PSD_META: dict[str, tuple[str, str]] = {
    "Ice_PSDnoNorm": ("cm-3", "concentration per bin (no norm)"),
    "Ice_PSDlinNorm": ("cm-3 um-1", "per bin width; use with linear diameter axis"),
    "Ice_PSDlogNorm": ("cm-3 log(um-1)", "per log[upper/lower bin]; use with log diameter axis"),
    "Ice_PSDMnoNormMajsiz": ("unknown", "major-size variant; check file"),
    "Ice_PSDlinNormMajsiz": ("unknown", "major-size variant; check file"),
    "Ice_PSDlogNormMajsiz": ("unknown", "major-size variant; check file"),
}

DEFAULT_N_TIMELINE = 200
diameter_max = 300.0  # um; restrict fits to the growth regime before the coarse-particle tail.
DEFAULT_XSCALING = ("elapsed", "elapsed", "log")
DEFAULT_YSCALING = ("log", "log", "log")
DEFAULT_ALL_TS_ALPHA = 0.75
DEFAULT_ZOOM_TS_ALPHA = 0.35
DEFAULT_THRESHOLD = 1.0e-10
DEFAULT_XLIM = [np.datetime64("2023-01-25T12:31:00"), np.datetime64("2023-01-25T13:14:00")]
DEFAULT_PANEL_LIMS = {
    # Top panel y capped at 500 um: coarse tail >500 um is sparse and distracts
    # from the growth region where the fits live (<=300 um, see diameter_max).
    "all_timeseries":  ([.01, 35.0],   [5.0, 600.0],  [1.0, 3000.0]),
    "zoom_timeseries": ([5.0, 16.0],   [20.0, 325.0], [1.0, 3000.0]),
    "hist":            ([20.0, 280.0], [1.0, 1500.0], [None, None]),
}
# for composite line (white,color,white,black)
DEFAULT_LINE_FIT_KWARGS = { "outer": {  "color": "black", 
                                        "ls": "solid", 
                                        "lw": 2.1,
                                        "alpha": 0.7,
                                        "edge_color": "white",
                                        "edge_alpha": 0.7,
                                        "edge_lw": 0.325  }, 
                            "inner": {  "color": "black", 
                                        "ls": "dashed", 
                                        "lw": 0.35,
                                        "alpha": 1.0,
                                        "edge_color": "white",
                                        "edge_alpha": 0.95,
                                        "edge_lw": 0.075  }, }
DEFAULT_MISSION_MARKERS = ["o", "^", "*"]
DEFAULT_MISSION_MSIZES = [30.0, 30.0, 30.0]
DEFAULT_CBAR_KW = {"shrink": 0.8, "aspect": 15, "pad": 0.0, "extend": "both"}
DEFAULT_PEAK_MK = {"marker": ">", "s": 50, "ec": "black", "lw": 0.6, "zorder": 100, "clip_on": False}
DEFAULT_MED_MK  = {"marker": "v", "s": 50, "ec": "black", "lw": 0.6, "zorder": 100, "clip_on": False}

# Trajectory panel (lower-left): qualitative Okabe-Ito palette per HOLIMO mission,
# percentile envelope band across ensemble members, ensemble-mean D(t) line.
MISSION_COLORS = ("#0072B2", "#D55E00", "#009E73")
ENVELOPE_PCT = (10.0, 90.0)
ENSEMBLE_LINE_COLOR = "#1f1f1f"
ENSEMBLE_BAND_COLOR = "#6e6e6e"

# Two reference exponents frame the optimal alpha: sub-linear (0.5) and linear (1.0).
# A denser fan obscures the ensemble envelope on the trajectory panel.
DEFAULT_SUBLINEAR_ALPHAS: tuple[float, ...] = (0.25, 0.5, 0.75, 1.0, 1.25)


# ----- small helpers ----------------------------------------------------------
def holimo_scale_cm3_to_litres(holimo_var: str) -> float:
    """Scale HOLIMO ice PSD cm^-3 -> L^-1; return 1.0 for unknown-unit variants."""
    meta = HOLIMO_ICE_PSD_META.get(holimo_var)
    return 1.0 if (meta is not None and meta[0] == "unknown") else float(HOLIMO_CM3_TO_L1)


def default_plume_cmap():
    return create_fade_cmap(make_pastel(create_new_jet3(), desaturation=0.35, darken=0.80), n_fade=2)


def _float_fmt(x: float, _pos: int) -> str:
    return f"{x:.3f}".rstrip("0").rstrip(".") if x < 1 else f"{x:.0f}"


def _diam_log_edges(centers: np.ndarray) -> np.ndarray:
    c = np.unique(np.sort(centers[np.isfinite(centers) & (centers > 0)]))
    if c.size < 2:
        raise ValueError("Need at least two positive diameter centers to build log edges.")
    lc = np.log(c)
    edges = np.empty(c.size + 1)
    edges[1:-1] = 0.5 * (lc[:-1] + lc[1:])
    edges[0]   = lc[0]  - 0.5 * (lc[1]  - lc[0])
    edges[-1]  = lc[-1] + 0.5 * (lc[-1] - lc[-2])
    return np.exp(edges)


def _elapsed_span_min(da: xr.DataArray) -> float:
    t = np.asarray(da["time"].values)
    return max(float((t[-1] - t[0]) / np.timedelta64(1, "m")), 0.0) if t.size >= 2 else 0.0


def _ensemble_label_parts(unit_m: str) -> tuple[str, str]:
    """Mathtext for d n_f/d ln D and its volume unit (L^-1 aware)."""
    dnd = r"$\mathrm{d}n_f/\mathrm{d}\ln D$"
    u = str(unit_m).strip()
    if u == "L-1":
        return dnd, r"$L^{-1}$"
    if "L-1" in u:
        return dnd, "$" + u.replace("L-1", r"L^{-1}") + "$"
    return dnd, u


def peak_indices(values: np.ndarray, *, n: int = 2) -> tuple[int, ...]:
    v = np.asarray(values, dtype=float)
    if v.size < 3:
        return (int(np.nanargmax(v)),) if v.size and np.isfinite(v).any() else ()
    idx = [i for i in range(1, v.size - 1)
           if np.isfinite(v[i]) and v[i] >= v[i - 1] and v[i] >= v[i + 1]]
    return tuple(sorted(idx or [int(np.nanargmax(v))], key=lambda i: v[i], reverse=True)[:n])


def median_diameter(diam: np.ndarray, weights: np.ndarray) -> float:
    cdf = np.nancumsum(weights)
    return float(np.interp(0.5, cdf / cdf[-1], diam)) if cdf[-1] > 0 else np.nan


def hist_profile(
    da: xr.DataArray, *, threshold: float = 0.0, bin_edges: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    extra = [d for d in da.dims if d not in ("time", "diameter")]
    if extra:
        da = da.mean(dim=extra, skipna=True)
    diam = np.asarray(da["diameter"].values, dtype=float)
    tvals = np.asarray(da["time"].values)
    vals = np.asarray(da.values, dtype=float)
    if bin_edges is None:
        bin_edges = _diam_log_edges(diam)
    vals = np.where(np.isfinite(vals) & (vals > threshold), vals, 0.0)
    t_min = ((tvals - tvals[0]) / np.timedelta64(1, "m")).astype(float)
    hist, _ = np.histogram(diam, bins=bin_edges, weights=np.trapezoid(vals, x=t_min, axis=0))
    return np.sqrt(bin_edges[:-1] * bin_edges[1:]), hist.astype(float)


def plot_hist_line(
    ax: plt.Axes, diam: np.ndarray, values: np.ndarray, color: str, linewidth: float,
    *, label: str | None = None, zorder: int = 10,
) -> None:
    ax.fill_between(diam, values, step="pre", color=color, alpha=0.2, zorder=zorder)
    ax.step(diam, values, color=color, lw=linewidth, alpha=0.7, zorder=zorder + 1, label=label)
    ax.step(diam, values, color="black", lw=0.35, alpha=1.0, zorder=zorder + 2)


# ----- ensemble assembly ------------------------------------------------------
def smooth_model_diameter_distributions(
    datasets: dict[str, dict[str, xr.Dataset]],
    *, variable: str = DEFAULT_VARIABLE, window_bins: int = DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
) -> dict[str, dict[str, xr.Dataset]]:
    """Centered rectangular moving average along `diameter`."""
    w = int(window_bins)
    if w <= 1:
        return datasets
    out: dict[str, dict[str, xr.Dataset]] = {}
    for label, run in datasets.items():
        out[label] = {}
        for kind, ds in run.items():
            if not isinstance(ds, xr.Dataset) or variable not in ds or "diameter" not in ds[variable].dims:
                out[label][kind] = ds
                continue
            ds2 = ds.copy(deep=True)
            attrs = ds2[variable].attrs.copy()
            ds2[variable] = ds2[variable].rolling(diameter=w, center=True, min_periods=1).mean()
            ds2[variable].attrs = attrs
            out[label][kind] = ds2
    return out


def build_ensemble_mean_datasets(
    datasets: dict[str, dict[str, xr.Dataset]],
    *, variable: str = DEFAULT_VARIABLE, reindex_freq: str = "10s",
) -> dict[str, dict[str, xr.Dataset]]:
    """Mean across runs on a common time grid, normalized by Δln D."""
    kinds = {k for run in datasets.values() for k in run}
    ens: dict[str, xr.Dataset] = {}
    for kind in kinds:
        runs = [
            r[kind] for r in datasets.values()
            if isinstance(r.get(kind), xr.Dataset) and variable in r[kind]
        ]
        if not runs:
            continue
        t_min = min(r[variable].time.values.min() for r in runs)
        t_max = max(r[variable].time.values.max() for r in runs)
        common_t = pd.date_range(t_min, t_max, freq=reindex_freq)

        das = []
        for ds_k in runs:
            da = ds_k[variable]
            if "cell" in da.dims:
                da = da.sum("cell", keep_attrs=True, skipna=True)
            if kind == "vertical" and "altitude" in da.dims:
                da = da.mean("altitude", keep_attrs=True, skipna=True)
            das.append(da.reindex(time=common_t, method="nearest", tolerance="5s", fill_value=0.0))

        da_mean = xr.concat(das, dim="run").mean("run", keep_attrs=True)
        first = runs[0]
        edges = (first["diameter_edges"].values if "diameter_edges" in first
                 else _diam_log_edges(np.asarray(da_mean.diameter.values, dtype=float)))
        dlnD = np.log(edges[1:]) - np.log(edges[:-1])
        ds_mean = xr.Dataset({variable: da_mean / xr.DataArray(dlnD, dims="diameter")})
        ds_mean.attrs.update(first.attrs)
        ds_mean[variable].attrs = das[0].attrs.copy()
        ds_mean["diameter_edges"] = xr.DataArray(
            np.asarray(edges, dtype=float), dims=("diameter_edge",),
            attrs={"long_name": "diameter bin edges"},
        )
        ens[kind] = ds_mean
    return {"Ensemble Mean": ens} if ens else datasets


# ----- HOLIMO overlay ---------------------------------------------------------
def _flatten_holimo_mission(
    da: xr.DataArray, time_lo: np.datetime64, time_hi: np.datetime64,
    seed: np.datetime64, scale: float, threshold: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (elapsed_min, diameter, value) for all finite points above threshold."""
    sel = da.sel(time=slice(time_lo, time_hi))
    vals = np.asarray(sel.values, dtype=float) * scale
    diam = np.asarray(sel["diameter"].values, dtype=float)
    elapsed = ((np.asarray(sel["time"].values) - np.datetime64(seed)) / np.timedelta64(1, "m")).astype(float)
    X, Y = np.meshgrid(elapsed, diam, indexing="ij")
    ok = np.isfinite(vals) & (vals > threshold) & (X >= 0)
    return X[ok], Y[ok], vals[ok]


def add_holimo_column_scatter(
    axes: list[plt.Axes], da_holimo: xr.DataArray, *,
    obs_ids: list[str],
    time_frames_plume: list[list[np.datetime64]],
    seeding_start_times: list[np.datetime64],
    threshold: float = DEFAULT_THRESHOLD,
    unit_conversion: float = DEFAULT_UNIT_CONVERSION,
    mission_markers: list[str] = DEFAULT_MISSION_MARKERS,
    mission_msizes: list[float] = DEFAULT_MISSION_MSIZES,
    alphas: tuple[float, float] = (DEFAULT_ALL_TS_ALPHA, DEFAULT_ZOOM_TS_ALPHA),
    scatter_kwargs: dict[str, Any] | None = None,
) -> list[tuple[str]]:
    """Overlay HOLIMO spectra; duplicate (time,diameter) points keep the max value."""
    cmap = default_plume_cmap()
    vmin, vmax = DEFAULT_PANEL_LIMS["all_timeseries"][2]
    kw = {
        "edgecolors": "white", 
        "linestyle": "-", 
        "linewidths": 0.2,
        "zorder": 120,
        "cmap": cmap, 
        "norm": LogNorm(vmin=float(vmin), vmax=float(vmax)),
        **(scatter_kwargs or {}),
    }
    boost = [2.0, 15.0]

    # Flatten all missions, then keep max per (elapsed, diameter) across all.
    frames = []
    for m, (obs_id, (t_lo, t_hi)) in enumerate(zip(obs_ids, time_frames_plume)):
        x, y, c = _flatten_holimo_mission(
            da_holimo, t_lo, t_hi, seeding_start_times[m], unit_conversion, threshold,
        )
        if x.size:
            frames.append(pd.DataFrame({"x": x, "y": y, "c": c, "m": m}))

    profiles = [(o,) for o in obs_ids]
    if not frames:
        return profiles

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("c").drop_duplicates(subset=["x", "y"], keep="last")
    for m in range(len(obs_ids)):
        sub = df[df["m"] == m]
        if sub.empty:
            continue
        for ax_idx, (ax, alpha) in enumerate(zip(axes, alphas)):
            # Drop markers outside the axis's diameter range (zoom panel caps at 300 um).
            y_lo, y_hi = ax.get_ylim()
            keep = (sub["y"] >= y_lo) & (sub["y"] <= y_hi)
            s = sub[keep]
            if s.empty:
                continue
            ax.scatter(
                s["x"].to_numpy(), 
                s["y"].to_numpy(), 
                c=s["c"].to_numpy(),
                marker=mission_markers[m],
                s=mission_msizes[m] + boost[min(ax_idx, len(boost) - 1)],
                # linewidths=0.04 if ax_idx == 0 else 0.2,
                alpha=alpha,
                **kw,
            )
    return profiles


def _mission_legend_handles(profiles: list[tuple[str]], markers: list[str]) -> list[Line2D]:
    return [
        Line2D([0], [0], marker=markers[i], ls="None", mfc="grey", mec="black",
               mew=0.4, ms=5, alpha=0.8, label=profiles[i][0])
        for i in range(len(profiles))
    ]


# ----- context + render -------------------------------------------------------
def plume_lagrangian_output(repo_root: Path) -> Path:
    return repo_root / DEFAULT_OUTPUT


def load_plume_lagrangian_context(
    repo_root: Path, *,
    processed_root: str | Path | None = None,
    holimo_file: str | Path | None = None,
    runs: list[dict[str, str]] | None = None,
    kinds: tuple[str, ...] = DEFAULT_KINDS,
    kind: str = DEFAULT_KIND,
    smooth_model_diameter_bins: int = DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
) -> dict[str, Any]:
    processed_root = repo_root / DEFAULT_PROCESSED_ROOT if processed_root is None else Path(processed_root)
    holimo_file = repo_root / DEFAULT_HOLIMO_FILE if holimo_file is None else Path(holimo_file)
    runs = DEFAULT_RUNS if runs is None else runs

    datasets = load_plume_path_runs(runs, processed_root=processed_root, kinds=kinds)
    try:
        xlim_int = build_common_xlim(datasets, kind="integrated", span_min=35)
    except ValueError:
        xlim_int = [np.datetime64("2023-01-25T12:29:00"), np.datetime64("2023-01-25T13:04:00")]
    diag = diagnostics_table(datasets, kind="integrated", variable=DEFAULT_VARIABLE, xlim=xlim_int)
    ensemble = smooth_model_diameter_distributions(
        build_ensemble_mean_datasets(datasets, variable=DEFAULT_VARIABLE),
        variable=DEFAULT_VARIABLE, window_bins=smooth_model_diameter_bins,
    )
    ds_holimo = prepare_holimo_for_overlay(
        str(holimo_file), DEFAULT_TIME_WINDOW_HOLIMO,
        resample_s=10, smoothing_time_bins=3, min_coverage_frac=0.01,
    )
    return {
        "repo_root": repo_root,
        "processed_root": Path(processed_root),
        "holimo_file": Path(holimo_file),
        "datasets": datasets,
        "diag": diag,
        "ensemble_datasets": ensemble,
        "ds_holimo": ds_holimo,
        "kind": kind,
        "smooth_model_diameter_bins": int(smooth_model_diameter_bins),
        "output_path": plume_lagrangian_output(repo_root),
    }


def _draw_plume_panels(fig, ensemble, kind, cmap, alphas: tuple[float, float] = (0.95, 0.25)):
    gs = fig.add_gridspec(2, 2, width_ratios=[0.65, 0.35], wspace=0.1, hspace=0.1)
    ax_sym  = fig.add_subplot(gs[0, 0:2])
    ax_el   = fig.add_subplot(gs[1, 0])
    ax_hist = fig.add_subplot(gs[1, 1])
    kwargs = dict(
        kind=kind, variable=DEFAULT_VARIABLE,
        cbar_kwargs=DEFAULT_CBAR_KW,
        common_xlim_minutes=35,
        xlim=[DEFAULT_MODEL_SEED, DEFAULT_XLIM[1]],
        cmap=cmap, log_norm=True, add_missing_data=True, holimo_overlay=None,
        add_colorbar=False, add_shared_labels=False, annote_letters=False,
        return_pmesh=True, add_holimo_legend=False, marker_size_scale=0.05,
    )
    handles = {}
    for ax, key, alpha, xscaling, yscaling in (
        (ax_sym, "all_timeseries", alphas[0], DEFAULT_XSCALING[0], DEFAULT_YSCALING[0]),
        (ax_el,  "zoom_timeseries", alphas[1], DEFAULT_XSCALING[1], DEFAULT_YSCALING[1]),
    ):
        x_r, y_r, z_r = DEFAULT_PANEL_LIMS[key]
        _, _, pmeshs, scatters = plot_plume_path_sum(
            ensemble,
            x_axis_fmt=xscaling,
            y_axis_fmt=yscaling,
            zlim=z_r,
            axes_override=[ax],
            cmap_alpha=alpha,
            **kwargs,
        )
        ax.set(xlim=x_r, ylim=y_r)
        handles[key] = {"pmeshs": pmeshs, "scatters": scatters}
    return ax_sym, ax_el, ax_hist, handles


def _per_run_D_mean_on_grid(
    datasets: dict[str, dict[str, xr.Dataset]],
    *, kind: str, variable: str, t_grid: np.ndarray,
) -> np.ndarray:
    """(n_runs, n_t) array of weighted-mean D interpolated in log-D space onto ``t_grid``."""
    # Local import avoids a circular import (plume_path_plot imports style_profiles only).
    from utilities.plume_path_plot import _assign_elapsed_time, _prepare_da
    rows: list[np.ndarray] = []
    for run in datasets.values():
        ds = run.get(kind)
        if not isinstance(ds, xr.Dataset) or variable not in ds:
            continue
        da = _prepare_da(ds, variable, sum_cell=True).sel(time=slice(*DEFAULT_XLIM))
        if da.sizes.get("time", 0) == 0:
            continue
        da = _assign_elapsed_time(da, DEFAULT_MODEL_SEED).sel(diameter=slice(None, diameter_max))
        t_r, D_r = _weighted_mean_D_per_time(da)
        if t_r.size < 2:
            continue
        order = np.argsort(t_r)
        rows.append(np.exp(np.interp(t_grid, t_r[order], np.log(D_r[order]),
                                     left=np.nan, right=np.nan)))
    if not rows:
        raise ValueError("No per-run trajectories available for envelope computation.")
    return np.vstack(rows)


def _mission_trajectory(
    da_holimo: xr.DataArray, t_lo: np.datetime64, t_hi: np.datetime64, seed: np.datetime64,
) -> tuple[np.ndarray, np.ndarray]:
    from utilities.plume_path_plot import _assign_elapsed_time
    sub = da_holimo.sel(time=slice(t_lo, t_hi))
    if sub.sizes.get("time", 0) == 0:
        return np.array([]), np.array([])
    sub = _assign_elapsed_time(sub, seed).sel(diameter=slice(None, diameter_max))
    return _weighted_mean_D_per_time(sub)


def _draw_trajectory_panel(
    ax: plt.Axes,
    datasets: dict[str, dict[str, xr.Dataset]],
    da_holimo: xr.DataArray,
    *, kind: str,
) -> None:
    """Growth trajectories on the lower-left panel.

    Draws per-run ensemble 10-90th percentile band + ensemble-mean D(t), and
    HOLIMO per-mission weighted-mean D(t) as connected colored markers. Fit
    overlays (model optimal, per-mission HOLIMO linear, fixed-alpha reference
    fan) are added separately by ``_add_growth_fits``.
    """
    x_lim, y_lim, _ = DEFAULT_PANEL_LIMS["zoom_timeseries"]
    t_grid = np.linspace(x_lim[0], x_lim[1], 400)

    runs_D = _per_run_D_mean_on_grid(
        datasets, kind=kind, variable=DEFAULT_VARIABLE, t_grid=t_grid,
    )
    with np.errstate(invalid="ignore"):
        p_lo = np.nanpercentile(runs_D, ENVELOPE_PCT[0], axis=0)
        p_hi = np.nanpercentile(runs_D, ENVELOPE_PCT[1], axis=0)
        mean_line = np.exp(np.nanmean(np.log(runs_D), axis=0))
    ok_band = np.isfinite(p_lo) & np.isfinite(p_hi)
    # zorder chosen to sit above the fixed-alpha reference fan (~206) but below
    # the fit overlays (~211) and mission markers (~150 from _draw_trajectory_panel).
    band_artist = ax.fill_between(
        t_grid[ok_band], p_lo[ok_band], p_hi[ok_band],
        color=ENSEMBLE_BAND_COLOR, alpha=0.35, lw=0, zorder=205,
        label=f"model {ENVELOPE_PCT[0]:.0f}-{ENVELOPE_PCT[1]:.0f}th pct.",
    )
    ok_line = np.isfinite(mean_line)
    (mean_artist,) = ax.plot(
        t_grid[ok_line], mean_line[ok_line],
        color=ENSEMBLE_LINE_COLOR, lw=2.0, alpha=0.95, zorder=207,
        path_effects=[pe.withStroke(linewidth=3.2, foreground="white", alpha=0.9)],
        label=r"model ensemble $\overline{D}(t)$",
    )

    mission_handles: list[Line2D] = []
    for mi, obs_id in enumerate(DEFAULT_OBS_IDS):
        t_lo, t_hi = DEFAULT_TIME_FRAMES_PLUME[mi]
        seed = np.datetime64(DEFAULT_SEEDING_START_TIMES[mi])
        t_o, D_o = _mission_trajectory(da_holimo, t_lo, t_hi, seed)
        color = MISSION_COLORS[mi % len(MISSION_COLORS)]
        marker = DEFAULT_MISSION_MARKERS[mi]
        ms = np.sqrt(DEFAULT_MISSION_MSIZES[mi]) * 1.1
        if t_o.size:
            order = np.argsort(t_o)
            ax.plot(
                t_o[order], D_o[order],
                color=color, lw=0.7, alpha=0.85, zorder=150,
                marker=marker, ms=ms, mec="black", mew=0.3, mfc=color,
            )
        mission_handles.append(
            Line2D([0], [0], marker=marker, color=color, mec="black", mew=0.3,
                   lw=0.7, ms=ms, label=obs_id)
        )

    ax.set(xlim=x_lim, ylim=y_lim, yscale="log")
    # Single consolidated legend: ensemble band, ensemble mean, then missions.
    # Mission legend is already shown on the top panel; colour identifies it here.
    traj_leg = ax.legend(
        handles=[band_artist, mean_artist, *mission_handles],
        loc="upper left", fontsize=7, title_fontsize=7, framealpha=0.8,
        labelspacing=0.3, borderaxespad=0.3,
    )
    traj_leg.set_zorder(500)
    ax.add_artist(traj_leg)


def _style_panels(ax_sym, ax_el, ax_hist):
    x_z, y_z = DEFAULT_PANEL_LIMS["zoom_timeseries"][:2]
    ax_sym.add_patch(plt.Rectangle(
        (x_z[0], y_z[0]), x_z[1] - x_z[0], y_z[1] - y_z[0],
        fill=False, ec="black", ls="--", lw=0.7, zorder=300,
    ))
    ax_el.set_title("")
    ax_sym.set_xticks([t for t in ax_sym.get_xticks() if not np.isclose(t, 0.0)])
    for ax in (ax_sym, ax_el):
        ax.xaxis.set_minor_locator(AutoMinorLocator(5))
        ax.tick_params(axis="x", which="minor", length=2.5, width=0.5)
    for sp in ax_el.spines.values():
        sp.set_color("black"); sp.set_linestyle("--")
    for ax in (ax_sym, ax_el):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["left"].set_visible(False)


def _weighted_mean_D_per_time(
    da: xr.DataArray, *, weighted: bool = True,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (elapsed_min, D_mean_logspace) per time step.

    ``weighted=True``  -> concentration-weighted geometric mean (weights = values).
    ``weighted=False`` -> unweighted geometric mean over "active" diameter bins
    (those with finite, positive values at that time step). Useful for a plain
    size-trend fit that is not biased by per-bin concentration.
    """
    extra = [d for d in da.dims if d not in ("time", "diameter")]
    if extra:
        da = da.mean(dim=extra, skipna=True)
    diam = np.asarray(da["diameter"].values, dtype=float)
    vals = np.asarray(da.values, dtype=float)
    active = np.isfinite(vals) & (vals > 0)
    log_d = np.log(diam)[None, :]
    with np.errstate(invalid="ignore", divide="ignore"):
        if weighted:
            w = np.where(active, vals, 0.0)
            den = w.sum(axis=1)
            num = (w * log_d).sum(axis=1)
        else:
            den = active.sum(axis=1).astype(float)
            num = np.where(active, log_d, 0.0).sum(axis=1)
        D_mean = np.exp(num / den)
    t_elapsed = np.asarray(da["time_elapsed"].values, dtype=float)
    ok = np.isfinite(t_elapsed) & np.isfinite(D_mean) & (D_mean > 0)
    return t_elapsed[ok], D_mean[ok]


def _draw_fit_line(
    ax: plt.Axes, x, y, *,
    outer: dict[str, Any] | None = None,
    inner: dict[str, Any] | None = None,
    zorder: int = 200,
    label: str | None = None,
) -> tuple[Line2D, Line2D]:
    """Composite fit line: thick colored band + fine inner trace, each haloed.

    Styles sourced from ``DEFAULT_LINE_FIT_KWARGS`` and overridden by the
    per-call ``outer`` / ``inner`` dicts. Halo is a ``path_effects.withStroke``
    of total width ``lw + 2*edge_lw``. When ``label`` is given, a single
    composite legend entry is stashed on ``ax._composite_handles``.
    """
    def _plot(kw: dict[str, Any], z: int) -> Line2D:
        (ln,) = ax.plot(
            x, 
            y, 
            color=kw["color"], 
            linewidth=kw["lw"], 
            ls=kw["ls"],
            alpha=kw["alpha"], 
            zorder=z,
            dash_capstyle="round", 
            solid_capstyle="round",
            path_effects=[pe.withStroke( linewidth=kw["lw"] + 2.0 * kw["edge_lw"],
                                         foreground=kw["edge_color"], 
                                         alpha=kw["edge_alpha"], )],
        )
        return ln

    l_out = _plot({**DEFAULT_LINE_FIT_KWARGS["outer"], **(outer or {})}, zorder)
    l_in = _plot({**DEFAULT_LINE_FIT_KWARGS["inner"], **(inner or {})}, zorder + 1)
    if label is not None:
        ax.__dict__.setdefault("_composite_handles", []).append((label, (l_out, l_in)))
    return l_out, l_in


# Sparser dash patterns than matplotlib defaults (more white between on-segments).
DASH_LOOSE = (0, (8, 4))  # replaces "--"

def _add_growth_fits(
    axes: list[plt.Axes], ax_legend: plt.Axes, da_model: xr.DataArray,
    obs_elapsed: dict[str, xr.DataArray], elapsed_wins: list[tuple[float, float]],
    alphas: tuple[float, ...] = DEFAULT_SUBLINEAR_ALPHAS,
) -> None:
    """Overlay growth fits on ``axes``; legend is placed on ``ax_legend`` only."""
    panel_lo, panel_hi = DEFAULT_PANEL_LIMS["zoom_timeseries"][0]
    hol_t0 = min(lo for lo, _ in elapsed_wins)
    hol_t1 = max(hi for _, hi in elapsed_wins)

    da_m = da_model.where(
        (da_model.time_elapsed >= panel_lo) & (da_model.time_elapsed <= panel_hi), drop=True,
    ).sel(diameter=slice(None, diameter_max))
    t_m, D_m = _weighted_mean_D_per_time(da_m)

    if t_m.size >= 2:
        a_mu, b_mu = np.polyfit(t_m, D_m, 1)
        # Power-law coefficients for D = C * t^alpha, alpha fixed per curve: OLS in log-log
        # gives log C = mean(log D - alpha * log t) when alpha is constrained.
        log_t_m, log_D_m = np.log(t_m), np.log(D_m)
        alpha_C = {
            float(a): float(np.exp(np.mean(log_D_m - float(a) * log_t_m))) for a in alphas
        }
        # Optimal 2-parameter power-law fit: log D = alpha* log t + log C*, OLS in log-log.
        alpha_star, log_C_star = np.polyfit(log_t_m, log_D_m, 1)
        alpha_star = float(alpha_star)
        C_star = float(np.exp(log_C_star))
        # RMSEs in log-D space for comparing optimal vs fixed-alpha vs linear fit.
        rmse_opt = float(np.sqrt(np.mean((log_D_m - (alpha_star * log_t_m + log_C_star)) ** 2)))
        rmse_fixed = {
            float(a): float(np.sqrt(np.mean((log_D_m - (a * log_t_m + np.log(alpha_C[float(a)]))) ** 2)))
            for a in alphas
        }
        rmse_lin = float(np.sqrt(np.mean((log_D_m - np.log(a_mu * t_m + b_mu)) ** 2)))
        print(
            f"[plume_lagrangian] model linear fit (weighted mean D): "
            f"n={t_m.size}  t=[{t_m.min():5.2f},{t_m.max():5.2f}]  "
            f"slope={a_mu:+7.3f} um/min  intercept={b_mu:+7.1f}"
        )
        rmse_fixed_str = ", ".join(f"a={a:.2f}:{r:.4f}" for a, r in rmse_fixed.items())
        print(
            f"[plume_lagrangian] model optimal power law: "
            f"alpha*={alpha_star:+6.3f}  C*={C_star:7.3f}  "
            f"rmse_logD={rmse_opt:.4f}  rmse_lin={rmse_lin:.4f}  "
            f"rmse_fixed=[{rmse_fixed_str}]"
        )
        # Model + α reference: full x extent on both panels. HOLIMO-only line clipped later.
        for idx, ax in enumerate(axes):
            is_legend = ax is ax_legend
            ax_lo, ax_hi = ax.get_xlim()
            t_lo_fit = max(ax_lo, 1e-3)
            t_hi_fit = ax_hi
            if t_hi_fit <= t_lo_fit:
                t_hi_fit = t_lo_fit + 1e-6
            t_fit = np.linspace(t_lo_fit, t_hi_fit, DEFAULT_N_TIMELINE)

            _draw_fit_line(
                ax, t_fit, a_mu * t_fit + b_mu,
                outer={"color": "#FFB343"},
                inner={"ls": (0, (8,  4))}, # corresponds to "loose dashed" line
                zorder=211,
                label="model D(t) linear fit",
            )
            _draw_fit_line(
                ax, t_fit, C_star * t_fit ** alpha_star,
                outer={"color": "#CD1C18"},
                inner={"ls": (0, (8,  4))}, # corresponds to "loose dashed" line
                zorder=212,
                label=rf"model optimal $D=C\,t^{{\alpha}}$ ($\alpha$={alpha_star:.2f})",
            )
            a_lo, a_hi = min(alphas), max(alphas)
            span = (a_hi - a_lo) or 1.0
            for i_a, a_fix in enumerate(alphas):
                seg = 8.0 - 6.0 * (a_fix - a_lo) / span
                C_fix = float(np.exp(np.mean(log_D_m - a_fix * log_t_m)))
                (ln,) = ax.plot(
                    t_fit, C_fix * t_fit ** a_fix,
                    color="black", ls=(0, (seg, seg)),
                    lw=0.65, alpha=0.95, zorder=206,
                )
                if is_legend:
                    ax.__dict__.setdefault("_composite_handles", []).append(
                        (rf"$D\propto t^{{{a_fix:.2f}}}$", ln)
                    )
                    if not np.isclose(a_fix, 0.75):
                        t_end = float(ax_hi)
                        y_ann = float(C_fix * (t_end ** a_fix))
                        if np.isfinite(y_ann):
                            n_lab = len(alphas)
                            dy_pt = (i_a - 0.5 * (n_lab - 1)) * 1.0
                            ax.annotate(
                                f"{a_fix:g}",
                                xy=(t_end, y_ann),
                                xytext=(4.0, dy_pt),
                                textcoords="offset points",
                                fontsize=4.4,
                                color="black",
                                ha="left",
                                va="center",
                                clip_on=False,
                                zorder=207,
                            )
            if is_legend:
                t_end = float(ax_hi)
                y_opt = float(C_star * (t_end ** alpha_star))
                if np.isfinite(y_opt):
                    ax.annotate(
                        f"{alpha_star:.2f}",
                        xy=(t_end, y_opt),
                        xytext=(4.0, 0.0),
                        textcoords="offset points",
                        fontsize=4.4,
                        color="#CD1C18",
                        ha="left",
                        va="center",
                        clip_on=False,
                        zorder=208,
                    )

    # Combined HOLIMO linear fit across all mission coverage windows; drawn on
    # every axis (both the top panel and the zoom panel).
    combined_t: list[np.ndarray] = []
    combined_D: list[np.ndarray] = []
    print("[plume_lagrangian] HOLIMO weighted-mean D(t) linear fit:")
    for mi, obs_id in enumerate(DEFAULT_OBS_IDS):
        da_o = obs_elapsed.get(obs_id)
        if da_o is None:
            continue
        lo, hi = elapsed_wins[mi]
        da_o = da_o.where(
            (da_o.time_elapsed >= lo) & (da_o.time_elapsed <= hi), drop=True,
        ).sel(diameter=slice(None, diameter_max))
        t_o, D_o = _weighted_mean_D_per_time(da_o)
        if t_o.size < 2:
            continue
        combined_t.append(t_o)
        combined_D.append(D_o)
        a_o, b_o = np.polyfit(t_o, D_o, 1)
        print(
            f"  {obs_id}: n={t_o.size:4d}  "
            f"t=[{t_o.min():5.2f},{t_o.max():5.2f}] min  "
            f"D=[{D_o.min():6.1f},{D_o.max():6.1f}] um  "
            f"D_first={D_o[0]:6.1f}  D_last={D_o[-1]:6.1f}  "
            f"slope={a_o:+7.3f} um/min  intercept={b_o:+7.1f}"
        )

    if combined_t:
        tc = np.concatenate(combined_t)
        Dc = np.concatenate(combined_D)
        a_c, b_c = np.polyfit(tc, Dc, 1)
        print(
            f"  combined: n={tc.size:4d}  "
            f"t=[{tc.min():5.2f},{tc.max():5.2f}] min  "
            f"slope={a_c:+7.3f} um/min  intercept={b_c:+7.1f}"
        )
        for ax in axes:
            ax_lo, ax_hi = ax.get_xlim()
            if ax is ax_legend:
                t_lo_c = max(ax_lo, hol_t0, 1e-3)
                t_hi_c = min(ax_hi, hol_t1)
            else:
                t_lo_c = max(ax_lo, 1e-3)
                t_hi_c = ax_hi
            if t_hi_c <= t_lo_c:
                t_hi_c = t_lo_c + 1e-6
            t_comb = np.linspace(t_lo_c, t_hi_c, DEFAULT_N_TIMELINE)
            _draw_fit_line(
                ax, t_comb, a_c * t_comb + b_c,
                outer={"color": "royalblue"},
                inner={"ls": (0, (8,  4)), "color": "white"}, # corresponds to "loose dashed" line
                zorder=209,
                label="HOLIMO linear fit",
            )

    # Build the fit-lines legend on axes that collected handles, skipping the
    # zoom panel (ax_legend). If a pre-existing legend is present (the HOLIMO
    # missions box on ax_sym), preserve it and place the new legend immediately
    # to its left with a shared bottom edge.
    for ax in axes:
        if ax is ax_legend:
            continue
        composite = ax.__dict__.get("_composite_handles", [])
        if not composite:
            continue
        labels = [l for l, _ in composite]
        handles = [h for _, h in composite]
        prev_leg = ax.get_legend()
        legend_kw = dict(
            handles=handles, labels=labels,
            handler_map={tuple: HandlerTuple(ndivide=1, pad=0.0)},
            fontsize=7, framealpha=0.8,
            borderaxespad=0.3, handlelength=2.6, labelspacing=0.3,
        )
        if prev_leg is not None:
            # Anchor new legend's lower-right at the missions legend's lower-left
            # so bottoms align and the new box sits to the left of the missions.
            ax.add_artist(prev_leg)
            ax.figure.canvas.draw()  # needed for get_window_extent to resolve
            bb = prev_leg.get_window_extent().transformed(ax.transAxes.inverted())
            leg = ax.legend( **legend_kw, loc="lower right",
                            bbox_to_anchor=(bb.x0-0.05*bb.x0, bb.y0), bbox_transform=ax.transAxes,
            )
        else:
            leg = ax.legend(**legend_kw, loc="lower right")
        if leg is not None:
            leg.set_zorder(500)
            leg.get_frame().set_alpha(0.8)


def render_plume_lagrangian_figure(
    context: dict[str, Any], *, output_path: str | Path | None = None,
) -> tuple[plt.Figure, Path]:
    ensemble = context["ensemble_datasets"]
    ds_holimo = context["ds_holimo"]
    kind = context["kind"]
    all_ts_alpha = DEFAULT_ALL_TS_ALPHA
    zoom_ts_alpha = DEFAULT_ZOOM_TS_ALPHA
    hol_scale = holimo_scale_cm3_to_litres(DEFAULT_HOLIMO_VAR)
    cmap = default_plume_cmap()

    fig_w = FULL_COL_IN
    fig = plt.figure(figsize=(fig_w, min(fig_w / (9.4 / 6.0), MAX_H_IN)), constrained_layout=True)
    ax_sym, ax_el, ax_hist, handles = _draw_plume_panels(
        fig, 
        ensemble, 
        kind, 
        cmap, 
        alphas=(all_ts_alpha, zoom_ts_alpha)
    )
    _style_panels(ax_sym, ax_el, ax_hist)

    unit_m = ensemble[next(iter(ensemble))][kind][DEFAULT_VARIABLE].attrs.get("units", "-")
    
    dnd, vol = _ensemble_label_parts(unit_m)
    fig.colorbar(handles["all_timeseries"]["pmeshs"][0], ax=ax_sym, **DEFAULT_CBAR_KW).set_label(f"ensemble-avg. {dnd} / ({vol})")

    da_holimo = ds_holimo[DEFAULT_HOLIMO_VAR]
    profiles = add_holimo_column_scatter(
        [ax_sym, ax_el], da_holimo,
        obs_ids = DEFAULT_OBS_IDS,
        time_frames_plume = DEFAULT_TIME_FRAMES_PLUME,
        seeding_start_times = DEFAULT_SEEDING_START_TIMES,
        threshold = DEFAULT_THRESHOLD,
        unit_conversion = hol_scale,
        alphas = (all_ts_alpha, zoom_ts_alpha),
        mission_markers = DEFAULT_MISSION_MARKERS,
        mission_msizes = DEFAULT_MISSION_MSIZES,
    )
    ax_sym.legend(
        handles=_mission_legend_handles(profiles, DEFAULT_MISSION_MARKERS),
        title="HOLIMO missions", fontsize=7, title_fontsize=7,
        loc="lower right", framealpha=0.8,
    )


    # --- histogram panel -----------------------------------------------------
    run_label = next(l for l, r in ensemble.items() if isinstance(r.get(kind), xr.Dataset))
    da_model = _assign_elapsed_time(
        _prepare_da(ensemble[run_label][kind], DEFAULT_VARIABLE, sum_cell=True).sel(time=slice(*DEFAULT_XLIM)),
        DEFAULT_MODEL_SEED,
    )
    y_elapsed = DEFAULT_PANEL_LIMS["zoom_timeseries"][1]
    be_mod = _diam_log_edges(np.asarray(da_model["diameter"].sel(diameter=slice(*y_elapsed)).values, dtype=float))
    be_hol = _diam_log_edges(np.asarray(da_holimo["diameter"].sel(diameter=slice(*y_elapsed)).values, dtype=float))

    elapsed_wins: list[tuple[float, float]] = []
    obs_elapsed: dict[str, xr.DataArray] = {}
    for mi, (obs_id, (t_lo, t_hi)) in enumerate(zip(DEFAULT_OBS_IDS, DEFAULT_TIME_FRAMES_PLUME)):
        seed = np.datetime64(DEFAULT_SEEDING_START_TIMES[mi])
        lo = float((np.datetime64(t_lo) - seed) / np.timedelta64(1, "m"))
        hi = float((np.datetime64(t_hi) - seed) / np.timedelta64(1, "m"))
        elapsed_wins.append((min(lo, hi), max(lo, hi)))
        da_obs = da_holimo.sel(time=slice(t_lo, t_hi))
        if da_obs.sizes.get("time", 0):
            obs_elapsed[obs_id] = _assign_elapsed_time(da_obs, seed)

    _add_growth_fits([ax_sym, ax_el], ax_el, da_model, obs_elapsed, elapsed_wins)

    el_lo = min(w[0] for w in elapsed_wins)
    el_hi = max(w[1] for w in elapsed_wins)
    da_mod_win = da_model.where(
        (da_model.time_elapsed >= el_lo) & (da_model.time_elapsed <= el_hi), drop=True,
    )
    d_mod, h_mod = hist_profile(da_mod_win, bin_edges=be_mod)
    dur_mod = _elapsed_span_min(da_mod_win)
    f_mod = h_mod / dur_mod if dur_mod > 0 else h_mod
    plot_hist_line(ax_hist, d_mod, f_mod, "orange", 1.75, label="COSMO-SPECS", zorder=13)

    h_obs = None
    d_obs = np.array([])
    dur_obs = 0.0
    for mi, obs_id in enumerate(DEFAULT_OBS_IDS):
        da_obs = obs_elapsed.get(obs_id)
        if da_obs is None:
            continue
        lo, hi = elapsed_wins[mi]
        da_obs = da_obs.where((da_obs.time_elapsed >= lo) & (da_obs.time_elapsed <= hi), drop=True)
        d_cur, h_cur = hist_profile(da_obs, bin_edges=be_hol, threshold=DEFAULT_THRESHOLD)
        if not d_cur.size:
            continue
        h_obs = h_cur.astype(float) if h_obs is None else (
            h_obs + h_cur if h_obs.shape == h_cur.shape else h_obs
        )
        d_obs = d_cur if not d_obs.size else d_obs
        dur_obs += _elapsed_span_min(da_obs)

    f_obs = h_obs / dur_obs if h_obs is not None and dur_obs > 0 else np.array([])
    if not (f_obs.size and np.isfinite(f_obs).any()):
        raise ValueError("No valid HOLIMO histogram data found.")
    f_obs_plot = f_obs * hol_scale

    hist_xlim, hist_ylim = DEFAULT_PANEL_LIMS["hist"][:2]
    plot_hist_line(ax_hist, d_obs, f_obs_plot, "royalblue", 1.75, label="HOLIMO", zorder=5)
    ax_hist.set(
        xscale=DEFAULT_XSCALING[-1], 
        yscale=DEFAULT_YSCALING[-1], 
        xlim=hist_xlim, 
        ylim=hist_ylim,
        ylabel=f"time-avg. {dnd} / ({vol})", 
        xlabel=r"D$_{\mathrm{eq}}$ / ($\mu$m)",
    )
    ax_hist.grid(True, which="major", ls="--", lw=0.25, alpha=0.6)
    ax_hist.grid(True, which="minor", ls=":",  lw=0.15, alpha=0.35)
    ax_hist.yaxis.tick_right()
    ax_hist.yaxis.set_label_position("right")
    fmt = plt.FuncFormatter(_float_fmt)
    ax_hist.xaxis.set_major_formatter(fmt)
    ax_hist.yaxis.set_major_formatter(fmt)

    x_edge = hist_xlim[1] * 0.92
    y_bot = 1.2 * ax_hist.get_ylim()[0]
    for i in peak_indices(f_mod, n=2):
        ax_hist.scatter(x_edge, f_mod[i], color="orange", **DEFAULT_PEAK_MK)
    for i in peak_indices(f_obs_plot, n=1):
        ax_hist.scatter(x_edge, f_obs_plot[i], color="royalblue", **DEFAULT_PEAK_MK)
    ax_hist.scatter(median_diameter(d_mod, f_mod),      y_bot, color="orange",    **DEFAULT_MED_MK)
    ax_hist.scatter(median_diameter(d_obs, f_obs_plot), y_bot, color="royalblue", **DEFAULT_MED_MK)
    ax_hist.legend(
        [Line2D([], [], color="orange", lw=2.0, alpha=0.8),
         Line2D([], [], color="royalblue", lw=2.0, alpha=0.8)],
        ["COSMO-SPECS", "HOLIMO"], loc="upper right", frameon=False, fontsize=7,
        handler_map={tuple: HandlerTuple(ndivide=None)},
    )

    ax_sym.set_title("")
    ax_sym.set_xlabel(r"time elapsed / (min)")
    ax_el.set_xlabel(r"time elapsed / (min)")
    ax_el.yaxis.set_major_formatter(fmt)
    fig.supylabel(r"equivalent diameter D$_{\mathrm{eq}}$ / ($\mu$m)")
    ax_sym.set_xlim(DEFAULT_PANEL_LIMS["all_timeseries"][0])

    # Mark the fit-relevant diameter cap on the top panel (fits use diameter_max=300 um).
    ax_sym.axhline(diameter_max, color="black", ls=":", lw=0.6, alpha=0.5, zorder=50)
    ax_el.annotate(
        rf"$D_{{\max}}={diameter_max:.0f}\,\mu$m (fit cutoff)",
        xy=(DEFAULT_PANEL_LIMS["zoom_timeseries"][0][1], 20),
        xytext=(-4, 3), textcoords="offset points",
        ha="right", va="bottom", fontsize=6, alpha=0.7,
    )

    # Publication-standard panel letters.
    for ax, letter in ((ax_sym, "A"), (ax_el, "B"), (ax_hist, "C")):
        ax.text(
            0.012, 0.97, f"({letter})", transform=ax.transAxes,
            ha="left", va="top", fontweight="semibold", fontsize=12,
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=1.5),
            zorder=1000,
        )

    out = context["output_path"] if output_path is None else Path(output_path)
    return fig, out


def save_plume_lagrangian_figure(fig: plt.Figure, output_path: str | Path, *, dpi: int = 500) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, bbox_inches="tight")
    print(f"saved -> {out.resolve().as_uri()}")
    return out
