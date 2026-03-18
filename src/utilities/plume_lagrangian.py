"""
Plume-lagrangian figure helpers promoted from notebook 03.

This module reuses the existing plume-path and HOLIMO utility layer and adds
the notebook-specific ensemble-mean assembly plus the histogram summary panel.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.colors import LogNorm
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

DEFAULT_PROCESSED_ROOT = Path("data") / "processed"
DEFAULT_HOLIMO_FILE = (
    Path("data")
    / "observations"
    / "holimo_data"
    / "CL_20230125_1000_1140_SM058_SM060_ts1.nc"
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
DEFAULT_TIME_WINDOW_HOLIMO = (
    np.datetime64("2023-01-25T10:10:00"),
    np.datetime64("2023-01-25T12:00:00"),
)
DEFAULT_TIME_FRAMES_PLUME = [
    [np.datetime64("2023-01-25T10:56:00"), np.datetime64("2023-01-25T11:04:00")],
    [np.datetime64("2023-01-25T10:35:00"), np.datetime64("2023-01-25T10:42:00")],
    [np.datetime64("2023-01-25T11:24:00"), np.datetime64("2023-01-25T11:29:00")],
]
DEFAULT_OBS_IDS = ["SM059", "SM058", "SM060"]
DEFAULT_GROWTH_TIMES_MIN = [6.1, 8.0, 9.1]
DEFAULT_SEEDING_START_TIMES = [
    np.datetime64("2023-01-25T10:50:00"),
    np.datetime64("2023-01-25T10:28:00"),
    np.datetime64("2023-01-25T11:15:00"),
]
DEFAULT_MODEL_SEED = np.datetime64("2023-01-25T12:30:00")
DEFAULT_KIND = "extreme"
DEFAULT_VARIABLE = "nf"
DEFAULT_HOLIMO_VAR = "Ice_PSDlogNorm"
DEFAULT_UNIT_CONVERSION = 1000.0
DEFAULT_THRESHOLD = 1.0e-10
DEFAULT_XLIM = [np.datetime64("2023-01-25T12:31:00"), np.datetime64("2023-01-25T13:14:00")]
DEFAULT_PANEL_LIMS = {
    "symlog": ([1.0, 45.0], [5.0, 1000.0], [1.0, 2000.0]),
    "elapsed": ([6.0, 15.0], [20.0, 300.0], [1.0, 2000.0]),
    "hist": ([20.0, 280.0], [1.0, 1000.0], [None, None]),
}
DEFAULT_MISSION_MARKERS = ["o", "^", "*"]
DEFAULT_MISSION_MSIZES = [30.0, 30.0, 30.0]
DEFAULT_CBAR_KW = {"shrink": 0.8, "aspect": 25, "pad": 0.0, "extend": "both"}
DEFAULT_PEAK_MK = {"marker": ">", "s": 80, "ec": "black", "lw": 0.6, "zorder": 100, "clip_on": True}
DEFAULT_MED_MK = {"marker": "v", "s": 70, "ec": "black", "lw": 0.9, "zorder": 100, "clip_on": True}


def default_plume_cmap():
    base = create_new_jet3()
    return create_fade_cmap(make_pastel(base, desaturation=0.25, darken=0.90), n_fade=2)


def _float_fmt(x: float, _pos: int) -> str:
    return f"{x:.3f}".rstrip("0").rstrip(".") if x < 1 else f"{x:.0f}"


def _diam_log_edges(centers: np.ndarray) -> np.ndarray:
    centers = np.unique(np.sort(centers[np.isfinite(centers) & (centers > 0)]))
    if centers.size < 2:
        raise ValueError("Need at least two positive diameter centers to build log edges.")
    log_centers = np.log(centers)
    edges = np.empty(centers.size + 1)
    edges[1:-1] = 0.5 * (log_centers[:-1] + log_centers[1:])
    edges[0] = log_centers[0] - 0.5 * (log_centers[1] - log_centers[0])
    edges[-1] = log_centers[-1] + 0.5 * (log_centers[-1] - log_centers[-2])
    return np.exp(edges)


def hist_profile(
    da: xr.DataArray,
    *,
    threshold: float = 0.0,
    bin_edges: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    extra_dims = [dim for dim in da.dims if dim not in ("time", "diameter")]
    if extra_dims:
        da = da.mean(dim=extra_dims, skipna=True)
    diam = np.asarray(da["diameter"].values, dtype=float)
    tvals = np.asarray(da["time"].values)
    vals = np.asarray(da.values, dtype=float)
    if bin_edges is None:
        bin_edges = _diam_log_edges(diam)
    vals = np.where(np.isfinite(vals) & (vals > threshold), vals, 0.0)
    t_min = ((tvals - tvals[0]) / np.timedelta64(1, "m")).astype(float)
    hist, _ = np.histogram(diam, bins=bin_edges, weights=np.trapezoid(vals, x=t_min, axis=0))
    return np.sqrt(bin_edges[:-1] * bin_edges[1:]), hist.astype(float)


def elapsed_duration_minutes(da: xr.DataArray) -> float:
    time_values = np.asarray(da["time"].values)
    if time_values.size < 2:
        return 0.0
    return max(float((time_values[-1] - time_values[0]) / np.timedelta64(1, "m")), 0.0)


def peak_indices(values: np.ndarray, *, n: int = 2) -> tuple[int, ...]:
    values = np.asarray(values, dtype=float)
    if values.size < 3:
        return (int(np.nanargmax(values)),) if values.size and np.isfinite(values).any() else ()
    idx = [
        i
        for i in range(1, values.size - 1)
        if np.isfinite(values[i]) and values[i] >= values[i - 1] and values[i] >= values[i + 1]
    ]
    return tuple(sorted(idx or [int(np.nanargmax(values))], key=lambda i: values[i], reverse=True)[:n])


def median_diameter(diam: np.ndarray, weights: np.ndarray) -> float:
    cdf = np.nancumsum(weights)
    return float(np.interp(0.5, cdf / cdf[-1], diam)) if cdf[-1] > 0 else np.nan


def plot_hist_line(
    ax: plt.Axes,
    diam: np.ndarray,
    values: np.ndarray,
    color: str,
    linewidth: float,
    *,
    label: str | None = None,
    zorder: int = 10,
) -> None:
    ax.fill_between(diam, values, step="pre", color=color, alpha=0.2, zorder=zorder)
    ax.step(diam, values, color=color, lw=linewidth, alpha=0.7, zorder=zorder + 1, label=label)
    ax.step(diam, values, color="black", lw=0.7, alpha=0.95, zorder=zorder + 2)


def build_ensemble_mean_datasets(
    datasets: dict[str, dict[str, xr.Dataset]],
    *,
    variable: str = DEFAULT_VARIABLE,
    reindex_freq: str = "10s",
) -> dict[str, dict[str, xr.Dataset]]:
    ensemble: dict[str, dict[str, xr.Dataset]] = {"Ensemble Mean": {}}
    all_kinds = set(kind for run in datasets.values() for kind in run.keys())

    for kind in all_kinds:
        time_grids: list[np.ndarray] = []
        diameter_edges = None
        for run in datasets.values():
            ds_kind = run.get(kind)
            if isinstance(ds_kind, xr.Dataset) and variable in ds_kind:
                time_grids.append(ds_kind[variable].time.values)
                if "diameter_edges" in ds_kind:
                    diameter_edges = ds_kind["diameter_edges"].values
        if not time_grids:
            continue

        t_min = min(grid.min() for grid in time_grids)
        t_max = max(grid.max() for grid in time_grids)
        common_time = pd.date_range(start=t_min, end=t_max, freq=reindex_freq)

        data_arrays: list[xr.DataArray] = []
        first_run = None
        for run in datasets.values():
            ds_kind = run.get(kind)
            if not isinstance(ds_kind, xr.Dataset) or variable not in ds_kind:
                continue
            da = ds_kind[variable]
            if "cell" in da.dims:
                da = da.sum("cell", keep_attrs=True, skipna=True)
            if kind == "vertical" and "altitude" in da.dims:
                da = da.mean("altitude", keep_attrs=True, skipna=True)
            da = da.reindex(time=common_time, method="nearest", tolerance="5s", fill_value=0.0)
            data_arrays.append(da)
            if first_run is None:
                first_run = ds_kind
        if not data_arrays or first_run is None:
            continue

        da_mean = xr.concat(data_arrays, dim="run").mean(dim="run", keep_attrs=True)
        ds_mean = xr.Dataset({variable: da_mean})
        ds_mean.attrs.update(first_run.attrs)
        ds_mean[variable].attrs = data_arrays[0].attrs.copy()
        if diameter_edges is None:
            diameter_edges = _diam_log_edges(np.asarray(ds_mean["diameter"].values, dtype=float))
        ds_mean["diameter_edges"] = xr.DataArray(
            np.asarray(diameter_edges, dtype=float),
            dims=("diameter_edge",),
            attrs={"long_name": "diameter bin edges"},
        )
        model_bin_width = np.log(diameter_edges[1:]) - np.log(diameter_edges[:-1])
        ds_mean[variable] = ds_mean[variable] / xr.DataArray(model_bin_width, dims=("diameter",))
        ensemble["Ensemble Mean"][kind] = ds_mean

    return ensemble if ensemble["Ensemble Mean"] else datasets


def _default_scatter_kwargs(cmap) -> dict[str, Any]:
    return {
        "edgecolors": "white",
        "linestyle": "-",
        "alpha": 0.85,
        "zorder": 120,
        "cmap": cmap,
        "norm": LogNorm(
            vmin=float(DEFAULT_PANEL_LIMS["symlog"][2][0]),
            vmax=float(DEFAULT_PANEL_LIMS["symlog"][2][1]),
        ),
    }


def add_holimo_column_scatter(
    axes: list[plt.Axes],
    da_holimo: xr.DataArray,
    *,
    obs_ids: list[str],
    time_frames_plume: list[list[np.datetime64]],
    seeding_start_times: list[np.datetime64],
    threshold: float = DEFAULT_THRESHOLD,
    unit_conversion: float = DEFAULT_UNIT_CONVERSION,
    mission_markers: list[str] = DEFAULT_MISSION_MARKERS,
    mission_msizes: list[float] = DEFAULT_MISSION_MSIZES,
    scatter_kwargs: dict[str, Any] | None = None,
) -> list[tuple[str]]:
    scatter_kwargs = dict(_default_scatter_kwargs(default_plume_cmap()), **(scatter_kwargs or {}))
    mission_profiles: list[tuple[str]] = []
    marker_size_boost = [0.0, 50.0]

    for mission_idx, (obs_id, (time_lo, time_hi)) in enumerate(zip(obs_ids, time_frames_plume)):
        da_sel = da_holimo.sel(time=slice(time_lo, time_hi))
        da_sel = xr.where(da_sel > threshold, da_sel, np.nan)
        seed_time = np.datetime64(seeding_start_times[mission_idx])
        diam = np.asarray(da_sel["diameter"].values, dtype=float)
        x_elapsed: list[float] = []
        y_diam: list[float] = []
        c_conc: list[float] = []

        for time_idx in range(int(da_sel.sizes.get("time", 0))):
            time_value = da_sel.time.values[time_idx]
            elapsed_min = float((np.datetime64(time_value) - seed_time) / np.timedelta64(1, "m"))
            if elapsed_min < 0:
                continue
            vals = np.asarray(da_sel.isel(time=time_idx).values, dtype=float).ravel() * unit_conversion
            ok = np.isfinite(vals) & (vals > threshold)
            if not ok.any():
                continue
            n_valid = int(ok.sum())
            x_elapsed.extend([elapsed_min] * n_valid)
            y_diam.extend(diam[ok])
            c_conc.extend(vals[ok])

        if x_elapsed:
            x_elapsed_arr = np.asarray(x_elapsed)
            y_diam_arr = np.asarray(y_diam)
            c_conc_arr = np.asarray(c_conc)
            for ax_idx, ax in enumerate(axes):
                size = mission_msizes[mission_idx] + marker_size_boost[min(ax_idx, len(marker_size_boost) - 1)]
                linewidths = 0.04 if ax_idx == 0 else 0.2
                ax.scatter(
                    x_elapsed_arr,
                    y_diam_arr,
                    c=c_conc_arr,
                    marker=mission_markers[mission_idx],
                    s=size,
                    linewidths=linewidths,
                    **scatter_kwargs,
                )
        mission_profiles.append((obs_id,))
    return mission_profiles


def _mission_legend_handles(
    mission_profiles: list[tuple[str]],
    mission_markers: list[str],
) -> list[Line2D]:
    return [
        Line2D(
            [0],
            [0],
            marker=mission_markers[idx],
            ls="None",
            mfc="grey",
            mec="black",
            mew=0.4,
            ms=5,
            alpha=0.8,
            label=mission_profiles[idx][0],
        )
        for idx in range(len(mission_profiles))
    ]


def plume_lagrangian_output(repo_root: Path) -> Path:
    return repo_root / DEFAULT_OUTPUT


def load_plume_lagrangian_context(
    repo_root: Path,
    *,
    processed_root: str | Path | None = None,
    holimo_file: str | Path | None = None,
    runs: list[dict[str, str]] | None = None,
    kinds: tuple[str, ...] = DEFAULT_KINDS,
    kind: str = DEFAULT_KIND,
) -> dict[str, Any]:
    processed_root = repo_root / DEFAULT_PROCESSED_ROOT if processed_root is None else Path(processed_root)
    holimo_file = repo_root / DEFAULT_HOLIMO_FILE if holimo_file is None else Path(holimo_file)
    runs = DEFAULT_RUNS if runs is None else runs

    datasets = load_plume_path_runs(runs, processed_root=processed_root, kinds=kinds)
    try:
        xlim_integrated = build_common_xlim(datasets, kind="integrated", span_min=35)
    except ValueError:
        xlim_integrated = [np.datetime64("2023-01-25T12:29:00"), np.datetime64("2023-01-25T13:04:00")]
    diag = diagnostics_table(datasets, kind="integrated", variable=DEFAULT_VARIABLE, xlim=xlim_integrated)
    ensemble_datasets = build_ensemble_mean_datasets(datasets, variable=DEFAULT_VARIABLE)
    ds_holimo = prepare_holimo_for_overlay(
        str(holimo_file),
        DEFAULT_TIME_WINDOW_HOLIMO,
        resample_s=10,
        smoothing_time_bins=3,
        min_coverage_frac=0.01,
    )
    return {
        "repo_root": repo_root,
        "processed_root": Path(processed_root),
        "holimo_file": Path(holimo_file),
        "datasets": datasets,
        "diag": diag,
        "ensemble_datasets": ensemble_datasets,
        "ds_holimo": ds_holimo,
        "kind": kind,
        "output_path": plume_lagrangian_output(repo_root),
    }


def render_plume_lagrangian_figure(
    context: dict[str, Any],
    *,
    output_path: str | Path | None = None,
) -> tuple[plt.Figure, Path]:
    ensemble_datasets = context["ensemble_datasets"]
    ds_holimo = context["ds_holimo"]
    kind = context["kind"]
    cmap = default_plume_cmap()

    fig_width = FULL_COL_IN
    fig_height = min(fig_width / (9.4 / 6.0), MAX_H_IN)
    fig = plt.figure(figsize=(fig_width, fig_height), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[0.65, 0.35], wspace=0.1, hspace=0.1)
    ax_symlog = fig.add_subplot(gs[0, 0:2])
    ax_elapsed = fig.add_subplot(gs[1, 0])
    ax_hist = fig.add_subplot(gs[1, 1])

    pmesh_ref = None
    for ax_pm, panel_key in [(ax_symlog, "symlog"), (ax_elapsed, "elapsed")]:
        x_range, y_range, z_range = DEFAULT_PANEL_LIMS[panel_key]
        _, _, pmesh = plot_plume_path_sum(
            ensemble_datasets,
            kind=kind,
            variable=DEFAULT_VARIABLE,
            x_axis_fmt=panel_key,
            zlim=z_range,
            axes_override=[ax_pm],
            cbar_kwargs=DEFAULT_CBAR_KW,
            common_xlim_minutes=35,
            xlim=[DEFAULT_MODEL_SEED, DEFAULT_XLIM[1]],
            cmap=cmap,
            log_norm=True,
            add_missing_data=True,
            holimo_overlay=None,
            add_colorbar=False,
            add_shared_labels=False,
            annote_letters=False,
            return_pmesh=True,
            add_holimo_legend=False,
        )
        ax_pm.set(xlim=x_range, ylim=y_range)
        if pmesh_ref is None:
            pmesh_ref = pmesh

    x_zoom, y_zoom = DEFAULT_PANEL_LIMS["elapsed"][:2]
    ax_symlog.add_patch(
        plt.Rectangle(
            (x_zoom[0], y_zoom[0]),
            x_zoom[1] - x_zoom[0],
            y_zoom[1] - y_zoom[0],
            fill=False,
            ec="black",
            ls="--",
            lw=0.7,
            zorder=300,
        )
    )
    ax_elapsed.set_title("")
    ax_symlog.set_xticks([tick for tick in ax_symlog.get_xticks() if not np.isclose(tick, 0.0)])
    for spine in ax_elapsed.spines.values():
        spine.set_color("black")
        spine.set_linestyle("--")
    for ax in (ax_symlog, ax_elapsed):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["left"].set_visible(False)

    unit = ensemble_datasets[next(iter(ensemble_datasets))][kind][DEFAULT_VARIABLE].attrs.get("units", "-")
    fig.colorbar(pmesh_ref, ax=ax_symlog, **DEFAULT_CBAR_KW).set_label(rf"{kind} nf per bin / ({unit})")

    da_holimo = ds_holimo[DEFAULT_HOLIMO_VAR]
    mission_profiles = add_holimo_column_scatter(
        [ax_symlog, ax_elapsed],
        da_holimo,
        obs_ids=DEFAULT_OBS_IDS,
        time_frames_plume=DEFAULT_TIME_FRAMES_PLUME,
        seeding_start_times=DEFAULT_SEEDING_START_TIMES,
        threshold=DEFAULT_THRESHOLD,
        unit_conversion=DEFAULT_UNIT_CONVERSION,
        mission_markers=DEFAULT_MISSION_MARKERS,
        mission_msizes=DEFAULT_MISSION_MSIZES,
    )
    ax_symlog.legend(
        handles=_mission_legend_handles(mission_profiles, DEFAULT_MISSION_MARKERS),
        title="HOLIMO missions",
        fontsize=7,
        title_fontsize=7,
        loc="lower right",
        framealpha=0.8,
    )

    run_label = next(label for label, run in ensemble_datasets.items() if isinstance(run.get(kind), xr.Dataset))
    da_model_elapsed = _assign_elapsed_time(
        _prepare_da(ensemble_datasets[run_label][kind], DEFAULT_VARIABLE, sum_cell=True).sel(
            time=slice(*DEFAULT_XLIM)
        ),
        DEFAULT_MODEL_SEED,
    )

    y_elapsed = DEFAULT_PANEL_LIMS["elapsed"][1]
    diam_mod = np.asarray(
        da_model_elapsed["diameter"].sel(diameter=slice(*y_elapsed)).values,
        dtype=float,
    )
    diam_hol = np.asarray(
        da_holimo["diameter"].sel(diameter=slice(*y_elapsed)).values,
        dtype=float,
    )
    be_mod = _diam_log_edges(diam_mod)
    be_hol = _diam_log_edges(diam_hol)

    elapsed_wins: list[tuple[float, float]] = []
    obs_elapsed: dict[str, xr.DataArray] = {}
    for mission_idx, (obs_id, (time_lo, time_hi)) in enumerate(zip(DEFAULT_OBS_IDS, DEFAULT_TIME_FRAMES_PLUME)):
        seed_time = np.datetime64(DEFAULT_SEEDING_START_TIMES[mission_idx])
        elapsed_lo = float((np.datetime64(time_lo) - seed_time) / np.timedelta64(1, "m"))
        elapsed_hi = float((np.datetime64(time_hi) - seed_time) / np.timedelta64(1, "m"))
        elapsed_wins.append((min(elapsed_lo, elapsed_hi), max(elapsed_lo, elapsed_hi)))
        da_obs = da_holimo.sel(time=slice(time_lo, time_hi))
        if da_obs.sizes.get("time", 0):
            obs_elapsed[obs_id] = _assign_elapsed_time(da_obs, seed_time)

    elapsed_lo_global = min(win[0] for win in elapsed_wins)
    elapsed_hi_global = max(win[1] for win in elapsed_wins)
    da_model_window = da_model_elapsed.where(
        (da_model_elapsed.time_elapsed >= elapsed_lo_global)
        & (da_model_elapsed.time_elapsed <= elapsed_hi_global),
        drop=True,
    )
    d_mod, h_mod = hist_profile(da_model_window, bin_edges=be_mod)
    dur_model = elapsed_duration_minutes(da_model_window)
    f_mod = h_mod / dur_model if dur_model > 0 else h_mod
    plot_hist_line(ax_hist, d_mod, f_mod, "orange", 3.0, label="COSMO-SPECS", zorder=13)

    h_obs_sum = None
    d_obs = np.array([])
    dur_obs = 0.0
    for mission_idx, obs_id in enumerate(DEFAULT_OBS_IDS):
        da_obs = obs_elapsed.get(obs_id)
        if da_obs is None:
            continue
        elapsed_lo, elapsed_hi = elapsed_wins[mission_idx]
        da_obs = da_obs.where(
            (da_obs.time_elapsed >= elapsed_lo) & (da_obs.time_elapsed <= elapsed_hi),
            drop=True,
        )
        d_cur, h_cur = hist_profile(da_obs, bin_edges=be_hol, threshold=DEFAULT_THRESHOLD)
        if not d_cur.size:
            continue
        if h_obs_sum is None:
            d_obs, h_obs_sum = d_cur, h_cur.astype(float)
        elif h_obs_sum.shape == h_cur.shape:
            h_obs_sum += h_cur
        dur_obs += elapsed_duration_minutes(da_obs)

    f_obs = h_obs_sum / dur_obs if h_obs_sum is not None and dur_obs > 0 else np.array([])
    if not (f_obs.size and np.isfinite(f_obs).any()):
        raise ValueError("No valid HOLIMO histogram data found.")

    plot_hist_line(ax_hist, d_obs, f_obs, "royalblue", 2.2, label="HOLIMO", zorder=5)
    hist_xlim, hist_ylim = DEFAULT_PANEL_LIMS["hist"][:2]
    ax_hist.set(xscale="log", yscale="log", xlim=hist_xlim, ylim=hist_ylim)
    ax_hist.set(ylabel="avg. concentration / bin / (L)", xlabel="equivalent diameter / (um)")
    ax_hist.grid(True, which="major", ls="--", lw=0.25, alpha=0.6)
    ax_hist.grid(True, which="minor", ls=":", lw=0.15, alpha=0.35)
    ax_hist.yaxis.tick_right()
    ax_hist.yaxis.set_label_position("right")
    fmt = plt.FuncFormatter(_float_fmt)
    ax_hist.xaxis.set_major_formatter(fmt)
    ax_hist.yaxis.set_major_formatter(fmt)

    x_edge = hist_xlim[1] * 0.92
    y_bottom = 1.2 * ax_hist.get_ylim()[0]
    for idx in peak_indices(f_mod, n=2):
        ax_hist.scatter(x_edge, f_mod[idx], color="orange", **DEFAULT_PEAK_MK)
    for idx in peak_indices(f_obs, n=1):
        ax_hist.scatter(x_edge, f_obs[idx], color="royalblue", **DEFAULT_PEAK_MK)
    ax_hist.scatter(median_diameter(d_mod, f_mod), y_bottom, color="orange", **DEFAULT_MED_MK)
    ax_hist.scatter(median_diameter(d_obs, f_obs), y_bottom, color="royalblue", **DEFAULT_MED_MK)

    ax_hist.legend(
        [Line2D([], [], color="orange", lw=3, alpha=0.8), Line2D([], [], color="royalblue", lw=2.2, alpha=0.8)],
        ["COSMO-SPECS", "HOLIMO"],
        loc="upper left",
        frameon=False,
        fontsize=9,
    )

    ax_symlog.set_title("")
    ax_symlog.set_xlabel("elapsed time (logarithmic) / (minutes)")
    ax_elapsed.set_xlabel("elapsed time (linear) / (minutes)")
    ax_elapsed.yaxis.set_major_formatter(fmt)
    fig.supylabel("equivalent diameter / (um)")
    ax_symlog.set_xlim(DEFAULT_PANEL_LIMS["symlog"][0])

    out = context["output_path"] if output_path is None else Path(output_path)
    return fig, out


def save_plume_lagrangian_figure(
    fig: plt.Figure,
    output_path: str | Path,
    *,
    dpi: int = 500,
) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, bbox_inches="tight")
    print(f"saved -> {out}")
    return out
