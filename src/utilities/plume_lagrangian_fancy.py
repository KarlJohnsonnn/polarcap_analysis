"""Reimagined top panel for the plume-lagrangian figure: growth-trajectory view.

Panel A replaces the pcolormesh+scatter Hovmoller with per-run concentration-weighted
mean diameter D_mean(t), a 10-90th percentile envelope across the ensemble, the
ensemble-mean power-law fit, and per-mission HOLIMO weighted-mean D(t) with
individual linear fits. Panels B/C are the unchanged zoom pcolormesh and the
time-averaged histogram from ``plume_lagrangian``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from matplotlib.collections import PathCollection, QuadMesh
from matplotlib.legend_handler import HandlerTuple
from matplotlib.lines import Line2D
from matplotlib.ticker import AutoMinorLocator

from utilities.plume_lagrangian import (
    DEFAULT_CBAR_KW,
    DEFAULT_HOLIMO_VAR,
    DEFAULT_KIND,
    DEFAULT_MISSION_MARKERS,
    DEFAULT_MISSION_MSIZES,
    DEFAULT_MODEL_SEED,
    DEFAULT_N_TIMELINE,
    DEFAULT_OBS_IDS,
    DEFAULT_PANEL_LIMS,
    DEFAULT_SEEDING_START_TIMES,
    DEFAULT_THRESHOLD,
    DEFAULT_TIME_FRAMES_PLUME,
    DEFAULT_VARIABLE,
    DEFAULT_XLIM,
    _add_growth_fits,
    _diam_log_edges,
    _draw_fit_line,
    _elapsed_span_min,
    _ensemble_label_parts,
    _weighted_mean_D_per_time,
    add_holimo_column_scatter,
    default_plume_cmap,
    diameter_max,
    hist_profile,
    holimo_scale_cm3_to_litres,
    median_diameter,
    peak_indices,
    plot_hist_line,
)
from utilities.plume_path_plot import _assign_elapsed_time, _prepare_da, plot_plume_path_sum
from utilities.style_profiles import FULL_COL_IN, MAX_H_IN

DEFAULT_OUTPUT_FANCY = (
    Path("output") / "gfx" / "png" / "03" / "figure12_ensemble_mean_plume_path_fancy.png"
)

# Okabe-Ito style qualitative palette for the three missions (not the dN/dlnD cmap).
MISSION_COLORS = ("#0072B2", "#D55E00", "#009E73")

# Percentile band for the per-run trajectory envelope.
ENVELOPE_PCT = (10.0, 90.0)
ENSEMBLE_LINE_COLOR = "#1f1f1f"
ENSEMBLE_BAND_COLOR = "#6e6e6e"


def plume_lagrangian_fancy_output(repo_root: Path) -> Path:
    return repo_root / DEFAULT_OUTPUT_FANCY


def _per_run_D_mean_on_grid(
    datasets: dict[str, dict[str, xr.Dataset]],
    *, kind: str, variable: str, t_grid: np.ndarray,
) -> np.ndarray:
    """Return array (n_runs, n_t_grid) of weighted-mean D interpolated onto ``t_grid``."""
    rows: list[np.ndarray] = []
    for run_label, run in datasets.items():
        ds = run.get(kind)
        if not isinstance(ds, xr.Dataset) or variable not in ds:
            continue
        da = _prepare_da(ds, variable, sum_cell=True)
        da = da.sel(time=slice(*DEFAULT_XLIM))
        if da.sizes.get("time", 0) == 0:
            continue
        da = _assign_elapsed_time(da, DEFAULT_MODEL_SEED).sel(diameter=slice(None, diameter_max))
        t_r, D_r = _weighted_mean_D_per_time(da)
        if t_r.size < 2:
            continue
        order = np.argsort(t_r)
        t_r, D_r = t_r[order], D_r[order]
        log_D = np.log(D_r)
        interp = np.interp(t_grid, t_r, log_D, left=np.nan, right=np.nan)
        rows.append(np.exp(interp))
    if not rows:
        raise ValueError("No per-run trajectories available for envelope computation.")
    return np.vstack(rows)


def _mission_trajectory(
    da_holimo: xr.DataArray, t_lo: np.datetime64, t_hi: np.datetime64, seed: np.datetime64,
) -> tuple[np.ndarray, np.ndarray]:
    sub = da_holimo.sel(time=slice(t_lo, t_hi))
    if sub.sizes.get("time", 0) == 0:
        return np.array([]), np.array([])
    sub = _assign_elapsed_time(sub, seed).sel(diameter=slice(None, diameter_max))
    return _weighted_mean_D_per_time(sub)


def _draw_trajectory_panel(
    ax: plt.Axes,
    datasets: dict[str, dict[str, xr.Dataset]],
    da_model_ensemble: xr.DataArray,
    da_holimo: xr.DataArray,
    elapsed_wins: list[tuple[float, float]],
    *, kind: str,
) -> None:
    x_lim, y_lim, _ = DEFAULT_PANEL_LIMS["all_timeseries"]
    t_grid = np.linspace(max(x_lim[0], 0.05), x_lim[1], 400)

    runs_D = _per_run_D_mean_on_grid(datasets, kind=kind, variable=DEFAULT_VARIABLE, t_grid=t_grid)
    with np.errstate(invalid="ignore"):
        p_lo = np.nanpercentile(runs_D, ENVELOPE_PCT[0], axis=0)
        p_hi = np.nanpercentile(runs_D, ENVELOPE_PCT[1], axis=0)
        mean_line = np.exp(np.nanmean(np.log(runs_D), axis=0))
    band_ok = np.isfinite(p_lo) & np.isfinite(p_hi)
    ax.fill_between(
        t_grid[band_ok], p_lo[band_ok], p_hi[band_ok],
        color=ENSEMBLE_BAND_COLOR, alpha=0.2, lw=0, zorder=40,
        label=f"model {ENVELOPE_PCT[0]:.0f}-{ENVELOPE_PCT[1]:.0f}th pct.",
    )
    mline_ok = np.isfinite(mean_line)
    ax.plot(
        t_grid[mline_ok], mean_line[mline_ok],
        color=ENSEMBLE_LINE_COLOR, lw=1.4, alpha=0.9, zorder=60,
        label=r"model ensemble $\overline{D}(t)$",
    )

    da_m = da_model_ensemble.sel(diameter=slice(None, diameter_max))
    panel_lo, panel_hi = DEFAULT_PANEL_LIMS["zoom_timeseries"][0]
    da_m_fit = da_m.where(
        (da_m.time_elapsed >= panel_lo) & (da_m.time_elapsed <= panel_hi), drop=True,
    )
    t_m, D_m = _weighted_mean_D_per_time(da_m_fit)
    if t_m.size >= 2:
        log_t, log_D = np.log(t_m), np.log(D_m)
        alpha_star, log_C_star = np.polyfit(log_t, log_D, 1)
        alpha_star, C_star = float(alpha_star), float(np.exp(log_C_star))
        t_axis = np.geomspace(max(x_lim[0], 1e-2), x_lim[1], DEFAULT_N_TIMELINE)
        _draw_fit_line(
            ax, t_axis, C_star * t_axis ** alpha_star,
            outer={"color": "#CD1C18"},
            inner={"ls": (0, (8, 4))},
            zorder=200,
            label=rf"model optimal $D=C\,t^{{\alpha}}$ ($\alpha$={alpha_star:.2f})",
        )

    combined_t: list[np.ndarray] = []
    combined_D: list[np.ndarray] = []
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
            t_o, D_o = t_o[order], D_o[order]
            ax.plot(
                t_o, D_o, color=color, lw=0.7, alpha=0.8, zorder=150,
                marker=marker, ms=ms, mec="black", mew=0.3, mfc=color,
            )
            lo, hi = elapsed_wins[mi]
            ok = np.isfinite(t_o) & np.isfinite(D_o)
            if ok.sum() >= 2:
                a_o, b_o = np.polyfit(t_o[ok], D_o[ok], 1)
                t_line = np.linspace(lo, hi, DEFAULT_N_TIMELINE)
                _draw_fit_line(
                    ax, t_line, a_o * t_line + b_o,
                    outer={"color": color, "lw": 1.2, "alpha": 0.9},
                    inner={"ls": (0, (8, 4)), "color": "white"},
                    zorder=180,
                )
                combined_t.append(t_o[ok])
                combined_D.append(D_o[ok])
        mission_handles.append(
            Line2D([0], [0], marker=marker, color=color, mec="black", mew=0.3,
                   lw=0.7, ms=ms, label=obs_id)
        )

    if combined_t:
        tc = np.concatenate(combined_t)
        Dc = np.concatenate(combined_D)
        a_c, b_c = np.polyfit(tc, Dc, 1)
        t_axis = np.linspace(x_lim[0], x_lim[1], DEFAULT_N_TIMELINE)
        _draw_fit_line(
            ax, t_axis, a_c * t_axis + b_c,
            outer={"color": "royalblue"},
            inner={"ls": (0, (8, 4)), "color": "white"},
            zorder=190,
            label="HOLIMO linear fit (combined)",
        )

    x_z, y_z = DEFAULT_PANEL_LIMS["zoom_timeseries"][:2]
    ax.add_patch(plt.Rectangle(
        (x_z[0], y_z[0]), x_z[1] - x_z[0], y_z[1] - y_z[0],
        fill=False, ec="black", ls="--", lw=0.7, zorder=300,
    ))

    ax.set(xscale="linear", yscale="log", xlim=x_lim, ylim=y_lim)
    ax.xaxis.set_minor_locator(AutoMinorLocator(5))
    ax.tick_params(axis="x", which="minor", length=2.5, width=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, which="major", ls="--", lw=0.25, alpha=0.5)
    ax.grid(True, which="minor", ls=":", lw=0.15, alpha=0.3)
    ax.set_xlabel(r"elapsed time / (min)")

    mission_leg = ax.legend(
        handles=mission_handles, title="HOLIMO missions",
        loc="lower right", fontsize=7, title_fontsize=7, framealpha=0.8,
    )
    ax.add_artist(mission_leg)
    composite = ax.__dict__.get("_composite_handles", [])
    labels = [l for l, _ in composite]
    handles = [h for _, h in composite]
    if handles:
        ax.figure.canvas.draw()
        bb = mission_leg.get_window_extent().transformed(ax.transAxes.inverted())
        fit_leg = ax.legend(
            handles=handles, labels=labels,
            handler_map={tuple: HandlerTuple(ndivide=1, pad=0.0)},
            loc="lower right", bbox_to_anchor=(bb.x0 - 0.05 * bb.x0, bb.y0),
            bbox_transform=ax.transAxes,
            fontsize=7, framealpha=0.8, borderaxespad=0.3,
            handlelength=2.6, labelspacing=0.3,
        )
        fit_leg.set_zorder(500)


def render_plume_lagrangian_fancy_figure(
    context: dict[str, Any], *, output_path: str | Path | None = None,
) -> tuple[plt.Figure, Path]:
    ensemble = context["ensemble_datasets"]
    datasets = context["datasets"]
    ds_holimo = context["ds_holimo"]
    kind = context["kind"]
    hol_scale = holimo_scale_cm3_to_litres(DEFAULT_HOLIMO_VAR)
    cmap = default_plume_cmap()

    fig_w = FULL_COL_IN
    fig = plt.figure(figsize=(fig_w, min(fig_w / (9.4 / 6.0), MAX_H_IN)), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, width_ratios=[0.65, 0.35], wspace=0.1, hspace=0.1)
    ax_traj = fig.add_subplot(gs[0, 0:2])
    ax_el = fig.add_subplot(gs[1, 0])
    ax_hist = fig.add_subplot(gs[1, 1])

    # --- zoom panel (ax_el) via existing pipeline ----------------------------
    x_r, y_r, z_r = DEFAULT_PANEL_LIMS["zoom_timeseries"]
    _, _, pmeshs, _ = plot_plume_path_sum(
        ensemble,
        kind=kind,
        variable=DEFAULT_VARIABLE,
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
        marker_size_scale=0.05,
        x_axis_fmt="elapsed",
        y_axis_fmt="log",
        zlim=z_r,
        axes_override=[ax_el],
        cmap_alpha=0.25,
    )
    ax_el.set(xlim=x_r, ylim=y_r)
    ax_el.set_title("")
    ax_el.xaxis.set_minor_locator(AutoMinorLocator(5))
    ax_el.tick_params(axis="x", which="minor", length=2.5, width=0.5)
    for sp in ax_el.spines.values():
        sp.set_color("black")
        sp.set_linestyle("--")
    ax_el.spines["top"].set_visible(False)
    ax_el.spines["right"].set_visible(False)
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["left"].set_visible(False)

    unit_m = ensemble[next(iter(ensemble))][kind][DEFAULT_VARIABLE].attrs.get("units", "-")
    dnd, vol = _ensemble_label_parts(unit_m)
    fig.colorbar(pmeshs[0], ax=ax_el, **DEFAULT_CBAR_KW).set_label(
        f"ensemble-avg. {dnd} / ({vol})"
    )

    da_holimo = ds_holimo[DEFAULT_HOLIMO_VAR]
    add_holimo_column_scatter(
        [ax_el], da_holimo,
        obs_ids=DEFAULT_OBS_IDS,
        time_frames_plume=DEFAULT_TIME_FRAMES_PLUME,
        seeding_start_times=DEFAULT_SEEDING_START_TIMES,
        threshold=DEFAULT_THRESHOLD,
        unit_conversion=hol_scale,
        alphas=(0.1,),
        mission_markers=DEFAULT_MISSION_MARKERS,
        mission_msizes=DEFAULT_MISSION_MSIZES,
    )

    run_label = next(l for l, r in ensemble.items() if isinstance(r.get(kind), xr.Dataset))
    da_model = _assign_elapsed_time(
        _prepare_da(ensemble[run_label][kind], DEFAULT_VARIABLE, sum_cell=True).sel(time=slice(*DEFAULT_XLIM)),
        DEFAULT_MODEL_SEED,
    )

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

    _add_growth_fits([ax_el], ax_el, da_model, obs_elapsed, elapsed_wins)

    # Shrink markers + mask faint mesh on the zoom panel to match the original figure.
    el_shrink, el_alpha = 0.5, 0.55
    for col in ax_el.collections:
        if isinstance(col, QuadMesh):
            arr = col.get_array()
            col.set_array(np.ma.masked_where(~(arr >= 1.0), arr))
            col.set_alpha(el_alpha)
        elif isinstance(col, PathCollection):
            col.set_sizes(col.get_sizes() * el_shrink)
            col.set_alpha(el_alpha)

    # --- histogram panel (identical treatment to the original figure) -------
    y_elapsed = DEFAULT_PANEL_LIMS["zoom_timeseries"][1]
    be_mod = _diam_log_edges(np.asarray(
        da_model["diameter"].sel(diameter=slice(*y_elapsed)).values, dtype=float,
    ))
    be_hol = _diam_log_edges(np.asarray(
        da_holimo["diameter"].sel(diameter=slice(*y_elapsed)).values, dtype=float,
    ))

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
        xscale="log", yscale="log",
        xlim=hist_xlim, ylim=hist_ylim,
        ylabel=f"time-avg. {dnd} / ({vol})",
        xlabel=r"D$_{\mathrm{eq}}$ / ($\mu$m)",
    )
    ax_hist.grid(True, which="major", ls="--", lw=0.25, alpha=0.6)
    ax_hist.grid(True, which="minor", ls=":", lw=0.15, alpha=0.35)
    ax_hist.yaxis.tick_right()
    ax_hist.yaxis.set_label_position("right")

    def _float_fmt(x: float, _pos: int) -> str:
        return f"{x:.3f}".rstrip("0").rstrip(".") if x < 1 else f"{x:.0f}"

    fmt = plt.FuncFormatter(_float_fmt)
    ax_hist.xaxis.set_major_formatter(fmt)
    ax_hist.yaxis.set_major_formatter(fmt)
    ax_el.yaxis.set_major_formatter(fmt)

    x_edge = hist_xlim[1] * 0.92
    y_bot = 1.2 * ax_hist.get_ylim()[0]
    from utilities.plume_lagrangian import DEFAULT_PEAK_MK, DEFAULT_MED_MK
    for i in peak_indices(f_mod, n=2):
        ax_hist.scatter(x_edge, f_mod[i], color="orange", **DEFAULT_PEAK_MK)
    for i in peak_indices(f_obs_plot, n=1):
        ax_hist.scatter(x_edge, f_obs_plot[i], color="royalblue", **DEFAULT_PEAK_MK)
    ax_hist.scatter(median_diameter(d_mod, f_mod), y_bot, color="orange", **DEFAULT_MED_MK)
    ax_hist.scatter(median_diameter(d_obs, f_obs_plot), y_bot, color="royalblue", **DEFAULT_MED_MK)
    ax_hist.legend(
        [Line2D([], [], color="orange", lw=2.0, alpha=0.8),
         Line2D([], [], color="royalblue", lw=2.0, alpha=0.8)],
        ["COSMO-SPECS", "HOLIMO"], loc="upper left", frameon=False, fontsize=7,
        handler_map={tuple: HandlerTuple(ndivide=None)},
    )

    # --- trajectory panel A --------------------------------------------------
    _draw_trajectory_panel(
        ax_traj, datasets, da_model, da_holimo, elapsed_wins, kind=kind,
    )

    ax_el.set_xlabel(r"elapsed time / (min)")
    fig.supylabel(r"equivalent diameter D$_{\mathrm{eq}}$ / ($\mu$m)")

    out = (
        plume_lagrangian_fancy_output(context["repo_root"])
        if output_path is None else Path(output_path)
    )
    return fig, out
