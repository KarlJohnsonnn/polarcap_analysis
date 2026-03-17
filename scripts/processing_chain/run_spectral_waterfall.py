#!/usr/bin/env python3
"""Render spectral waterfall PNG frames (and optional MP4) from YAML config.

Each height-layer panel is split into upper (liquid) and lower (ice) sub-axes
with independent y-limits (ylim_W / ylim_F) and shared x-axis.

Plot styles (--plot-style): bars | lines | steps.
Y-axis scaling (--yscale):  symlog (default) | linear | log.

Outputs
-------
- Frames: notebooks/output/05/<cs_run>/spectral_waterfall_<kind>_exp<id>_stn_all_<range>_itime<k>.png
- MP4:    notebooks/output/05/spectral_waterfall_<kind>_..._evolution_nframes<n>.mp4  (--mp4)
"""
from __future__ import annotations

import argparse
import glob
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities import (  # noqa: E402
    MAX_H_IN,
    MM,
    PROCESS_PLOT_ORDER,
    SINGLE_COL_IN,
    apply_publication_style,
    build_fixed_legend,
    load_process_budget_data,
    merge_liq_ice_net,
    normalize_net_stacks,
    panel_process_values,
    panel_concentration_profile,
    proc_color,
    proc_hatch,
    stn_label,
)
from utilities.compute_fabric import is_server  # noqa: E402


# ── Plotting ─────────────────────────────────────────────────────────────────

def _draw_bars(ax, d, widths, order, net_merged, cfg_plot):
    """Stacked bar chart: positive up, negative down."""
    n = len(d)
    any_data = False
    bottom_pos, bottom_neg = np.zeros(n), np.zeros(n)
    for p in order:
        c, net_arr = net_merged[p]
        pos_part, neg_part = np.maximum(0.0, net_arr), np.minimum(0.0, net_arr)
        h = proc_hatch(p)
        if np.any(pos_part > 0):
            ax.bar(d, pos_part, width=widths, bottom=bottom_pos, color=c,
                   edgecolor=cfg_plot["bar_edge_color"], linewidth=cfg_plot["bar_edge_linewidth"],
                   alpha=cfg_plot["pos_alpha"], hatch=h)
            bottom_pos += pos_part
            any_data = True
        if np.any(neg_part < 0):
            ax.bar(d, neg_part, width=widths, bottom=bottom_neg, color=c,
                   edgecolor=cfg_plot["bar_edge_color"], linewidth=cfg_plot["bar_edge_linewidth"],
                   alpha=cfg_plot["neg_alpha"], hatch=h)
            bottom_neg += neg_part
            any_data = True
    return any_data


def _draw_lines(ax, d, order, net_merged, cfg_plot):
    """Line + fill_between: white underlay, then fill (alpha 0.25), then colored line (alpha 0.7) on top."""
    any_data = False
    fill_alpha = cfg_plot.get("fill_alpha", 0.25)
    line_alpha = cfg_plot.get("line_alpha", 0.7)
    white_lw = cfg_plot.get("rate_white_linewidth", 2.5)
    color_lw = cfg_plot.get("rate_color_linewidth", 0.8)
    black_lw = cfg_plot.get("rate_black_linewidth", 0.5)
    for p in order:
        c, net_arr = net_merged[p]
        if not np.any(np.abs(net_arr) > 0):
            continue
        any_data = True
        ax.plot(d, net_arr, color="white", linewidth=white_lw, linestyle="-", zorder=1)
        ax.fill_between(d, 0, net_arr, color=c, alpha=fill_alpha, linewidth=color_lw, zorder=2)
        ax.plot(d, net_arr, color="black", linewidth=black_lw, linestyle="-", alpha=line_alpha, zorder=3)
    return any_data


def _draw_steps(ax, d, order, net_merged, cfg_plot):
    """Step + fill_between: white underlay, then fill (alpha 0.25), then colored step line (alpha 0.7) on top."""
    any_data = False
    fill_alpha = cfg_plot.get("fill_alpha", 0.25)
    line_alpha = cfg_plot.get("line_alpha", 0.7)
    white_lw = cfg_plot.get("rate_white_linewidth", 2.5)
    color_lw = cfg_plot.get("rate_color_linewidth", 0.8)
    black_lw = cfg_plot.get("rate_black_linewidth", 0.5)
    for p in order:
        c, net_arr = net_merged[p]
        if not np.any(np.abs(net_arr) > 0):
            continue
        any_data = True
        ax.step(d, net_arr, color="white", where="mid", linewidth=white_lw, zorder=1)
        ax.fill_between(d, 0, net_arr, color=c, alpha=fill_alpha, step="mid", linewidth=color_lw, zorder=2)
        ax.step(d, net_arr, color="black", where="mid", linewidth=black_lw, alpha=line_alpha, zorder=3)
    return any_data


def _draw_phase(ax, plot_style, d, widths, order, net_map, cfg_plot):
    """Dispatch to bars/lines/steps based on plot_style."""
    if plot_style == "lines":
        any_data = _draw_lines(ax, d, order, net_map, cfg_plot)
    elif plot_style == "steps":
        any_data = _draw_steps(ax, d, order, net_map, cfg_plot)
    else:
        any_data = _draw_bars(ax, d, widths, order, net_map, cfg_plot)

    if any_data:
        for p in order:
            c, net_arr = net_map[p]
            if not np.any(np.abs(net_arr) > 0):
                continue
            mean_rate = float(np.nanmean(net_arr))
            if not np.isfinite(mean_rate):
                continue
            # Triangle points left, vertex aligned with right end of y-axis major tick
            ax.plot(
                0.985,
                mean_rate,
                marker="<",
                color=c,
                transform=ax.get_yaxis_transform(),
                clip_on=False,
                markersize=5,
                markeredgecolor="black",
                markeredgewidth=0.4,
                zorder=10,
            )

    return any_data


def _plot_psd_mean_triangle(ax, x, color, *, markersize=6, y_offset_pt=-6.0) -> None:
    """Place an upward triangle below the PSD axis with its tip aligned to the tick end."""
    trans = ax.get_xaxis_transform() + matplotlib.transforms.ScaledTranslation(
        0.0,
        y_offset_pt / 72.0,
        ax.figure.dpi_scale_trans,
    )
    ax.plot(
        x,
        0.0,
        marker="^",
        color=color,
        transform=trans,
        clip_on=False,
        markersize=markersize,
        markeredgecolor="black",
        markeredgewidth=0.5,
        zorder=10,
    )


def _plot_psd_max_triangle(ax, y, color, *, markersize=6) -> None:
    """Place a phase-colored triangle on the left PSD boundary; vertex at left end of y-axis tick."""
    # Center slightly right of spine so tip (left edge of ">") aligns with spine
    ax.plot(
        0.015,
        y,
        marker=">",
        color=color,
        transform=ax.get_yaxis_transform(),
        clip_on=False,
        markersize=markersize,
        markeredgecolor="black",
        markeredgewidth=0.5,
        zorder=10,
    )


def _add_psd_max_textbox(ax, labels) -> None:
    """Show PSD maxima for liquid and ice as a compact textbox in the upper-left half."""
    if not labels:
        return
    ax.text(
        0.12,
        0.78,
        "\n".join(labels),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize="xx-small",
        color="0.15",
        bbox=dict(facecolor="white", alpha=0.85, edgecolor="0.75", boxstyle="round,pad=0.18"),
        zorder=11,
    )


def _format_psd_ax(ax, *, row, col, n_cols, xlim, ylim, grid_linewidth, psd_yscale, conc_unit_label, station_idx, station_labels, spec_label) -> None:
    """Format the compact shared PSD strip axis above each liquid/ice pair."""
    from matplotlib.ticker import FuncFormatter, LogLocator

    ax.set_xscale("log")
    ax.set_xlim(*xlim)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.set_yscale(psd_yscale)
    ax.set_ylim(*ylim)
    if psd_yscale == "log":
        ax.yaxis.set_major_locator(LogLocator(base=10.0))
        ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=np.arange(2, 10) * 0.1))
    # Match rates style: label only every second tick
    def _psd_ytick_fmt(y, pos):
        return f"{y:g}" if pos % 2 == 0 else ""
    ax.yaxis.set_major_formatter(FuncFormatter(_psd_ytick_fmt))
    ax.grid(True, which="major", linestyle="--", linewidth=grid_linewidth, color="k", alpha=0.18)
    if psd_yscale == "log":
        ax.grid(True, which="minor", linestyle=":", linewidth=grid_linewidth * 0.7, color="k", alpha=0.12)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))
    ax.tick_params(which="both", direction="out", bottom=True, top=False, labelbottom=False, labeltop=False)
    ax.set_ylabel(conc_unit_label, fontsize="xx-small")
    ax.yaxis.set_ticks_position("both")
    ax.yaxis.set_label_position("right")
    ax.tick_params(axis="y", labelsize="xx-small")
    ax.text(
        0.02,
        0.84,
        "PSD",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize="xx-small",
        fontstyle="italic",
        color="0.45",
    )
    if row == 0:
        ax.set_title(stn_label(station_idx, station_labels))

def _add_psd_legend(ax, cfg_plot) -> None:
    """Add small legend for Liquid/Ice PSD curves on the PSD axis."""
    import matplotlib.patches as mpatches

    c_w = cfg_plot.get("psd_color_W", "steelblue")
    c_f = cfg_plot.get("psd_color_F", "sienna")
    alpha = cfg_plot.get("psd_fill_alpha", 0.4)
    handles = [
        mpatches.Patch(facecolor=c_w, edgecolor=c_w, alpha=alpha, label="Liquid"),
        mpatches.Patch(facecolor=c_f, edgecolor=c_f, alpha=alpha, label="Ice"),
    ]
    ax.legend(
        handles=handles,
        loc="upper right",
        fontsize="xx-small",
        frameon=True,
        framealpha=0.9,
        edgecolor="0.7",
        handlelength=1.2,
        handleheight=0.8,
        handletextpad=0.5,
    )


def _add_process_cmap(fig, process_order, *, show_active_only=False) -> None:
    """Add a discrete process colormap with process-name tick labels on the right."""
    from matplotlib import colors

    labels = [p for p in process_order] if not show_active_only else [p for p in process_order if p]
    cmap = colors.ListedColormap([proc_color(p) for p in labels], name="proc_labels")
    bounds = np.arange(len(labels) + 1)
    norm = colors.BoundaryNorm(bounds, cmap.N)
    sm = matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(
        sm,
        ax=fig.axes,
        location="right",
        fraction=0.045,
        pad=0.02,
        ticks=np.arange(len(labels)) + 0.5,
    )
    cbar.ax.set_yticklabels([p.replace("_", " ").title() for p in labels])
    cbar.ax.tick_params(labelsize="x-small")
    cbar.outline.set_linewidth(0.6)


def plot_spectral_waterfall(
    *,
    spec_rates_w: dict[str, Any],
    spec_rates_f: dict[str, Any],
    size_ranges: dict[str, Any],
    range_key: str,
    diameter_um: np.ndarray,
    station_ids: list[int],
    station_labels: dict[int, str],
    height_sel_m: list[float],
    twindow: slice,
    unit_label: str,
    kind_label: str,
    cfg_plot: dict[str, Any],
    normalize_mode: str = "none",
    plot_style: str = "bars",
    yscale: str = "symlog",
    spec_conc_w: Optional[xr.DataArray] = None,
    spec_conc_f: Optional[xr.DataArray] = None,
    conc_unit_label: str = "",
) -> tuple[Any, Any]:
    """Spectral waterfall with one shared PSD strip above liquid and ice rate panels.

    plot_style : 'bars' | 'lines' | 'steps'.
    yscale     : 'symlog' | 'linear' | 'log'.
    """
    bin_slice = size_ranges[range_key]["slice"]
    xlim = (min(cfg_plot["xlim_W"][0], cfg_plot["xlim_F"][0]), max(cfg_plot["xlim_W"][1], cfg_plot["xlim_F"][1]))
    ylim_W, ylim_F = tuple(cfg_plot["ylim_W"]), tuple(cfg_plot["ylim_F"])
    linthresh_W, linthresh_F = cfg_plot["linthresh_W"], cfg_plot["linthresh_F"]
    spec_label = f"Liq / Ice ({range_key})"

    n_hl = len(height_sel_m) - 1
    n_cols = len(station_ids)
    fig = plt.figure(
        figsize=(SINGLE_COL_IN * max(1, n_cols), min(n_hl * 125 * MM, MAX_H_IN)),
        constrained_layout=True,
    )
    fig.set_constrained_layout_pads(h_pad=2 * MM)
    gs = fig.add_gridspec(n_hl, n_cols, hspace=0.005, wspace=0.005)

    axes_psd = np.empty((n_hl, n_cols), dtype=object)
    axes_liq = np.empty((n_hl, n_cols), dtype=object)
    axes_ice = np.empty((n_hl, n_cols), dtype=object)
    for r in range(n_hl):
        for c in range(n_cols):
            sub = gs[r, c].subgridspec(3, 1, height_ratios=[1.5, 2.3, 2.3], hspace=0.001)
            axes_psd[r, c] = fig.add_subplot(sub[0])
            axes_liq[r, c] = fig.add_subplot(sub[1], sharex=axes_psd[r, c])
            axes_ice[r, c] = fig.add_subplot(sub[2], sharex=axes_psd[r, c])
            for _ax in (axes_psd[r, c], axes_liq[r, c], axes_ice[r, c]):
                _ax.set_facecolor(cfg_plot.get("panel_group_bg", "0.98"))

    global_active: set[str] = set()
    d = np.asarray(diameter_um[bin_slice])
    n = len(d)
    diff = np.diff(d) if n > 1 else np.array([max(d[0] * 0.1, 1e-8)])
    bin_width = np.concatenate([diff, [diff[-1]]]) if n > 1 else diff
    widths = cfg_plot["bar_width_frac_merged"] * bin_width

    for row in range(n_hl):
        h0, h1 = height_sel_m[row], height_sel_m[row + 1]
        for col, station_idx in enumerate(station_ids):
            ax_psd = axes_psd[row, col]
            ax_liq = axes_liq[row, col]
            ax_ice = axes_ice[row, col]
            psd_max_labels: list[str] = []
            net_w = panel_process_values(spec_rates_w, list(spec_rates_w.keys()), station_idx, h0, h1, twindow, bin_slice)
            net_f = panel_process_values(spec_rates_f, list(spec_rates_f.keys()), station_idx, h0, h1, twindow, bin_slice)
            psd_ylim = (
                min(cfg_plot["psd_ylim_W"][0], cfg_plot["psd_ylim_F"][0]),
                max(cfg_plot["psd_ylim_W"][1], cfg_plot["psd_ylim_F"][1]),
            )
            if cfg_plot.get("show_psd_twin", False):
                _format_psd_ax(
                    ax_psd,
                    row=row,
                    col=col,
                    n_cols=n_cols,
                    xlim=xlim,
                    ylim=psd_ylim,
                    grid_linewidth=cfg_plot.get("grid_linewidth", 0.15),
                    psd_yscale=cfg_plot.get("psd_yscale", "log"),
                    conc_unit_label=conc_unit_label,
                    station_idx=station_idx,
                    station_labels=station_labels,
                    spec_label=spec_label,
                )
            else:
                ax_psd.set_visible(False)

            for phase, ax_ph, net_src in [
                ("liq", ax_liq, net_w),
                ("ice", ax_ice, net_f),
            ]:
                ylim_ph = ylim_W if phase == "liq" else ylim_F
                linthresh_ph = linthresh_W if phase == "liq" else linthresh_F
                if net_src:
                    nm = merge_liq_ice_net(net_src, {}, n) if phase == "liq" else merge_liq_ice_net({}, net_src, n)
                    nm = normalize_net_stacks(nm, normalize_mode)
                    global_active |= {p for p, (_, arr) in nm.items() if np.any(np.abs(arr) > 0)}
                    weights = {p: float(np.sum(np.abs(arr))) for p, (_, arr) in nm.items()}
                    order = sorted(nm.keys(), key=lambda p: (PROCESS_PLOT_ORDER.index(p) if p in PROCESS_PLOT_ORDER else 999, -weights.get(p, 0.0)))
                    if not _draw_phase(ax_ph, plot_style, d, widths, order, nm, cfg_plot):
                        ax_ph.text(0.5, 0.5, "no signal", transform=ax_ph.transAxes, ha="center", va="center", color="grey")
                else:
                    ax_ph.text(0.5, 0.5, "no signal", transform=ax_ph.transAxes, ha="center", va="center", color="grey")
                _format_ax(ax_ph, phase=phase, row=row, col=col, n_hl=n_hl, n_cols=n_cols, xlim=xlim, ylim=ylim_ph, linthresh=linthresh_ph,
                           normalize_mode=normalize_mode, yscale=yscale, cfg_plot=cfg_plot, unit_label=unit_label,
                           station_idx=station_idx, station_labels=station_labels, spec_label=spec_label, h0=h0, h1=h1)
                
                if cfg_plot.get("show_psd_twin", False):
                    spec_conc = spec_conc_w if phase == "liq" else spec_conc_f
                    if spec_conc is not None:
                        conc_arr = panel_concentration_profile(spec_conc, station_idx, h0, h1, twindow, bin_slice)
                        if np.any(conc_arr > 0):
                            psd_color = cfg_plot["psd_color_W"] if phase == "liq" else cfg_plot["psd_color_F"]
                            psd_alpha = cfg_plot.get("psd_fill_alpha", 0.4)
                            psd_floor = np.full_like(d, max(psd_ylim[0], np.finfo(float).tiny), dtype=float)
                            ax_psd.step(d, psd_floor, where="mid", color='white', alpha=psd_alpha, linewidth=cfg_plot.get("psd_white_linewidth", 2.0))
                            ax_psd.fill_between(
                                d,
                                psd_floor,
                                conc_arr,
                                step="mid",
                                color=psd_color,
                                alpha=psd_alpha,
                                linewidth=1.2,
                            )
                            ax_psd.step(d, conc_arr, where="mid", color='black', alpha=1.0, linewidth=cfg_plot.get("psd_black_linewidth", 0.8))
                            
                            max_val = float(np.nanmax(conc_arr))
                            psd_ylim_phase = cfg_plot["psd_ylim_W"] if phase == "liq" else cfg_plot["psd_ylim_F"]
                            within_ylim = (
                                np.isfinite(max_val)
                                and max_val > 0.0
                                and psd_ylim_phase[0] <= max_val <= psd_ylim_phase[1]
                            )
                            if within_ylim:
                                mean_d = float(np.average(d, weights=conc_arr))
                                _plot_psd_mean_triangle(ax_psd, mean_d, psd_color)
                                _plot_psd_max_triangle(ax_psd, max_val, psd_color)
                                phase_tag = "Liq" if phase == "liq" else "Ice"
                                psd_max_labels.append(f"{phase_tag} max: {max_val:.1e}")
                        
            if cfg_plot.get("show_psd_twin", False):
                _add_psd_max_textbox(ax_psd, psd_max_labels)
                _add_psd_legend(ax_psd, cfg_plot)

    for r in range(n_hl):
        for c in range(n_cols):
            for ax in (axes_psd[r, c], axes_liq[r, c], axes_ice[r, c]):
                ax.spines["top"].set_visible(False)
                ax.spines["bottom"].set_visible(False)

    show_proc_labels = str(cfg_plot.get("show_proc_labels", "legend")).lower()
    if show_proc_labels == "cmap":
        _add_process_cmap(fig, PROCESS_PLOT_ORDER)
    else:
        build_fixed_legend(
            fig, global_active, PROCESS_PLOT_ORDER,
            handletextpad=cfg_plot.get("legend_handletextpad", 0.8),
            columnspacing=cfg_plot.get("legend_columnspacing", 1.4),
        )

    tw_str = f"{str(twindow.start)[11:19]} - {str(twindow.stop)[11:19]}"
    norm_tag = f" (relative:{normalize_mode})" if normalize_mode != "none" else ""
    fig.suptitle(f"View D - {kind_label} spectral budget [{unit_label}]{norm_tag} -- {tw_str}", fontweight="semibold")
    
    fig.supxlabel("Diameter [µm]", fontsize="medium")
    y_lbl = f"Process Rates [{unit_label}]" if normalize_mode == "none" else "Relative Process Rates [-]"
    fig.supylabel(y_lbl, fontsize="medium")
    
    return fig, (axes_psd, axes_liq, axes_ice)


def _format_ax(ax, *, phase, row, col, n_hl, n_cols, xlim, ylim, linthresh,
               normalize_mode, yscale, cfg_plot, unit_label, station_idx,
               station_labels, spec_label, h0, h1) -> None:
    """Apply axis formatting for a liquid (upper) or ice (lower) sub-axis."""
    from matplotlib.ticker import FuncFormatter, SymmetricalLogLocator

    ax.set_xscale("log")
    ax.set_xlim(*xlim)

    if yscale == "symlog":
        ax.set_yscale("symlog", linthresh=linthresh, linscale=cfg_plot["linscale"])
        ax.yaxis.set_major_locator(SymmetricalLogLocator(linthresh=linthresh, base=10))
    elif yscale == "log":
        ax.set_yscale("log")
    else:
        ax.set_yscale("linear")

    ax.set_ylim(*ylim)
    ax.axhline(0, color="grey", linewidth=cfg_plot["zero_linewidth"], linestyle="--")
    ax.grid(True, which="major", linestyle="--", linewidth=cfg_plot["grid_linewidth"], color="k", alpha=cfg_plot["grid_alpha"])
    ax.set_axisbelow(True)
    ax.tick_params(which="both", direction="out")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))

    def _ytick_fmt(y, pos):
        return f"{y:g}" if pos % 2 == 0 else ""
    ax.yaxis.set_major_formatter(FuncFormatter(_ytick_fmt))
    ax.yaxis.set_ticks_position("both")

    phase_lbl = "Liquid" if phase == "liq" else "Ice"
    ax.text(
        0.02, 0.92, phase_lbl, transform=ax.transAxes, ha="left", va="top",
        fontsize="small", fontweight="medium", color="0.15",
        bbox=dict(facecolor="white", alpha=0.85, edgecolor="none", pad=0.15),
    )

    if phase == "liq":
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
    elif row < n_hl - 1:
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
    if phase == "liq":
        panel_txt = f"{h1:.0f} – {h0:.0f} m"
        ax.text(0.95, 0.92, panel_txt, transform=ax.transAxes, ha="right", va="top", fontweight="semibold",
                bbox=dict(facecolor="white", edgecolor="white", alpha=cfg_plot["panel_bbox_alpha"], boxstyle="round,pad=0.05"))


# ── I/O helpers ──────────────────────────────────────────────────────────────

def _save_frame(fig: Any, stem: str, out_dir: Path, dpi: int, png_compress: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_dir / f"{stem}.png", dpi=dpi, pil_kwargs={"compress_level": png_compress, "optimize": False})


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_time_window(cfg_yaml: dict[str, Any], cfg_loaded: dict[str, Any]) -> list[np.datetime64]:
    seed_start = cfg_loaded["seed_start"]
    vals = cfg_yaml.get("plotting", {}).get("time_spacing_min")
    if isinstance(vals, list) and len(vals) >= 2:
        return [seed_start + np.timedelta64(int(float(t) * 60), "s") for t in vals]
    return cfg_loaded["time_window"]


def _waterfall_cfg(cfg_yaml: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "kind": "N",
        "linthresh_W": 1e-14,
        "linthresh_F": 1e-14,
        "linscale": 0.1,
        "xlim_W": (0.001, 4e3),
        "xlim_F": (0.001, 4e3),
        "ylim_W": (-1e1, 1e1),
        "ylim_F": (-1e1, 1e1),
        "bar_edge_color": "black",
        "bar_edge_linewidth": 0.35,
        "bar_width_frac_merged": 0.95,
        "pos_alpha": 0.6,
        "neg_alpha": 0.6,
        "grid_linewidth": 0.15,
        "grid_alpha": 0.5,
        "zero_linewidth": 0.4,
        "panel_bbox_alpha": 0.35,
        "normalize_mode": "none",
        "plot_style": "bars",
        "yscale": "symlog",
        "line_linewidth": 0.6,
        "fill_alpha": 0.25,
        "line_alpha": 0.7,
        "rate_white_linewidth": 2.0,
        "rate_color_linewidth": 0.8,
        "rate_black_linewidth": 0.5,
        "show_psd_twin": True,
        "psd_color_W": "steelblue",
        "psd_color_F": "sienna",
        "psd_fill_alpha": 0.4,         # 0.25 was too faint; outline uses same alpha
        "psd_color_linewidth": 1.2, # thin step outline at fill alpha to show PSD shape
        "psd_black_linewidth": 0.5,
        "psd_white_linewidth": 2.0,
        "psd_yscale": "log",
        "psd_ylim_W": (1e-10, 1.0),
        "psd_ylim_F": (1e-10, 1.0),
        "psd_xlim_W": (0.001, 4e3), 
        "psd_xlim_F": (0.001, 4e3),
        "show_proc_labels": "legend",
    }
    cfg_raw = cfg_yaml.get("plotting", {}).get("spectral_waterfall", {})
    cfg = {**defaults, **cfg_raw}
    # Back-compat: old configs may use pos_liq_alpha / neg_liq_alpha
    if "pos_liq_alpha" in cfg and "pos_alpha" not in cfg_raw:
        cfg["pos_alpha"] = cfg.pop("pos_liq_alpha")
    if "neg_liq_alpha" in cfg and "neg_alpha" not in cfg_raw:
        cfg["neg_alpha"] = cfg.pop("neg_liq_alpha")
    cfg["kind"] = "Q" if str(cfg["kind"]).upper() == "Q" else "N"
    cfg["normalize_mode"] = str(cfg.get("normalize_mode", "none")).lower()
    cfg["plot_style"] = str(cfg.get("plot_style", "bars")).lower()
    if cfg["plot_style"] not in ("bars", "lines", "steps"):
        raise ValueError(f"plot_style must be 'bars' or 'lines', got '{cfg['plot_style']}'")
    cfg["yscale"] = str(cfg.get("yscale", "symlog")).lower()
    if cfg["yscale"] not in ("symlog", "linear", "log"):
        raise ValueError(f"yscale must be 'symlog', 'linear', or 'log', got '{cfg['yscale']}'")
    cfg["show_proc_labels"] = str(cfg.get("show_proc_labels", "legend")).lower()
    if cfg["show_proc_labels"] not in ("legend", "cmap"):
        raise ValueError(f"show_proc_labels must be 'legend' or 'cmap', got '{cfg['show_proc_labels']}'")
    # Back-compat: old configs may use psd_step_color / psd_step_linewidth
    if "psd_step_color" in cfg and "psd_color_W" not in cfg_raw and "psd_color_F" not in cfg_raw:
        cfg["psd_color_W"] = cfg["psd_color_F"] = cfg.pop("psd_step_color")
    if "psd_step_linewidth" in cfg and "psd_outline_linewidth" not in cfg_raw:
        cfg["psd_outline_linewidth"] = cfg.pop("psd_step_linewidth")
    for k in ("xlim_W", "xlim_F", "ylim_W", "ylim_F", "psd_xlim_W", "psd_xlim_F", "psd_ylim_W", "psd_ylim_F"):
        if k in cfg:
            cfg[k] = tuple(float(v) for v in cfg[k])
    for k in ("linthresh_W", "linthresh_F", "linscale"):
        cfg[k] = float(cfg[k])
    return cfg


def _ffmpeg_path() -> str:
    if is_server():
        return "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    return "ffmpeg"


def _build_mp4(ffmpeg_cmd: str, frames: list[str], mp4_path: Path, fps: int) -> None:
    list_file = mp4_path.parent / f"concat_{mp4_path.stem}.txt"
    with list_file.open("w", encoding="utf-8") as f:
        for p in frames:
            f.write(f"file '{Path(p).resolve()}'\n")
    try:
        subprocess.run(
            [ffmpeg_cmd, "-y", "-f", "concat", "-safe", "0", "-i", str(list_file),
             "-vf", f"scale=trunc(iw/2)*2:trunc(ih/2)*2,setpts=N/({fps}*TB)",
             "-r", str(fps), "-c:v", "libx264", "-pix_fmt", "yuv420p", str(mp4_path.resolve())],
            check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as exc:
        print(exc.stderr)
        raise
    finally:
        list_file.unlink(missing_ok=True)


def _station_tag(station_ids: list[int]) -> str:
    if not station_ids:
        return "stn_none"
    if len(station_ids) == 1:
        return f"stn{station_ids[0]}"
    return "stn" + "-".join(str(s) for s in station_ids)


def _parse_csv_ints(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return [int(v) for v in vals] if vals else None


def _parse_csv_strs(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return vals if vals else None


def _parse_min_max(s: str) -> tuple[float, float]:
    """Parse 'MIN MAX' or 'MIN,MAX' into (float, float). Allows negative MIN without -- prefix issues."""
    s = s.strip()
    if "," in s:
        a, b = s.split(",", 1)
        return (float(a.strip()), float(b.strip()))
    parts = s.split()
    if len(parts) == 2:
        return (float(parts[0]), float(parts[1]))
    raise argparse.ArgumentTypeError("expected MIN MAX or MIN,MAX")


class _MinMaxAction(argparse.Action):
    """Accept 1 value (MIN,MAX) or 2 values (MIN MAX) so --ylim-w -0.01 0.01 works."""

    def __init__(self, option_strings, dest, **kwargs):
        kwargs["nargs"] = "+"
        kwargs["metavar"] = ("MIN", "MAX")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            if len(values) == 1 and "," in values[0]:
                parsed = _parse_min_max(values[0])
            elif len(values) == 2:
                parsed = (float(values[0]), float(values[1]))
            else:
                raise argparse.ArgumentError(self, "expected MIN MAX or MIN,MAX")
            setattr(namespace, self.dest, [parsed])
        except (ValueError, argparse.ArgumentTypeError) as e:
            raise argparse.ArgumentError(self, f"expected MIN MAX or MIN,MAX: {e}") from e


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate spectral waterfall PNG frames and optional MP4 from YAML/CLI options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --config notebooks/config/process_budget.yaml --mp4\n"
            "  # Number concentration (N); use = or quoted for negative ylim:\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --kind N --ylim-w=-1,1 --ylim-f=-1,1 --linthresh-w 1e-9 --linthresh-f 1e-9 --xlim-w 0.001,4000 --xlim-f 0.001,4000\n"
            "  # Mass concentration (Q):\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --kind Q --ylim-w=-0.01,0.01 --ylim-f=-0.01,0.01 --linthresh-w 1e-6 --linthresh-f 1e-6 --xlim-w 0.001,4000\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --normalize-mode bin --exp-ids 1 --range-keys ALLBB\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --workers 8 --mp4\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --mp4-only   # build MP4 from existing PNGs only\n"
        ),
    )
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "notebooks" / "config" / "process_budget.yaml", help="Path to process_budget.yaml")
    parser.add_argument("--workers", type=int, default=None, help="Thread workers for frame rendering.")
    parser.add_argument("--exp-ids", type=str, default=None, help="Comma-separated experiment indices, e.g. '1,2'.")
    parser.add_argument("--range-keys", type=str, default=None, help="Comma-separated range keys, e.g. 'ALLBB,CRYBB'.")
    parser.add_argument("--station-ids", type=str, default=None, help="Comma-separated station indices, e.g. '0,1,2'.")
    parser.add_argument("--kind", type=str, default=None, choices=["N", "Q", "n", "q"], help="N (number) or Q (mass).")
    parser.add_argument("--normalize-mode", type=str, default=None, choices=["none", "bin", "panel"], help="Relative normalization mode.")
    parser.add_argument("--plot-style", type=str, default=None, choices=["bars", "lines", "steps"], help="'bars' (stacked) or 'lines' (line+fill_between) or 'steps' (step function).")
    parser.add_argument("--yscale", type=str, default=None, choices=["symlog", "linear", "log"], help="Y-axis scale: symlog, linear, or log.")
    parser.add_argument("--linthresh-w", type=float, default=None, help="Symlog linear threshold for liquid axis (e.g. 1e-9).")
    parser.add_argument("--linthresh-f", type=float, default=None, help="Symlog linear threshold for ice axis.")
    parser.add_argument("--linscale", type=float, default=None, help="Symlog linscale (e.g. 0.01).")
    parser.add_argument("--xlim-w", action=_MinMaxAction, default=None, help="X-axis limits liquid: MIN MAX or MIN,MAX (e.g. --xlim-w 0.1 4000 or --xlim-w=0.1,4000).")
    parser.add_argument("--xlim-f", action=_MinMaxAction, default=None, help="X-axis limits ice.")
    parser.add_argument("--ylim-w", action=_MinMaxAction, default=None, help="Y-axis limits liquid: MIN MAX or MIN,MAX (e.g. --ylim-w -0.01 0.01).")
    parser.add_argument("--ylim-f", action=_MinMaxAction, default=None, help="Y-axis limits ice.")
    parser.add_argument("--psd-ylim-w", action=_MinMaxAction, default=None, help="PSD twin Y-axis limits liquid.")
    parser.add_argument("--psd-ylim-f", action=_MinMaxAction, default=None, help="PSD twin Y-axis limits ice.")
    parser.add_argument("--mp4", action="store_true", help="Build MP4 after frame generation.")
    parser.add_argument("--mp4-only", action="store_true", help="Skip PNG generation; only build MP4 from existing frames (implies --mp4).")
    parser.add_argument("--no-psd-twin", action="store_true", help="Disable the PSD twin axis.")
    args = parser.parse_args()
    if args.mp4_only:
        args.mp4 = True

    cfg_yaml = _read_yaml(args.config)
    cfg = load_process_budget_data(REPO_ROOT, config_path=args.config)
    apply_publication_style()
    matplotlib.use("Agg")

    cs_run = cfg_yaml.get("ensemble", {}).get("cs_run", "unknown_cs_run")
    frame_root = REPO_ROOT / "scripts" / "processing_chain" / "output" / "05" / cs_run
    mp4_root = REPO_ROOT / "scripts" / "processing_chain" / "output" / "05"
    frame_root.mkdir(parents=True, exist_ok=True)
    mp4_root.mkdir(parents=True, exist_ok=True)

    station_ids = _parse_csv_ints(args.station_ids) or cfg_yaml.get("selection", {}).get("plot_station_ids", cfg["plot_stn_ids"])
    height_sel_m = cfg_yaml.get("plotting", {}).get("height_sel_m", cfg["height_sel_m"])
    time_window = _build_time_window(cfg_yaml, cfg)
    if len(time_window) < 2:
        raise ValueError("Need at least two time points in plotting.time_spacing_min.")

    sw_cfg = _waterfall_cfg(cfg_yaml)
    if args.kind is not None:
        sw_cfg["kind"] = args.kind.upper()
    if args.normalize_mode is not None:
        sw_cfg["normalize_mode"] = args.normalize_mode.lower()
    if args.plot_style is not None:
        sw_cfg["plot_style"] = args.plot_style.lower()
    if args.yscale is not None:
        sw_cfg["yscale"] = args.yscale.lower()
    if args.linthresh_w is not None:
        sw_cfg["linthresh_W"] = float(args.linthresh_w)
    if args.linthresh_f is not None:
        sw_cfg["linthresh_F"] = float(args.linthresh_f)
    if args.linscale is not None:
        sw_cfg["linscale"] = float(args.linscale)
    if args.xlim_w is not None:
        print(f"args.xlim_w: {args.xlim_w}")
        sw_cfg["xlim_W"] = args.xlim_w[0]
    if args.xlim_f is not None:
        print(f"args.xlim_f: {args.xlim_f}")
        sw_cfg["xlim_F"] = args.xlim_f[0]
    if args.ylim_w is not None:
        print(f"args.ylim_w: {args.ylim_w}")
        sw_cfg["ylim_W"] = args.ylim_w[0]
    if args.ylim_f is not None:
        print(f"args.ylim_f: {args.ylim_f}")
        sw_cfg["ylim_F"] = args.ylim_f[0]
    if args.psd_ylim_w is not None:
        sw_cfg["psd_ylim_W"] = args.psd_ylim_w[0]
    if args.psd_ylim_f is not None:
        sw_cfg["psd_ylim_F"] = args.psd_ylim_f[0]
    if args.no_psd_twin:
        sw_cfg["show_psd_twin"] = False
    render_cfg = cfg_yaml.get("plotting", {}).get("render", {})
    frame_dpi = int(render_cfg.get("frame_dpi", 300))
    frame_png_compress = int(render_cfg.get("frame_png_compress", 1))
    mp4_fps = int(render_cfg.get("mp4_fps", 1))

    plot_exp_ids = _parse_csv_ints(args.exp_ids) or cfg_yaml.get("selection", {}).get("plot_experiment_ids", cfg["plot_exp_ids"])
    plot_range_keys = _parse_csv_strs(args.range_keys) or cfg_yaml.get("plotting", {}).get("plot_range_keys", cfg["plot_range_keys"])
    bad_ranges = [rk for rk in plot_range_keys if rk not in cfg["size_ranges"]]
    if bad_ranges:
        raise ValueError(f"Unknown range key(s): {bad_ranges}. Valid: {', '.join(cfg['size_ranges'].keys())}")
    ffmpeg_cmd = _ffmpeg_path()
    workers = max(1, int(args.workers if args.workers is not None else render_cfg.get("workers", 4)))
    kind = sw_cfg["kind"]
    kind_label = "number" if kind == "N" else "mass"
    kind_dir = "N" if kind == "N" else "M"
    stn_tag = _station_tag(station_ids)
    cs_run_tag = cs_run.replace("/", "_")

    def render_task(task: tuple[int, str, int]) -> str:
        eid, range_key, itime = task
        r = cfg["rates_by_exp"][eid]
        tw = slice(time_window[itime], time_window[itime + 1])
        
        spec_conc_w = r.get(f"spec_conc_{kind}_W") if sw_cfg.get("show_psd_twin") else None
        spec_conc_f = r.get(f"spec_conc_{kind}_F") if sw_cfg.get("show_psd_twin") else None
        conc_unit_label = r"#/m³" if kind == "N" else r"g/m³"
        
        fig, _ = plot_spectral_waterfall(
            spec_rates_w=r[f"spec_rates_{kind}_W"],
            spec_rates_f=r[f"spec_rates_{kind}_F"],
            size_ranges=cfg["size_ranges"],
            range_key=range_key,
            diameter_um=cfg["diameter_um"],
            station_ids=station_ids,
            station_labels=cfg["station_labels"],
            height_sel_m=height_sel_m,
            twindow=tw,
            unit_label=r[f"unit_{kind}"],
            kind_label=kind_label,
            cfg_plot=sw_cfg,
            normalize_mode=sw_cfg["normalize_mode"],
            plot_style=sw_cfg["plot_style"],
            yscale=sw_cfg["yscale"],
            spec_conc_w=spec_conc_w,
            spec_conc_f=spec_conc_f,
            conc_unit_label=conc_unit_label,
        )
        stem = f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime{itime}"
        _save_frame(fig, stem, frame_root / f"exp{eid}" / kind_dir, frame_dpi, frame_png_compress)
        plt.close(fig)
        return stem

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    t_start = time.perf_counter()
    for eid in plot_exp_ids:
        for range_key in plot_range_keys:
            frame_dir = frame_root / f"exp{eid}" / kind_dir
            pattern = str(frame_dir / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime*.png")

            if not args.mp4_only:
                tasks = [(eid, range_key, i) for i in range(len(time_window) - 1)]
                desc = f"exp={eid} {range_key}"
                t_batch = time.perf_counter()
                with ThreadPoolExecutor(max_workers=min(workers, len(tasks))) as ex:
                    futures = ex.map(render_task, tasks)
                    if tqdm is not None:
                        list(tqdm(futures, total=len(tasks), desc=desc, unit="frame", ncols=90))
                    else:
                        for i, _ in enumerate(futures, 1):
                            elapsed = time.perf_counter() - t_batch
                            print(f"\r  {desc}: {i}/{len(tasks)} frames  [{elapsed:.1f}s]", end="", flush=True)
                        print()
                dt_batch = time.perf_counter() - t_batch

            frames = sorted(glob.glob(pattern), key=lambda x: int(Path(x).stem.split("_itime")[-1]))
            n_frames = len(frames)
            if args.mp4_only:
                print(f"  exp={eid} {range_key}: {n_frames} existing frames in {frame_dir}")
            else:
                print(f"  {n_frames} frames in {frame_dir}  ({dt_batch:.1f}s, {dt_batch / max(n_frames, 1):.2f}s/frame)")

            if args.mp4 and n_frames > 0:
                mp4_path = mp4_root / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_evolution_nframes{n_frames}.mp4"
                _build_mp4(ffmpeg_cmd, frames, mp4_path, mp4_fps)
                print(f"  MP4: {mp4_path}")
                if sys.platform == "darwin":
                    subprocess.run(["open", "-R", str(mp4_path)])
            elif args.mp4 and n_frames == 0:
                print(f"  No frames found for {pattern}; MP4 skipped.")
            elif not args.mp4:
                print("  MP4 generation skipped (pass --mp4 to enable).")

    dt_total = time.perf_counter() - t_start
    print(f"\nDone. Total wall time: {dt_total:.1f}s ({dt_total / 60:.1f}min)")


if __name__ == "__main__":
    main()
