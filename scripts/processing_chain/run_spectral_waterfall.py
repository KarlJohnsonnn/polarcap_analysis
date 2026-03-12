#!/usr/bin/env python3
"""Render spectral waterfall PNG frames (and optional MP4) from YAML config.

Outputs:
- Frames: notebooks/output/05/<cs_run>/spectral_waterfall_<kind>_exp<id>_stn_all_<range>_itime<k>.png
- MP4:    notebooks/output/05/spectral_waterfall_<kind>_exp<id>_stn_all_<range>_evolution_nframes<n>.mp4
          (only when --mp4 is provided)
"""

from __future__ import annotations

import argparse
import glob
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
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
    load_process_budget_data,
    proc_color,
    stn_label,
)
from utilities.compute_fabric import is_server  # noqa: E402


def _pad_1d(arr: np.ndarray, n: int, fill_value: float = 0.0) -> np.ndarray:
    a = np.asarray(arr)
    if a.size >= n:
        return a[:n]
    return np.pad(a, (0, n - a.size), constant_values=fill_value)


def _nonneg(arr: np.ndarray) -> np.ndarray:
    a = np.asarray(arr)
    a = np.nan_to_num(a, nan=0.0, posinf=0.0, neginf=0.0)
    return np.maximum(0.0, a)


def _nonneg_negative(arr: np.ndarray) -> np.ndarray:
    a = np.asarray(arr)
    a = np.nan_to_num(a, nan=0.0, posinf=0.0, neginf=0.0)
    return np.maximum(0.0, -a)


def _build_liq_ice_stacks(
    stack_w: list[tuple[str, str, np.ndarray]],
    stack_f: list[tuple[str, str, np.ndarray]],
    n: int,
) -> dict[str, tuple[str, np.ndarray, np.ndarray]]:
    out: dict[str, tuple[str, np.ndarray, np.ndarray]] = {}
    for p, c, arr in stack_w:
        out[p] = (c, _nonneg(_pad_1d(arr, n)), np.zeros(n))
    for p, c, arr in stack_f:
        ice = _nonneg(_pad_1d(arr, n))
        if p not in out:
            out[p] = (proc_color(p), np.zeros(n), ice)
        else:
            out[p] = (out[p][0], out[p][1], ice)
    return out


def _build_liq_ice_stacks_neg(
    stack_w: list[tuple[str, str, np.ndarray]],
    stack_f: list[tuple[str, str, np.ndarray]],
    n: int,
) -> dict[str, tuple[str, np.ndarray, np.ndarray]]:
    out: dict[str, tuple[str, np.ndarray, np.ndarray]] = {}
    for p, c, arr in stack_w:
        out[p] = (c, _nonneg_negative(_pad_1d(arr, n)), np.zeros(n))
    for p, c, arr in stack_f:
        ice = _nonneg_negative(_pad_1d(arr, n))
        if p not in out:
            out[p] = (proc_color(p), np.zeros(n), ice)
        else:
            out[p] = (out[p][0], out[p][1], ice)
    return out


def _panel_process_values(
    proc_map: dict[str, Any],
    procs: list[str],
    station_idx: int,
    h0: float,
    h1: float,
    twindow: slice,
    bin_slice: slice,
) -> dict[str, np.ndarray]:
    if not procs:
        return {}
    import xarray as xr

    stacked = xr.concat([proc_map[p] for p in procs], dim="process")
    stacked = (
        stacked.isel(station=station_idx)
        .sel(height_level=slice(h0, h1))
        .mean(dim="height_level")
        .sel(time=twindow)
    )
    if stacked.sizes.get("time", 0) == 0:
        return {}
    vals = np.asarray(stacked.mean(dim="time").isel(bins=bin_slice).values)
    return {p: vals[i] for i, p in enumerate(procs)}


def _ordered_processes(keys: list[str], weights: dict[str, float]) -> list[str]:
    return sorted(
        keys,
        key=lambda p: (
            PROCESS_PLOT_ORDER.index(p) if p in PROCESS_PLOT_ORDER else 999,
            -weights.get(p, 0.0),
        ),
    )


def _normalize_stacks(
    stack_pos: list[tuple[str, str, np.ndarray]],
    stack_neg: list[tuple[str, str, np.ndarray]],
    mode: str,
) -> tuple[list[tuple[str, str, np.ndarray]], list[tuple[str, str, np.ndarray]]]:
    """Normalize process stacks for relative plotting.

    Modes:
    - none: no normalization
    - bin: per-bin normalization (sum positive per bin = +1, sum negative magnitude per bin = -1)
    - panel: panel max-abs normalization (largest absolute contribution in panel = 1)
    """
    if mode == "none":
        return stack_pos, stack_neg

    if mode == "bin":
        pos_arrays = [np.maximum(0.0, np.asarray(arr)) for _, _, arr in stack_pos]
        neg_arrays = [np.maximum(0.0, -np.asarray(arr)) for _, _, arr in stack_neg]
        pos_sum = np.sum(pos_arrays, axis=0) if pos_arrays else None
        neg_sum = np.sum(neg_arrays, axis=0) if neg_arrays else None

        out_pos: list[tuple[str, str, np.ndarray]] = []
        for p, c, arr in stack_pos:
            a = np.maximum(0.0, np.asarray(arr))
            if pos_sum is None:
                out_pos.append((p, c, a))
            else:
                out_pos.append((p, c, np.divide(a, pos_sum, out=np.zeros_like(a), where=pos_sum > 0)))

        out_neg: list[tuple[str, str, np.ndarray]] = []
        for p, c, arr in stack_neg:
            amag = np.maximum(0.0, -np.asarray(arr))
            if neg_sum is None:
                out_neg.append((p, c, -amag))
            else:
                norm = np.divide(amag, neg_sum, out=np.zeros_like(amag), where=neg_sum > 0)
                out_neg.append((p, c, -norm))
        return out_pos, out_neg

    if mode == "panel":
        vals = []
        vals.extend([np.abs(np.asarray(arr)) for _, _, arr in stack_pos])
        vals.extend([np.abs(np.asarray(arr)) for _, _, arr in stack_neg])
        vmax = float(np.nanmax(np.concatenate([v.ravel() for v in vals]))) if vals else 0.0
        if vmax <= 0:
            return stack_pos, stack_neg
        out_pos = [(p, c, np.asarray(arr) / vmax) for p, c, arr in stack_pos]
        out_neg = [(p, c, np.asarray(arr) / vmax) for p, c, arr in stack_neg]
        return out_pos, out_neg

    raise ValueError(f"Unknown normalize mode: {mode}")


def plot_spectral_waterfall(
    *,
    spec_rates_w: dict[str, Any],
    spec_rates_f: dict[str, Any],
    spec_rates_w_pos: dict[str, Any],
    spec_rates_w_neg: dict[str, Any],
    spec_rates_f_pos: dict[str, Any],
    spec_rates_f_neg: dict[str, Any],
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
) -> tuple[Any, Any]:
    from matplotlib.ticker import FixedLocator, FuncFormatter

    spec_cols = [
        (
            size_ranges[range_key]["slice"],
            size_ranges[range_key]["slice"],
            f"Liquid + Ice ({range_key})",
            ("W", "F"),
            (
                min(cfg_plot["xlim_W"][0], cfg_plot["xlim_F"][0]),
                max(cfg_plot["xlim_W"][1], cfg_plot["xlim_F"][1]),
            ),
            (
                min(cfg_plot["ylim_W"][0], cfg_plot["ylim_F"][0]),
                max(cfg_plot["ylim_W"][1], cfg_plot["ylim_F"][1]),
            ),
            max(cfg_plot["linthresh_W"], cfg_plot["linthresh_F"]),
        )
    ]
    n_hl = len(height_sel_m) - 1
    n_cols = len(station_ids)
    fig, axes = plt.subplots(
        n_hl,
        n_cols,
        figsize=(SINGLE_COL_IN * max(1, n_cols), min(n_hl * 80 * MM, MAX_H_IN)),
        constrained_layout=True,
    )
    fig.set_constrained_layout_pads(h_pad=0)
    if n_hl == 1:
        axes = axes[np.newaxis, :]
    if n_cols == 1:
        axes = axes[:, np.newaxis]

    dicts = {"W": spec_rates_w, "F": spec_rates_f}
    dicts_pos = {"W": spec_rates_w_pos, "F": spec_rates_f_pos}
    dicts_neg = {"W": spec_rates_w_neg, "F": spec_rates_f_neg}

    for row in range(n_hl):
        h0, h1 = height_sel_m[row], height_sel_m[row + 1]
        for col, station_idx in enumerate(station_ids):
            ax = axes[row, col]
            any_data = False
            (bs_w, bs_f, spec_label, (_kw, _kf), xlim, ylim, linthresh) = spec_cols[0]
            col_specs = [(bs_w, "W"), (bs_f, "F")]
            all_stacks: list[tuple[np.ndarray, list[Any], list[Any]]] = []

            for bin_slice, spectrum_key in col_specs:
                spec_rates = dicts[spectrum_key]
                procs = list(spec_rates.keys())
                d = diameter_um[bin_slice]
                pos_map = _panel_process_values(
                    dicts_pos[spectrum_key],
                    [p for p in procs if p in dicts_pos[spectrum_key]],
                    station_idx,
                    h0,
                    h1,
                    twindow,
                    bin_slice,
                )
                neg_map = _panel_process_values(
                    dicts_neg[spectrum_key],
                    [p for p in procs if p in dicts_neg[spectrum_key]],
                    station_idx,
                    h0,
                    h1,
                    twindow,
                    bin_slice,
                )
                stack_pos: list[tuple[str, str, np.ndarray]] = []
                stack_neg: list[tuple[str, str, np.ndarray]] = []
                for p in procs:
                    c = proc_color(p)
                    pvals = pos_map.get(p)
                    nvals = neg_map.get(p)
                    if pvals is not None and np.any(np.isfinite(pvals) & (pvals > 0)):
                        stack_pos.append((p, c, np.asarray(pvals)))
                        any_data = True
                    if nvals is not None and np.any(np.isfinite(nvals) & (nvals < 0)):
                        stack_neg.append((p, c, np.asarray(nvals)))
                        any_data = True
                stack_pos, stack_neg = _normalize_stacks(stack_pos, stack_neg, normalize_mode)
                all_stacks.append((np.asarray(d), stack_pos, stack_neg))

            if len(all_stacks) == 2:
                d_w, sp_w, sn_w = all_stacks[0]
                d_f, sp_f, sn_f = all_stacks[1]
                n = min(len(d_w), len(d_f))
                d_w = d_w[:n]
                d_f = d_f[:n]
                x_single = (d_w + d_f) / 2.0
                diff_w = np.diff(d_w) if n > 1 else np.array([max(d_w[0] * 0.1, 1e-8)])
                bin_width = np.concatenate([diff_w, [diff_w[-1]]]) if n > 1 else diff_w
                widths = cfg_plot["bar_width_frac_merged"] * bin_width

                procs_pos = _build_liq_ice_stacks(sp_w, sp_f, n)
                procs_neg = _build_liq_ice_stacks_neg(sn_w, sn_f, n)
                pos_weights = {
                    p: float(np.sum(procs_pos[p][1] + procs_pos[p][2])) for p in procs_pos
                }
                neg_weights = {
                    p: float(np.sum(procs_neg[p][1] + procs_neg[p][2])) for p in procs_neg
                }
                order_pos = _ordered_processes(list(procs_pos.keys()), pos_weights)
                order_neg = _ordered_processes(list(procs_neg.keys()), neg_weights)

                bottom = np.zeros(n)
                for p in order_pos:
                    c, liq, ice = procs_pos[p]
                    ax.bar(
                        x_single,
                        ice,
                        width=widths,
                        bottom=bottom,
                        color=c,
                        edgecolor=cfg_plot["bar_edge_color"],
                        linewidth=cfg_plot["bar_edge_linewidth"],
                        alpha=cfg_plot["pos_ice_alpha"],
                        hatch=cfg_plot["ice_hatch"],
                    )
                    bottom += ice
                    ax.bar(
                        x_single,
                        liq,
                        width=widths,
                        bottom=bottom,
                        color=c,
                        label=p,
                        edgecolor=cfg_plot["bar_edge_color"],
                        linewidth=cfg_plot["bar_edge_linewidth"],
                        alpha=cfg_plot["pos_liq_alpha"],
                        hatch=cfg_plot["liq_hatch"],
                    )
                    bottom += liq

                bottom_neg = np.zeros(n)
                for p in order_neg:
                    c, liq, ice = procs_neg[p]
                    ax.bar(
                        x_single,
                        ice,
                        width=widths,
                        bottom=-bottom_neg - ice,
                        color=c,
                        edgecolor=cfg_plot["bar_edge_color"],
                        linewidth=cfg_plot["bar_edge_linewidth"],
                        alpha=cfg_plot["neg_ice_alpha"],
                        hatch=cfg_plot["ice_hatch"],
                    )
                    bottom_neg += ice
                    ax.bar(
                        x_single,
                        liq,
                        width=widths,
                        bottom=-bottom_neg - liq,
                        color=c,
                        label=p,
                        edgecolor=cfg_plot["bar_edge_color"],
                        linewidth=cfg_plot["bar_edge_linewidth"],
                        alpha=cfg_plot["neg_liq_alpha"],
                        hatch=cfg_plot["liq_hatch"],
                    )
                    bottom_neg += liq

            ax.set_xlim(*xlim)
            ax.set_ylim(*((-1.05, 1.05) if normalize_mode != "none" else ylim))
            ax.set_xscale("log")
            if normalize_mode == "none":
                ax.set_yscale(
                    "symlog",
                    linthresh=linthresh,
                    linscale=cfg_plot["linscale"],
                )
            else:
                ax.set_yscale("linear")
            ax.axhline(0, color="grey", linewidth=cfg_plot["zero_linewidth"], linestyle="--")
            ax.grid(
                True,
                which="major",
                linestyle="--",
                linewidth=cfg_plot["grid_linewidth"],
                color="k",
                alpha=cfg_plot["grid_alpha"],
            )
            ax.set_axisbelow(True)
            ax.tick_params(which="both", direction="out")
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))
            yticks = ax.get_yticks()
            ylabels = [f"{yt:g}" if i % 2 == 0 else "" for i, yt in enumerate(yticks)]
            ax.yaxis.set_major_locator(FixedLocator(yticks))
            ax.set_yticklabels(ylabels)
            ax.yaxis.set_ticks_position("both")
            ax.yaxis.set_tick_params(right=True, which="both")

            if row == n_hl - 1:
                ax.set_xlabel("Diameter [µm]")
            elif row == 0:
                ax.set_title(stn_label(station_idx, station_labels) if n_cols > 1 else spec_label)
            else:
                ax.set_xticklabels([])
            if col == 0:
                if normalize_mode == "none":
                    ax.set_ylabel(f"Rate [{unit_label}]")
                else:
                    ax.set_ylabel("Relative rate [-]")
            else:
                ax.set_yticklabels([])
            panel_txt = f"{h1:.0f} - {h0:.0f} m"
            ax.text(
                0.95,
                0.95,
                panel_txt,
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontweight="semibold",
                bbox=dict(
                    facecolor="white",
                    edgecolor="white",
                    alpha=cfg_plot["panel_bbox_alpha"],
                    boxstyle="round,pad=0.05",
                ),
            )
            if not any_data:
                ax.text(0.5, 0.5, "no signal", transform=ax.transAxes, ha="center", va="center", color="grey")

    legend_by_label: dict[str, Any] = {}
    for ax in axes.flatten():
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        hs, ls = ax.get_legend_handles_labels()
        for h, lbl in zip(hs, ls):
            if lbl and lbl not in legend_by_label:
                legend_by_label[lbl] = h
    if legend_by_label:
        fig.legend(
            list(legend_by_label.values()),
            list(legend_by_label.keys()),
            loc="lower center",
            bbox_to_anchor=(0.5, -0.02),
            ncol=min(6, max(1, len(legend_by_label))),
            frameon=False,
        )
    tw_str = f"{str(twindow.start)[11:19]} - {str(twindow.stop)[11:19]}"
    if normalize_mode == "none":
        fig.suptitle(f"View D - {kind_label} spectral budget [{unit_label}] -- {tw_str}", fontweight="semibold")
    else:
        fig.suptitle(
            f"View D - {kind_label} spectral budget (relative:{normalize_mode}) -- {tw_str}",
            fontweight="semibold",
        )
    return fig, axes


def _save_frame(fig: Any, stem: str, out_dir: Path, dpi: int, png_compress: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        out_dir / f"{stem}.png",
        dpi=dpi,
        pil_kwargs={"compress_level": png_compress, "optimize": False},
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_time_window(cfg_yaml: dict[str, Any], cfg_loaded: dict[str, Any]) -> list[np.datetime64]:
    seed_start = cfg_loaded["seed_start"]
    plotting = cfg_yaml.get("plotting", {})
    vals = plotting.get("time_spacing_min")
    if isinstance(vals, list) and len(vals) >= 2:
        return [seed_start + np.timedelta64(int(float(t) * 60), "s") for t in vals]
    return cfg_loaded["time_window"]


def _waterfall_cfg(cfg_yaml: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "kind": "N",
        "linthresh_W": 1e-9,
        "linthresh_F": 1e-9,
        "linscale": 0.1,
        "xlim_W": (0.001, 4e3),
        "xlim_F": (0.001, 4e3),
        "ylim_W": (-1e1, 1e1),
        "ylim_F": (-1e1, 1e1),
        "bar_edge_color": "black",
        "bar_edge_linewidth": 0.35,
        "bar_width_frac_merged": 0.95,
        "pos_ice_alpha": 0.6,
        "pos_liq_alpha": 0.35,
        "neg_ice_alpha": 0.6,
        "neg_liq_alpha": 0.35,
        "ice_hatch": "....",
        "liq_hatch": None,
        "rate_linewidth": 0.5,
        "rate_linewidth_neg": 0.5,
        "rate_fill_linewidth": 0.9,
        "rate_edge_linewidth": 0.2,
        "grid_linewidth": 0.15,
        "grid_alpha": 0.5,
        "zero_linewidth": 0.4,
        "panel_bbox_alpha": 0.35,
        "normalize_mode": "none",  # one of: none, bin, panel
    }
    cfg_raw = cfg_yaml.get("plotting", {}).get("spectral_waterfall", {})
    cfg = {**defaults, **cfg_raw}
    cfg["kind"] = "Q" if str(cfg["kind"]).upper() == "Q" else "N"
    cfg["normalize_mode"] = str(cfg.get("normalize_mode", "none")).lower()
    for k in ("xlim_W", "xlim_F", "ylim_W", "ylim_F"):
        cfg[k] = tuple(cfg[k])
    return cfg


def _ffmpeg_path() -> str:
    if is_server():
        return "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    return "ffmpeg"


def _build_mp4(
    ffmpeg_cmd: str,
    frames: list[str],
    mp4_path: Path,
    fps: int,
) -> None:
    list_file = mp4_path.parent / f"concat_{mp4_path.stem}.txt"
    with list_file.open("w", encoding="utf-8") as f:
        for p in frames:
            f.write(f"file '{Path(p).resolve()}'\n")
    try:
        subprocess.run(
            [
                ffmpeg_cmd,
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(list_file),
                "-vf",
                f"scale=trunc(iw/2)*2:trunc(ih/2)*2,setpts=N/({fps}*TB)",
                "-r",
                str(fps),
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                str(mp4_path.resolve()),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        print(exc.stderr)
        raise
    finally:
        list_file.unlink(missing_ok=True)


def _station_tag(station_ids: list[int]) -> str:
    """Compact station tag for output filenames."""
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate spectral waterfall PNG frames and optional MP4 from YAML/CLI options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Frames only (use YAML defaults)\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py\n"
            "\n"
            "  # Frames + MP4 with explicit config\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py \\\n"
            "    --config notebooks/config/process_budget.yaml --mp4\n"
            "\n"
            "  # One experiment/range/station subset\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py \\\n"
            "    --exp-ids 1 --range-keys ALLBB --station-ids 0 --kind N\n"
            "\n"
            "  # Relative plotting (small processes more visible)\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py \\\n"
            "    --normalize-mode bin --exp-ids 1 --range-keys ALLBB\n"
            "\n"
            "  # Use more workers and generate MP4\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py \\\n"
            "    --workers 8 --mp4\n"
        ),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=REPO_ROOT / "notebooks" / "config" / "process_budget.yaml",
        help="Path to process_budget.yaml",
    )
    parser.add_argument("--workers", type=int, default=None, help="Thread workers for frame rendering.")
    parser.add_argument(
        "--exp-ids",
        type=str,
        default=None,
        help="Comma-separated experiment indices override, e.g. '1,2'.",
    )
    parser.add_argument(
        "--range-keys",
        type=str,
        default=None,
        help="Comma-separated range keys override, e.g. 'ALLBB,CRYBB'.",
    )
    parser.add_argument(
        "--station-ids",
        type=str,
        default=None,
        help="Comma-separated station indices override, e.g. '0,1,2'.",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default=None,
        choices=["N", "Q", "n", "q"],
        help="Override plotting.spectral_waterfall.kind (N number, Q mass).",
    )
    parser.add_argument(
        "--normalize-mode",
        type=str,
        default=None,
        choices=["none", "bin", "panel"],
        help="Relative normalization mode: none, bin (per-bin fractions), panel (max-abs=1).",
    )
    parser.add_argument(
        "--mp4",
        action="store_true",
        help="Also build MP4 after PNG frame generation (default: frames only).",
    )
    args = parser.parse_args()

    cfg_yaml = _read_yaml(args.config)
    cfg = load_process_budget_data(REPO_ROOT, config_path=args.config)
    apply_publication_style()
    matplotlib.use("Agg")

    cs_run = cfg_yaml.get("ensemble", {}).get("cs_run", "unknown_cs_run")
    frame_root = REPO_ROOT / "notebooks" / "output" / "05" / cs_run
    mp4_root = REPO_ROOT / "notebooks" / "output" / "05"
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
    render_cfg = cfg_yaml.get("plotting", {}).get("render", {})
    frame_dpi = int(render_cfg.get("frame_dpi", 300))
    frame_png_compress = int(render_cfg.get("frame_png_compress", 1))
    mp4_fps = int(render_cfg.get("mp4_fps", 1))

    plot_exp_ids = _parse_csv_ints(args.exp_ids) or cfg_yaml.get("selection", {}).get("plot_experiment_ids", cfg["plot_exp_ids"])
    plot_range_keys = _parse_csv_strs(args.range_keys) or cfg_yaml.get("plotting", {}).get("plot_range_keys", cfg["plot_range_keys"])
    bad_ranges = [rk for rk in plot_range_keys if rk not in cfg["size_ranges"]]
    if bad_ranges:
        valid = ", ".join(cfg["size_ranges"].keys())
        raise ValueError(f"Unknown range key(s): {bad_ranges}. Valid: {valid}")
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
        fig, _ = plot_spectral_waterfall(
            spec_rates_w=r[f"spec_rates_{kind}_W"],
            spec_rates_f=r[f"spec_rates_{kind}_F"],
            spec_rates_w_pos=r[f"spec_rates_{kind}_W_pos"],
            spec_rates_w_neg=r[f"spec_rates_{kind}_W_neg"],
            spec_rates_f_pos=r[f"spec_rates_{kind}_F_pos"],
            spec_rates_f_neg=r[f"spec_rates_{kind}_F_neg"],
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
        )
        stem = f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime{itime}"
        frame_dir = frame_root / f"exp{eid}" / kind_dir
        _save_frame(fig, stem, frame_dir, frame_dpi, frame_png_compress)
        plt.close(fig)
        return stem

    for eid in plot_exp_ids:
        for range_key in plot_range_keys:
            tasks = [(eid, range_key, i) for i in range(len(time_window) - 1)]
            print(f"Generating {len(tasks)} frames: exp={eid} range={range_key}")
            with ThreadPoolExecutor(max_workers=min(workers, len(tasks))) as ex:
                list(ex.map(render_task, tasks))

            frame_dir = frame_root / f"exp{eid}" / kind_dir
            pattern = str(
                frame_dir / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime*.png"
            )
            frames = sorted(
                glob.glob(pattern),
                key=lambda x: int(Path(x).stem.split("_itime")[-1]),
            )
            n_frames = len(frames)
            print(f"Created animated: {n_frames} frames in {frame_dir}")
            if args.mp4:
                mp4_path = mp4_root / (
                    f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_"
                    f"evolution_nframes{n_frames}.mp4"
                )
                _build_mp4(ffmpeg_cmd, frames, mp4_path, mp4_fps)
                print(f"Created MP4: {mp4_path}")
            else:
                print("MP4 generation skipped (pass --mp4 to enable).")


if __name__ == "__main__":
    main()
