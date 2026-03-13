#!/usr/bin/env python3
"""Render spectral waterfall PNG frames (and optional MP4) from YAML config.

Uses NET process rates for physically correct bar stacking. Legend is
fixed-size: all processes always shown, active = filled+hatched, inactive =
hollow outline.

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
from typing import Any

import matplotlib
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
    build_fixed_legend,
    load_process_budget_data,
    merge_liq_ice_net,
    normalize_net_stacks,
    panel_process_values,
    proc_hatch,
    stn_label,
)
from utilities.compute_fabric import is_server  # noqa: E402


# ── Plotting ─────────────────────────────────────────────────────────────────

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
) -> tuple[Any, Any]:
    """Spectral waterfall bar chart: net process rates vs diameter."""
    from matplotlib.ticker import FixedLocator, FuncFormatter

    bin_slice = size_ranges[range_key]["slice"]
    xlim = (min(cfg_plot["xlim_W"][0], cfg_plot["xlim_F"][0]), max(cfg_plot["xlim_W"][1], cfg_plot["xlim_F"][1]))
    ylim = (min(cfg_plot["ylim_W"][0], cfg_plot["ylim_F"][0]), max(cfg_plot["ylim_W"][1], cfg_plot["ylim_F"][1]))
    linthresh = max(cfg_plot["linthresh_W"], cfg_plot["linthresh_F"])
    spec_label = f"Liquid + Ice ({range_key})"

    n_hl = len(height_sel_m) - 1
    n_cols = len(station_ids)
    fig, axes = plt.subplots(
        n_hl, n_cols,
        figsize=(SINGLE_COL_IN * max(1, n_cols), min(n_hl * 80 * MM, MAX_H_IN)),
        constrained_layout=True,
    )
    fig.set_constrained_layout_pads(h_pad=0)
    if n_hl == 1:
        axes = axes[np.newaxis, :]
    if n_cols == 1:
        axes = axes[:, np.newaxis]

    global_active: set[str] = set()
    d = np.asarray(diameter_um[bin_slice])
    n = len(d)
    diff = np.diff(d) if n > 1 else np.array([max(d[0] * 0.1, 1e-8)])
    bin_width = np.concatenate([diff, [diff[-1]]]) if n > 1 else diff
    widths = cfg_plot["bar_width_frac_merged"] * bin_width

    for row in range(n_hl):
        h0, h1 = height_sel_m[row], height_sel_m[row + 1]
        for col, station_idx in enumerate(station_ids):
            ax = axes[row, col]

            net_w = panel_process_values(spec_rates_w, list(spec_rates_w.keys()), station_idx, h0, h1, twindow, bin_slice)
            net_f = panel_process_values(spec_rates_f, list(spec_rates_f.keys()), station_idx, h0, h1, twindow, bin_slice)

            if not net_w and not net_f:
                ax.text(0.5, 0.5, "no signal", transform=ax.transAxes, ha="center", va="center", color="grey")
                _format_ax(ax, row, col, n_hl, n_cols, xlim, ylim, linthresh, normalize_mode, cfg_plot, unit_label, station_idx, station_labels, spec_label, h0, h1)
                continue

            net_merged = merge_liq_ice_net(net_w, net_f, n)
            net_merged = normalize_net_stacks(net_merged, normalize_mode)

            active_here = {p for p, (_, arr) in net_merged.items() if np.any(np.abs(arr) > 0)}
            global_active |= active_here

            weights = {p: float(np.sum(np.abs(arr))) for p, (_, arr) in net_merged.items()}
            order = sorted(
                net_merged.keys(),
                key=lambda p: (PROCESS_PLOT_ORDER.index(p) if p in PROCESS_PLOT_ORDER else 999, -weights.get(p, 0.0)),
            )

            any_data = False
            bottom_pos, bottom_neg = np.zeros(n), np.zeros(n)
            for p in order:
                c, net_arr = net_merged[p]
                pos_part = np.maximum(0.0, net_arr)
                neg_part = np.minimum(0.0, net_arr)
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

            if not any_data:
                ax.text(0.5, 0.5, "no signal", transform=ax.transAxes, ha="center", va="center", color="grey")

            _format_ax(ax, row, col, n_hl, n_cols, xlim, ylim, linthresh, normalize_mode, cfg_plot, unit_label, station_idx, station_labels, spec_label, h0, h1)

    for ax in axes.flatten():
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)

    build_fixed_legend(fig, global_active, PROCESS_PLOT_ORDER)

    tw_str = f"{str(twindow.start)[11:19]} - {str(twindow.stop)[11:19]}"
    if normalize_mode == "none":
        fig.suptitle(f"View D - {kind_label} spectral budget [{unit_label}] -- {tw_str}", fontweight="semibold")
    else:
        fig.suptitle(f"View D - {kind_label} spectral budget (relative:{normalize_mode}) -- {tw_str}", fontweight="semibold")
    return fig, axes


def _format_ax(ax, row, col, n_hl, n_cols, xlim, ylim, linthresh, normalize_mode, cfg_plot, unit_label, station_idx, station_labels, spec_label, h0, h1) -> None:
    """Apply shared axis formatting."""
    from matplotlib.ticker import FixedLocator, FuncFormatter

    ax.set_xlim(*xlim)
    ax.set_ylim(*((-1.05, 1.05) if normalize_mode != "none" else ylim))
    ax.set_xscale("log")
    if normalize_mode == "none":
        ax.set_yscale("symlog", linthresh=linthresh, linscale=cfg_plot["linscale"])
    else:
        ax.set_yscale("linear")
    ax.axhline(0, color="grey", linewidth=cfg_plot["zero_linewidth"], linestyle="--")
    ax.grid(True, which="major", linestyle="--", linewidth=cfg_plot["grid_linewidth"], color="k", alpha=cfg_plot["grid_alpha"])
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
        ax.set_ylabel(f"Rate [{unit_label}]" if normalize_mode == "none" else "Relative rate [-]")
    else:
        ax.set_yticklabels([])
    panel_txt = f"{h1:.0f} - {h0:.0f} m"
    ax.text(0.95, 0.95, panel_txt, transform=ax.transAxes, ha="right", va="top", fontweight="semibold",
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
        "pos_alpha": 0.6,
        "neg_alpha": 0.6,
        "grid_linewidth": 0.15,
        "grid_alpha": 0.5,
        "zero_linewidth": 0.4,
        "panel_bbox_alpha": 0.35,
        "normalize_mode": "none",
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
    for k in ("xlim_W", "xlim_F", "ylim_W", "ylim_F"):
        cfg[k] = tuple(cfg[k])
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


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate spectral waterfall PNG frames and optional MP4 from YAML/CLI options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --config notebooks/config/process_budget.yaml --mp4\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --exp-ids 1 --range-keys ALLBB --station-ids 0 --kind N\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --normalize-mode bin --exp-ids 1 --range-keys ALLBB\n"
            "  python scripts/processing_chain/run_spectral_waterfall.py --workers 8 --mp4\n"
        ),
    )
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "notebooks" / "config" / "process_budget.yaml", help="Path to process_budget.yaml")
    parser.add_argument("--workers", type=int, default=None, help="Thread workers for frame rendering.")
    parser.add_argument("--exp-ids", type=str, default=None, help="Comma-separated experiment indices, e.g. '1,2'.")
    parser.add_argument("--range-keys", type=str, default=None, help="Comma-separated range keys, e.g. 'ALLBB,CRYBB'.")
    parser.add_argument("--station-ids", type=str, default=None, help="Comma-separated station indices, e.g. '0,1,2'.")
    parser.add_argument("--kind", type=str, default=None, choices=["N", "Q", "n", "q"], help="N (number) or Q (mass).")
    parser.add_argument("--normalize-mode", type=str, default=None, choices=["none", "bin", "panel"], help="Relative normalization mode.")
    parser.add_argument("--mp4", action="store_true", help="Build MP4 after frame generation.")
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
            frame_dir = frame_root / f"exp{eid}" / kind_dir
            pattern = str(frame_dir / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime*.png")
            frames = sorted(glob.glob(pattern), key=lambda x: int(Path(x).stem.split("_itime")[-1]))
            n_frames = len(frames)
            print(f"  {n_frames} frames in {frame_dir}  ({dt_batch:.1f}s, {dt_batch / max(n_frames, 1):.2f}s/frame)")

            if args.mp4:
                mp4_path = mp4_root / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_evolution_nframes{n_frames}.mp4"
                _build_mp4(ffmpeg_cmd, frames, mp4_path, mp4_fps)
                print(f"  MP4: {mp4_path}")
            else:
                print("  MP4 generation skipped (pass --mp4 to enable).")

    dt_total = time.perf_counter() - t_start
    print(f"\nDone. Total wall time: {dt_total:.1f}s ({dt_total / 60:.1f}min)")


if __name__ == "__main__":
    main()
