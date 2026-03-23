#!/usr/bin/env python3
"""Render a ridge-growth quicklook and summary stats from one exported CSV.

The figure is intentionally compact but broad:
- time series for ridge altitude, mean diameters, and growth rates
- histograms for the diameter distributions
- duration bars for liquid/ice diameter regimes
- a pcolormesh of robustly standardized metrics over time

This is aimed at quick scientific inspection of the ridge-growth CSVs written by
the spectral-waterfall workflow in ``output/gfx/csv/05/...``. Use ``--regenerate-csv`` to
rebuild that CSV from the same YAML/Zarr path as ``run_spectral_waterfall`` (no PNG frames).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import TwoSlopeNorm
from matplotlib.ticker import FuncFormatter

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.spectral_waterfall import export_ridge_growth_csv  # noqa: E402
from utilities.style_profiles import (  # noqa: E402
    FULL_COL_IN,
    MAX_H_IN,
    apply_publication_style,
    format_elapsed_minutes_tick,
)

DEFAULT_INPUT = (
    REPO_ROOT
    / "output"
    / "gfx"
    / "csv"
    / "05"
    / "cs-eriswil__20260304_110254"
    / "ridge_growth_Q_stn0.csv"
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "psd_process_evolution.yaml"


def _parse_csv_ints_cli(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return [int(v) for v in vals] if vals else None


def _parse_csv_strs_cli(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return vals if vals else None


def _parse_ridge_growth_csv_name(path: Path) -> tuple[str, list[int]] | None:
    """Infer spectral kind (N|Q) and station ids from ``ridge_growth_<KIND>_stn….csv`` stem."""
    parts = path.stem.split("_")
    if len(parts) != 4 or parts[0] != "ridge" or parts[1] != "growth":
        return None
    k = parts[2].upper()
    if k not in ("N", "Q"):
        return None
    tag = parts[3]
    if not tag.startswith("stn") or len(tag) <= 3:
        return None
    rest = tag[3:]
    try:
        stns = [int(x) for x in rest.split("-") if x.strip() != ""] if "-" in rest else [int(rest)]
    except ValueError:
        return None
    return (k, stns) if stns else None


METRIC_META: dict[str, tuple[str, str]] = {
    "z_ridge_m": ("ridge altitude", "m"),
    "z_offset_m": ("ridge - anchor altitude", "m"),
    "D_liq_um": ("liquid mean diameter", "um"),
    "D_ice_um": ("ice mean diameter", "um"),
    "dD_liq_dt_um_s": ("liquid growth rate", "um s-1"),
    "dD_ice_dt_um_s": ("ice growth rate", "um s-1"),
}

HEATMAP_METRICS: list[tuple[str, str]] = [
    ("z_offset_m", "z_ridge - z_anchor"),
    ("D_liq_um", "D_liq"),
    ("D_ice_um", "D_ice"),
    ("dD_liq_dt_um_s", "dD_liq/dt"),
    ("dD_ice_dt_um_s", "dD_ice/dt"),
]

DIAMETER_REGIME_EDGES_UM = np.array([0.0, 5.0, 25.0, 50.0, 75.0, 100.0, np.inf], dtype=float)
DIAMETER_REGIME_LABELS = ["<5", "5-20", "20-50", "50-100", ">=100"]


def _default_figure_path(input_csv: Path) -> Path:
    return REPO_ROOT / "output" / "gfx" / "png" / "05" / input_csv.parent.name / f"{input_csv.stem}_quicklook.png"


def _default_stats_path(input_csv: Path) -> Path:
    return input_csv.with_name(f"{input_csv.stem}_quicklook_stats.csv")


def _parse_ice_ok(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.copy()
    return series.astype(str).str.strip().str.upper().map({"TRUE": True, "FALSE": False}).fillna(False)


def _time_weights_minutes(t_mid_min: np.ndarray) -> np.ndarray:
    vals = np.asarray(t_mid_min, dtype=float)
    if vals.size == 0:
        return vals
    if vals.size == 1:
        return np.array([1.0], dtype=float)
    edges = np.empty(vals.size + 1, dtype=float)
    edges[1:-1] = 0.5 * (vals[:-1] + vals[1:])
    edges[0] = max(0.0, vals[0] - 0.5 * (vals[1] - vals[0]))
    edges[-1] = vals[-1] + 0.5 * (vals[-1] - vals[-2])
    return np.diff(edges)


def _robust_zscore(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    out = np.full(arr.shape, np.nan, dtype=float)
    good = np.isfinite(arr)
    if not np.any(good):
        return out
    core = arr[good]
    med = float(np.nanmedian(core))
    mad = float(np.nanmedian(np.abs(core - med)))
    scale = 1.4826 * mad if mad > 0 else float(np.nanstd(core))
    if not np.isfinite(scale) or scale <= 0:
        out[good] = 0.0
        return out
    out[good] = (core - med) / scale
    return np.clip(out, -3.0, 3.0)


def _weighted_fraction(mask: np.ndarray, weights: np.ndarray) -> float:
    good = np.isfinite(weights) & (weights > 0)
    if not np.any(good):
        return np.nan
    return float(100.0 * np.sum(weights[good] * mask[good]) / np.sum(weights[good]))


def _duration_by_regime(values: np.ndarray, weights_min: np.ndarray) -> np.ndarray:
    vals = np.asarray(values, dtype=float)
    weights = np.asarray(weights_min, dtype=float)
    out = np.zeros(len(DIAMETER_REGIME_LABELS), dtype=float)
    good = np.isfinite(vals) & np.isfinite(weights) & (weights > 0)
    if not np.any(good):
        return out
    idx = np.digitize(vals[good], DIAMETER_REGIME_EDGES_UM[1:-1], right=False)
    for ii in range(len(out)):
        out[ii] = float(np.sum(weights[good][idx == ii]))
    return out


def _linear_trend_per_min(x_min: np.ndarray, y: np.ndarray) -> float:
    good = np.isfinite(x_min) & np.isfinite(y)
    if int(np.sum(good)) < 2:
        return np.nan
    slope, _intercept = np.polyfit(x_min[good], y[good], 1)
    return float(slope)


def _summary_rows(df: pd.DataFrame) -> pd.DataFrame:
    t_min = df["t_mid_min"].to_numpy(dtype=float)
    rows: list[dict[str, object]] = []
    for key, (label, unit) in METRIC_META.items():
        vals = pd.to_numeric(df[key], errors="coerce").to_numpy(dtype=float)
        good = np.isfinite(vals)
        if not np.any(good):
            rows.append(
                {
                    "metric": key,
                    "label": label,
                    "unit": unit,
                    "count": 0,
                    "nan_fraction": 1.0,
                    "min": np.nan,
                    "q25": np.nan,
                    "median": np.nan,
                    "mean": np.nan,
                    "q75": np.nan,
                    "max": np.nan,
                    "std": np.nan,
                    "trend_per_min": np.nan,
                }
            )
            continue
        core = vals[good]
        rows.append(
            {
                "metric": key,
                "label": label,
                "unit": unit,
                "count": int(core.size),
                "nan_fraction": float(1.0 - core.size / len(vals)),
                "min": float(np.nanmin(core)),
                "q25": float(np.nanquantile(core, 0.25)),
                "median": float(np.nanmedian(core)),
                "mean": float(np.nanmean(core)),
                "q75": float(np.nanquantile(core, 0.75)),
                "max": float(np.nanmax(core)),
                "std": float(np.nanstd(core)),
                "trend_per_min": _linear_trend_per_min(t_min, vals),
            }
        )
    return pd.DataFrame(rows)


def _event_rows(df: pd.DataFrame) -> pd.DataFrame:
    dt_min = df["dt_window_min"].to_numpy(dtype=float)
    ice_growth = pd.to_numeric(df["dD_ice_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    liq_growth = pd.to_numeric(df["dD_liq_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    ice_d = pd.to_numeric(df["D_ice_um"], errors="coerce").to_numpy(dtype=float)
    liq_d = pd.to_numeric(df["D_liq_um"], errors="coerce").to_numpy(dtype=float)
    rows = [
        {"event": "liquid growth > 0", "unit": "%", "value": _weighted_fraction(liq_growth > 0, dt_min)},
        {"event": "ice growth > 0", "unit": "%", "value": _weighted_fraction(ice_growth > 0, dt_min)},
        {"event": "ice growth < 0", "unit": "%", "value": _weighted_fraction(ice_growth < 0, dt_min)},
        {"event": "D_ice >= 50 um", "unit": "%", "value": _weighted_fraction(ice_d >= 50.0, dt_min)},
        {"event": "D_ice >= 100 um", "unit": "%", "value": _weighted_fraction(ice_d >= 100.0, dt_min)},
        {"event": "D_liq >= 10 um", "unit": "%", "value": _weighted_fraction(liq_d >= 10.0, dt_min)},
    ]
    return pd.DataFrame(rows)


def _load_growth_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {
        "exp_id",
        "range_key",
        "itime",
        "t_lo",
        "t_hi",
        "t_mid_sec_from_start",
        "station",
        "z_ridge_m",
        "z_anchor_m",
        "D_liq_um",
        "D_ice_um",
        "dD_liq_dt_um_s",
        "dD_ice_dt_um_s",
        "ice_ok",
        "n_regress_pts",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required CSV columns: {missing}")
    df["t_lo"] = pd.to_datetime(df["t_lo"], errors="coerce")
    df["t_hi"] = pd.to_datetime(df["t_hi"], errors="coerce")
    df = df.sort_values(["itime", "t_mid_sec_from_start"]).reset_index(drop=True)
    df["ice_ok"] = _parse_ice_ok(df["ice_ok"])
    df["t_mid_min"] = pd.to_numeric(df["t_mid_sec_from_start"], errors="coerce") / 60.0
    df["dt_window_min"] = _time_weights_minutes(df["t_mid_min"].to_numpy(dtype=float))
    df["z_offset_m"] = pd.to_numeric(df["z_ridge_m"], errors="coerce") - pd.to_numeric(df["z_anchor_m"], errors="coerce")
    return df


def _format_time_axis(ax: plt.Axes, span_min: float) -> None:
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _pos: format_elapsed_minutes_tick(x, span_min, zero_if_close=True)))
    ax.set_xlim(left=0.0)


def _add_panel_tag(ax: plt.Axes, tag: str) -> None:
    ax.text(
        0.01,
        0.99,
        tag,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8,
        fontweight="bold",
        color="0.2",
    )


def _render_quicklook(df: pd.DataFrame, *, title: str) -> plt.Figure:
    span_min = float(np.nanmax(df["t_mid_min"])) if len(df) else 1.0
    fig = plt.figure(figsize=(FULL_COL_IN, min(MAX_H_IN, FULL_COL_IN * 0.92)), constrained_layout=True)
    gs = fig.add_gridspec(3, 2, width_ratios=[1.15, 1.0], height_ratios=[1.0, 1.0, 1.0])

    ax_d = fig.add_subplot(gs[0, 0])
    ax_g = fig.add_subplot(gs[1, 0], sharex=ax_d)
    ax_z = fig.add_subplot(gs[2, 0], sharex=ax_d)
    ax_heat = fig.add_subplot(gs[0, 1])
    ax_hist = fig.add_subplot(gs[1, 1])
    ax_bar = fig.add_subplot(gs[2, 1])

    t_min = df["t_mid_min"].to_numpy(dtype=float)
    d_liq = pd.to_numeric(df["D_liq_um"], errors="coerce").to_numpy(dtype=float)
    d_ice = pd.to_numeric(df["D_ice_um"], errors="coerce").to_numpy(dtype=float)
    g_liq = pd.to_numeric(df["dD_liq_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    g_ice = pd.to_numeric(df["dD_ice_dt_um_s"], errors="coerce").to_numpy(dtype=float)
    z_ridge = pd.to_numeric(df["z_ridge_m"], errors="coerce").to_numpy(dtype=float)
    z_anchor = pd.to_numeric(df["z_anchor_m"], errors="coerce").to_numpy(dtype=float)

    c_liq = "#4C72B0"
    c_ice = "#DD8452"
    c_anchor = "0.35"

    ax_d.plot(t_min, d_liq, color=c_liq, lw=1.2, label="liquid")
    ax_d.plot(t_min, d_ice, color=c_ice, lw=1.2, label="ice")
    ax_d.set_ylabel(r"$D$ / ($\mu$m)")
    ax_d.legend(loc="upper left", frameon=False, ncol=2)
    ax_d.grid(True, alpha=0.22)
    _add_panel_tag(ax_d, "a")

    ax_g.axhline(0.0, color="0.55", lw=0.7, ls="--")
    ax_g.plot(t_min, g_liq, color=c_liq, lw=1.1, label="liquid")
    ax_g.plot(t_min, g_ice, color=c_ice, lw=1.1, label="ice")
    ax_g.set_ylabel(r"$\mathrm{d}D/\mathrm{d}t$ / ($\mu$m s$^{-1}$)")
    ax_g.grid(True, alpha=0.22)
    _add_panel_tag(ax_g, "b")

    ax_z.plot(t_min, z_ridge, color="k", lw=1.1, label="ridge")
    ax_z.plot(t_min, z_anchor, color=c_anchor, lw=0.95, ls=":", label="anchor")
    ax_z.set_ylabel("z / (m)")
    ax_z.set_xlabel("time from start / (min)")
    ax_z.grid(True, alpha=0.22)
    ax_z.legend(loc="upper right", frameon=False)
    _format_time_axis(ax_z, span_min)
    _add_panel_tag(ax_z, "c")

    for ax in (ax_d, ax_g):
        _format_time_axis(ax, span_min)
        ax.tick_params(labelbottom=False)

    heat = np.vstack([_robust_zscore(pd.to_numeric(df[key], errors="coerce").to_numpy(dtype=float)) for key, _ in HEATMAP_METRICS])
    t_edges = np.empty(len(t_min) + 1, dtype=float)
    dt = _time_weights_minutes(t_min)
    t_edges[0] = max(0.0, t_min[0] - 0.5 * dt[0])
    t_edges[1:] = t_edges[0] + np.cumsum(dt)
    y_edges = np.arange(len(HEATMAP_METRICS) + 1, dtype=float)
    pcm = ax_heat.pcolormesh(
        t_edges,
        y_edges,
        heat,
        shading="flat",
        cmap="RdBu_r",
        norm=TwoSlopeNorm(vmin=-3.0, vcenter=0.0, vmax=3.0),
    )
    ax_heat.set_yticks(np.arange(len(HEATMAP_METRICS)) + 0.5)
    ax_heat.set_yticklabels([label for _key, label in HEATMAP_METRICS])
    ax_heat.set_xlabel("time from start / (min)")
    ax_heat.set_ylabel("metric")
    _format_time_axis(ax_heat, span_min)
    cbar = fig.colorbar(pcm, ax=ax_heat, pad=0.01)
    cbar.set_label("robust anomaly / (-)")
    _add_panel_tag(ax_heat, "d")

    bins = np.linspace(
        float(np.nanmin(np.concatenate([d_liq[np.isfinite(d_liq)], d_ice[np.isfinite(d_ice)]]))),
        float(np.nanmax(np.concatenate([d_liq[np.isfinite(d_liq)], d_ice[np.isfinite(d_ice)]]))),
        18,
    )
    ax_hist.hist(d_liq[np.isfinite(d_liq)], bins=bins, color=c_liq, alpha=0.45, label="liquid")
    ax_hist.hist(d_ice[np.isfinite(d_ice)], bins=bins, color=c_ice, alpha=0.45, label="ice")
    ax_hist.set_xlabel(r"$D$ / ($\mu$m)")
    ax_hist.set_ylabel("count / (-)")
    ax_hist.grid(True, alpha=0.22)
    ax_hist.legend(loc="upper right", frameon=False)
    _add_panel_tag(ax_hist, "e")

    durations_liq = _duration_by_regime(d_liq, df["dt_window_min"].to_numpy(dtype=float))
    durations_ice = _duration_by_regime(d_ice, df["dt_window_min"].to_numpy(dtype=float))
    yy = np.arange(len(DIAMETER_REGIME_LABELS), dtype=float)
    ax_bar.barh(yy + 0.18, durations_liq, height=0.34, color=c_liq, alpha=0.8, label="liquid")
    ax_bar.barh(yy - 0.18, durations_ice, height=0.34, color=c_ice, alpha=0.8, label="ice")
    ax_bar.set_yticks(yy)
    ax_bar.set_yticklabels(DIAMETER_REGIME_LABELS)
    ax_bar.set_xlabel("time spent / (min)")
    ax_bar.set_ylabel(r"$D$ regime / ($\mu$m)")
    ax_bar.grid(True, axis="x", alpha=0.22)
    ax_bar.legend(loc="lower right", frameon=False)
    _add_panel_tag(ax_bar, "f")

    stats = _summary_rows(df)
    events = _event_rows(df)
    txt = "\n".join(
        [
            f"n windows: {len(df)}",
            f"duration: {np.nansum(df['dt_window_min']):.1f} min",
            f"median D_ice: {stats.loc[stats.metric == 'D_ice_um', 'median'].iloc[0]:.1f} um",
            f"max D_ice: {stats.loc[stats.metric == 'D_ice_um', 'max'].iloc[0]:.1f} um",
            f"ice growth > 0: {events.loc[events.event == 'ice growth > 0', 'value'].iloc[0]:.1f} %",
        ]
    )
    ax_heat.text(
        0.98,
        0.02,
        txt,
        transform=ax_heat.transAxes,
        ha="right",
        va="bottom",
        fontsize=6,
        color="0.15",
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "0.8", "boxstyle": "round,pad=0.18"},
    )

    fig.suptitle(title, x=0.52, y=1.01, fontsize=9, fontweight="bold")
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a ridge-growth statistical quicklook from one CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input ridge-growth CSV.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="YAML used with export_ridge_growth_csv when --regenerate-csv is set.",
    )
    parser.add_argument(
        "--regenerate-csv",
        action="store_true",
        help="Recompute ridge_growth_*.csv via spectral-waterfall logic, write to --input, then quicklook.",
    )
    parser.add_argument("--kind", type=str, default=None, choices=["N", "Q", "n", "q"], help="Mass (Q) or number (N); default from CSV filename.")
    parser.add_argument("--exp-ids", type=str, default=None, help="Override selection.plot_experiment_ids, e.g. 1,2.")
    parser.add_argument("--range-keys", type=str, default=None, help="Override plotting.plot_range_keys, e.g. ALLBB,CRYBB.")
    parser.add_argument("--station-ids", type=str, default=None, help="Override plot stations, e.g. 0; default from CSV filename.")
    parser.add_argument("--output", type=Path, default=None, help="Output PNG path.")
    parser.add_argument("--stats-output", type=Path, default=None, help="Output summary CSV path.")
    parser.add_argument("--dpi", type=int, default=400, help="PNG output DPI.")
    args = parser.parse_args()

    if args.regenerate_csv:
        inferred = _parse_ridge_growth_csv_name(args.input)
        if inferred is None and args.kind is None:
            raise SystemExit(
                "Cannot infer kind/stations from --input name; use ridge_growth_<N|Q>_stn<ID>.csv "
                "or pass --kind and --station-ids."
            )
        kind_u = (args.kind.strip().upper() if args.kind else None) or (inferred[0] if inferred else None)
        stn_ids = _parse_csv_ints_cli(args.station_ids) or (inferred[1] if inferred else None)
        if kind_u is None or stn_ids is None:
            raise SystemExit("For --regenerate-csv, set --kind and --station-ids if the input path is non-standard.")
        export_ridge_growth_csv(
            repo_root=REPO_ROOT,
            config_path=args.config,
            kind=kind_u,
            output_csv=args.input,
            exp_ids=_parse_csv_ints_cli(args.exp_ids),
            range_keys=_parse_csv_strs_cli(args.range_keys),
            station_ids=stn_ids,
        )

    apply_publication_style()
    df = _load_growth_csv(args.input)
    stats_df = _summary_rows(df)
    events_df = _event_rows(df)
    out_png = args.output or _default_figure_path(args.input)
    out_csv = args.stats_output or _default_stats_path(args.input)

    station = int(df["station"].iloc[0])
    exp_id = int(df["exp_id"].iloc[0])
    range_key = str(df["range_key"].iloc[0])
    kind_tag = args.input.stem.split("_")[2] if len(args.input.stem.split("_")) >= 3 else "?"
    title = (
        f"Ridge-growth quicklook — kind {kind_tag}, exp {exp_id}, "
        f"station {station}, range {range_key}"
    )

    fig = _render_quicklook(df, title=title)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=args.dpi)
    plt.close(fig)

    stats_out = stats_df.copy()
    stats_out.insert(0, "section", "metric_summary")
    events_out = events_df.copy()
    events_out.insert(0, "section", "event_summary")
    merged = pd.concat([stats_out, events_out], ignore_index=True, sort=False)
    merged.to_csv(out_csv, index=False)

    print(f"saved figure -> {out_png.resolve().as_uri()}")
    print(f"saved stats  -> {out_csv.resolve().as_uri()}")
    print(stats_df.to_string(index=False))
    print(events_df.to_string(index=False))


if __name__ == "__main__":
    main()
