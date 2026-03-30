#!/usr/bin/env python3
"""One-shot growth statistics: spectral ridge-growth CSV, PSD registry, LV1 ridge metrics, merged summary.

Pulls together ``export_ridge_growth_csv`` / quicklook stats (``run_ridge_growth_quicklook``),
``collect_psd_stats`` (``run_psd_stats``), ``build_growth_summary_from_dataframes``
(``run_growth_summary``), and optional ``compute_ridge_metrics_dataframes`` (``run_ridge_metrics``).
``run_process_dominance`` is still a placeholder upstream — nothing to merge yet.

Use ``--all-experiments`` when the ridge-growth CSV contains multiple (exp_id, range_key, station)
blocks: each slice gets its own subfolder under ``--out-dir``, then ``ensemble/`` holds CSV + LaTeX
+ a small multi-experiment summary figure aggregating numeric scalars across slices.
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

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
GROWTH_DIR = Path(__file__).resolve().parent
REGISTRY_DIR = REPO_ROOT / "data" / "registry"
for _p in (SRC_DIR, GROWTH_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from utilities.spectral_waterfall import export_ridge_growth_csv  # noqa: E402
from utilities.style_profiles import apply_publication_style  # noqa: E402
from utilities.table_paths import (  # noqa: E402
    growth_bundle_output_dir,
    resolve_registry_input,
    spectral_growth_output_paths,
    sync_tree,
)

import run_growth_summary as gsm  # noqa: E402
import run_psd_stats as psd_mod  # noqa: E402
import run_ridge_growth_quicklook as rgq  # noqa: E402
import run_ridge_metrics as rrm  # noqa: E402

DEFAULT_GROWTH_CSV = spectral_growth_output_paths("cs-eriswil__20260304_110254", "Q", "stn0", repo_root=REPO_ROOT)["canonical"]
DEFAULT_CONFIG = REPO_ROOT / "config" / "psd_process_evolution.yaml"
RIDGE_METRICS_CSV = resolve_registry_input("ridge_metrics.csv", repo_root=REPO_ROOT)

_REQUIRED_GROWTH_COLS = {
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


def _read_growth_csv_raw(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = sorted(_REQUIRED_GROWTH_COLS - set(df.columns))
    if missing:
        raise ValueError(f"Missing required CSV columns: {missing}")
    df["t_lo"] = pd.to_datetime(df["t_lo"], errors="coerce")
    df["t_hi"] = pd.to_datetime(df["t_hi"], errors="coerce")
    return df


def _augment_growth_slice(df: pd.DataFrame) -> pd.DataFrame:
    """Per-(exp, range, station) window weights; do not reuse whole-file ``t_mid_min`` rows."""
    out = df.sort_values(["itime", "t_mid_sec_from_start"]).reset_index(drop=True)
    out["ice_ok"] = rgq._parse_ice_ok(out["ice_ok"])
    out["t_mid_min"] = pd.to_numeric(out["t_mid_sec_from_start"], errors="coerce") / 60.0
    out["dt_window_min"] = rgq._time_weights_minutes(out["t_mid_min"].to_numpy(dtype=float))
    out["z_offset_m"] = pd.to_numeric(out["z_ridge_m"], errors="coerce") - pd.to_numeric(
        out["z_anchor_m"], errors="coerce"
    )
    return out


def _slice_tag(exp_id: int, range_key: str, station: int) -> str:
    rk = str(range_key).replace("/", "-").replace(" ", "_")
    return f"exp{int(exp_id)}_{rk}_stn{int(station)}"


def _ensemble_agg_numeric(wide: pd.DataFrame, *, id_cols: tuple[str, ...]) -> pd.DataFrame:
    num = wide.drop(columns=[c for c in id_cols if c in wide.columns], errors="ignore").select_dtypes(
        include=[np.number]
    )
    if num.empty:
        return pd.DataFrame()
    g = num.agg(["count", "mean", "std", "min", "median", "max"])
    return g.T.reset_index().rename(columns={"index": "quantity"})


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


def _filter_cs_exp(df: pd.DataFrame, cs_run: str, exp_id: int) -> pd.DataFrame:
    if df.empty or "cs_run" not in df.columns:
        return pd.DataFrame()
    m = (df["cs_run"].astype(str) == str(cs_run)) & (df["exp_id"].astype(str) == str(exp_id))
    return df.loc[m].reset_index(drop=True)


def _save_latex_table(df: pd.DataFrame, path: Path, *, caption: str, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        path.write_text(
            f"% empty table: {caption}\n\\begin{{table}}[htbp]\\centering\\caption{{{caption}}}\\label{{{label}}}\\end{{table}}\n",
            encoding="utf-8",
        )
        return

    def _fmt_float(v: float) -> str:
        if isinstance(v, (float, np.floating)) and (np.isnan(v) or np.isinf(v)):
            return "---"
        return f"{float(v):.4g}"

    tabular = df.to_latex(index=False, na_rep="---", escape=True, float_format=_fmt_float)
    body = (
        "\\begin{table}[htbp]\n"
        "\\centering\n"
        "\\small\n"
        f"\\caption{{{caption}}}\n"
        f"\\label{{{label}}}\n"
        f"{tabular}\n"
        "\\end{table}\n"
    )
    path.write_text(body, encoding="utf-8")


def _render_dashboard(
    df_growth: pd.DataFrame,
    stats_df: pd.DataFrame,
    events_df: pd.DataFrame,
    *,
    title: str,
    ridge_row: pd.DataFrame,
    growth_row: pd.DataFrame,
) -> plt.Figure:
    """Compact time-series + text table (matplotlib); not publication-final."""
    fig = plt.figure(figsize=(10.5, 5.2), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.35, 1.0])
    ax_l = fig.add_subplot(gs[0, 0])
    ax_r = fig.add_subplot(gs[0, 1])
    ax_r.axis("off")

    t_min = pd.to_numeric(df_growth["t_mid_min"], errors="coerce").to_numpy(dtype=float)
    d_liq = pd.to_numeric(df_growth["D_liq_um"], errors="coerce").to_numpy(dtype=float)
    d_ice = pd.to_numeric(df_growth["D_ice_um"], errors="coerce").to_numpy(dtype=float)
    ax_l.plot(t_min, d_liq, color="#4C72B0", lw=1.1, label=r"$D_{\mathrm{liq}}$")
    ax_l.plot(t_min, d_ice, color="#DD8452", lw=1.1, label=r"$D_{\mathrm{ice}}$")
    ax_l.set_xlabel("time / (min)")
    ax_l.set_ylabel(r"$D$ / ($\mu$m)")
    ax_l.grid(True, alpha=0.25)
    ax_l.legend(loc="best", frameon=False)
    ax_l.set_title("Spectral ridge mean diameters")

    lines = [title, "", "— Spectral CSV (window-weighted) —"]
    for _, r in events_df.iterrows():
        lines.append(f"{r['event']}: {r['value']:.1f} {r['unit']}")
    ice_med = stats_df.loc[stats_df["metric"] == "D_ice_um", "median"]
    if len(ice_med):
        lines.append(f"D_ice median: {float(ice_med.iloc[0]):.2f} um")
    lines.append("")
    lines.append("— LV1 ridge metrics (run row) —")
    if ridge_row.empty:
        lines.append("(no row for this cs_run / exp_id)")
    else:
        rr = ridge_row.iloc[0]
        for col in ("alpha_peak_median", "ridge_peak_max_um", "ridge_mean_max_um", "time_end_min"):
            if col in rr.index and pd.notna(rr[col]):
                lines.append(f"{col}: {float(rr[col]):.4g}")
    lines.append("")
    lines.append("— Growth summary (merged PSD × LV1) —")
    if growth_row.empty:
        lines.append("(no merged row)")
    else:
        gr = growth_row.iloc[0]
        psd_cols = [c for c in sorted(gr.index) if str(c).startswith("psd_")]
        for col in psd_cols[:16]:
            v = gr[col]
            if pd.isna(v) or str(v).strip() == "":
                continue
            lines.append(f"{col}: {v}")
        if len(psd_cols) > 16:
            lines.append("…")

    ax_r.text(0.02, 0.98, "\n".join(lines[: min(35, len(lines))]), transform=ax_r.transAxes, va="top", ha="left", fontsize=7, family="monospace")
    fig.suptitle("Growth stats bundle — dashboard", fontsize=10, fontweight="bold")
    return fig


def _run_bundle_slice(
    *,
    dfa: pd.DataFrame,
    growth_csv: Path,
    cs_run: str,
    kind_tag: str,
    exp_id: int,
    range_key: str,
    station: int,
    out_dir: Path,
    psd_df: pd.DataFrame,
    ridge_full: pd.DataFrame,
    growth_full: pd.DataFrame,
    dpi: int,
    gcsv_rel: str,
    emit_shared_registry_csvs: bool = True,
) -> dict[str, object]:
    """Write csv/tex/png for one (exp_id, range_key, station) slice; return rows for ensemble tables."""
    out_dir = out_dir.resolve()
    csv_dir = out_dir / "csv"
    tex_dir = out_dir / "tex"
    png_dir = out_dir / "png"
    for d in (csv_dir, tex_dir, png_dir):
        d.mkdir(parents=True, exist_ok=True)

    stats_df = rgq._summary_rows(dfa)
    events_df = rgq._event_rows(dfa)
    stats_out = stats_df.copy()
    stats_out.insert(0, "section", "metric_summary")
    events_out = events_df.copy()
    events_out.insert(0, "section", "event_summary")
    quicklook_stats = pd.concat([stats_out, events_out], ignore_index=True, sort=False)
    quicklook_stats.insert(0, "cs_run", cs_run)
    quicklook_stats.insert(1, "spectral_kind", kind_tag)
    quicklook_stats.insert(2, "exp_id", exp_id)
    quicklook_stats.insert(3, "range_key", range_key)
    quicklook_stats.insert(4, "station", station)
    quicklook_stats.to_csv(csv_dir / "spectral_ridge_growth_stats.csv", index=False)

    manifest: list[dict[str, object]] = [
        {"component": "spectral_ridge_growth_csv", "path": gcsv_rel, "status": "ok"},
        {"component": "process_dominance", "path": "", "status": "skipped_placeholder"},
        {"slice": _slice_tag(exp_id, range_key, station)},
    ]

    if psd_df.empty:
        manifest.append({"component": "psd_stats", "path": "", "status": "empty"})
        psd_sub = pd.DataFrame()
    else:
        if emit_shared_registry_csvs:
            psd_df.to_csv(csv_dir / "psd_stats_registry_snapshot.csv", index=False)
            manifest.append({"component": "psd_stats", "path": str(csv_dir / "psd_stats_registry_snapshot.csv"), "status": "ok"})
        else:
            manifest.append({"component": "psd_stats", "path": "(see parent csv/)", "status": "ok"})
        psd_sub = _filter_cs_exp(psd_df, cs_run, exp_id)
    psd_sub.to_csv(csv_dir / "psd_stats_this_run.csv", index=False)

    if emit_shared_registry_csvs and not ridge_full.empty:
        ridge_full.to_csv(csv_dir / "ridge_metrics_lv1_all.csv", index=False)
    ridge_row = _filter_cs_exp(ridge_full, cs_run, exp_id) if not ridge_full.empty else pd.DataFrame()
    ridge_row.to_csv(csv_dir / "ridge_metrics_this_run.csv", index=False)

    growth_row = _filter_cs_exp(growth_full, cs_run, exp_id) if not growth_full.empty else pd.DataFrame()
    growth_row.to_csv(csv_dir / "growth_summary_this_run.csv", index=False)
    if emit_shared_registry_csvs and not growth_full.empty:
        growth_full.to_csv(csv_dir / "growth_summary_all.csv", index=False)
        manifest.append({"component": "growth_summary_merge", "path": str(csv_dir / "growth_summary_all.csv"), "status": "ok"})
    elif not growth_full.empty:
        manifest.append({"component": "growth_summary_merge", "path": "(see parent csv/)", "status": "ok"})
    else:
        manifest.append({"component": "growth_summary_merge", "path": "", "status": "skipped_missing_ridge_or_psd"})

    try:
        out_rel = str(out_dir.relative_to(REPO_ROOT))
    except ValueError:
        out_rel = str(out_dir)
    ctx = pd.DataFrame(
        [
            {
                "cs_run": cs_run,
                "exp_id": exp_id,
                "range_key": range_key,
                "station": station,
                "spectral_kind": kind_tag,
                "n_growth_windows": len(dfa),
                "bundle_out_dir": out_rel,
            }
        ]
    )
    ctx.to_csv(csv_dir / "bundle_context.csv", index=False)
    pd.DataFrame(manifest).to_csv(csv_dir / "bundle_manifest.csv", index=False)

    _save_latex_table(stats_df, tex_dir / "spectral_metric_summary.tex", caption="Spectral ridge-growth metrics", label="tab:growth_bundle_metrics")
    _save_latex_table(events_df, tex_dir / "spectral_event_summary.tex", caption="Spectral ridge-growth event fractions", label="tab:growth_bundle_events")
    _save_latex_table(psd_sub.head(24), tex_dir / "psd_windows_this_run.tex", caption="PSD window statistics (this run)", label="tab:growth_bundle_psd")
    _save_latex_table(ridge_row, tex_dir / "ridge_lv1_this_run.tex", caption="LV1 ridge metrics (this run)", label="tab:growth_bundle_lv1")
    _save_latex_table(growth_row, tex_dir / "growth_summary_this_run.tex", caption="Merged growth summary (this run)", label="tab:growth_bundle_summary")

    (tex_dir / "bundle_tables_input.tex").write_text(
        "% Auto-generated: \\input{} from the same directory.\n"
        "\\input{spectral_metric_summary}\n"
        "\\input{spectral_event_summary}\n"
        "\\input{psd_windows_this_run}\n"
        "\\input{ridge_lv1_this_run}\n"
        "\\input{growth_summary_this_run}\n",
        encoding="utf-8",
    )

    title_q = f"kind {kind_tag}, exp {exp_id}, stn {station}, {range_key}"
    fig_q = rgq._render_quicklook(dfa, title=f"Ridge-growth quicklook — {title_q}")
    fig_q.savefig(png_dir / "bundle_quicklook.png", dpi=dpi)
    plt.close(fig_q)

    fig_d = _render_dashboard(dfa, stats_df, events_df, title=title_q, ridge_row=ridge_row, growth_row=growth_row)
    fig_d.savefig(png_dir / "bundle_dashboard.png", dpi=dpi)
    plt.close(fig_d)

    spec_wide: dict[str, object] = {
        "cs_run": cs_run,
        "exp_id": exp_id,
        "range_key": range_key,
        "station": station,
        "spectral_kind": kind_tag,
        "n_growth_windows": len(dfa),
    }
    for _, r in stats_df.iterrows():
        m = str(r["metric"])
        for col in ("median", "mean", "max", "min", "std", "trend_per_min", "q25", "q75"):
            if col in r.index and pd.notna(r[col]):
                try:
                    spec_wide[f"spectral_{m}_{col}"] = float(r[col])
                except (TypeError, ValueError):
                    spec_wide[f"spectral_{m}_{col}"] = r[col]

    evt_wide: dict[str, object] = {
        "cs_run": cs_run,
        "exp_id": exp_id,
        "range_key": range_key,
        "station": station,
    }
    for _, r in events_df.iterrows():
        slug = (
            str(r["event"])
            .lower()
            .replace(" ", "_")
            .replace(">=", "ge")
            .replace("<=", "le")
            .replace(">", "gt")
            .replace("<", "lt")
        )
        evt_wide[f"evt_{slug}"] = float(r["value"]) if pd.notna(r["value"]) else np.nan

    return {
        "manifest": manifest,
        "spectral_wide": spec_wide,
        "events_wide": evt_wide,
        "ridge_row": ridge_row,
        "growth_row": growth_row,
    }


def _write_ensemble_bundle(
    ensemble_dir: Path,
    spectral_rows: list[dict[str, object]],
    events_rows: list[dict[str, object]],
    ridge_rows: list[pd.DataFrame],
    growth_rows: list[pd.DataFrame],
    dpi: int,
) -> None:
    """Cross-experiment CSV + LaTeX + a small comparison figure."""
    ensemble_dir = ensemble_dir.resolve()
    csv_e = ensemble_dir / "csv"
    tex_e = ensemble_dir / "tex"
    png_e = ensemble_dir / "png"
    for d in (csv_e, tex_e, png_e):
        d.mkdir(parents=True, exist_ok=True)

    spec_df = pd.DataFrame(spectral_rows)
    evt_df = pd.DataFrame(events_rows)
    spec_df.to_csv(csv_e / "ensemble_spectral_metrics_wide.csv", index=False)
    evt_df.to_csv(csv_e / "ensemble_event_fractions_wide.csv", index=False)

    id_cols = ("cs_run", "exp_id", "range_key", "station", "spectral_kind")
    spec_agg = _ensemble_agg_numeric(spec_df, id_cols=id_cols)
    spec_agg.to_csv(csv_e / "ensemble_spectral_metrics_aggregate.csv", index=False)
    evt_id = ("cs_run", "exp_id", "range_key", "station")
    evt_agg = _ensemble_agg_numeric(evt_df, id_cols=evt_id)
    evt_agg.to_csv(csv_e / "ensemble_event_fractions_aggregate.csv", index=False)

    ridge_parts = [r for r in ridge_rows if not r.empty]
    ridge_cat = pd.concat(ridge_parts, ignore_index=True) if ridge_parts else pd.DataFrame()
    if not ridge_cat.empty:
        ridge_cat.to_csv(csv_e / "ensemble_ridge_lv1_rows.csv", index=False)
        rnum = ridge_cat.drop(columns=["cs_run", "exp_id", "expname"], errors="ignore").select_dtypes(include=[np.number])
        if not rnum.empty:
            pd.DataFrame(
                {"quantity": rnum.columns, "mean": rnum.mean(), "std": rnum.std(ddof=0), "min": rnum.min(), "max": rnum.max()}
            ).to_csv(csv_e / "ensemble_ridge_lv1_numeric_agg.csv", index=False)

    growth_parts = [r for r in growth_rows if not r.empty]
    growth_cat = pd.concat(growth_parts, ignore_index=True) if growth_parts else pd.DataFrame()
    if not growth_cat.empty:
        growth_cat.to_csv(csv_e / "ensemble_growth_summary_rows.csv", index=False)
        gnum = growth_cat.select_dtypes(include=[np.number])
        if not gnum.empty:
            pd.DataFrame(
                {"quantity": gnum.columns, "mean": gnum.mean(), "std": gnum.std(ddof=0), "min": gnum.min(), "max": gnum.max()}
            ).to_csv(csv_e / "ensemble_growth_summary_numeric_agg.csv", index=False)

    _save_latex_table(spec_df, tex_e / "ensemble_spectral_wide.tex", caption="Spectral metrics by experiment slice", label="tab:ensemble_spectral_wide")
    _save_latex_table(spec_agg.head(40), tex_e / "ensemble_spectral_aggregate.tex", caption="Spectral metrics — across experiments", label="tab:ensemble_spectral_agg")
    _save_latex_table(evt_df, tex_e / "ensemble_events_wide.tex", caption="Event fractions by experiment slice", label="tab:ensemble_events_wide")
    (tex_e / "bundle_ensemble_input.tex").write_text(
        "% Ensemble-level tables (\\input from this directory).\n"
        "\\input{ensemble_spectral_wide}\n"
        "\\input{ensemble_spectral_aggregate}\n"
        "\\input{ensemble_events_wide}\n",
        encoding="utf-8",
    )

    if len(spec_df) == 0:
        fig, ax = plt.subplots(figsize=(5.0, 2.5), constrained_layout=True)
        ax.text(0.5, 0.5, "No spectral rows for ensemble figure.", ha="center", va="center")
        ax.axis("off")
        fig.savefig(png_e / "ensemble_spectral_bars.png", dpi=dpi)
        plt.close(fig)
    else:
        label_col = spec_df.apply(
            lambda r: f"e{r['exp_id']}\n{r.get('range_key', '')}\nst{r.get('station', '')}",
            axis=1,
        )
        fig, axes = plt.subplots(1, 2, figsize=(9.0, 4.0), constrained_layout=True)
        ice_med = pd.to_numeric(spec_df.get("spectral_D_ice_um_median"), errors="coerce")
        liq_med = pd.to_numeric(spec_df.get("spectral_D_liq_um_median"), errors="coerce")
        x = np.arange(len(spec_df))
        axes[0].bar(x - 0.18, liq_med, width=0.35, label=r"$D_{\mathrm{liq}}$ med.", color="#4C72B0")
        axes[0].bar(x + 0.18, ice_med, width=0.35, label=r"$D_{\mathrm{ice}}$ med.", color="#DD8452")
        axes[0].set_xticks(x)
        axes[0].set_xticklabels(label_col.tolist(), fontsize=7)
        axes[0].set_ylabel(r"median $D$ / ($\mu$m)")
        axes[0].legend(loc="best", frameon=False, fontsize=7)
        axes[0].set_title("Spectral ridge — median diameters")
        axes[0].grid(True, axis="y", alpha=0.25)

        ice_ddt = pd.to_numeric(spec_df.get("spectral_dD_ice_dt_um_s_median"), errors="coerce")
        axes[1].bar(x, ice_ddt, color="#595959")
        axes[1].set_xticks(x)
        axes[1].set_xticklabels(label_col.tolist(), fontsize=7)
        axes[1].set_ylabel(r"median $\mathrm{d}D_{\mathrm{ice}}/\mathrm{d}t$")
        axes[1].set_title("Spectral ridge — median ice growth rate")
        axes[1].axhline(0.0, color="0.5", lw=0.8, ls="--")
        axes[1].grid(True, axis="y", alpha=0.25)

        fig.suptitle("Ensemble summary across experiment slices", fontsize=10, fontweight="bold")
        fig.savefig(png_e / "ensemble_spectral_bars.png", dpi=dpi)
        plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(description="Unified growth / PSD / LV1 stats, CSV + LaTeX + quick figures.")
    p.add_argument("--ridge-growth-csv", type=Path, default=DEFAULT_GROWTH_CSV, help="Spectral ridge_growth_*.csv path.")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="YAML for optional CSV regeneration.")
    p.add_argument("--regenerate-growth-csv", action="store_true", help="Call export_ridge_growth_csv before stats.")
    p.add_argument("--all-experiments", action="store_true", help="One subfolder per (exp_id, range_key, station); write ensemble/ summary.")
    p.add_argument("--kind", type=str, default=None, choices=["N", "Q", "n", "q"])
    p.add_argument("--exp-ids", type=str, default=None)
    p.add_argument("--range-keys", type=str, default=None)
    p.add_argument("--station-ids", type=str, default=None)
    p.add_argument("--out-dir", type=Path, default=None, help="Bundle output root (default: output/growth_bundle/<cs_run>).")
    p.add_argument("--stats-dir", type=Path, default=psd_mod.STATS_DIR, help="PSD figure13 stats CSV directory.")
    p.add_argument("--legacy-stats-dir", type=Path, default=psd_mod.LEGACY_STATS_DIR)
    p.add_argument("--legacy-table-dir", type=Path, default=psd_mod.LEGACY_TABLE_DIR)
    p.add_argument("--rebuild-ridge-metrics", action="store_true", help="Recompute LV1 ridge metrics via registry.")
    p.add_argument("--registry", type=Path, default=rrm.REGISTRY_CSV)
    p.add_argument("--processed-root", type=Path, default=rrm.PROCESSED_ROOT)
    p.add_argument("--update-ridge-registry", action="store_true", help="If rebuilding, also write data/registry ridge CSVs.")
    p.add_argument("--dpi", type=int, default=200)
    args = p.parse_args()

    growth_csv = args.ridge_growth_csv.resolve()
    inferred = rgq._parse_ridge_growth_csv_name(growth_csv)
    if args.regenerate_growth_csv:
        if inferred is None and args.kind is None:
            raise SystemExit("Regenerate needs ridge_growth_<N|Q>_stn<ID>.csv or --kind and --station-ids.")
        kind_u = (args.kind.strip().upper() if args.kind else None) or (inferred[0] if inferred else None)
        stn_ids = _parse_csv_ints(args.station_ids) or (inferred[1] if inferred else None)
        if kind_u is None or stn_ids is None:
            raise SystemExit("Regenerate: set --kind and --station-ids for non-standard paths.")
        export_ridge_growth_csv(
            repo_root=REPO_ROOT,
            config_path=args.config,
            kind=kind_u,
            output_csv=growth_csv,
            exp_ids=_parse_csv_ints(args.exp_ids),
            range_keys=_parse_csv_strs(args.range_keys),
            station_ids=stn_ids,
        )

    apply_publication_style()
    raw = _read_growth_csv_raw(growth_csv)
    groups = list(raw.groupby(["exp_id", "range_key", "station"], sort=True))
    n_groups = len(groups)
    if n_groups > 1 and not args.all_experiments:
        raise SystemExit(
            f"CSV has {n_groups} (exp_id, range_key, station) slices. Re-run with --all-experiments "
            "to write per-slice bundles plus ensemble/, or export a single-slice CSV."
        )

    cs_run = growth_csv.parent.name
    kind_tag = growth_csv.stem.split("_")[2] if len(growth_csv.stem.split("_")) >= 3 else "?"
    try:
        gcsv_rel = str(growth_csv.relative_to(REPO_ROOT))
    except ValueError:
        gcsv_rel = str(growth_csv)

    base_out = args.out_dir
    if base_out is None:
        base_out = growth_bundle_output_dir(cs_run, repo_root=REPO_ROOT)["canonical"]
    base_out = base_out.resolve()

    psd_df = psd_mod.collect_psd_stats(
        stats_dir=args.stats_dir,
        legacy_stats_dir=args.legacy_stats_dir,
        legacy_table_dir=args.legacy_table_dir,
        registry_csv=psd_mod.REGISTRY_CSV,
    )
    ridge_full = pd.read_csv(RIDGE_METRICS_CSV, dtype=str) if RIDGE_METRICS_CSV.is_file() else pd.DataFrame()
    rts_lv1 = pd.DataFrame()
    rcells_lv1 = pd.DataFrame()
    if args.rebuild_ridge_metrics:
        rsum, rts_lv1, rcells_lv1 = rrm.compute_ridge_metrics_dataframes(args.registry, args.processed_root)
        if not rsum.empty:
            ridge_full = rsum
            if args.update_ridge_registry:
                RIDGE_METRICS_CSV.parent.mkdir(parents=True, exist_ok=True)
                rsum.to_csv(RIDGE_METRICS_CSV, index=False)
                rrm.OUT_TS_CSV.parent.mkdir(parents=True, exist_ok=True)
                rts_lv1.to_csv(rrm.OUT_TS_CSV, index=False)
                rcells_lv1.to_csv(RIDGE_METRICS_CSV.with_name("ridge_cell_metrics.csv"), index=False)

    growth_full = (
        gsm.build_growth_summary_from_dataframes(ridge_full, psd_df)
        if (not ridge_full.empty and not psd_df.empty)
        else pd.DataFrame()
    )

    multi = n_groups > 1 or args.all_experiments
    parent_csv = base_out / "csv"
    parent_csv.mkdir(parents=True, exist_ok=True)
    if multi and not psd_df.empty:
        psd_df.to_csv(parent_csv / "psd_stats_registry_snapshot.csv", index=False)
    if multi and not ridge_full.empty:
        ridge_full.to_csv(parent_csv / "ridge_metrics_lv1_all.csv", index=False)
    if multi and not growth_full.empty:
        growth_full.to_csv(parent_csv / "growth_summary_all.csv", index=False)
    if args.rebuild_ridge_metrics and not rts_lv1.empty:
        rts_lv1.to_csv(parent_csv / "ridge_timeseries_lv1_all.csv", index=False)
        rcells_lv1.to_csv(parent_csv / "ridge_cell_metrics_lv1_all.csv", index=False)

    spectral_rows: list[dict[str, object]] = []
    events_rows: list[dict[str, object]] = []
    ridge_row_list: list[pd.DataFrame] = []
    growth_row_list: list[pd.DataFrame] = []

    for (exp_id, range_key, station), sub_raw in groups:
        dfa = _augment_growth_slice(sub_raw)
        eid = int(exp_id)
        rk = str(range_key)
        stn = int(station)
        slice_dir = base_out / _slice_tag(eid, rk, stn) if multi else base_out
        res = _run_bundle_slice(
            dfa=dfa,
            growth_csv=growth_csv,
            cs_run=cs_run,
            kind_tag=kind_tag,
            exp_id=eid,
            range_key=rk,
            station=stn,
            out_dir=slice_dir,
            psd_df=psd_df,
            ridge_full=ridge_full,
            growth_full=growth_full,
            dpi=args.dpi,
            gcsv_rel=gcsv_rel,
            emit_shared_registry_csvs=not multi,
        )
        spectral_rows.append(res["spectral_wide"])
        events_rows.append(res["events_wide"])
        ridge_row_list.append(res["ridge_row"])
        growth_row_list.append(res["growth_row"])

    if multi:
        ens = base_out / "ensemble"
        _write_ensemble_bundle(ens, spectral_rows, events_rows, ridge_row_list, growth_row_list, args.dpi)
        print(f"Per-slice bundles under -> {base_out}")
        print(f"Ensemble summary -> {ens}")
    else:
        print(f"Bundle output -> {base_out}")
        print(f"  csv: {base_out / 'csv'}")
        print(f"  tex: {base_out / 'tex'}")
        print(f"  png: {base_out / 'png'}")

    default_bundle_dir = growth_bundle_output_dir(cs_run, repo_root=REPO_ROOT)["canonical"].resolve()
    if base_out == default_bundle_dir:
        sync_tree(base_out, growth_bundle_output_dir(cs_run, repo_root=REPO_ROOT)["legacy"])


if __name__ == "__main__":
    main()
