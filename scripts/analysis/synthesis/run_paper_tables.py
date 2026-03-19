#!/usr/bin/env python3
"""Compile manuscript-facing CSV and LaTeX tables from promoted analysis outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.paper_tables import (  # noqa: E402
    add_provenance,
    copy_if_changed,
    ensure_parent,
    load_manifest,
    render_table_environment,
    resolve_repo_path,
)

DEFAULT_MANIFEST = REPO_ROOT / "scripts" / "analysis" / "synthesis" / "paper_tables.yaml"


def _read_csv(path_like: str | Path, **kwargs) -> pd.DataFrame:
    path = resolve_repo_path(path_like)
    return pd.read_csv(path, **kwargs)


def _paper_subset(only_flares: bool) -> pd.DataFrame:
    subset = _read_csv("data/registry/paper_core_subset.csv", dtype=str)
    subset["exp_id"] = subset["exp_id"].astype(str)
    if only_flares:
        subset = subset[subset["is_reference"].astype(str).str.upper() == "FALSE"].copy()
    return subset


def _semi_join_paper_subset(df: pd.DataFrame, *, only_flares: bool = True) -> pd.DataFrame:
    subset = _paper_subset(only_flares=only_flares)[["cs_run", "exp_id", "expname", "ref_expname", "pair_method"]].drop_duplicates()
    out = df.copy()
    out["exp_id"] = out["exp_id"].astype(str)
    return out.merge(subset, on=["cs_run", "exp_id", "expname"], how="inner", suffixes=("", "_subset"))


def _true_mask(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().isin({"TRUE", "YES"})


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _safe_stat(series: pd.Series, op: str) -> float:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return np.nan
    if op == "min":
        return float(vals.min())
    if op == "max":
        return float(vals.max())
    if op == "median":
        return float(vals.median())
    raise ValueError(f"Unsupported stat op: {op}")


def build_experiment_matrix() -> pd.DataFrame:
    df = _paper_subset(only_flares=False).copy()
    df["exp_id"] = pd.to_numeric(df["exp_id"], errors="coerce")
    df["flare_emission"] = pd.to_numeric(df["flare_emission"], errors="coerce")
    for col in ("ishape", "ikeis"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["cs_run", "exp_id"]).reset_index(drop=True)


def build_initiation_metrics() -> pd.DataFrame:
    df = _read_csv("data/registry/first_ice_onset_metrics.csv", dtype=str)
    df = _semi_join_paper_subset(df, only_flares=True)
    df = _to_numeric(
        df,
        [
            "station_idx",
            "ice_onset_min_since_seed",
            "peak_excess_ice_number_m3",
            "inp_onset_min_since_seed",
            "peak_excess_inp_number_m3",
        ],
    )
    grouped_rows: list[dict[str, object]] = []
    group_cols = ["cs_run", "exp_id", "expname", "ref_expname", "pair_method"]
    for keys, grp in df.groupby(group_cols, dropna=False):
        grp = grp.sort_values("station_idx").reset_index(drop=True)
        ice_found = _true_mask(grp["ice_onset_found"])
        inp_found = _true_mask(grp["inp_onset_found"])
        grouped_rows.append(
            {
                **dict(zip(group_cols, keys)),
                "station_count": int(grp["station_idx"].nunique()),
                "ice_onset_station_count": int(ice_found.sum()),
                "ice_onset_min_min_since_seed": _safe_stat(grp.loc[ice_found, "ice_onset_min_since_seed"], "min"),
                "ice_onset_median_min_since_seed": _safe_stat(grp.loc[ice_found, "ice_onset_min_since_seed"], "median"),
                "ice_onset_max_min_since_seed": _safe_stat(grp.loc[ice_found, "ice_onset_min_since_seed"], "max"),
                "peak_excess_ice_number_m3_max": _safe_stat(grp["peak_excess_ice_number_m3"], "max"),
                "inp_onset_station_count": int(inp_found.sum()),
                "inp_onset_median_min_since_seed": _safe_stat(grp.loc[inp_found, "inp_onset_min_since_seed"], "median"),
                "peak_excess_inp_number_m3_max": _safe_stat(grp["peak_excess_inp_number_m3"], "max"),
            }
        )
    return pd.DataFrame(grouped_rows).sort_values(["cs_run", "exp_id"]).reset_index(drop=True)


def build_process_attribution() -> pd.DataFrame:
    df = _read_csv("data/registry/pilot_process_attribution_matrix.csv", dtype=str)
    df = _semi_join_paper_subset(df, only_flares=True)
    df = _to_numeric(
        df,
        [
            "station_idx",
            "rate_n_f__dominant_fraction",
            "rate_n_w__dominant_fraction",
            "rate_n_f__runner_up_fraction",
            "rate_n_w__runner_up_fraction",
        ],
    )
    out = pd.DataFrame(
        {
            "cs_run": df["cs_run"],
            "expname": df["expname"],
            "station": df["station_idx"].fillna(0).astype(int).map(lambda idx: f"S{idx + 1}"),
            "ice_dominant_process": df["rate_n_f__dominant_process"].fillna(""),
            "ice_dominant_fraction": df["rate_n_f__dominant_fraction"],
            "ice_runner_up_process": df["rate_n_f__runner_up_process"].fillna(""),
            "liq_dominant_process": df["rate_n_w__dominant_process"].fillna(""),
            "liq_dominant_fraction": df["rate_n_w__dominant_fraction"],
            "liq_runner_up_process": df["rate_n_w__runner_up_process"].fillna(""),
            "window_minutes": (
                (pd.to_datetime(df["window_end"]) - pd.to_datetime(df["window_start"])).dt.total_seconds() / 60.0
            ),
        }
    )
    return out.sort_values(["cs_run", "expname", "station"]).reset_index(drop=True)


def build_growth_summary() -> pd.DataFrame:
    df = _read_csv("data/registry/growth_summary.csv", dtype=str)
    df = _semi_join_paper_subset(df, only_flares=True)
    df = _to_numeric(
        df,
        [
            "exp_id",
            "n_cells",
            "n_times",
            "alpha_peak_median",
            "alpha_mean_median",
            "ridge_peak_end_um",
            "ridge_peak_late_um",
            "time_end_min",
        ],
    )
    keep = [
        "cs_run",
        "exp_id",
        "expname",
        "ref_expname",
        "n_cells",
        "n_times",
        "alpha_peak_median",
        "alpha_mean_median",
        "ridge_peak_end_um",
        "ridge_peak_late_um",
        "time_end_min",
    ]
    return df[keep].sort_values(["cs_run", "exp_id"]).reset_index(drop=True)


def _select_featured_run(psd: pd.DataFrame, growth: pd.DataFrame) -> str:
    if not growth.empty:
        growth = _to_numeric(growth, ["n_cells", "time_end_min"])
        growth = growth.sort_values(["n_cells", "time_end_min", "expname"], ascending=[False, False, False])
        featured = str(growth.iloc[0]["expname"]).strip()
        if featured:
            return featured
    number_runs = psd.loc[psd["basis"].astype(str).str.lower() == "number", "run_id"].dropna().astype(str)
    if not number_runs.empty:
        return sorted(number_runs.unique())[-1]
    run_ids = psd["run_id"].dropna().astype(str)
    if run_ids.empty:
        raise ValueError("No run_id values available in psd_stats.csv")
    return sorted(run_ids.unique())[-1]


def build_psd_stats_selected() -> pd.DataFrame:
    psd = _read_csv("data/registry/psd_stats.csv", dtype=str)
    growth = _read_csv("data/registry/growth_summary.csv", dtype=str)
    featured_run_id = _select_featured_run(psd, growth)
    psd = _to_numeric(
        psd,
        [
            "t_lo_min",
            "t_hi_min",
            "liq_mean_diam_um",
            "liq_std_diam_um",
            "liq_net_tendency_per_min",
            "ice_mean_diam_um",
            "ice_std_diam_um",
            "ice_net_tendency_per_min",
            "alpha_ice_mean_diam",
            "obs_ice_mean_diam_um",
            "obs_ice_std_diam_um",
        ],
    )
    sel = psd[(psd["run_id"].astype(str) == featured_run_id) & (psd["variant"].astype(str).str.lower() == "number")].copy()
    if sel.empty:
        sel = psd[(psd["run_id"].astype(str) == featured_run_id) & (psd["basis"].astype(str).str.lower() == "number")].copy()
    if sel.empty:
        sel = psd[psd["run_id"].astype(str) == featured_run_id].copy()
    if sel.empty:
        number_rows = psd[psd["basis"].astype(str).str.lower() == "number"].copy()
        if not number_rows.empty:
            featured_run_id = sorted(number_rows["run_id"].dropna().astype(str).unique())[-1]
            sel = number_rows[number_rows["run_id"].astype(str) == featured_run_id].copy()
            exact_variant = sel[sel["variant"].astype(str).str.lower() == "number"].copy()
            if not exact_variant.empty:
                sel = exact_variant
    if sel.empty:
        latest_run_id = sorted(psd["run_id"].dropna().astype(str).unique())[-1]
        featured_run_id = latest_run_id
        sel = psd[psd["run_id"].astype(str) == featured_run_id].copy()
    sel["selected_run_id"] = featured_run_id
    keep = [
        "time_frame_min",
        "liq_mean_diam_um",
        "liq_std_diam_um",
        "liq_net_tendency_per_min",
        "ice_mean_diam_um",
        "ice_std_diam_um",
        "ice_net_tendency_per_min",
        "alpha_ice_mean_diam",
        "obs_match_ids",
        "obs_ice_mean_diam_um",
        "obs_ice_std_diam_um",
        "selected_run_id",
    ]
    return sel.sort_values(["t_lo_min", "t_hi_min"]).reset_index(drop=True)[keep]


def build_phase_budget_summary() -> pd.DataFrame:
    df = _read_csv("output/gfx/csv/01/cloud_phase_budget_summary_20260304110446_ALLBB.csv")
    return _to_numeric(
        df,
        ["liq_source_g_m2", "liq_sink_g_m2", "ice_source_g_m2", "ice_sink_g_m2"],
    ).reset_index(drop=True)


def build_phase_budget_long() -> pd.DataFrame:
    df = _read_csv("output/gfx/csv/01/cloud_phase_budget_summary_20260304110446_ALLBB_long.csv")
    return _to_numeric(df, ["t_lo_min", "t_hi_min", "column_net_g_m2"]).reset_index(drop=True)


BUILDERS = {
    "experiment_matrix": build_experiment_matrix,
    "initiation_metrics": build_initiation_metrics,
    "process_attribution": build_process_attribution,
    "growth_summary": build_growth_summary,
    "psd_stats_selected": build_psd_stats_selected,
    "phase_budget_summary": build_phase_budget_summary,
    "phase_budget_long": build_phase_budget_long,
}


def compile_table(spec: dict[str, object], sync_draft: bool) -> tuple[str, int]:
    table_id = str(spec["id"])
    builder_name = str(spec["builder"])
    builder = BUILDERS[builder_name]
    df = builder()
    source_csvs = [str(path) for path in spec.get("source_csvs", [])]
    out_df = add_provenance(
        df,
        table_id=table_id,
        source_csvs=source_csvs,
        selection_rule=str(spec.get("selection_rule", "")),
    )

    csv_output = resolve_repo_path(spec["csv_output"])
    tex_output = resolve_repo_path(spec["tex_output"])
    ensure_parent(csv_output)
    ensure_parent(tex_output)
    out_df.to_csv(csv_output, index=False)
    tex_text = render_table_environment(df, spec, source_csvs)
    tex_output.write_text(tex_text, encoding="utf-8")

    if sync_draft and spec.get("draft_output"):
        copy_if_changed(tex_output, resolve_repo_path(spec["draft_output"]))

    return table_id, len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile manuscript-facing paper tables from promoted analysis outputs.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="YAML manifest describing manuscript tables.")
    parser.add_argument(
        "--table-ids",
        nargs="*",
        default=None,
        help="Optional subset of table ids from the manifest.",
    )
    parser.add_argument(
        "--no-sync-draft",
        action="store_true",
        help="Write compiled TeX under data/registry/paper_tables only; do not copy into article_draft/PolarCAP/tables.",
    )
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    wanted = set(args.table_ids or [])
    table_specs = [spec for spec in manifest["tables"] if not wanted or spec["id"] in wanted]
    if wanted and len(table_specs) != len(wanted):
        found = {spec["id"] for spec in table_specs}
        missing = ", ".join(sorted(wanted - found))
        raise SystemExit(f"Unknown table ids in manifest: {missing}")

    for spec in table_specs:
        table_id, n_rows = compile_table(spec, sync_draft=not args.no_sync_draft)
        print(f"compiled {table_id}: {n_rows} row(s)")


if __name__ == "__main__":
    main()
