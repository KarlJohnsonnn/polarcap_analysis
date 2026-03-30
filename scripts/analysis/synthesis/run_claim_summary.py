#!/usr/bin/env python3
"""Produce a compact claim/evidence register from compiled paper tables."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.table_paths import (  # noqa: E402
    registry_output_paths,
    resolve_paper_table_input,
    sync_file,
)

PAPER_TABLES_DIR = REPO_ROOT / "output" / "tables" / "paper"
OUT_PATHS = registry_output_paths("claim_register.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]


def _safe_median(series: pd.Series) -> float | None:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return None
    return float(vals.median())


def _safe_max(series: pd.Series) -> float | None:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return None
    return float(vals.max())


def _mode_text(series: pd.Series) -> str:
    vals = series.astype(str).str.strip()
    vals = vals[vals != ""]
    if vals.empty:
        return ""
    return vals.value_counts().index[0]


def build_claim_register(paper_tables_dir: Path = PAPER_TABLES_DIR) -> pd.DataFrame:
    experiment = pd.read_csv(resolve_paper_table_input("tab_experiment_matrix", repo_root=REPO_ROOT))
    initiation = pd.read_csv(resolve_paper_table_input("tab_initiation_metrics", repo_root=REPO_ROOT))
    process = pd.read_csv(resolve_paper_table_input("tab_process_attribution", repo_root=REPO_ROOT))
    growth = pd.read_csv(resolve_paper_table_input("tab_growth_summary", repo_root=REPO_ROOT))
    phase_budget = pd.read_csv(resolve_paper_table_input("tab_phase_budget_summary", repo_root=REPO_ROOT))

    rows: list[dict[str, object]] = []

    flare_subset = experiment[experiment["is_reference"].astype(str).str.upper() == "FALSE"].copy()
    rows.append(
        {
            "claim_id": "dataset.paper_subset_size",
            "section": "methods",
            "claim_text": "The paper subset contains promoted flare and reference experiments with stable registry gating.",
            "evidence_metric": "paper_subset_rows",
            "evidence_value": int(len(experiment)),
            "evidence_status": "supported" if len(experiment) > 0 else "no_data",
            "source_table": "tab_experiment_matrix.csv",
            "note": f"flare_rows={len(flare_subset)}",
        }
    )

    med_ice_onset = _safe_median(initiation["ice_onset_median_min_since_seed"])
    rows.append(
        {
            "claim_id": "results.first_ice_onset",
            "section": "results",
            "claim_text": "First excess-ice onset occurs within minutes of seeding in the promoted flare subset.",
            "evidence_metric": "median_ice_onset_min_since_seed",
            "evidence_value": med_ice_onset,
            "evidence_status": "supported" if med_ice_onset is not None else "no_data",
            "source_table": "tab_initiation_metrics.csv",
            "note": "",
        }
    )

    peak_excess_ice = _safe_max(initiation["peak_excess_ice_number_m3_max"])
    rows.append(
        {
            "claim_id": "results.peak_excess_ice",
            "section": "results",
            "claim_text": "Promoted flare runs show measurable excess ice relative to matched references.",
            "evidence_metric": "max_peak_excess_ice_number_m3",
            "evidence_value": peak_excess_ice,
            "evidence_status": "supported" if peak_excess_ice is not None else "no_data",
            "source_table": "tab_initiation_metrics.csv",
            "note": "",
        }
    )

    dominant_ice = _mode_text(process["ice_dominant_process"])
    rows.append(
        {
            "claim_id": "results.early_ice_process",
            "section": "results",
            "claim_text": "The pilot early-window matrix identifies a repeatable dominant frozen-number pathway.",
            "evidence_metric": "mode_ice_dominant_process",
            "evidence_value": dominant_ice,
            "evidence_status": "supported" if dominant_ice else "no_data",
            "source_table": "tab_process_attribution.csv",
            "note": "",
        }
    )

    dominant_liq = _mode_text(process["liq_dominant_process"])
    rows.append(
        {
            "claim_id": "results.early_liq_process",
            "section": "results",
            "claim_text": "The pilot early-window matrix identifies the main competing liquid-number pathway.",
            "evidence_metric": "mode_liq_dominant_process",
            "evidence_value": dominant_liq,
            "evidence_status": "supported" if dominant_liq else "no_data",
            "source_table": "tab_process_attribution.csv",
            "note": "",
        }
    )

    alpha_peak = _safe_median(growth["alpha_peak_median"])
    rows.append(
        {
            "claim_id": "results.growth_alpha",
            "section": "results",
            "claim_text": "The promoted growth summary supports a coherent power-law growth regime along plume trajectories.",
            "evidence_metric": "median_alpha_peak",
            "evidence_value": alpha_peak,
            "evidence_status": "supported" if alpha_peak is not None else "no_data",
            "source_table": "tab_growth_summary.csv",
            "note": "",
        }
    )

    ridge_end = _safe_median(growth["ridge_peak_end_um"])
    rows.append(
        {
            "claim_id": "results.ridge_size",
            "section": "results",
            "claim_text": "Tracked plume ridges grow into large-particle sizes on the order of hundreds of micrometers.",
            "evidence_metric": "median_ridge_peak_end_um",
            "evidence_value": ridge_end,
            "evidence_status": "supported" if ridge_end is not None else "no_data",
            "source_table": "tab_growth_summary.csv",
            "note": "",
        }
    )

    common_liq_source = _mode_text(phase_budget["liq_source"])
    rows.append(
        {
            "claim_id": "appendix.phase_budget_context",
            "section": "appendix",
            "claim_text": "The compact cloud phase-budget summary provides a stable context table for the promoted overview case.",
            "evidence_metric": "mode_liq_source_process",
            "evidence_value": common_liq_source,
            "evidence_status": "supported" if common_liq_source else "no_data",
            "source_table": "tab_phase_budget_summary.csv",
            "note": "",
        }
    )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a small manuscript claim/evidence register from compiled paper tables.")
    parser.add_argument("--paper-tables-dir", type=Path, default=PAPER_TABLES_DIR, help="Directory with compiled paper table CSV files.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output claim register CSV path.")
    args = parser.parse_args()

    out = build_claim_register(args.paper_tables_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    sync_file(args.output, [OUT_PATHS["legacy"]])
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
