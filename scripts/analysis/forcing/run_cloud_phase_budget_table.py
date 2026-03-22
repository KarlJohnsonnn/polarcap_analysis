#!/usr/bin/env python3
"""Build a phase-integrated cloud budget summary table for the overview figure."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.cloud_field_overview import (  # noqa: E402
    build_cloud_phase_budget_tables,
    load_cloud_field_overview_context,
    save_cloud_phase_budget_tables,
)

DEFAULT_CONFIG = REPO_ROOT / "config" / "psd_process_evolution.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build phase-integrated budget tables that complement the cloud overview figure."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="YAML config used for process-budget loading.")
    parser.add_argument(
        "--exp-idx",
        type=int,
        default=0,
        help="Index into cfg['plot_exp_ids']; falls back to literal exp id when out of range.",
    )
    parser.add_argument(
        "--active-range-key",
        type=str,
        default=None,
        help="Override the active process-budget size range key.",
    )
    parser.add_argument(
        "--plot-start",
        type=str,
        default=None,
        help="Absolute plot start time, e.g. 2023-01-25T12:25:00.",
    )
    parser.add_argument(
        "--plot-end",
        type=str,
        default=None,
        help="Absolute plot end time, e.g. 2023-01-25T13:05:00.",
    )
    parser.add_argument("--extpar-low", type=Path, default=None, help="Override the 400 m ExtPar file path.")
    parser.add_argument("--extpar-high", type=Path, default=None, help="Override the 100 m ExtPar file path.")
    args = parser.parse_args()

    context = load_cloud_field_overview_context(
        REPO_ROOT,
        args.config,
        exp_idx=args.exp_idx,
        active_range_key=args.active_range_key,
        plot_start=args.plot_start,
        plot_end=args.plot_end,
        extpar_low=args.extpar_low,
        extpar_high=args.extpar_high,
    )
    long_df, summary_df = build_cloud_phase_budget_tables(context)
    outputs = save_cloud_phase_budget_tables(long_df, summary_df, context["phase_budget_paths"])

    print("Cloud phase budget summary:")
    print(summary_df.to_string(index=False))
    for key, path in outputs.items():
        print(f"saved {key} -> {path}")


if __name__ == "__main__":
    main()
