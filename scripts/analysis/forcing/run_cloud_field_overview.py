#!/usr/bin/env python3
"""Render the notebook 01 cloud-field overview figure as a reproducible script."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.cloud_field_overview import (  # noqa: E402
    load_cloud_field_overview_context,
    render_cloud_field_overview,
    save_cloud_field_overview,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402

DEFAULT_CONFIG = REPO_ROOT / "config" / "process_budget.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render the cloud-field overview figure with ridge-sampled process tendencies."
)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="YAML config used for process-budget loading.",
    )
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
    parser.add_argument(
        "--extpar-low",
        type=Path,
        default=None,
        help="Override the 400 m ExtPar file path.",
    )
    parser.add_argument(
        "--extpar-high",
        type=Path,
        default=None,
        help="Override the 100 m ExtPar file path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PNG path. Defaults to output/gfx/png/01/<figure-name>.png.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="PNG output DPI.",
    )
    args = parser.parse_args()

    apply_publication_style()
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
    print(
        "Cloud overview:",
        f"exp={context['exp_label']}",
        f"range={context['active_range_key']}",
        f"stations={context['n_stations']}",
        f"plot={context['plot_start']}..{context['plot_end']}",
    )
    fig = render_cloud_field_overview(context)
    output_path = args.output or context["output_path"]
    save_cloud_field_overview(fig, output_path, dpi=args.dpi)


if __name__ == "__main__":
    main()
