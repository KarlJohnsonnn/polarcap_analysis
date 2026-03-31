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

DEFAULT_CONFIG = REPO_ROOT / "config" / "psd_process_evolution.yaml"
# Manuscript-facing caption; keep this wording aligned with gallery and registry copies.
FIGURE_CAPTION = """Cloud-scale context for the ALLBB seeded plume. Rows S1-S3 follow the three analysis
sites from the near-source liquid region to the downwind ice-growth and precipitation regime. For each site,
the first two panels show time-height liquid water content (QW) and frozen-plus-rimed water content (QFW);
dashed lines mark the station-specific analysis window, and the black curve in the QFW panel marks the
diagnosed ice-plume ridge. The right-hand panels show signed liquid and ice process tendencies averaged along
that ridge, highlighting the transition from a liquid-dominated source region near seeding to downstream
ice-growth over the observational and precipitation sites analyzed in the later figures."""


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
    parser.add_argument(
        "--flare-ensemble-mean",
        action="store_true",
        help=(
            "Average all seeded (non-reference) flare members: meteogram fields and Q process rates, "
            "with QFW ridges and symlog scales from the mean fields."
        ),
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
        flare_ensemble_mean=args.flare_ensemble_mean,
    )
    flare_tail = ""
    if context["flare_ensemble_mean"]:
        flare_tail = (
            f" flare_mean_n={context['flare_ensemble_n']} idx={context['flare_ensemble_indices']}"
        )
    print(
        "Cloud overview:",
        f"exp={context['exp_label']}",
        f"range={context['active_range_key']}",
        f"stations={context['n_stations']}",
        f"plot={context['plot_start']}..{context['plot_end']}{flare_tail}",
    )
    fig = render_cloud_field_overview(context, show_maps=False)
    output_path = args.output or context["output_path"]
    save_cloud_field_overview(fig, output_path, dpi=args.dpi)


if __name__ == "__main__":
    main()
