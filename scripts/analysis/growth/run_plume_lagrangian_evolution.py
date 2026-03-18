#!/usr/bin/env python3
"""Render the notebook 03 plume-lagrangian summary figure as a reproducible script."""
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

from utilities.plume_lagrangian import (  # noqa: E402
    DEFAULT_KIND,
    DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
    load_plume_lagrangian_context,
    render_plume_lagrangian_figure,
    save_plume_lagrangian_figure,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render the plume-lagrangian evolution figure promoted from notebook 03."
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        default=REPO_ROOT / "data" / "processed",
        help="Processed plume-path root directory.",
    )
    parser.add_argument(
        "--holimo-file",
        type=Path,
        default=REPO_ROOT
        / "data"
        / "observations"
        / "holimo_data"
        / "CL_20230125_1000_1140_SM058_SM060_ts1.nc",
        help="HOLIMO NetCDF file used for overlay and histogram panels.",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default=DEFAULT_KIND,
        help="Plume-path dataset kind to plot, typically 'extreme'.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PNG path. Defaults to output/gfx/png/03/figure12_ensemble_mean_plume_path_foo.png.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=500,
        help="PNG output DPI.",
    )
    parser.add_argument(
        "--smooth-model-diameter-bins",
        type=int,
        default=DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
        help="Centered rectangular smoothing window along model diameter bins. Use 3 for the requested smoothing.",
    )
    args = parser.parse_args()

    apply_publication_style()
    context = load_plume_lagrangian_context(
        REPO_ROOT,
        processed_root=args.processed_root,
        holimo_file=args.holimo_file,
        kind=args.kind,
        smooth_model_diameter_bins=args.smooth_model_diameter_bins,
    )
    print(context["diag"].to_string(index=False))
    print(f"model diameter smoothing bins -> {context['smooth_model_diameter_bins']}")
    fig, output_path = render_plume_lagrangian_figure(context, output_path=args.output)
    save_plume_lagrangian_figure(fig, output_path, dpi=args.dpi)


if __name__ == "__main__":
    main()
