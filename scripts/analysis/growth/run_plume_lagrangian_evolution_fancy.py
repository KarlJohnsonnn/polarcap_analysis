#!/usr/bin/env python3
"""Render the plume-lagrangian figure with the trajectory-style top panel.

The top panel shows per-run concentration-weighted mean diameter D_mean(t) as a
10-90th percentile envelope with the ensemble-mean curve, the optimal model
power-law fit D = C * t^alpha, per-mission HOLIMO weighted-mean D(t) markers
with individual linear fits, and a dashed box marking the zoom region. Panels B
(zoom pcolormesh + HOLIMO scatter + fits) and C (time-averaged histogram) are
unchanged from the original ``run_plume_lagrangian_evolution.py``.
"""
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
    save_plume_lagrangian_figure,
)
from utilities.plume_lagrangian_fancy import (  # noqa: E402
    DEFAULT_OUTPUT_FANCY,
    render_plume_lagrangian_fancy_figure,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render the fancy plume-lagrangian figure with a trajectory top panel.",
    )
    parser.add_argument("--processed-root", type=Path, default=REPO_ROOT / "data" / "processed")
    parser.add_argument(
        "--holimo-file",
        type=Path,
        default=REPO_ROOT / "data" / "observations" / "holimo_data"
        / "CL_20230125_1000_1140_SM058_SM060_ts1.nc",
    )
    parser.add_argument("--kind", type=str, default=DEFAULT_KIND)
    parser.add_argument(
        "--output", type=Path, default=None,
        help=f"Output PNG path. Defaults to {DEFAULT_OUTPUT_FANCY}.",
    )
    parser.add_argument("--dpi", type=int, default=500)
    parser.add_argument(
        "--smooth-model-diameter-bins", type=int,
        default=DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
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
    fig, output_path = render_plume_lagrangian_fancy_figure(context, output_path=args.output)
    save_plume_lagrangian_figure(fig, output_path, dpi=args.dpi)


if __name__ == "__main__":
    main()
