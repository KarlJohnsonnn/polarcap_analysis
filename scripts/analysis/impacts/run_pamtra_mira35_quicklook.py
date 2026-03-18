#!/usr/bin/env python3
"""Render a publication-style PAMTRA vs MIRA35 quicklook."""
# pyright: reportMissingImports=false

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

REPO_ROOT = Path(__file__).resolve().parents[3]
UTILITIES_DIR = REPO_ROOT / "src" / "utilities"
if str(UTILITIES_DIR) not in sys.path:
    sys.path.insert(0, str(UTILITIES_DIR))

from pamtra_mira35_quicklook import (  # noqa: E402
    DEFAULT_HEIGHT_ASL_M,
    DEFAULT_MODEL_T0,
    DEFAULT_OBSERVATION_IDS,
    build_quicklook_context,
    default_output_path,
    render_quicklook,
)
from style_profiles import apply_publication_style  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pamtra_file", type=Path, help="PAMTRA NetCDF file written by run_pamtra_plume_paths.py.")
    parser.add_argument(
        "--mira-dir",
        type=Path,
        default=Path("/Users/schimmel/data/observations/eriswil/mira35/20230125"),
        help="Directory containing raw MIRA35 *.mmclx files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional PNG path. Defaults to output/gfx/png/06/pamtra_mira35_quicklook_<stem>.png.",
    )
    parser.add_argument(
        "--height-asl",
        type=float,
        default=DEFAULT_HEIGHT_ASL_M,
        help="Radar altitude above sea level in meters added to the range coordinate.",
    )
    parser.add_argument(
        "--model-t0",
        type=str,
        default=str(DEFAULT_MODEL_T0),
        help="Model seeding start used as t0 for the aligned x-axis.",
    )
    parser.add_argument(
        "--observation-mission",
        type=str,
        default="composite",
        choices=("composite", *DEFAULT_OBSERVATION_IDS),
        help="Use the curated composite of all missions or a single plume-lagrangian mission.",
    )
    parser.add_argument(
        "--observation-t0",
        type=str,
        default=None,
        help="Observation seeding start used as t0. If omitted, the script auto-selects from known cases.",
    )
    parser.add_argument("--time-start", type=str, default=None, help="Optional UTC start time, e.g. 2023-01-25T09:28:20.")
    parser.add_argument("--time-end", type=str, default=None, help="Optional UTC end time, e.g. 2023-01-25T09:30:20.")
    parser.add_argument("--height-min", type=float, default=None, help="Optional minimum altitude in meters a.s.l.")
    parser.add_argument("--height-max", type=float, default=None, help="Optional maximum altitude in meters a.s.l.")
    parser.add_argument(
        "--keep-pamtra-mdv-sign",
        action="store_true",
        help="Use PAMTRA mean Doppler velocity as stored instead of applying the legacy sign flip.",
    )
    parser.add_argument("--dpi", type=int, default=400, help="PNG output DPI.")
    parser.add_argument("--hide-suptitle", action="store_true", help="Skip the figure suptitle.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = args.output or default_output_path(
        REPO_ROOT,
        args.pamtra_file,
        observation_label=args.observation_mission,
    )

    apply_publication_style()
    context = build_quicklook_context(
        args.pamtra_file,
        args.mira_dir,
        flip_pamtra_mdv=not args.keep_pamtra_mdv_sign,
        height_asl_m=args.height_asl,
        model_t0=args.model_t0,
        observation_mission=args.observation_mission,
        observation_t0=args.observation_t0,
        time_start=args.time_start,
        time_end=args.time_end,
        height_min_m=args.height_min,
        height_max_m=args.height_max,
    )
    written = render_quicklook(
        context,
        output,
        dpi=args.dpi,
        show_suptitle=not args.hide_suptitle,
    )

    print(f"saved figure -> {written}")
    print(f"observation label -> {context.observation_label}")
    print(f"observation reflectivity source -> {context.reflectivity_source}")
    print(f"observation velocity source -> {context.velocity_source}")
    print(f"observation t0 -> {context.observation_t0}")
    print(f"model t0 -> {context.model_t0}")
    print(f"elapsed window -> {context.elapsed_start_min:.2f} to {context.elapsed_end_min:.2f} min")
    print(f"height window -> {context.height_min_m:.1f} to {context.height_max_m:.1f} m a.s.l.")


if __name__ == "__main__":
    main()
