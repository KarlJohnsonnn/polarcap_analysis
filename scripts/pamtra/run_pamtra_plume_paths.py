#!/usr/bin/env python3
"""Run PAMTRA on vertical LV1 plume-path NetCDF files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
UTILITIES_ROOT = REPO_ROOT / "src" / "utilities"
if str(UTILITIES_ROOT) not in sys.path:
    sys.path.insert(0, str(UTILITIES_ROOT))

from pamtra_forward import PamtraConfig, discover_plume_path_inputs, run_pamtra_on_plume_path


def _default_output_dir(source: Path) -> Path:
    if source.parent.name == "lv1_paths":
        return source.parent.parent / "pamtra"
    return source.parent / "pamtra"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more vertical plume-path NetCDF files or directories containing them.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for PAMTRA NetCDF output. Defaults to a sibling `pamtra/` folder.",
    )
    parser.add_argument(
        "--frequencies",
        nargs="+",
        type=float,
        default=[35.5],
        help="Radar frequencies in GHz.",
    )
    parser.add_argument(
        "--limit-times",
        type=int,
        default=None,
        help="Limit the number of time steps for smoke tests.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing PAMTRA NetCDF output files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = PamtraConfig(frequencies_ghz=tuple(args.frequencies))
    sources = discover_plume_path_inputs(args.inputs, kind="vertical")
    if not sources:
        raise SystemExit("No vertical plume-path files found in the provided inputs.")

    for source in sources:
        output_dir = args.output_dir or _default_output_dir(source)
        output_path = output_dir / f"{source.stem}_pamtra.nc"
        if output_path.exists() and not args.overwrite:
            print(f"skip existing {output_path}")
            continue
        written = run_pamtra_on_plume_path(
            source,
            output_path,
            config=config,
            limit_times=args.limit_times,
        )
        print(written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
