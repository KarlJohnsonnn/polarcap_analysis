#!/usr/bin/env python3
"""Render the notebook 04 PSD waterfall figures as reproducible scripts."""
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

from utilities.psd_waterfall import (  # noqa: E402
    DEFAULT_PLOT_KINDS,
    load_psd_waterfall_context,
    render_all_psd_waterfall_cases,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402


def _parse_csv(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [item.strip() for item in raw.split(",") if item.strip()]
    return vals or None


def _parse_run_labels(raw: str | None, available: list[str]) -> list[str] | None:
    if raw is None:
        return None
    if raw in available:
        return [raw]
    if "|" in raw:
        vals = [item.strip() for item in raw.split("|") if item.strip()]
        return vals or None
    vals = [item.strip() for item in raw.split(",") if item.strip()]
    return vals or None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render the PSD waterfall figure suite promoted from notebook 04."
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
        help="HOLIMO NetCDF file for optional observational overlays.",
    )
    parser.add_argument(
        "--plot-kinds",
        type=str,
        default=",".join(DEFAULT_PLOT_KINDS),
        help="Comma-separated plot kinds: mass, number, mass_small, number_small.",
    )
    parser.add_argument(
        "--run-labels",
        type=str,
        default=None,
        help="Optional run-label subset. Use an exact quoted label or separate multiple labels with '|'.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "scripts" / "processing_chain" / "output" / "04",
        help="Output root for PNG figures plus structured CSV and LaTeX stats tables.",
    )
    parser.add_argument(
        "--use-holimo",
        action="store_true",
        help="Overlay HOLIMO PSDs and merge HOLIMO stats into the LaTeX table.",
    )
    parser.add_argument(
        "--show-stats-table",
        action="store_true",
        help="Embed the compact diagnostics table into each PNG panel grid.",
    )
    parser.add_argument(
        "--hide-suptitle",
        action="store_true",
        help="Skip the figure suptitle.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=400,
        help="PNG output DPI.",
    )
    args = parser.parse_args()

    apply_publication_style()
    context = load_psd_waterfall_context(
        REPO_ROOT,
        processed_root=args.processed_root,
        holimo_file=args.holimo_file,
        use_holimo=args.use_holimo,
    )
    print(context["diag"].to_string(index=False))

    outputs = render_all_psd_waterfall_cases(
        context,
        plot_kinds=tuple(_parse_csv(args.plot_kinds) or DEFAULT_PLOT_KINDS),
        run_labels=_parse_run_labels(args.run_labels, list(context["cs_run_datasets"].keys())),
        output_root=args.output_root,
        show_stats_table=args.show_stats_table,
        show_suptitle=not args.hide_suptitle,
        dpi=args.dpi,
    )

    for item in outputs:
        print(f"saved figure -> {item['figure_path']}")
        print(item["stats_df"].to_string(index=False))
        print(f"saved stats  -> {item['stats_csv_path']}")
        print(f"saved latex -> {item['table_path']}")


if __name__ == "__main__":
    main()
