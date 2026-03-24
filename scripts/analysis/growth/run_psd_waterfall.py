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
    get_psd_waterfall_cli_defaults,
    load_psd_waterfall_context,
    render_all_psd_waterfall_cases,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402

_PSD_CLI = get_psd_waterfall_cli_defaults(REPO_ROOT)

# Manuscript-facing caption; keep this wording aligned with the registry and figure13 gallery variants.
FIGURE_CAPTION = """Altitude-resolved particle size distribution evolution in successive
post-seeding windows for the promoted plume-path runs. Each panel compares liquid and frozen spectra in
either number or mass space, making it possible to track where excess ice first appears and how condensate
shifts toward larger diameters and lower levels as the plume matures. Together, the number and mass views
separate initial crystal production from subsequent growth and fallout."""


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
        default=_PSD_CLI["processed_root"],
        help="Processed plume-path root directory (default from config/psd_waterfall.yaml).",
    )
    parser.add_argument(
        "--holimo-file",
        type=Path,
        default=_PSD_CLI["holimo_file"],
        help="HOLIMO NetCDF file for optional observational overlays (default from config/psd_waterfall.yaml).",
    )
    parser.add_argument(
        "--plot-kinds",
        type=str,
        default=",".join(_PSD_CLI["plot_kinds"]),
        help="Comma-separated plot kinds: mass, number, mass_small, number_small (default from config/psd_waterfall.yaml).",
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
        default=_PSD_CLI["output_root"],
        help="Output root for PNG figures plus structured CSV and LaTeX stats tables (default from config/psd_waterfall.yaml).",
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
        plot_kinds=tuple(_parse_csv(args.plot_kinds) or _PSD_CLI["plot_kinds"]),
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
