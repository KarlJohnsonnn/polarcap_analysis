#!/usr/bin/env python3
"""Notebook-03 plume-lagrangian figure: single entry (publication PNG and/or presenter series).

Edit ``CFG`` below to override defaults. Use ``--presenter`` for progressive ``_presenter/`` PNGs only.
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
    DEFAULT_CFG,
    load_context,
    print_model_growth_fit_table,
    render_figure,
    save_figure,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402

CFG = DEFAULT_CFG  # e.g. replace(DEFAULT_CFG, kind="extreme", all_alpha=0.8)

TAB_PLUME_EVOLUTION_TEX = (
    REPO_ROOT / "article_draft/PolarCAP/tables/tab_plume_evolution.tex"
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fig.12 plume path: main PNG + table, or --presenter frame series.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples (from polarcap_analysis repo root):
  python scripts/analysis/growth/run_plume_lagrangian_evolution.py
  python scripts/analysis/growth/run_plume_lagrangian_evolution.py --presenter
""",
    )
    parser.add_argument(
        "--presenter",
        action="store_true",
        help="Only write <stem>_presenter/ PNGs; skip single publication PNG here.",
    )
    args = parser.parse_args()

    apply_publication_style()
    ctx = load_context(REPO_ROOT, CFG)
    tex = print_model_growth_fit_table(ctx)
    if tex is not None:
        TAB_PLUME_EVOLUTION_TEX.parent.mkdir(parents=True, exist_ok=True)
        TAB_PLUME_EVOLUTION_TEX.write_text(tex, encoding="utf-8")
        print(f"[plume_lagrangian] wrote LaTeX table -> {TAB_PLUME_EVOLUTION_TEX.resolve().as_uri()}")

    if args.presenter:
        _, paths = render_figure(ctx, presenter_mode=True)
        for p in paths:
            print(f"[presenter] {p.resolve().as_uri()}")
        return

    fig, out = render_figure(ctx)
    save_figure(fig, out, dpi=500)


if __name__ == "__main__":
    main()
