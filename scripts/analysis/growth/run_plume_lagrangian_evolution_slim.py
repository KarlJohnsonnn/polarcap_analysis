#!/usr/bin/env python3
"""Slim runner for notebook-03 plume-lagrangian figure.

Edit ``CFG`` below if you need to override defaults; no CLI flags.
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.plume_lagrangian_slim import (  # noqa: E402
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
    apply_publication_style()
    ctx = load_context(REPO_ROOT, CFG)
    tex = print_model_growth_fit_table(ctx)
    if tex is not None:
        TAB_PLUME_EVOLUTION_TEX.parent.mkdir(parents=True, exist_ok=True)
        TAB_PLUME_EVOLUTION_TEX.write_text(tex, encoding="utf-8")
        print(f"[plume_lagrangian_slim] wrote LaTeX table -> {TAB_PLUME_EVOLUTION_TEX.resolve().as_uri()}")
    fig, out = render_figure(ctx)
    save_figure(fig, out, dpi=500)


if __name__ == "__main__":
    main()
