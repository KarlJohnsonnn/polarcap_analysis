"""
Matplotlib style profiles and publication figure helpers.

Styles: timeseries, 2d, hist, publication (use_style("name") or get_style("name")).
Publication style: single column 89 mm, full width 183 mm, 300 DPI, 7 pt body;
Okabe–Ito colorblind-safe process colors. See conceptviz.app blog for journal specs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

BASE_STYLE = {
    "font.size": 15.5,
    "font.weight": "normal",
    "axes.titlepad": 5.0,
    "xtick.top": True,
    "xtick.major.top": True,
    "xtick.major.width": 1.0,
    "xtick.major.size": 3.0,
    "ytick.major.width": 1.0,
    "ytick.major.size": 3.0,
    "ytick.minor.width": 1.0,
    "ytick.minor.size": 3.0,
    "ytick.right": True,
    "axes.linewidth": 1.0,
    "savefig.dpi": 300,
    "figure.dpi": 110,
}


STYLE_TIMESERIES = {
    **BASE_STYLE,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linewidth": 0.6,
    "lines.linewidth": 1.8,
    "lines.markersize": 4.0,
    "legend.frameon": False,
    "xtick.minor.visible": True,
    "ytick.minor.visible": True,
}


STYLE_2D = {
    **BASE_STYLE,
    "axes.grid": False,
    "image.interpolation": "nearest",
    "xtick.minor.visible": False,
    "ytick.minor.visible": False,
    "axes.labelpad": 4.0,
}


STYLE_HIST = {
    **BASE_STYLE,
    "axes.grid": True,
    "grid.alpha": 0.20,
    "grid.linewidth": 0.5,
    "patch.edgecolor": "black",
    "patch.linewidth": 0.7,
    "xtick.minor.visible": True,
    "ytick.minor.visible": True,
}

# ── Publication figure (Nature/Science/Cell) ───────────────────────────────────
MM = 1 / 25.4
SINGLE_COL_IN = 89 * MM
FULL_COL_IN = 183 * MM
MAX_H_IN = 247 * MM

PUBLICATION_RCPARAMS: dict[str, Any] = {
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.format": "pdf",
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.01,
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
    "font.size": 7,
    "axes.labelsize": 7,
    "axes.titlesize": 8,
    "xtick.labelsize": 6,
    "ytick.labelsize": 6,
    "legend.fontsize": 6,
    "legend.title_fontsize": 7,
    "figure.titlesize": 9,
    "figure.titleweight": "bold",
    "lines.linewidth": 0.8,
    "axes.linewidth": 0.5,
    "xtick.major.width": 0.5,
    "ytick.major.width": 0.5,
    "xtick.minor.width": 0.4,
    "ytick.minor.width": 0.4,
    "patch.linewidth": 0.5,
    "axes.grid": False,
}

PROC_COLORS: dict[str, str] = {
    "CONDENSATION": "#0072B2",
    "DROP_COLLISION": "#E69F00",
    "DROP_INS_COLLISION": "#E6B800",
    "RIMING": "#CC79A7",
    "CONTACT_FREEZING": "#999999",
    "AGGREGATION": "#D55E00",
    "IMMERSION_FREEZING": "#009E73",
    "HOMOGENEOUS_FREEZING": "#56B4E9",
    "BREAKUP": "#F0E442",
    "MELTING": "#8c564b",
    "DEPOSITION": "#6A3D9A",
    "DEPOSITION_NUCLEATION": "#9E77B5",
    "REFREEZING": "#b5e0f0",
}

# Per-process hatching for visual distinction beyond colour (grayscale-safe).
PROC_HATCH: dict[str, str | None] = {
    "CONDENSATION": None,
    "BREAKUP": "xx",
    "DROP_COLLISION": "//",
    "DROP_INS_COLLISION": "\\\\",
    "IMMERSION_FREEZING": "...",
    "HOMOGENEOUS_FREEZING": "ooo",
    "CONTACT_FREEZING": "++",
    "RIMING": "---",
    "DEPOSITION": "|||",
    "AGGREGATION": "OO",
    "REFREEZING": "**",
    "MELTING": "\\\\//",
}

# Processes that release or consume latent heat (Energy); used for hatching in View A.
ENERGY_PROCESSES: frozenset[str] = frozenset({
    "CONDENSATION", "DEPOSITION", "MELTING", "REFREEZING",
    "IMMERSION_FREEZING", "HOMOGENEOUS_FREEZING", "CONTACT_FREEZING",
})
# Split for different visual cues: release (e.g. condensation, freezing) vs consume (e.g. melting).
HEAT_RELEASE_PROCESSES: frozenset[str] = frozenset({
    "CONDENSATION", "DEPOSITION", "REFREEZING",
    "IMMERSION_FREEZING", "HOMOGENEOUS_FREEZING", "CONTACT_FREEZING",
})
HEAT_CONSUME_PROCESSES: frozenset[str] = frozenset({"MELTING"})


STYLE_REGISTRY = {
    "timeseries": STYLE_TIMESERIES,
    "2d": STYLE_2D,
    "hist": STYLE_HIST,
    "publication": PUBLICATION_RCPARAMS,
}


def get_style(kind: str) -> dict:
    try:
        return STYLE_REGISTRY[kind]
    except KeyError as exc:
        valid = ", ".join(STYLE_REGISTRY.keys())
        raise ValueError(f"Unknown style '{kind}'. Use one of: {valid}") from exc


def use_style(kind: str):
    return mpl.rc_context(get_style(kind))


def apply_publication_style() -> None:
    """Set matplotlib rcParams to publication standards (300 DPI, 7 pt body, etc.)."""
    plt.rcParams.update(PUBLICATION_RCPARAMS)


def save_fig(
    fig: plt.Figure,
    stem: str,
    fmt: str = "pdf",
    out_dir: str | Path = "output",
) -> None:
    """Save figure at 300 DPI; use PDF/SVG for vector (Nature preferred)."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / f"{stem}.{fmt}")
    print(f"saved → {out}/{stem}.{fmt}")


def proc_color(name: str) -> str:
    """Return hex color for process name (Okabe–Ito); fallback #333333 if unknown."""
    return PROC_COLORS.get(name, "#333333")


def proc_hatch(name: str) -> str | None:
    """Return hatch pattern for process name; None if solid fill."""
    return PROC_HATCH.get(name)


def build_fixed_legend(
    fig: plt.Figure,
    active_procs: set[str],
    process_order: list[str],
    *,
    ncol: int = 6,
    bbox_y: float = -0.02,
    handletextpad: float = 0.8,
    columnspacing: float = 1.4,
) -> None:
    """Fixed-size legend showing all processes; active = filled+hatched, inactive = hollow outline."""
    import matplotlib.patches as mpatches

    handles, labels = [], []
    for p in process_order:
        is_active = p in active_procs
        c = proc_color(p)
        h = proc_hatch(p)
        patch = mpatches.FancyBboxPatch(
            (0, 0), 1, 1, boxstyle="round,pad=0.1",
            facecolor=c if is_active else "white",
            edgecolor=c,
            linewidth=1.0 if is_active else 0.6,
            alpha=0.85 if is_active else 0.35,
            hatch=h if is_active else None,
        )
        handles.append(patch)
        labels.append(p.replace("_", " ").title())
    fig.legend(
        handles, labels,
        loc="lower center", bbox_to_anchor=(0.5, bbox_y),
        ncol=min(ncol, max(1, len(handles))),
        frameon=False, handlelength=1.6, handleheight=1.0,
        handletextpad=handletextpad, columnspacing=columnspacing,
    )


def log_axis_formatter() -> FuncFormatter:
    """Format log-scale axis ticks for masses/concentrations.
    Values with |log10(x)| > 5 are shown as $10^{n}$; others as compact decimal (<1) or integer (>=1).
    """
    def _fmt(x: float, _) -> str:
        if x <= 0 or not np.isfinite(x):
            return ""
        n = int(round(np.log10(x)))
        if abs(n) > 5:
            return rf"$10^{{{n}}}$"
        if x < 1:
            return f"{x:.3f}".rstrip("0").rstrip(".")
        return f"{x:.0f}"
    return FuncFormatter(_fmt)


def format_elapsed_minutes_tick(x: float, span: float, *, zero_if_close: bool = False) -> str:
    """
    Format elapsed-time tick labels for minute/hour axes.

    - Under 1 minute: one decimal without trailing zeros
    - Up to 60 minutes: integer minutes
    - Above 60 minutes span: hours with one decimal and "h" suffix
    """
    if zero_if_close and np.isclose(x, 0.0):
        return "0"
    if x < 1:
        return f"{x:.1f}".rstrip("0").rstrip(".")
    return f"{x:.0f}" if span <= 60 else f"{x/60:.1f}h"

