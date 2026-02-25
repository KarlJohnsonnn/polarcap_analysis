from __future__ import annotations

import matplotlib as mpl


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


STYLE_REGISTRY = {
    "timeseries": STYLE_TIMESERIES,
    "2d": STYLE_2D,
    "hist": STYLE_HIST,
}


def get_style(kind: str) -> dict:
    try:
        return STYLE_REGISTRY[kind]
    except KeyError as exc:
        valid = ", ".join(STYLE_REGISTRY.keys())
        raise ValueError(f"Unknown style '{kind}'. Use one of: {valid}") from exc


def use_style(kind: str):
    return mpl.rc_context(get_style(kind))

