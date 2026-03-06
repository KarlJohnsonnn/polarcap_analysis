# /work/bb1262/user/schimmel/cosmo-specs-torch/utils/plot_bulk_timeseries.py
from __future__ import annotations

import numpy as np
import matplotlib.colors as mcolors
import colormaps as cmaps

import matplotlib.patheffects as PathEffects
import matplotlib.pyplot as plt
import xarray as xr

from utilities import haversine_distance
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import warnings

BulkVars = Sequence[str]
Pos = Sequence[Tuple[int, int]]

# plotting_styles.py
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
    "axes.grid": False,              # avoid clutter over pcolormesh/imshow
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
    "2d": STYLE_2D,                  # time-height, time-diameter, maps
    "hist": STYLE_HIST,              # 1D/2D hist summaries
}

def get_style(kind: str) -> dict:
    try:
        return STYLE_REGISTRY[kind]
    except KeyError as exc:
        valid = ", ".join(STYLE_REGISTRY.keys())
        raise ValueError(f"Unknown style '{kind}'. Use one of: {valid}") from exc

def use_style(kind: str):
    """Context manager: with use_style('timeseries'): ..."""
    return mpl.rc_context(get_style(kind))


"""
Examples for the plotting_styles

import matplotlib.pyplot as plt
from plotting_styles import use_style

# time series
with use_style("timeseries"):
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.plot(t, y1, label="run A")
    ax.plot(t, y2, label="run B")
    ax.set_xlabel("time")
    ax.set_ylabel("ice mass / kg")
    ax.legend()
    fig.tight_layout()

# 2D (time-height or time-diameter)
with use_style("2d"):
    fig, ax = plt.subplots(figsize=(7.5, 4.0))
    pm = ax.pcolormesh(time, altitude, field, shading="auto")
    fig.colorbar(pm, ax=ax, label="ICNC / L$^{-1}$")
    ax.set_xlabel("time")
    ax.set_ylabel("altitude / m")
    fig.tight_layout()

# histogram
with use_style("hist"):
    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    ax.hist(values, bins=40, density=True)
    ax.set_xlabel("w / m s$^{-1}$")
    ax.set_ylabel("PDF")
    fig.tight_layout()

"""


# for time-height plots
def create_fade_cmap(pyplot_cmap, n_fade=32):
    """Create colormap with fade effect"""
    fcolor = pyplot_cmap(0.0)
    fade_colors = np.ones((n_fade, 4))
    fade_colors[:, 3] = np.linspace(0, 1, n_fade)[::-1]
    for i in range(3):
        fade_colors[:, i] = np.linspace(fcolor[i], 1.0, n_fade)
    return mcolors.ListedColormap(np.vstack((fade_colors[::-1], pyplot_cmap(np.linspace(0, 1, 128)))))

def create_new_jet(n_colors=128):
    # for time-height plots
    cmap_new_timeheight_np = np.vstack([
        cmaps.matter(np.linspace(0, 1, n_colors)[::-1]),
        cmaps.haline(np.linspace(0, 1, n_colors)[::-1])])
    return mcolors.ListedColormap(cmap_new_timeheight_np[::-1])

def create_new_jet2(n_colors=128):
    cmap_new_timeheight_np2 = np.vstack([
        cmaps.matter(np.linspace(0, 1, n_colors)[::-1]),
        cmaps.haline(np.linspace(0, 1, n_colors)[::-1]),
        cmaps.greys(np.linspace(0.2, 0.8, 8)[::-1]),
    ])
    return mcolors.ListedColormap(cmap_new_timeheight_np2[::-1])

def create_new_jet3(n_colors=256, n_trans=16, vmin=0.1, vmax=0.9):
    ice_colors = cmaps.ice(np.linspace(1, 0.5, 32))
    bk_colors  = cmaps.BkBlAqGrYeOrReViWh200(np.linspace(vmin, vmax, n_colors))
    transition = np.linspace(ice_colors[-1], bk_colors[0], n_trans)[1:-1]
    return mcolors.ListedColormap(np.vstack([ice_colors, transition, bk_colors]))

def make_pastel(cmap, desaturation=0.4, darken=0.85, n=256):
    """Desaturate and slightly darken a colormap for a muted pastel look.
    desaturation: 0 = original, 1 = fully grey.
    darken: <1 reduces brightness, >1 increases it.
    """
    colors = cmap(np.linspace(0, 1, n))
    # Desaturate: blend toward per-row luminance grey
    lum = 0.2989 * colors[:, 0] + 0.5870 * colors[:, 1] + 0.1140 * colors[:, 2]
    for i in range(3):
        colors[:, i] = (1 - desaturation) * colors[:, i] + desaturation * lum
    # Darken
    colors[:, :3] *= darken
    colors[:, :3] = np.clip(colors[:, :3], 0, 1)
    return mcolors.ListedColormap(colors)

new_jet2 = create_new_jet2()
new_fjet2 = create_fade_cmap(new_jet2, 1)

new_jet = create_new_jet()
new_fjet = create_fade_cmap(new_jet, 2)

new_jet3 = create_new_jet3()
new_fjet3 = create_fade_cmap(new_jet3, 2)

def add_missing_data_patches(ax, da, min_consecutive=2, add_legend=True, y_extend=[1, 1000], **patch_kw):
    """Add hatched rectangles for runs of missing data (≥ min_consecutive time steps). Call from any plot cell."""
    from matplotlib.dates import dates
    from matplotlib.patches import Patch
    time_values = da.time.values
    d0 = da.diameter.values[0]-y_extend[0]
    d1 = da.diameter.values[-1]+y_extend[1]
    no_data_mask = ~(da['qfw'].sum(dim='diameter') > 0)
    mask = np.atleast_1d(np.asarray(no_data_mask)).ravel()
    if not mask.any():
        return
    ti = np.where(mask)[0]
    gaps = np.diff(ti) > 1
    starts = np.concatenate(([0], np.where(gaps)[0] + 1))
    ends = np.concatenate((np.where(gaps)[0] + 1, [len(ti)]))
    opts = dict(facecolor='0.92', edgecolor='0.1', alpha=0.5, linewidth=0.6, hatch='///', zorder=1, **patch_kw)
    for s, e in zip(starts, ends):
        if e - s < min_consecutive:
            continue
        t0, t1 = time_values[ti[s]], time_values[ti[e - 1]]
        x0, x1 = dates.date2num(t0), dates.date2num(t1)
        ax.add_patch(plt.Rectangle((x0, d0), x1 - x0, d1 - d0, **opts))
    if add_legend:
        ax.legend(handles=[Patch(facecolor=opts['facecolor'], edgecolor=opts['edgecolor'], alpha=opts['alpha'], linewidth=opts['linewidth'], hatch=opts['hatch'], label='No data')], loc='upper right', framealpha=0.95)


def plot_2d_model_and_holimo_bulk_timeseries(data_container, plot_kwargs):
    fig, ax = plt.subplots(nrows=3, ncols=2, figsize=(16, 10), sharex=True, sharey=False)

    for i, da in enumerate(data_container):
        da = da.squeeze()
        da.T.plot(ax=ax[i // 2, i % 2], **plot_kwargs)

    for iax in ax.flatten():
        iax.set_ylim(1e-3, 1e+3)

    for iax in ax[:, 1]:
        iax.set_ylim(1e+0, 1e+3)

    fig.tight_layout()
    return fig, ax



def make_bulk_figure(nrows: int = 3, ncols: int = 2, figsize=(16, 15)) -> Tuple[plt.Figure, np.ndarray]:
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
    return fig, axes


def setup_bulk_axes(
    axes: np.ndarray,
    axis_labels: Sequence[str],
    ylims: Optional[Sequence[Tuple[float, float]]] = None,
    log_axes: Optional[Sequence[bool]] = None,
    xlabel: str = "time (UTC)",
    label_pos=(1.0, 1.01),
    label_fontsize=24,
) -> None:
    if log_axes is None:
        log_axes = [False] * len(axis_labels)
    for ax, label, is_log in zip(axes.flat, axis_labels, log_axes):
        ax.text(
            label_pos[0],
            label_pos[1],
            label,
            transform=ax.transAxes,
            fontsize=label_fontsize,
            fontweight="bold",
            va="bottom",
            ha="right",
            zorder=99,
        )
        if is_log:
            ax.set_yscale("log")
        ax.set_xlabel(xlabel)

    if ylims is not None:
        for ax, ylim in zip(axes.flat, ylims):
            ax.set_ylim(*ylim)

# Note: legacy helpers were removed; use setup_bulk_axes for consistent styling



def _choose_hue(da: xr.DataArray, preferred: Sequence[str] = ("expname", "station")) -> Optional[str]:
    for cand in preferred:
        if cand in da.dims:
            return cand
    return None


def _first_existing(ds: xr.Dataset, names: Iterable[str]) -> List[str]:
    out = []
    for n in names:
        if n in ds:
            out.append(n)
        else:
            warnings.warn(f"Variable '{n}' not in dataset; skipping.")
    return out


def _to_lines_labels(
    dsm: xr.Dataset,
    da: xr.DataArray,
    ax: plt.Axes,
    hue: Optional[str],
    style_main: Optional[dict],
    style_overlay: Optional[dict],
    labeler: Optional[Callable[[Union[int, str]], str]],
) -> Tuple[List[plt.Line2D], List[str]]:
    lines: List[plt.Line2D] = []
    labels: List[str] = []
    style_main = dict(alpha=0.85, lw=1.5, linestyle="-") | (style_main or {})
    style_overlay = dict(alpha=0.45, lw=0.25, linestyle="--", color="black") | (style_overlay or {})

    if hue and hue in da.dims:
        # plot colored lines per hue group
        l1 = da.plot.line(ax=ax, x="time", hue=hue, add_legend=False, **style_main)
        da.plot.line(ax=ax, x="time", hue=hue, add_legend=False, **style_overlay)
        # xarray returns a list of lines for the first call
        lines = list(l1)
        coord_vals = list(da[hue].values)
        if labeler:
            labels = [labeler(v) for v in coord_vals]
        else:
            labels = [str(v) for v in coord_vals]
    else:
        # single line
        l1 = da.squeeze().plot.line(ax=ax, x="time", add_legend=False, **style_main)
        da.squeeze().plot.line(ax=ax, x="time", add_legend=False, **style_overlay)
        # xarray returns a list even for single line
        lines = list(l1)
        labels = [""]

    return lines, labels


def plot_1d_model_bulk_ts(
    axes: np.ndarray,
    dsm: xr.Dataset,
    *,
    variables: Optional[BulkVars] = None,
    positions: Optional[Pos] = None,
    axis_labels: Optional[Sequence[str]] = None,
    ylims: Optional[Sequence[Tuple[float, float]]] = None,
    yscale: str = "log",
    hue: Optional[str] = None,
    reduce_dims: Optional[Sequence[str]] = ("height_level", "height_level2"),
    reduction: str = "mean",
    target_height: Optional[float] = None,
    model_height_var: str = "HMLd",
    style_main: Optional[dict] = None,
    style_overlay: Optional[dict] = None,
    labeler: Optional[Callable[[Union[int, str]], str]] = None,
) -> Tuple[List[plt.Line2D], List[str]]:
    """
    Plot model bulk time series on a 3x2 grid (by default).

    - axes: 2D axes array with shape (3, 2) or matching positions
    - dsm: Dataset containing bulk variables and a 'time' coordinate
    - variables: names to plot; defaults to typical 6-model bulk variables
    - positions: positions per variable in axes, defaults to 3x2 top-left to bottom-right
    - yscale: 'log' or 'linear' (sets which panels are log by convention)
    - hue: dimension to use for colored lines ('expname' or 'station'); chosen automatically if None
    - labeler: optional function to convert hue coordinate to label text
    """
    if variables is None:
        variables = ["nw_bulk", "nf_bulk", "mdw_bulk", "mdf_bulk", "qw_bulk", "qfw_bulk"]
    if positions is None:
        positions = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)]
    if axis_labels is None:
        axis_labels = [r"N$_{liq}$", r"N$_{ice}$", r"D$_{liq}$", r"D$_{ice}$", r"C$_{liq}$", r"C$_{ice}$"]

    variables = _first_existing(dsm, variables)
    if not variables:
        return [], []

    if hue is None:
        # decide hue based on first existing variable
        hue = _choose_hue(dsm[variables[0]])

    # conventional log flags matching default variables
    if yscale == "log":
        log_axes = [True, True, False, False, False, True]
    else:
        log_axes = [False] * len(variables)

    lines_all: List[plt.Line2D] = []
    labels_ref: List[str] = []

    def _reduce_for_lineplot(da_in: xr.DataArray) -> xr.DataArray:
        """Reduce extra dims so that only time and optionally hue remain.
        If target_height is provided and model_height_var exists, select nearest height level.
        Otherwise, apply the specified reduction across listed reduce_dims if present.
        """
        da2 = da_in
        # Select nearest height level by physical altitude if requested
        if target_height is not None and model_height_var in dsm and any(rd in da2.dims for rd in (reduce_dims or [])):
            # Build representative height coordinate per height dim
            for hdim in (reduce_dims or []):
                if hdim in da2.dims and model_height_var in dsm:
                    hvar = dsm[model_height_var]
                    # hvar often has dims (time, hdim); take time-mean to get 1D heights
                    try:
                        if hdim in hvar.dims:
                            h1d = hvar.mean(dim=[d for d in hvar.dims if d != hdim])
                            da2 = da2.sel({hdim: h1d.sel({hdim: slice(None)}).values}, method="nearest")
                            # Now select nearest index by value
                            da2 = da2.sel({hdim: float(target_height)}, method="nearest")
                    except Exception:
                        # Fallback to simple reduction if selection fails
                        pass
        # Generic reduction across remaining reduce_dims
        if reduce_dims:
            for rd in reduce_dims:
                if rd in da2.dims:
                    if reduction == "sum":
                        da2 = da2.sum(dim=rd, skipna=True)
                    elif reduction == "max":
                        da2 = da2.max(dim=rd, skipna=True)
                    elif reduction == "min":
                        da2 = da2.min(dim=rd, skipna=True)
                    else: # if reduction == "mean"
                        da2 = da2.mean(dim=rd, skipna=True)
                        
        # Drop size-1 dims aside from time and hue
        for d in list(da2.dims):
            if d not in ("time", hue) and da2.sizes.get(d, 1) == 1:
                da2 = da2.squeeze(d)
        return da2

    for ivar, var in enumerate(variables):
        i, j = positions[ivar]
        ax = axes[i, j]
        try:
            da_plot = _reduce_for_lineplot(dsm[var])
            # Ensure we have at most 2 dims (time and hue or just time)
            extra_dims = set(da_plot.dims) - {"time", hue} if hue else set(da_plot.dims) - {"time"}
            if extra_dims:
                # Reduce any remaining dims by mean
                da_plot = da_plot.mean(dim=list(extra_dims), skipna=True)
            lines, labels = _to_lines_labels(dsm, da_plot, ax, hue, style_main, style_overlay, labeler)
        except Exception as e:
            warnings.warn(f"Failed plotting {var}: {e}")
            continue

        # collect legend anchors from the first panel only to avoid duplicates
        if ivar == 0:
            lines_all.extend(lines)
            labels_ref = labels

        # panel title with mean and units if present
        try:
            m = da_plot.mean(skipna=True)
            units = getattr(m, "attrs", {}).get("units", getattr(m, "attrs", {}).get("unit", ""))
            mval = float(np.asarray(m.values))
            ax.set_title(f"(time series) mean({var}) = {mval:.3g} {units}".rstrip())
        except Exception:
            ax.set_title(f"(time series) {var}")

    setup_bulk_axes(axes, axis_labels[: len(variables)], ylims, log_axes)

    return lines_all, labels_ref


def plot_holimo_bulk_ts(
    axes: np.ndarray,
    ds: xr.Dataset,
    *,
    ylims: Optional[Sequence[Tuple[float, float]]] = None,
    yscale: str = "linear",
    plot_interpolated: bool = True,
    interp_style: Optional[dict] = None,
    base_style: Optional[dict] = None,
) -> List[List[plt.Line2D]]:
    """
    Plot HOLIMO bulk time series into a 3x2 grid.

    Expected variables in hd:
      - Water_concentration, Water_meanD, Water_content
      - Ice_concentration,   Ice_meanD,   Ice_content
    Optional interpolated series:
      - *_interp and time_interp
    """
    # panels mapping
    mapping = [
        ("Water_concentration", (0, 0), True),
        ("Ice_concentration", (0, 1), True),
        ("Water_meanD", (1, 0), False),
        ("Ice_meanD", (1, 1), False),
        ("Water_content", (2, 0), False),
        ("Ice_content", (2, 1), True),
    ]

    base_style = dict(alpha=0.85, lw=1.0, color='royalblue') | (base_style or {})
    interp_style = dict(alpha=0.9, lw=3.0, color='darkblue') | (interp_style or {})

    lines_by_panel: List[List[plt.Line2D]] = []

    for var, pos, default_log in mapping:
        i, j = pos
        ax = axes[i, j]
        if var not in ds:
            warnings.warn(f"HOLIMO var '{var}' missing; skipping.")
            lines_by_panel.append([])
            continue

        # base line
        lbase = ds[var].plot(ax=ax, **base_style)
        lines_panel = list(lbase if isinstance(lbase, (list, tuple)) else [lbase])

        # interpolated line overlay if available
        interp_name = f"{var}_rebinned_t"
        if plot_interpolated and interp_name in ds and "time_rebinned" in ds:
            ax.plot(ds["time_rebinned"].values, ds[interp_name].values, **interp_style)

        # log scale selection
        if yscale == "log" or (yscale == "auto" and default_log):
            ax.set_yscale("log" if var.endswith("concentration") or var.endswith("content") else "linear")

        lines_by_panel.append(lines_panel)

    # apply axes cosmetics
    axis_labels = [r"N$_{liq}$", r"N$_{ice}$", r"D$_{liq}$", r"D$_{ice}$", r"C$_{liq}$", r"C$_{ice}$"]
    if yscale == "log":
        log_axes = [True, True, False, False, False, True]
    elif yscale == "auto":
        log_axes = [m[2] for m in mapping]
    else:
        log_axes = [False] * 6

    setup_bulk_axes(axes, axis_labels, ylims, log_axes)
    return axes, lines_by_panel



def add_grouped_legends(
    ax: plt.Axes,
    colors_map: Dict[str, str],
    linestyle_map: Dict[str, str],
    operation_linewidth: Optional[Dict[str, float]] = None,
    *,
    combine: bool = False,
    legend_kwargs: Optional[Dict[str, Union[str, float, int, Tuple[float, float]]]] = None,
) -> Tuple:
    """Create compact grouped legends next to the given axes.

    Groups:
      - Dataset/phase via line color: holimo_water, holimo_ice, model_water, model_ice
      - Processing level via linestyle: original, rebinned_t, rebinned_t_rebinned_b
      - Operation/statistic via line width: min, median, mean, max, std

    Also applies the linewidth mapping to existing lines whose label matches an
    operation key (useful when lines were created via hue).
    """
    from matplotlib.lines import Line2D

    if operation_linewidth is None:
        operation_linewidth = {"min": 0.9, "median": 1.1, "mean": 1.5, "max": 2.0, "std": 1.0}

    # Remove any existing legends (auto or previously added)
    try:
        import matplotlib.legend as mlegend
        for child in list(ax.get_children()):
            if isinstance(child, mlegend.Legend):
                child.remove()
    except Exception:
        # Fallback: remove the default one if present
        autoleg = ax.get_legend()
        if autoleg is not None:
            autoleg.remove()

    # Apply linewidth mapping to existing lines based on their label
    for line in ax.get_lines():
        label = line.get_label()
        if label in operation_linewidth:
            try:
                line.set_linewidth(operation_linewidth[label])
            except Exception:
                pass

    # Legend A: dataset/phase (use color)
    source_handles = [
        Line2D([0], [0], color=colors_map.get("holimo_water", "#1f77b4"), lw=1.6, linestyle="-"),
        Line2D([0], [0], color=colors_map.get("holimo_ice", "#ff7f0e"), lw=1.6, linestyle="-"),
        Line2D([0], [0], color=colors_map.get("model_water", "#2ca02c"), lw=1.6, linestyle="-"),
        Line2D([0], [0], color=colors_map.get("model_ice", "#d62728"), lw=1.6, linestyle="-"),
    ]
    source_labels = ["Holimo (water)", "Holimo (ice)", "Model (water)", "Model (ice)"]
    # Legend B: processing level (use linestyle)
    proc_handles = [
        Line2D([0], [0], color="black", lw=1.6, linestyle=linestyle_map.get("original", "-")),
        Line2D([0], [0], color="black", lw=1.6, linestyle=linestyle_map.get("rebinned_t", "--")),
        Line2D([0], [0], color="black", lw=1.6, linestyle=linestyle_map.get("rebinned_t_rebinned_b", ":")),
    ]
    proc_labels = ["original", "rebinned_t", "rebinned_t_rebinned_b"]

    # Legend C: operation/statistic (use linewidth)
    op_handles = [
        Line2D([0], [0], color="black", lw=operation_linewidth["min"]),
        Line2D([0], [0], color="black", lw=operation_linewidth["median"]),
        Line2D([0], [0], color="black", lw=operation_linewidth["mean"]),
        Line2D([0], [0], color="black", lw=operation_linewidth["max"]),
        Line2D([0], [0], color="black", lw=operation_linewidth["std"]),
    ]
    op_labels = ["min", "median", "mean", "max", "std"]

    if combine:
        # One combined legend with all entries in one column
        combined_handles = source_handles + proc_handles + op_handles
        combined_labels = source_labels + proc_labels + op_labels
        default_kwargs = dict(loc="best", frameon=True, fontsize=9, ncol=1)
        if legend_kwargs:
            default_kwargs.update(legend_kwargs)
        leg = ax.legend(combined_handles, combined_labels, **default_kwargs)
        return (leg,)

    # Separate legends (default)
    leg1 = ax.legend(
        source_handles,
        source_labels,
        title="Dataset / Phase",
        loc="upper left",
        bbox_to_anchor=(1.02, 1.00),
        frameon=True,
        fontsize=9,
        title_fontsize=10,
    )
    ax.add_artist(leg1)

    leg2 = ax.legend(
        proc_handles,
        proc_labels,
        title="Processing",
        loc="upper left",
        bbox_to_anchor=(1.02, 0.72),
        frameon=True,
        fontsize=9,
        title_fontsize=10,
    )
    ax.add_artist(leg2)

    leg3 = ax.legend(
        op_handles,
        op_labels,
        title="Operation",
        loc="upper left",
        bbox_to_anchor=(1.02, 0.47),
        frameon=True,
        fontsize=9,
        title_fontsize=10,
    )
    ax.add_artist(leg3)

    return leg1, leg2, leg3


def print_reduction_history(ds: xr.Dataset, latest_only: bool = True) -> str:
    """Return and print a compact reduction history summary from ds.attrs."""
    history = ds.attrs.get("reduction_history", [])
    if not history:
        msg = "No reduction operations recorded."
        print(msg)
        return msg

    items = [history[-1]] if latest_only else history
    lines: List[str] = ["Reduction history:"]
    for i, h in enumerate(items, 1):
        hdr = f"[{h.get('timestamp','')}] {h.get('type','')}"
        if h.get("type") == "time_resample":
            hdr += f" step={h.get('step')} agg={h.get('agg')} input_len={h.get('input_len')} output_len={h.get('output_len')}"
        lines.append(hdr)
        aff = h.get("affected_vars", [])
        if aff:
            lines.append("  vars: " + ", ".join(aff))
    summary = "\n".join(lines)
    print(summary)
    return summary




# ============================================================================
# PLOTTING FUNCTIONS (Interactive)
# ============================================================================

def fmt_title(da, namelist_dict, long_name=''):
    fmt_flare_emis = f"flare_emission = {namelist_dict['flare_sbm']['flare_emission']:_.2f}"
    fmt_varname = f"{long_name  if long_name else da.attrs['long_name']} / ({da.attrs['units']})"
    fmt_max_val = f"max = {da.max().values:_.2f}"
    fmt_text = fmt_varname + "   -   " + fmt_flare_emis + "   -   " + fmt_max_val 
    return fmt_text

def plot_3d_col_wrap(da,
                     figsize=(6, 5),
                     col_wrap=4,
                     cmap='coolwarm',
                     add_colorbar=True,
                     add_legend=False,
                     sharex=True,
                     sharey=True,
                     robust=True,
                     norm=None, 
                     bbox_to_anchor=(0.0, 0.0)
                     ):

    if da is None:
        print("No 3D data loaded")
        return None, None

    g = da.plot(
            x='longitude',      # longitude dimension
            y='latitude',       # latitude dimension
            col='time',         # create separate panels for each time step
            col_wrap=col_wrap,  # number of columns (e.g., 3 panels per row)
            figsize=figsize,   # adjust figure size as needed
            cmap=cmap,          # colormap
            sharex=sharex,
            sharey=sharey,
            add_colorbar=add_colorbar,
            robust=robust,      # uses 2nd and 98th percentiles for color limits
            cbar_kwargs={"orientation": "horizontal",
                         "location": "bottom",
                         "shrink": 0.2,
                        # "anchor": (0.0, 0.0, 1.0, 0.1)
                         },
            norm=norm,
    )
    #
    for i, iax in enumerate(g.axs.flatten()):
        if i >= da.time.size:
            continue
        iax.set_title(f'{np.datetime_as_string(da.time.values[i], unit="s")[-8:]}')
        iax.scatter(*(7.8730,   47.0695),   s=60, marker='x', color='red',   label="obervation")
        iax.scatter(*(7.90522,  47.07425),  s=60, marker='x', color='green', label="seeding")
        if sharex:
            iax.set_xlabel("")
    #
    if add_legend:
        g.axs.flat[0].legend(loc='lower left', ncols=2, bbox_to_anchor=bbox_to_anchor)
    g.fig.canvas.draw_idle()
    return g.fig, g.axs


def logscale_FacetGrid(
    da,
    norm=None,
    vmin=None,
    vmax=None,
    plot_kw=None,
    flare_org_nml=None,
    cmap='jet',
    long_name='',
    figsize_height=0.3,
    fontsize=20,
    suptitle_y=1.05,
):
    """
    Create logarithmic scale 3D FacetGrid plot with colorbar.

    Parameters
    ----------
    da : xarray.DataArray
        Data object
    vmin, vmax : float
        Normalization bounds for LogNorm
    plot_kw : dict
        Plotting keyword arguments
    flare_org_nml : dict
        Title formatting parameters
    cmap : str or Colormap
        Colormap to use
    long_name : str, optional
        Custom variable name for title
    figsize_height : float
        Height of colorbar figure
    fontsize : int
        Font size for title
    suptitle_y : float
        Y position of title
    """
    plot_kw = plot_kw or {}
    flare_org_nml = flare_org_nml or {}
    
    if norm is None:
        norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)
    
    # Colorbar
    fig, ax = plt.subplots(figsize=(plot_kw['figsize'][0], figsize_height))
    plt.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap=cmap),
        cax=ax, orientation='horizontal'
    )
    
    # Main plot
    f, ax_plot = plot_3d_col_wrap(da, norm=norm, cmap=cmap, **plot_kw)
    title_kw = {'long_name': long_name} if long_name else {}
    f.suptitle(fmt_title(da, flare_org_nml, **title_kw), fontsize=fontsize, y=suptitle_y)
    
    return f, ax_plot





def get_extpar_data(extpar_file):
    """Load and process ExtPar data."""
    data_extpar = xr.open_mfdataset(extpar_file, chunks='auto')
    lat2D = data_extpar['lat'].values[7:-7, 7:-7]
    lon2D = data_extpar['lon'].values[7:-7, 7:-7]
    height = data_extpar['HSURF'].values[7:-7, 7:-7]
    return lat2D, lon2D, height

def find_nearest_grid_point(lat, lon, lat2D, lon2D):
    """Find nearest grid point indices for given lat/lon."""
    distances = (lat2D - lat)**2 + (lon2D - lon)**2
    iy, ix = np.unravel_index(distances.argmin(), distances.shape)
    return ix, iy

def get_unique_meteogram_locations(plume_lats, plume_lons, lat2D, lon2D):
    """Get unique meteogram locations - one per grid cell."""
    unique_cells = {}
    
    for lat, lon in zip(plume_lats, plume_lons):
        ix, iy = find_nearest_grid_point(lat, lon, lat2D, lon2D)
        cell_id = (ix, iy)
        
        # Store only if this grid cell hasn't been used
        if cell_id not in unique_cells:
            unique_cells[cell_id] = (lat2D[iy, ix], lon2D[iy, ix], ix, iy)
    
    return unique_cells

def print_meteogram_list(unique_cells, resolution_name):
    """Print meteogram station list in Fortran namelist format."""
    from datetime import datetime
    today = datetime.today().strftime('%Y%m%d%H%M%S')
    
    print(f'\n=== Meteogram stations for {resolution_name} ===')
    print('   i     j       lat      lon            station_name')
    print('--------------------------------------------------------')
    
    for idx, (cell_id, (lat, lon, ix, iy)) in enumerate(sorted(unique_cells.items())):
        idx_str = f'{0:4d}, {0:4d}, '
        lat_str = f'{lat:.4f}, '
        lon_str = f'{lon:.4f}, '
        station_name = f"'{str(idx).zfill(2)}_{resolution_name}_{today}', "
        print(idx_str, lat_str, lon_str, station_name)


def set_name_tick_params(ax):
    ax.tick_params(which='both', direction='inout', top=True, right=True, bottom=True, left=True)
    ax.minorticks_on()
    ax.tick_params(which='major', length=5)
    ax.tick_params(which='minor', length=3)
    ax.xaxis.set_ticks_position('both')
    ax.yaxis.set_ticks_position('both')
    ax.grid(True, which='major', linestyle='--', linewidth='0.11', color='black', alpha=0.5, zorder=99.1)
    ax.grid(True, which='minor', linestyle=':', linewidth='0.075', color='black', alpha=0.25, zorder=99.1)
    ax.set_axisbelow(False)

def enumerate_subplots(axes, letter_list=['(A)', '(B)', '(C)', '(D)']):
    # Subplot labels
    for (ax, _), label in zip(axes, letter_list):
        ax.text(0.02, 0.98, label, transform=ax.transAxes, ha='left', va='top', fontweight='semibold', fontsize=14)



def add_ruler(axes,
              lat_start,
              lon_start,
              lat_end,
              lon_end,
              minor_tick_interval=500,
              major_tick_interval=1000,
              vertical_line_height=0.0,
              add_top_axis_lines=False,
              minor_ticklabels=False,
              major_ticklabels=True,
              lw=1.0,
              alpha=0.6):
    """Add distance ruler to lat/lon plot with ticks at 500m and 1000m intervals.
    
    Parameters:
    -----------
    axes : matplotlib.axes.Axes or array of Axes
        The axes to draw the ruler on
    lat_start, lon_start : float
        Starting latitude and longitude
    lat_end, lon_end : float
        Ending latitude and longitude
    minor_tick_interval : float, optional
        Minor tick interval in meters
    major_tick_interval : float, optional
        Major tick interval in meters
    vertical_line_height : float, optional
        Height of vertical lines in latitude units
    """
    # Ensure axes is an array
    axes = np.atleast_1d(axes)
    
    # Calculate total distance and direction vector
    total_distance = haversine_distance(lat_start, lon_start, lat_end, lon_end)
    dx, dy = lon_end - lon_start, lat_end - lat_start
    
    # Define styles for white outline and black line
    styles = [
        {'alpha': alpha - 0.3, 'linewidth': lw, 'color': 'white', 'zorder': 98},  # White outline
        {'alpha': alpha, 'linewidth': lw - 0.5, 'color': 'black', 'zorder': 99}  # Black line
    ]
    
    # Draw ruler at different intervals
    for interval, label_ticks in [(minor_tick_interval, minor_ticklabels),
                                  (major_tick_interval, major_ticklabels)]:
        num_ticks = max(1, int(total_distance / interval))
        o_factor = 0.15 / num_ticks  # Perpendicular tick size factor
        
        for ax in axes.flatten():
            # Draw ruler line with both styles
            for style in styles:
                ax.plot([lon_start, lon_end], [lat_start, lat_end], **style)
                if add_top_axis_lines:
                    ax.plot([lon_start, lon_end], [lat_start+vertical_line_height, lat_end+vertical_line_height], **style)
            
            # Draw tick marks and labels
            for i in range(num_ticks + 1):
                fraction = i / num_ticks
                lat_tick = lat_start + fraction * dy
                lon_tick = lon_start + fraction * dx
                
                # Calculate perpendicular offsets for tick marks
                o_lon, o_lat = -o_factor * dy, o_factor * dx
                
                # Draw tick mark
                if 0 < i < num_ticks:
                    for style in styles:
                        ax.plot([lon_tick + o_lon, lon_tick - o_lon],
                                [lat_tick + o_lat, lat_tick - o_lat], **style)
                        if add_top_axis_lines:
                            ax.plot([lon_tick + o_lon, lon_tick - o_lon],
                                    [lat_tick + o_lat + vertical_line_height, lat_tick - o_lat + vertical_line_height], **style)
                
                # Add label to the tick (only for 1000m intervals)
                if label_ticks and (0 < i < num_ticks):
                    label = ax.text(
                        lon_tick - o_lon - 0.001,
                        lat_tick + o_lat - 0.009,
                        f'{num_ticks-i:.0f}',
                        fontsize=10,
                        fontweight='semibold',
                        alpha=0.45
                    )
                    label.set_path_effects([
                        PathEffects.withStroke(linewidth=1.2, foreground='w', alpha=0.5)
                    ])
            
            # Add vertical lines at 0km and 8km points if requested
            if add_top_axis_lines:
                # 0km point (start of ruler)
                for style in styles:
                    ax.plot([lon_start, lon_start],
                            [lat_start, lat_start + vertical_line_height],
                            **style)
                    ax.plot([lon_end, lon_end],
                            [lat_end, lat_end + vertical_line_height],
                            **style)




