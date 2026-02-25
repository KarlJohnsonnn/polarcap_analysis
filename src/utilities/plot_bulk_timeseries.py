# /work/bb1262/user/schimmel/cosmo-specs-torch/utils/plot_bulk_timeseries.py
from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import warnings

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt


BulkVars = Sequence[str]
Pos = Sequence[Tuple[int, int]]


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

    base_style = dict(alpha=0.85, lw=1.0) | (base_style or {})
    interp_style = dict(alpha=0.9, lw=3.0) | (interp_style or {})

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
        interp_name = f"{var}_interp"
        if plot_interpolated and interp_name in ds and "time_interp" in ds:
            ax.plot(ds["time_interp"].values, ds[interp_name].values, **interp_style)

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


## Legacy plotting function removed; use plot_holimo_bulk_ts(axes, hd, ...) above
