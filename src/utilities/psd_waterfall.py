"""
PSD waterfall helpers promoted from notebook 04.

Defaults merge ``config/psd_waterfall.yaml`` (under the repo root) with the
``DEFAULT_*`` module constants. ``plotting.axis_tick_label_pt`` sets diameter /
concentration / altitude tick label sizes (pt). ``load_psd_waterfall_context`` attaches
``psd_waterfall_settings`` to the returned context for rendering.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import yaml

from matplotlib import colors
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.transforms import Bbox
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.ticker import FuncFormatter

from utilities.holimo_helpers import load_and_prepare_holimo
from utilities.plotting import create_new_jet3, make_pastel
from utilities.plume_loader import load_plume_path_runs
from utilities.plume_path_plot import build_common_xlim, diagnostics_table
from utilities.style_profiles import FULL_COL_IN, MAX_H_IN, SINGLE_COL_IN

# Scale factor for gallery/publication: larger figure for higher effective resolution
PSD_WATERFALL_FIG_SCALE = 1.2

xr.set_options(keep_attrs=True)

DEFAULT_PROCESSED_ROOT = Path("data") / "processed"
DEFAULT_HOLIMO_FILE = (
    Path("data")
    / "observations"
    / "holimo_data"
    / "CL_20230125_1000_1140_SM058_SM060_ts1.nc"
)
DEFAULT_OUTPUT_ROOT = Path("output") / "gfx"
DEFAULT_MODEL_SEED = np.datetime64("2023-01-25T12:30:00")
DEFAULT_HOLIMO_WINDOW = (
    np.datetime64("2023-01-25T10:10:00"),
    np.datetime64("2023-01-25T12:00:00"),
)
DEFAULT_TIME_FRAMES_PLUME = [
    [np.datetime64("2023-01-25T10:55:00"), np.datetime64("2023-01-25T11:10:00")],
    [np.datetime64("2023-01-25T10:35:00"), np.datetime64("2023-01-25T10:50:00")],
    [np.datetime64("2023-01-25T11:24:00"), np.datetime64("2023-01-25T11:39:00")],
]
DEFAULT_OBS_IDS = ["SM059", "SM058", "SM060"]
DEFAULT_GROWTH_TIMES_MIN = [6.1, 8.0, 9.1]
DEFAULT_ALT_BANDS = [
    (1350, 1275),
    (1275, 1200),
    (1200, 1125),
    (1125, 1050),
    (1050, 975),
    (975, 900),
    (900, 850),
    (850, 800),
]
DEFAULT_TIME_WINDOWS = [
    "0.25min",
    "0.5min",
    "1min",
    "2.5min",
    "5min",
    "7.5min",
    "10min",
    "15min",
    "25min",
]
DEFAULT_PLOT_KINDS = ("mass", "number", "mass_small", "number_small")
DEFAULT_COL_WRAP = 3  # Panel columns per row in ``plot_psd_waterfall`` when YAML omits ``plotting.col_wrap``.
DEFAULT_SHOW_SUPTITLE = True  # When YAML omits ``waterfall.show_suptitle``.
DEFAULT_TICK_LABEL_PT_DIAMETER = 10.0  # Log-diameter axis tick labels (panels); YAML ``plotting.axis_tick_label_pt``.
DEFAULT_TICK_LABEL_PT_CONCENTRATION = 10.0  # Synthetic concentration scale ($10^n$) on first column.
DEFAULT_TICK_LABEL_PT_ALTITUDE = 10.0  # Altitude colorbar tick labels.
DEFAULT_VARSETS = {
    "mass":         ("qw", "qfw", 1e-6, 1e3, 1.0,  9e3, False),
    "number":       ("nw", "nf",  5e-1, 9e7, 1.0,  9e3, False),
    "mass_small":   ("qw", "qfw", 1e-6, 1e3, 1e-3, 9e3, True),
    "number_small": ("nw", "nf",  5e-1, 9e7, 1e-3, 9e3, True),
}
DEFAULT_RUNS: list[dict[str, str]] = [
    {"label": "400m, inp 1e6, ccn 400 (analytic)",   "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211431"},
    {"label": "400m, inp 1e6, ccn 400 (planar)",     "cs_run": "cs-eriswil__20260127_211338", "exp_id": "20260127211551"},
    {"label": "400m, inp 1e6, ccn 400 (spherical)",  "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131550"},
    {"label": "400m, inp 1e6, ccn 400 (columnar 2)", "cs_run": "cs-eriswil__20260121_131528", "exp_id": "20260121131632"},
]

PSD_WATERFALL_CONFIG_REL = Path("config") / "psd_waterfall.yaml"


def _raw_psd_waterfall_yaml(repo_root: Path) -> dict[str, Any]:
    path = repo_root / PSD_WATERFALL_CONFIG_REL
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _edges_to_alt_bands(edges: Sequence[float | int]) -> list[tuple[float, float]]:
    e = [float(x) for x in edges]
    return [(e[i], e[i + 1]) for i in range(len(e) - 1)]


def _spacing_min_to_time_windows(spacing: Sequence[float | int]) -> list[str]:
    xs = sorted({float(x) for x in spacing if float(x) > 0})
    return [f"{x}min" for x in xs]


def _parse_varsets_from_yaml(raw: Any) -> dict[str, tuple[Any, ...]] | None:
    if not isinstance(raw, dict):
        return None
    out: dict[str, tuple[Any, ...]] = {}
    for k, v in raw.items():
        if not isinstance(v, (list, tuple)) or len(v) != 7:
            continue
        out[str(k)] = (
            str(v[0]),
            str(v[1]),
            float(v[2]),
            float(v[3]),
            float(v[4]),
            float(v[5]),
            bool(v[6]),
        )
    return out or None


def _parse_runs_yaml(raw: Any) -> list[dict[str, str]] | None:
    if not isinstance(raw, list):
        return None
    out: list[dict[str, str]] = []
    for r in raw:
        if isinstance(r, dict):
            out.append({str(k): str(v) for k, v in r.items()})
    return out


def _parse_axis_tick_label_pt(raw: Any) -> tuple[float, float, float]:
    """Return (diameter, concentration, altitude) tick label sizes in points from YAML dict."""
    dd, dc, da = DEFAULT_TICK_LABEL_PT_DIAMETER, DEFAULT_TICK_LABEL_PT_CONCENTRATION, DEFAULT_TICK_LABEL_PT_ALTITUDE
    if not isinstance(raw, dict):
        return (dd, dc, da)

    def _num(key: str, default: float) -> float:
        v = raw.get(key)
        return default if v is None else float(v)

    return (_num("diameter", dd), _num("concentration", dc), _num("altitude", da))


@dataclass
class PsdWaterfallSettings:
    """Resolved defaults from ``config/psd_waterfall.yaml`` with Python fallbacks."""

    processed_root_rel: Path
    holimo_file_rel: Path
    output_root_rel: Path
    model_seed: np.datetime64
    holimo_window: tuple[np.datetime64, np.datetime64]
    time_frames_plume: tuple[tuple[np.datetime64, np.datetime64], ...]
    obs_ids: tuple[str, ...]
    growth_times_min: tuple[float, ...]
    alt_bands: tuple[tuple[float, float], ...]
    time_windows: tuple[str, ...]
    plot_kinds: tuple[str, ...]
    col_wrap: int
    show_suptitle: bool
    tick_label_pt_diameter: float
    tick_label_pt_concentration: float
    tick_label_pt_altitude: float
    varsets: dict[str, tuple[Any, ...]]
    runs: tuple[dict[str, str], ...]

    def resolved_paths(self, repo_root: Path) -> tuple[Path, Path, Path]:
        def _abs(p: Path) -> Path:
            return p if p.is_absolute() else repo_root / p

        return _abs(self.processed_root_rel), _abs(self.holimo_file_rel), _abs(self.output_root_rel)


def build_psd_waterfall_settings(repo_root: Path) -> PsdWaterfallSettings:
    raw = _raw_psd_waterfall_yaml(repo_root)
    pt = raw.get("plotting") or {}
    tm = raw.get("time") or {}
    wf = raw.get("waterfall") or {}

    model_seed = np.datetime64(str(tm.get("seed_start", DEFAULT_MODEL_SEED)))

    if pt.get("altitude_bands"):
        alt_bands = tuple(_edges_to_alt_bands(pt["altitude_bands"]))
    else:
        alt_bands = tuple(DEFAULT_ALT_BANDS)

    tw = pt.get("time_windows")
    if not tw and pt.get("time_spacing_min") is not None:
        tw = _spacing_min_to_time_windows(pt["time_spacing_min"])
    if not tw:
        tw = list(DEFAULT_TIME_WINDOWS)
    time_windows = tuple(str(x) for x in tw)

    plot_kinds = tuple(str(x) for x in pt["plot_kinds"]) if pt.get("plot_kinds") else DEFAULT_PLOT_KINDS

    cw_raw = pt.get("col_wrap")
    col_wrap = int(cw_raw) if cw_raw is not None else DEFAULT_COL_WRAP
    col_wrap = max(1, col_wrap)

    t_d, t_c, t_a = _parse_axis_tick_label_pt(pt.get("axis_tick_label_pt"))

    vs = _parse_varsets_from_yaml(pt.get("varsets"))
    varsets = vs if vs else dict(DEFAULT_VARSETS)

    runs_raw = pt.get("runs")
    if runs_raw is not None:
        parsed = _parse_runs_yaml(runs_raw)
        runs = tuple(parsed) if parsed else tuple(DEFAULT_RUNS)
    else:
        runs = tuple(DEFAULT_RUNS)

    obs_ids = tuple(str(x) for x in pt["obs_ids"]) if pt.get("obs_ids") else tuple(DEFAULT_OBS_IDS)
    growth_times_min = (
        tuple(float(x) for x in pt["growth_times_min"]) if pt.get("growth_times_min") else tuple(DEFAULT_GROWTH_TIMES_MIN)
    )

    holimo_w = wf.get("holimo_window")
    if holimo_w and isinstance(holimo_w, (list, tuple)) and len(holimo_w) == 2:
        holimo_window = (np.datetime64(str(holimo_w[0])), np.datetime64(str(holimo_w[1])))
    else:
        holimo_window = DEFAULT_HOLIMO_WINDOW

    tfp_raw = wf.get("time_frames_plume")
    if tfp_raw and isinstance(tfp_raw, list):
        tfp: list[tuple[np.datetime64, np.datetime64]] = []
        for pair in tfp_raw:
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                tfp.append((np.datetime64(str(pair[0])), np.datetime64(str(pair[1]))))
        time_frames_plume = tuple(tfp) if tfp else tuple((a, b) for a, b in DEFAULT_TIME_FRAMES_PLUME)
    else:
        time_frames_plume = tuple((a, b) for a, b in DEFAULT_TIME_FRAMES_PLUME)

    proc = Path(wf.get("processed_root", DEFAULT_PROCESSED_ROOT))
    holimo = Path(wf.get("holimo_file", DEFAULT_HOLIMO_FILE))
    out_root = Path(wf.get("output_root", DEFAULT_OUTPUT_ROOT))

    if "show_suptitle" in wf:
        show_suptitle = bool(wf["show_suptitle"])
    else:
        show_suptitle = DEFAULT_SHOW_SUPTITLE

    return PsdWaterfallSettings(
        processed_root_rel=proc,
        holimo_file_rel=holimo,
        output_root_rel=out_root,
        model_seed=model_seed,
        holimo_window=holimo_window,
        time_frames_plume=time_frames_plume,
        obs_ids=obs_ids,
        growth_times_min=growth_times_min,
        alt_bands=alt_bands,
        time_windows=time_windows,
        plot_kinds=plot_kinds,
        col_wrap=col_wrap,
        show_suptitle=show_suptitle,
        tick_label_pt_diameter=t_d,
        tick_label_pt_concentration=t_c,
        tick_label_pt_altitude=t_a,
        varsets=varsets,
        runs=runs,
    )


def get_psd_waterfall_cli_defaults(repo_root: Path) -> dict[str, Any]:
    """Path and plot-kind defaults for ``run_psd_waterfall`` argparse (from YAML + fallbacks)."""
    s = build_psd_waterfall_settings(repo_root)
    pr, hf, gfx = s.resolved_paths(repo_root)
    return {
        "processed_root": pr,
        "holimo_file": hf,
        "output_root": gfx,
        "plot_kinds": s.plot_kinds,
        "col_wrap": s.col_wrap,
        "show_suptitle": s.show_suptitle,
    }


def default_psd_waterfall_cmap():
    return make_pastel(create_new_jet3(), desaturation=0.25, darken=0.90)


def _basis_for_var_kind(var_kind: str) -> str:
    return "mass" if var_kind.startswith("mass") else "number"


def _net_tendency_label(basis: str) -> str:
    return r"$\Delta q / \Delta t$ (g m$^{-3}$ min$^{-1}$)" if basis == "mass" else r"$\Delta N / \Delta t$ (L$^{-1}$ min$^{-1}$)"


def waterfall_output_root(repo_root: Path, settings: PsdWaterfallSettings | None = None) -> Path:
    """Return the gfx output root from YAML ``waterfall.output_root`` or ``DEFAULT_OUTPUT_ROOT``."""
    s = settings or build_psd_waterfall_settings(repo_root)
    return s.resolved_paths(repo_root)[2]


def get_moments(conc: np.ndarray, diam: np.ndarray) -> tuple[float, float]:
    """Return weighted mean and variance."""
    tot = np.nansum(conc)
    if tot <= 0:
        return np.nan, np.nan
    mu = np.nansum(conc * diam) / tot
    var = np.nansum(conc * (diam - mu) ** 2) / tot
    return float(mu), float(var)


def collapse_cell_dim(da: xr.DataArray) -> xr.DataArray:
    """Sum over cell if present."""
    return da.sum("cell") if "cell" in da.dims else da


def make_time_bounds(
    t0: np.datetime64 | None,
    time_windows: Sequence[str],
    default_start: np.datetime64,
) -> list[np.datetime64]:
    """Return consecutive panel boundaries from a start time and offsets."""
    t_start = np.datetime64(t0) if t0 is not None else default_start
    return [
        t_start,
        *[
            t_start + np.timedelta64(int(pd.Timedelta(tw).total_seconds()), "s")
            for tw in time_windows
        ],
    ]


def compute_layer_colors(
    da_w: xr.DataArray | None,
    alt_bands: Sequence[tuple[float, float]],
    t_lo: np.datetime64,
    t_hi: np.datetime64,
    cmap,
    n_bands: int,
) -> list[Any]:
    """Color by mean liquid signal in each altitude band when available."""
    if da_w is None:
        return [cmap(i / n_bands) for i in range(n_bands)]

    w_cmap = plt.get_cmap("coolwarm")
    w_norm = colors.Normalize(vmin=-2, vmax=2)
    out = []
    for hi, lo in alt_bands:
        w_slab = da_w.sel(time=slice(t_lo, t_hi), altitude=slice(hi, lo))
        if "cell" in w_slab.dims:
            w_slab = w_slab.mean("cell")
        out.append(w_cmap(w_norm(w_slab.mean().values)))
    return out


def make_phase_styles(color) -> dict[str, dict[str, Any]]:
    """Return fill styles for liquid and frozen PSDs."""
    c = color[:3]
    return {
        "nw": {"ec": (*c, 0.8), "ls": "--", "lw": 0.85, "fc": (*c, 0.2)},
        "nf": {"ec": (*c, 1.0), "ls": "-", "lw": 0.85, "fc": (*c, 0.6), "hatch": "////"},
    }


def draw_reference_grid(ax: Axes, n_bands: int, x_shift: float, y_shift: float) -> None:
    """Draw the slanted reference grid in log-diameter space."""
    for diam in [1e-3, 1e-2, 1e-1, 1, 10, 100, 1000]:
        xs = [diam * 10 ** ((i - n_bands + 1) * x_shift) for i in range(n_bands)]
        ys = [i * y_shift for i in range(n_bands)]
        ax.plot(xs, ys, "k:", lw=0.2, alpha=0.8, zorder=1)


def _axes_x_extra_for_cb_ct_labels(
    nf: xr.DataArray,
    t_lo: np.datetime64,
    t_hi: np.datetime64,
    alt_bands: Sequence[tuple[float, float]],
    zlim: tuple[float, float],
    xlim: tuple[float, float],
) -> float:
    """Axes-fraction shift right for CB/CT when lowest-altitude ice exceeds ``zlim[0]`` far to the right."""
    if not alt_bands or "altitude" not in nf.dims:
        return 0.0
    hi, lo = alt_bands[-1]
    sub = nf.sel(time=slice(t_lo, t_hi), altitude=slice(hi, lo))
    if sub.size == 0:
        return 0.0
    if "cell" in sub.dims:
        sub = sub.mean("cell", skipna=True)
    if "time" in sub.dims:
        sub = sub.mean("time", skipna=True)
    if "altitude" in sub.dims:
        sub = sub.mean("altitude", skipna=True)
    vals = np.asarray(sub.values, dtype=float).ravel()
    diam = np.asarray(nf.diameter.values, dtype=float)
    n = min(vals.size, diam.size)
    if n == 0:
        return 0.0
    vals, diam = vals[:n], diam[:n]
    mask = np.isfinite(vals) & (vals > float(zlim[0]))
    if not np.any(mask):
        return 0.0
    d_right = float(np.nanmax(diam[mask]))
    x_lo, x_hi = float(xlim[0]), float(xlim[1])
    if d_right <= 0 or x_lo <= 0 or x_hi <= x_lo:
        return 0.0
    log_lo, log_hi = np.log10(x_lo), np.log10(x_hi)
    span = log_hi - log_lo
    if span <= 0:
        return 0.0
    pos = (np.log10(np.clip(d_right, x_lo, x_hi)) - log_lo) / span
    # Ramp extra offset as ice signal reaches the upper part of the log-x axis (overlap with right-anchored text).
    start, full, max_extra = 0.68, 0.90, 0.12
    if pos <= start:
        return 0.0
    if pos >= full:
        return max_extra
    return max_extra * (pos - start) / (full - start)


def draw_cloud_top_base(
    ax: Axes,
    nw: xr.DataArray,
    nf: xr.DataArray,
    t_lo: np.datetime64,
    t_hi: np.datetime64,
    cloud_thresh: float,
    alt_bands: Sequence[tuple[float, float]],
    diam: np.ndarray,
    n_bands: int,
    x_shift: float,
    y_shift: float,
    idx_faint_start: int = 0,
    *,
    zlim: tuple[float, float] = (1e0, 1e6),
    xlim: tuple[float, float] = (1e-3, 3e3),
) -> None:
    """Draw cloud-top and cloud-base markers in projected coordinates."""
    alt_tops = np.array(alt_bands)[:, 0]
    prof = (nw + nf).sel(time=slice(t_lo, t_hi)).mean("time").sum("diameter")
    active = prof.altitude[prof > cloud_thresh]
    if len(active) == 0:
        return

    label_x_extra = _axes_x_extra_for_cb_ct_labels(nf, t_lo, t_hi, alt_bands, zlim, xlim)
    h0, h1 = alt_bands[0][0], alt_bands[1][0]
    for name, alt in [("CT", active.max().values), ("CB", active.min().values)]:
        idx_x = int(np.argmin(np.abs(alt - alt_tops)))
        x_off = (idx_x - n_bands + 1) * x_shift
        d_proj = diam * (10**x_off)
        y_pos = (alt - h0) / (h1 - h0) * y_shift
        ax.plot(
            [d_proj[idx_faint_start], d_proj[-1]],
            [y_pos, y_pos],
            c="black",
            ls="--",
            lw=0.6,
            alpha=0.7,
            zorder=3,
        )
        ax.text(
            0.99 + x_off * 0.1 + label_x_extra,
            y_pos + 0.6,
            f"{name} ({int(alt)}m)",
            transform=ax.get_yaxis_transform(),
            ha="right",
            va="top",
            fontsize=8,
            color="black",
            alpha=0.8,
            clip_on=False,
            zorder=5,
        )


def draw_concentration_scale(
    ax: Axes,
    xlim: tuple[float, float],
    zlim: tuple[float, float],
    n_bands: int,
    y_shift: float,
    *,
    tick_label_pt: float = DEFAULT_TICK_LABEL_PT_CONCENTRATION,
) -> None:
    """Draw the concentration axis glyph in first-column panels."""
    y_front = (n_bands - 1) * y_shift
    sb_x = xlim[0] * 2
    log_min, log_max = np.log10(zlim[0]), np.log10(zlim[1])
    height = log_max - log_min

    ax.plot([sb_x, sb_x], [y_front, y_front + height], "k-", lw=1.5, zorder=100)
    for idx in range(0, int(height) + 1, 2):
        y_tick = y_front + idx
        val_exp = int(log_min + idx)
        ax.plot([sb_x, sb_x * 0.6], [y_tick, y_tick], "k-", lw=1, zorder=100)
        ax.text(
            sb_x * 0.4,
            y_tick,
            f"$10^{{{val_exp}}}$",
            ha="right",
            va="center",
            weight="bold",
            fontsize=tick_label_pt,
        )


def build_stats_rows(
    nw: xr.DataArray,
    nf: xr.DataArray,
    diam: np.ndarray,
    t_lo: np.datetime64,
    t_hi: np.datetime64,
) -> list[list[str]]:
    """Return liquid and frozen mean-diameter diagnostics for one time window."""
    dt_s = (t_hi - t_lo) / np.timedelta64(1, "s")
    rows = []

    for da in [nw, nf]:
        sum_dims = [dim for dim in ("altitude", "diameter") if dim in da.dims]
        sel = {"time": slice(t_lo, t_hi), "diameter": slice(diam[30], None)}

        sub = da.sel(sel).mean("time")
        if "altitude" in sub.dims:
            sub = sub.sum("altitude")

        tot = sub.sum(skipna=True).values
        if tot > 0:
            mu = (sub * diam[30:]).sum(skipna=True).values / tot
            var = (sub.values * (diam[30:] - mu) ** 2).sum() / tot
            std = np.sqrt(var)
        else:
            mu, std = np.nan, np.nan

        ts = da.sum(sum_dims).sel(time=slice(t_lo, t_hi))
        rate = (ts[-1] - ts[0]) / dt_s if len(ts) > 1 and dt_s > 0 else 0
        rows.append([f"{mu:.1f}", f"{std:.1f}", f"{rate / 60:.2f}"])

    return list(map(list, zip(*rows)))


def build_psd_stats_dataframe(
    prepared: dict[str, Any],
    *,
    var_kind: str,
    run_id: str,
    run_label: str,
    holimo_obs: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    """Build a machine-readable PSD window statistics table."""
    basis = _basis_for_var_kind(var_kind)
    t_ref = prepared["bounds"][0]
    rows: list[dict[str, Any]] = []

    for panel in prepared["panel_data"]:
        dt_lo = float((panel["t_lo"] - t_ref) / np.timedelta64(1, "m"))
        dt_hi = float((panel["t_hi"] - t_ref) / np.timedelta64(1, "m"))
        stats_rows = panel["stats_rows"]

        obs_match_ids = ""
        obs_ice_mean_diam_um = np.nan
        obs_ice_std_diam_um = np.nan
        if holimo_obs:
            matches = [obs for obs in holimo_obs if dt_lo <= obs.get("growth_min", np.nan) < dt_hi]
            if matches:
                obs_match_ids = ",".join(obs["id"] for obs in matches)
                psd = np.concatenate([np.asarray(obs["psd"], dtype=float) for obs in matches])
                diam = np.concatenate([np.asarray(obs["diameter"], dtype=float) for obs in matches])
                obs_mean, obs_var = get_moments(psd, diam)
                obs_ice_mean_diam_um = float(obs_mean) if np.isfinite(obs_mean) else np.nan
                obs_ice_std_diam_um = float(np.sqrt(obs_var)) if np.isfinite(obs_var) else np.nan

        rows.append(
            {
                "variant": var_kind,
                "basis": basis,
                "run_id": str(run_id),
                "run_label": run_label,
                "time_frame_min": f"{dt_lo:.2f}-{dt_hi:.2f}",
                "t_lo_min": dt_lo,
                "t_hi_min": dt_hi,
                "t_mid_min": 0.5 * (dt_lo + dt_hi),
                "liq_mean_diam_um": float(stats_rows[0][0]),
                "liq_std_diam_um": float(stats_rows[1][0]),
                "liq_net_tendency_per_min": float(stats_rows[2][0]),
                "ice_mean_diam_um": float(stats_rows[0][1]),
                "ice_std_diam_um": float(stats_rows[1][1]),
                "ice_net_tendency_per_min": float(stats_rows[2][1]),
                "obs_match_ids": obs_match_ids,
                "obs_ice_mean_diam_um": obs_ice_mean_diam_um,
                "obs_ice_std_diam_um": obs_ice_std_diam_um,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["alpha_ice_mean_diam"] = np.nan
    t_eps = 1e-6
    for idx in range(1, len(df)):
        t_prev = max(float(df.iloc[idx - 1]["t_mid_min"]), t_eps)
        t_curr = max(float(df.iloc[idx]["t_mid_min"]), t_eps)
        d_prev = max(float(df.iloc[idx - 1]["ice_mean_diam_um"]), 1e-6)
        d_curr = max(float(df.iloc[idx]["ice_mean_diam_um"]), 1e-6)
        denom = np.log(t_curr) - np.log(t_prev)
        if t_prev > 0 and t_curr > 0 and denom != 0:
            df.loc[df.index[idx], "alpha_ice_mean_diam"] = (np.log(d_curr) - np.log(d_prev)) / denom
    return df


def prepared_to_latex_table(
    prepared: dict[str, Any],
    *,
    var_kind: str = "unknown",
    run_id: str = "",
    run_label: str = "",
    holimo_obs: list[dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, str]:
    """Return a presentation table and LaTeX for the PSD window diagnostics."""

    def fmt_pair(model_val: float, holimo_val: float | None, nd: int = 1) -> str:
        model_txt = f"{model_val:.{nd}f}"
        if holimo_val is None or not np.isfinite(holimo_val):
            return model_txt
        holimo_txt = f"{holimo_val:.{nd}f}"
        if model_val > holimo_val:
            return rf"\textbf{{{model_txt}}}/{holimo_txt}"
        if holimo_val > model_val:
            return rf"{model_txt}/\textbf{{{holimo_txt}}}"
        return f"{model_txt}/{holimo_txt}"

    stats_df = build_psd_stats_dataframe(
        prepared,
        var_kind=var_kind,
        run_id=run_id,
        run_label=run_label,
        holimo_obs=holimo_obs,
    )
    if stats_df.empty:
        return stats_df, stats_df.to_latex(index=False, escape=False)

    rows = []
    for row in stats_df.to_dict(orient="records"):
        label = str(row["time_frame_min"])
        if row["obs_match_ids"]:
            label = f"{label}/{row['obs_match_ids']}"
        rows.append(
            {
                "time_frame_min": label,
                "liq_mean_diam_um": f"{float(row['liq_mean_diam_um']):.1f}",
                "liq_std_diam_um": f"{float(row['liq_std_diam_um']):.1f}",
                "liq_net_tendency_per_min": f"{float(row['liq_net_tendency_per_min']):.2f}",
                "ice_mean_diam_um": fmt_pair(float(row["ice_mean_diam_um"]), row["obs_ice_mean_diam_um"], nd=1),
                "ice_std_diam_um": fmt_pair(float(row["ice_std_diam_um"]), row["obs_ice_std_diam_um"], nd=1),
                "ice_net_tendency_per_min": f"{float(row['ice_net_tendency_per_min']):.2f}",
                "alpha_ice_mean_diam": (
                    "—" if not np.isfinite(row["alpha_ice_mean_diam"]) else f"{float(row['alpha_ice_mean_diam']):.2f}"
                ),
            }
        )

    df = pd.DataFrame(rows)
    return df, df.to_latex(index=False, escape=False)


def save_latex_table(
    prepared: dict[str, Any],
    out_file: str | Path,
    *,
    var_kind: str = "unknown",
    run_id: str = "",
    run_label: str = "",
    holimo_obs: list[dict[str, Any]] | None = None,
) -> Path:
    """Write the diagnostics table to a LaTeX file."""
    _, latex = prepared_to_latex_table(
        prepared,
        var_kind=var_kind,
        run_id=run_id,
        run_label=run_label,
        holimo_obs=holimo_obs,
    )
    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(latex, encoding="utf-8")
    return out_path


def save_psd_stats_csv(stats_df: pd.DataFrame, out_file: str | Path) -> Path:
    """Write the structured PSD statistics CSV."""
    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stats_df.to_csv(out_path, index=False)
    return out_path


def add_stats_table(ax: Axes, stats_rows: list[list[str]], *, basis: str = "number") -> None:
    """Render the compact diagnostics table into one panel."""
    labels = [
        r"$\mu$ ($\mu$m)",
        r"$\sigma$ ($\mu$m)",
        _net_tendency_label(basis),
    ]
    table_content = [row + [label] for row, label in zip(stats_rows, labels)]
    table = ax.table(
        cellText=table_content,
        colLabels=["Liquid", "Frozen", ""],
        loc="upper right",
        bbox=Bbox.from_bounds(0.7, 0.75, 0.25, 0.22),
    )
    table.auto_set_font_size(False)
    table.set_fontsize(6)
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_linewidth(0)
        cell.set_text_props(ha="left" if col_idx == 2 else "right")


def format_waterfall_panel(
    ax: Axes,
    idx: int,
    t_lo: np.datetime64,
    t_hi: np.datetime64,
    t_start: np.datetime64,
    xlim: tuple[float, float],
    *,
    diameter_tick_label_pt: float = DEFAULT_TICK_LABEL_PT_DIAMETER,
) -> None:
    """Apply shared axis formatting for each panel."""
    dt_lo = (t_lo - t_start) / np.timedelta64(1, "m")
    dt_hi = (t_hi - t_start) / np.timedelta64(1, "m")
    panel_label = f"({chr(65 + idx)}) {dt_lo:.1f} — {dt_hi:.1f} min"

    ax.set(xscale="log", xlim=xlim, yticks=[])
    for spine in ["left", "top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))
    ax.tick_params(axis="x", which="major", labelsize=diameter_tick_label_pt)
    ax.text(
        0.02,
        0.96,
        panel_label,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        fontweight="bold",
        bbox={"facecolor": "white", "edgecolor": "0.25", "alpha": 0.9, "boxstyle": "round,pad=0.2"},
    )


def add_altitude_colorbar(
    fig: Figure,
    axes,
    cmap,
    n_bands: int,
    alt_bands: Sequence[tuple[float, float]],
    *,
    tick_label_pt: float = DEFAULT_TICK_LABEL_PT_ALTITUDE,
) -> None:
    """Add the discrete altitude colorbar with band boundaries."""
    colors = [cmap(i / n_bands) for i in range(n_bands)]
    cb_bounds = [band[1] for band in alt_bands[::-1]] + [alt_bands[0][0]]
    norm = mcolors.BoundaryNorm(cb_bounds, n_bands)
    sm = plt.cm.ScalarMappable(cmap=mcolors.ListedColormap(colors[::-1]), norm=norm)
    cbar = fig.colorbar(sm, ax=axes, pad=0.02, shrink=0.75, aspect=40)
    cbar.set_label("Altitude / m", fontsize=tick_label_pt)
    cbar.set_ticks(cb_bounds)
    cbar.set_ticklabels([f"{int(level)}" for level in cb_bounds])
    cbar.ax.tick_params(labelsize=tick_label_pt)


def build_holimo_obs_series(
    ds_hd: xr.Dataset,
    time_frames_plume: list[list[np.datetime64]],
    obs_ids: list[str],
    growth_times_min: list[float],
    *,
    var_candidates: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Build observed ice PSD sums tied to plume-growth timestamps."""
    if var_candidates is None:
        var_candidates = ["Ice_Pristine_PSDnoNorm", "Ice_PSDnoNorm", "Ice_PSDMnoNorm", "Ice_PSDlinNorm", "Ice_PSDlogNorm"]

    obs_var = next((name for name in var_candidates if name in ds_hd.data_vars), None)
    if obs_var is None or ds_hd.time.size < 2:
        return []

    dt_s = float((ds_hd.time[1] - ds_hd.time[0]) / np.timedelta64(1, "s"))
    if not np.isfinite(dt_s) or dt_s <= 0:
        return []

    out = []
    for obs_id, growth_min, (t0, t1) in zip(obs_ids, growth_times_min, time_frames_plume):
        obs = (ds_hd[obs_var] * dt_s).sel(time=slice(t0, t1)).sum("time")
        unit = obs.attrs.get("unit", obs.attrs.get("units", ""))
        unit_factor = 1e3 if "cm-3" in unit else 1.0
        out.append(
            {
                "id": obs_id,
                "growth_min": float(growth_min),
                "diameter": obs.diameter.values,
                "psd": np.asarray(obs.values, dtype=float) * unit_factor,
                "unit": unit.replace("cm-3", "L-1") if "cm-3" in unit else unit,
            }
        )
    return out


def overlay_holimo_obs(
    ax: Axes,
    holimo_obs: list[dict[str, Any]],
    t_lo: np.datetime64,
    t_hi: np.datetime64,
    t_start: np.datetime64,
    zlim: tuple[float, float],
    log_zlim_lo: float,
) -> None:
    """Overlay observed PSD in the panel matching the growth-time window."""
    dt_lo = (t_lo - t_start) / np.timedelta64(1, "m")
    dt_hi = (t_hi - t_start) / np.timedelta64(1, "m")

    magenta = (1.0, 0.0, 1.0, 1.0)
    obs_style = make_phase_styles(magenta)["nf"]

    for obs in holimo_obs:
        growth_min = obs["growth_min"]
        if not (dt_lo <= growth_min < dt_hi):
            continue

        y = np.where(obs["psd"] > zlim[0], np.log10(obs["psd"] + 1e-14) - log_zlim_lo, np.nan)

        faint = dict(obs_style)
        faint.update({"fc": (*obs_style["fc"][:3], 0.04), "ec": (*obs_style["ec"][:3], 0.1), "hatch": None})
        n_vals = len(obs["diameter"])
        i_split = max(2, n_vals // 3)

        ax.fill_between(obs["diameter"][:i_split], 0, y[:i_split], step="mid", **faint, zorder=120)
        ax.fill_between(obs["diameter"][i_split - 1 :], 0, y[i_split - 1 :], step="mid", **obs_style, zorder=121)
        ax.plot(obs["diameter"], y, c="magenta", lw=1.0, ls="-", drawstyle="steps-mid", zorder=122)
        ax.text(
            0.02,
            0.86,
            f"HOLIMO {obs['id']} (gt={growth_min:.1f} min)",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=7.5,
            color="magenta",
            weight="bold",
        )


def prepare_psd_waterfall_data(
    da_nw: xr.DataArray,
    da_nf: xr.DataArray,
    *,
    time_windows: Sequence[str],
    t0: np.datetime64,
    alt_bands: Sequence[tuple[float, float]],
    da_w: xr.DataArray | None = None,
    cmap=None,
    x_shift: float = -0.1,
    min_diam: float = 0.01,
) -> dict[str, Any]:
    """Precompute slices and sums once for fast plotting."""
    cmap = plt.get_cmap("jet") if cmap is None else cmap
    nf, nw = (collapse_cell_dim(da) for da in [da_nf, da_nw])
    bounds = make_time_bounds(t0, time_windows, nf.time.values[0])

    has_altitude = "altitude" in nf.dims and "altitude" in nw.dims
    alt_bands_used = alt_bands if has_altitude else [(0.0, 0.0)]
    n_bands = len(alt_bands_used)

    diam = nf.diameter.values
    idx_faint_start = int(np.argmin(np.abs(min_diam - diam)))
    delta_t = (nf.time[1] - nf.time[0]).values / np.timedelta64(1, "s")

    panel_data = []
    for idx in range(len(bounds) - 1):
        t_lo, t_hi = bounds[idx], bounds[idx + 1]
        layer_colors = (
            compute_layer_colors(da_w, alt_bands_used, t_lo, t_hi, cmap, n_bands) if has_altitude else [cmap(0.6)]
        )

        bands = []
        for i_band, ((hi, lo), layer_color) in enumerate(zip(alt_bands_used, layer_colors)):
            x_off = (i_band - n_bands + 1) * x_shift
            d_proj = diam * (10**x_off)

            if has_altitude:
                nw_slab = (nw * delta_t).sel(time=slice(t_lo, t_hi), altitude=slice(hi, lo))
                nf_slab = (nf * delta_t).sel(time=slice(t_lo, t_hi), altitude=slice(hi, lo))
                n_alt_band = max(1, int(nw_slab.altitude.size))
                nw_slab = nw_slab.sum(["time", "altitude"]) / n_alt_band
                nf_slab = nf_slab.sum(["time", "altitude"]) / n_alt_band
            else:
                nw_slab = (nw * delta_t).sel(time=slice(t_lo, t_hi)).sum("time")
                nf_slab = (nf * delta_t).sel(time=slice(t_lo, t_hi)).sum("time")

            bands.append(
                {
                    "idx": i_band,
                    "hi": hi,
                    "lo": lo,
                    "color": layer_color,
                    "d_proj": d_proj,
                    "nw_slab": nw_slab.values,
                    "nf_slab": nf_slab.values,
                }
            )

        stats_rows = build_stats_rows(nw, nf, diam, t_lo, t_hi)
        panel_data.append({"idx": idx, "t_lo": t_lo, "t_hi": t_hi, "bands": bands, "stats_rows": stats_rows})

    return {
        "nf": nf,
        "nw": nw,
        "bounds": bounds,
        "diam": diam,
        "n_bands": n_bands,
        "idx_faint_start": idx_faint_start,
        "panel_data": panel_data,
        "has_altitude": has_altitude,
        "alt_bands_used": alt_bands_used,
    }


def plot_psd_waterfall(
    prepared: dict[str, Any],
    *,
    alt_bands: Sequence[tuple[float, float]],
    zlim: tuple[float, float] = (1e0, 1e6),
    xlim: tuple[float, float] = (1e-3, 3e3),
    cmap=None,
    n_cols: int = 3,
    y_shift: float = -0.75,
    x_shift: float = -0.125,
    cloud_thresh: float = 1e0,
    holimo_obs: list[dict[str, Any]] | None = None,
    show_cloud_bounds: bool = True,
    show_stats_table: bool = True,
    show_small_diameters: bool = True,
    y_label: str = r"Number Concentration (at lowest altitude) / L$^{-1}$",
    stats_basis: str = "number",
    diameter_tick_label_pt: float = DEFAULT_TICK_LABEL_PT_DIAMETER,
    concentration_tick_label_pt: float = DEFAULT_TICK_LABEL_PT_CONCENTRATION,
    altitude_tick_label_pt: float = DEFAULT_TICK_LABEL_PT_ALTITUDE,
) -> tuple[Figure, Any]:
    """3D-effect waterfall plot for liquid and frozen PSD fields."""
    cmap = plt.get_cmap("jet") if cmap is None else cmap
    nf = prepared["nf"]
    nw = prepared["nw"]
    bounds = prepared["bounds"]
    diam = prepared["diam"]
    n_bands = prepared["n_bands"]
    idx_faint_start = prepared["idx_faint_start"]
    panel_data = prepared["panel_data"]
    has_altitude = prepared.get("has_altitude", True)
    alt_bands_used = prepared.get("alt_bands_used", alt_bands)
    log_zlim_lo = np.log10(zlim[0])

    n_panels = len(panel_data)
    n_rows = -(-n_panels // n_cols)
    fig_width = (FULL_COL_IN if n_cols > 1 else SINGLE_COL_IN) * PSD_WATERFALL_FIG_SCALE
    fig_height = min(fig_width * (n_rows / max(n_cols, 1)) * (2.8 / 4.5), MAX_H_IN) * PSD_WATERFALL_FIG_SCALE
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height), layout="constrained")

    for idx, ax in enumerate(np.atleast_1d(axes).flat):
        if idx >= n_panels:
            ax.set_visible(False)
            continue

        panel = panel_data[idx]
        t_lo, t_hi = panel["t_lo"], panel["t_hi"]

        for band in panel["bands"]:
            band_idx = band["idx"]
            y_off = band_idx * y_shift
            styles = make_phase_styles(band["color"])

            y_nw = np.where(band["nw_slab"] > zlim[0], np.log10(band["nw_slab"] + 1e-14) - log_zlim_lo + y_off, np.nan)
            y_nf = np.where(band["nf_slab"] > zlim[0], np.log10(band["nf_slab"] + 1e-14) - log_zlim_lo + y_off, np.nan)

            if show_small_diameters:
                ax.fill_between(band["d_proj"], y_off, y_nw, step="mid", **styles["nw"])
            else:
                faint_nw = dict(styles["nw"])
                faint_nw.update({"fc": (*styles["nw"]["fc"][:3], 0.025), "ec": (*styles["nw"]["ec"][:3], 0.05), "hatch": None})
                ax.fill_between(band["d_proj"][idx_faint_start:31], y_off, y_nw[idx_faint_start:31], step="mid", **faint_nw)
                ax.fill_between(band["d_proj"][30:], y_off, y_nw[30:], step="mid", **styles["nw"])

            ax.fill_between(band["d_proj"], y_off, y_nf, step="mid", **styles["nf"])

        if holimo_obs:
            overlay_holimo_obs(ax, holimo_obs, t_lo, t_hi, bounds[0], zlim, log_zlim_lo)

        draw_reference_grid(ax, n_bands, x_shift, y_shift)
        if show_cloud_bounds and has_altitude and n_bands > 1:
            draw_cloud_top_base(
                ax,
                nw,
                nf,
                t_lo,
                t_hi,
                cloud_thresh,
                alt_bands_used,
                diam,
                n_bands,
                x_shift,
                y_shift,
                idx_faint_start,
                zlim=zlim,
                xlim=xlim,
            )

        if idx % n_cols == 0:
            draw_concentration_scale(
                ax, xlim, zlim, n_bands, y_shift, tick_label_pt=concentration_tick_label_pt
            )

        if show_stats_table:
            add_stats_table(ax, panel["stats_rows"], basis=stats_basis)
        format_waterfall_panel(
            ax, idx, t_lo, t_hi, bounds[0], xlim, diameter_tick_label_pt=diameter_tick_label_pt
        )

    if has_altitude:
        add_altitude_colorbar(
            fig, axes, cmap, n_bands, alt_bands_used, tick_label_pt=altitude_tick_label_pt
        )
    fig.supylabel(y_label, fontsize=12)
    fig.supxlabel(r"Diameter / $\mu$m", fontsize=12)
    return fig, axes


def prepare_waterfall_inputs(
    ds_vertical: xr.Dataset,
    *,
    var_name_ice: str = "nf",
    var_name_liq: str = "nw",
) -> tuple[xr.DataArray, xr.DataArray]:
    """Prepare liquid and ice arrays and harmonize units."""
    plot_da_ice = xr.where(ds_vertical[var_name_ice] > 0, ds_vertical[var_name_ice], np.nan)
    plot_da_liquid = xr.where(ds_vertical[var_name_liq] > 0, ds_vertical[var_name_liq], np.nan) * 1e3
    if var_name_liq.lower().startswith("q"):
        plot_da_liquid.attrs["units"] = "g m^-3"
    else:
        plot_da_liquid.attrs["units"] = "L^-1"
    return plot_da_liquid, plot_da_ice


def _y_label_for_var_kind(var_kind: str) -> str:
    return r"Mass Concentration (at lowest altitude)" if var_kind.startswith("mass") else r"Number Concentration (at lowest altitude) / L$^{-1}$"


def _load_holimo_obs(holimo_file: Path, settings: PsdWaterfallSettings) -> list[dict[str, Any]]:
    ds_holimo, _, _ = load_and_prepare_holimo(str(holimo_file))
    ds_holimo = ds_holimo.sel(time=slice(*settings.holimo_window))
    ds_holimo = ds_holimo.assign_coords({"diameter": ds_holimo.diameter * 1e6})
    ds_hd10 = ds_holimo.resample(time="10s").mean()
    tfp = [[t[0], t[1]] for t in settings.time_frames_plume]
    return build_holimo_obs_series(ds_hd10, tfp, list(settings.obs_ids), list(settings.growth_times_min))


def load_psd_waterfall_context(
    repo_root: Path,
    *,
    processed_root: str | Path | None = None,
    holimo_file: str | Path | None = None,
    runs: list[dict[str, str]] | None = None,
    use_holimo: bool = False,
) -> dict[str, Any]:
    settings = build_psd_waterfall_settings(repo_root)
    pr_def, holimo_def, _ = settings.resolved_paths(repo_root)
    processed_root = pr_def if processed_root is None else Path(processed_root)
    holimo_file = holimo_def if holimo_file is None else Path(holimo_file)
    runs = list(settings.runs) if runs is None else runs

    datasets = load_plume_path_runs(runs, processed_root=processed_root, kinds=("integrated", "extreme"))
    kind = "extreme"
    try:
        xlim = build_common_xlim(datasets, kind=kind, span_min=35)
    except ValueError:
        xlim = [np.datetime64("2023-01-25T12:29:00"), np.datetime64("2023-01-25T13:04:00")]
    diag = diagnostics_table(datasets, kind=kind, variable="nf", xlim=xlim)

    cs_run_datasets = load_plume_path_runs(runs, processed_root=processed_root, kinds=("vertical", "integrated"))
    holimo_obs = _load_holimo_obs(holimo_file, settings) if use_holimo else None
    cfg = {run["label"]: {"flare_start_datetime": run.get("flare_start_datetime")} for run in runs if "label" in run}

    return {
        "repo_root": repo_root,
        "processed_root": Path(processed_root),
        "holimo_file": Path(holimo_file),
        "datasets": datasets,
        "diag": diag,
        "cs_run_datasets": cs_run_datasets,
        "cfg": cfg,
        "holimo_obs": holimo_obs,
        "output_root": waterfall_output_root(repo_root, settings),
        "psd_waterfall_settings": settings,
    }


def _output_paths(gfx_root: Path, var_kind: str, run_id: str) -> tuple[Path, Path, Path]:
    """Return (figure_path, table_path, stats_path) under output/gfx/png|tex|csv/04."""
    figure_path = gfx_root / "png" / "04" / f"figure13_psd_alt_time_{var_kind}_{run_id}.png"
    table_path = gfx_root / "tex" / "04" / f"figure13_psd_stats_{var_kind}_{run_id}.tex"
    stats_path = gfx_root / "csv" / "04" / f"figure13_psd_stats_{var_kind}_{run_id}.csv"
    return figure_path, table_path, stats_path


def render_psd_waterfall_case(
    context: dict[str, Any],
    run_label: str,
    var_kind: str,
    *,
    output_root: str | Path | None = None,
    col_wrap: int | None = None,
    show_cloud_bounds: bool = True,
    show_stats_table: bool = False,
    show_suptitle: bool | None = None,
    dpi: int = 400,
) -> dict[str, Any]:
    """Render one run/variable-kind combination and save figure plus LaTeX table."""
    settings: PsdWaterfallSettings = context.get("psd_waterfall_settings") or build_psd_waterfall_settings(
        context["repo_root"]
    )
    use_suptitle = settings.show_suptitle if show_suptitle is None else show_suptitle
    if var_kind not in settings.varsets:
        raise KeyError(f"Unknown var_kind '{var_kind}'. Valid: {', '.join(settings.varsets)}")

    n_cols = max(1, int(col_wrap)) if col_wrap is not None else settings.col_wrap

    run_ds = context["cs_run_datasets"][run_label]
    ds_case = run_ds.get("vertical", run_ds.get("integrated"))
    if ds_case is None:
        raise KeyError(f"No 'vertical' or 'integrated' dataset found for '{run_label}'")
    if "cell" in ds_case.dims:
        ds_case = ds_case.sum("cell")

    var_l, var_i, z_lo, z_hi, x_lo, x_hi, show_small_diameters = settings.varsets[var_kind]
    basis = _basis_for_var_kind(var_kind)
    plot_da_liquid, plot_da_ice = prepare_waterfall_inputs(ds_case, var_name_liq=var_l, var_name_ice=var_i)
    prepared = prepare_psd_waterfall_data(
        plot_da_liquid,
        plot_da_ice,
        time_windows=settings.time_windows,
        t0=settings.model_seed,
        alt_bands=settings.alt_bands,
        da_w=None,
        cmap=default_psd_waterfall_cmap(),
        x_shift=-0.1,
        min_diam=0.01,
    )
    fig, _ = plot_psd_waterfall(
        prepared,
        alt_bands=settings.alt_bands,
        zlim=(z_lo, z_hi),
        xlim=(x_lo, x_hi),
        cmap=default_psd_waterfall_cmap(),
        n_cols=n_cols,
        y_shift=-0.7,
        x_shift=-0.2,
        holimo_obs=context.get("holimo_obs"),
        show_cloud_bounds=show_cloud_bounds,
        show_stats_table=show_stats_table,
        show_small_diameters=show_small_diameters,
        y_label=_y_label_for_var_kind(var_kind),
        stats_basis=basis,
        diameter_tick_label_pt=settings.tick_label_pt_diameter,
        concentration_tick_label_pt=settings.tick_label_pt_concentration,
        altitude_tick_label_pt=settings.tick_label_pt_altitude,
    )

    run_id = ds_case.attrs.get(
        "run_id", next((run.get("exp_id") for run in settings.runs if run["label"] == run_label), run_label)
    )
    if use_suptitle:
        fig.suptitle(
            f"PSD altitude–time evolution ({var_kind}): liquid and frozen along plume path — {run_id}",
            fontsize=13,
            weight="semibold",
        )

    gfx_root = context["output_root"] if output_root is None else Path(output_root)
    figure_path, table_path, stats_path = _output_paths(Path(gfx_root), var_kind, run_id)
    figure_path.parent.mkdir(parents=True, exist_ok=True)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(figure_path, bbox_inches="tight", dpi=dpi)

    stats_df = build_psd_stats_dataframe(
        prepared,
        var_kind=var_kind,
        run_id=run_id,
        run_label=run_label,
        holimo_obs=context.get("holimo_obs"),
    )
    stats_csv_path = save_psd_stats_csv(stats_df, stats_path)
    tex_path = save_latex_table(
        prepared,
        table_path,
        var_kind=var_kind,
        run_id=run_id,
        run_label=run_label,
        holimo_obs=context.get("holimo_obs"),
    )

    return {
        "run_label": run_label,
        "run_id": run_id,
        "var_kind": var_kind,
        "prepared": prepared,
        "stats_df": stats_df,
        "stats_csv_path": stats_csv_path,
        "figure_path": figure_path,
        "table_path": tex_path,
        "figure": fig,
    }


def render_all_psd_waterfall_cases(
    context: dict[str, Any],
    *,
    plot_kinds: tuple[str, ...] | None = None,
    run_labels: list[str] | None = None,
    output_root: str | Path | None = None,
    col_wrap: int | None = None,
    show_cloud_bounds: bool = True,
    show_stats_table: bool = False,
    show_suptitle: bool | None = None,
    dpi: int = 400,
) -> list[dict[str, Any]]:
    """Render and save all requested run/variable-kind combinations."""
    settings: PsdWaterfallSettings = context.get("psd_waterfall_settings") or build_psd_waterfall_settings(
        context["repo_root"]
    )
    if plot_kinds is None:
        plot_kinds = settings.plot_kinds
    run_labels = list(context["cs_run_datasets"].keys()) if run_labels is None else run_labels
    outputs = []
    for var_kind in plot_kinds:
        for run_label in run_labels:
            outputs.append(
                render_psd_waterfall_case(
                    context,
                    run_label,
                    var_kind,
                    output_root=output_root,
                    col_wrap=col_wrap,
                    show_cloud_bounds=show_cloud_bounds,
                    show_stats_table=show_stats_table,
                    show_suptitle=show_suptitle,
                    dpi=dpi,
                )
            )
    return outputs
