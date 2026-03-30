"""Spectral waterfall renderer: PNG frames, MP4, optional growth footer.

Core module for ``run_spectral_waterfall`` CLI wrappers and notebooks. Default config path uses
``REPO_ROOT`` (repository root: ``src/utilities/`` → parents[2]).
"""
from __future__ import annotations

import argparse
import csv
import glob
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

from utilities import (  # noqa: E402
    MAX_H_IN,
    MM,
    PROCESS_PLOT_ORDER,
    SINGLE_COL_IN,
    apply_publication_style,
    build_fixed_legend,
    first_plume_ridge_anchor,
    format_elapsed_minutes_tick,
    load_process_budget_data,
    merge_liq_ice_net,
    normalize_net_stacks,
    panel_process_values,
    panel_concentration_profile,
    ridge_process_values,
    ridge_concentration_profile,
    ridge_window_field_mean,
    ridge_window_stats,
    proc_color,
    proc_hatch,
    stn_label,
)
from utilities.compute_fabric import is_server  # noqa: E402
from utilities.table_paths import spectral_growth_output_paths, sync_file  # noqa: E402

# CSV columns → Zarr names (first match per experiment). Empty tuple = filled only by derivation / YAML.
# Verified against Meteogram_*_nVar136_*.zarr (COSMO-SPECS): ``T``, ``W`` (half levels), ``VW``/``VF``
# spectral fall speeds; supersaturation not stored — derived from ``QV``, ``T``, ``PML`` when present.
_GROWTH_ENV_DEFAULTS: dict[str, tuple[str, ...]] = {
    "T_ridge_K": ("T", "TT", "TABS"),
    "w_ridge_m_s": ("W", "WW"),
    "S_wat_ridge": (),
    "S_ice_ridge": (),
    "vfall_liq_ridge_m_s": ("VW",),
    "vfall_ice_ridge_m_s": ("VF",),
}


def _resolve_ds_var(ds_exp: xr.Dataset, candidates: tuple[str, ...]) -> Optional[xr.DataArray]:
    for name in candidates:
        if name in ds_exp.data_vars:
            return ds_exp[name]
        hits = [k for k in ds_exp.data_vars if str(k).upper() == name.upper()]
        if hits:
            return ds_exp[hits[0]]
    return None


def _qs_mixing_liquid_kgkg(T_k: xr.DataArray, p_hpa: xr.DataArray) -> xr.DataArray:
    """Saturation mixing ratio (liquid) from Tetens; *T_k* in K, *p_hpa* in hPa."""

    def _core(t: np.ndarray, p: np.ndarray) -> np.ndarray:
        t = np.asarray(t, dtype=float)
        p = np.asarray(p, dtype=float)
        Tc = t - 273.15
        es = 6.112 * np.exp((17.67 * Tc) / (Tc + 243.5))
        es = np.clip(es, 1e-9, None)
        return 0.62198 * es / np.maximum(p - es, 1e-9)

    return xr.apply_ufunc(_core, T_k, p_hpa, dask="allowed")


def _qs_mixing_ice_kgkg(T_k: xr.DataArray, p_hpa: xr.DataArray) -> xr.DataArray:
    """Saturation mixing ratio (ice) — Magnus-style; *T_k* in K, *p_hpa* in hPa."""

    def _core(t: np.ndarray, p: np.ndarray) -> np.ndarray:
        t = np.asarray(t, dtype=float)
        p = np.asarray(p, dtype=float)
        Tc = t - 273.15
        es = 6.112 * np.exp((21.8743003 * Tc) / (Tc + 265.49))
        es = np.clip(es, 1e-9, None)
        return 0.62198 * es / np.maximum(p - es, 1e-9)

    return xr.apply_ufunc(_core, T_k, p_hpa, dask="allowed")


def _derived_supersaturation_qv(ds_exp: xr.Dataset) -> dict[str, xr.DataArray]:
    """Return ``S_wat_ridge`` / ``S_ice_ridge`` fields as (qv/qs - 1) on ``height_level``."""
    need = ("QV", "T", "PML")
    if not all(k in ds_exp.data_vars for k in need):
        return {}
    T, qv, p = ds_exp["T"], ds_exp["QV"], ds_exp["PML"]
    if T.dims != qv.dims or T.dims != p.dims:
        return {}
    qs_l = _qs_mixing_liquid_kgkg(T, p)
    qs_i = _qs_mixing_ice_kgkg(T, p)
    return {"S_wat_ridge": (qv / qs_l) - 1.0, "S_ice_ridge": (qv / qs_i) - 1.0}


def _growth_env_fields(ds_exp: xr.Dataset, sw_cfg: dict[str, Any]) -> dict[str, xr.DataArray]:
    if not sw_cfg.get("growth_csv_include_environment", True):
        return {}
    umap = sw_cfg.get("growth_csv_env_field_map")
    out: dict[str, xr.DataArray] = {}
    for col, default_cands in _GROWTH_ENV_DEFAULTS.items():
        if isinstance(umap, dict) and col in umap and umap[col]:
            raw = umap[col]
            cands = tuple(raw) if isinstance(raw, (list, tuple)) else (str(raw),)
        else:
            cands = default_cands
        if not cands:
            continue
        da = _resolve_ds_var(ds_exp, cands)
        if da is not None:
            out[col] = da
    if sw_cfg.get("growth_csv_derive_supersaturation", True):
        for k, da in _derived_supersaturation_qv(ds_exp).items():
            if k not in out:
                out[k] = da
    return out


@dataclass(frozen=True)
class GrowthOverlay:
    """Per-station growth metrics for one animation frame (ridge mode)."""

    z_ridge_m: float
    z_anchor_m: Optional[float]  # pre-descent ridge reference (CSV column ``z_anchor_m``; figure label)
    d_liq_um: float
    d_ice_um: float
    d_liq_um_s: float
    d_ice_um_s: float
    ice_ok: bool
    n_regress: int


def _sorted_frame_paths(pattern: str, *, expected_count: int | None = None) -> list[str]:
    parsed: list[tuple[int, str]] = []
    for path_str in glob.glob(pattern):
        stem = Path(path_str).stem
        suffix = stem.split("_itime")[-1]
        try:
            idx = int(suffix)
            if expected_count is not None and not (0 <= idx < expected_count):
                continue
            parsed.append((idx, path_str))
        except ValueError:
            continue
    return [path_str for _, path_str in sorted(parsed, key=lambda item: item[0])]


def _remove_existing_frames(pattern: str) -> int:
    removed = 0
    for path_str in glob.glob(pattern):
        try:
            Path(path_str).unlink()
            removed += 1
        except FileNotFoundError:
            continue
    return removed


def spectral_mean_diameter(diameter_um: np.ndarray, conc_arr: np.ndarray) -> float:
    """Spectral number- or mass-weighted mean diameter (µm); NaN if no mass in spectrum."""
    d = np.asarray(diameter_um, dtype=float)
    w = np.asarray(conc_arr, dtype=float)
    s = float(np.nansum(w))
    if s <= 0.0 or not np.isfinite(s):
        return float("nan")
    return float(np.nansum(d * w) / s)


def _window_mid_epoch_sec(t0: np.datetime64, t_lo: np.datetime64, t_hi: np.datetime64) -> float:
    """Seconds from *t0* to midpoint of [t_lo, t_hi]."""
    lo = np.datetime64(t_lo, "ns").astype(np.int64)
    hi = np.datetime64(t_hi, "ns").astype(np.int64)
    t0i = np.datetime64(t0, "ns").astype(np.int64)
    mid = 0.5 * (lo + hi)
    return float(mid - t0i) / 1.0e9


def _growth_footer_trail(
    ser: dict[str, np.ndarray], key: str, end: int, *, mask_ok: bool = False
) -> tuple[np.ndarray, np.ndarray]:
    """Slice ``ser`` to ``0..end`` (inclusive); return finite (t_mid, y) pairs, optional ``ice_ok`` mask."""
    t = np.asarray(ser["t_mid"][: end + 1], dtype=float)
    y = np.asarray(ser[key][: end + 1], dtype=float)
    m = np.isfinite(t) & np.isfinite(y)
    if mask_ok:
        m &= np.asarray(ser["ice_ok"][: end + 1], dtype=bool)
    return t[m], y[m]


def _growth_slope_um_s(t_sec: np.ndarray, d_um: np.ndarray) -> tuple[float, int]:
    """Linear regression slope d/dt (µm/s); needs >=2 finite pairs."""
    t_sec = np.asarray(t_sec, dtype=float)
    d_um = np.asarray(d_um, dtype=float)
    ok = np.isfinite(t_sec) & np.isfinite(d_um)
    n = int(np.sum(ok))
    if n < 2:
        return float("nan"), n
    t = t_sec[ok]
    d = d_um[ok]
    coeffs = np.polyfit(t, d, 1)
    return float(coeffs[0]), n


def _pre_descent_ridge_reference(z_raw: np.ndarray) -> tuple[float, int]:
    """Pre-descent plateau height and first index where raw ridge height is shown.

    *z_ref_m* is the maximum finite raw ridge height strictly before the first
    sustained descent: two consecutive finite frames both strictly below the
    running maximum of all **previous** finite samples. If no descent occurs,
    *z_ref_m* is the global finite maximum and the returned start index is *n*
    (entire series may be pinned to *z_ref_m*).

    Returns
    -------
    z_ref_m, start_raw_idx
        Display ``z_m[it] = z_ref_m`` for ``it < start_raw_idx``, else ``z_raw[it]``.
    """
    z = np.asarray(z_raw, dtype=float)
    n = int(z.size)
    if n == 0:
        return float("nan"), 0
    fin = np.isfinite(z)
    if not np.any(fin):
        return float("nan"), n

    descent_start = n
    for it in range(n - 1):
        if not fin[it] or not fin[it + 1]:
            continue
        prev = z[:it]
        prev_fin = prev[np.isfinite(prev)]
        if prev_fin.size == 0:
            continue
        run_max_before = float(np.max(prev_fin))
        if z[it] < run_max_before and z[it + 1] < run_max_before:
            descent_start = it
            break

    if descent_start == n:
        z_ref = float(np.nanmax(z))
    else:
        head = z[:descent_start]
        head_fin = head[np.isfinite(head)]
        z_ref = float(np.nanmax(head_fin)) if head_fin.size > 0 else float("nan")
    return z_ref, descent_start


def _apply_tiny_ice_mask_for_growth(
    *,
    kind: str,
    sw_cfg: dict[str, Any],
    tmid_sec: float,
    sum_ice: float,
    arr: dict[str, Any],
    it: int,
) -> None:
    """Drop ice mean diameter when ridge-integrated ice is below a noise floor (optional time window).

    Mass- or number-weighted ⟨D⟩ uses ∑(D·c)/∑c; when ∑c is tiny (spin-up residue, float noise),
    a few large-bin counts can yield spurious ⟨D⟩ ≫ 1 µm though there is effectively no ice.

    If ``growth_ice_mask_until_min`` is set, the mask applies only while *t_mid* (minutes from
    ``time_window[0]``) is strictly below that value; if unset, the floor applies at all times.
    """
    until = sw_cfg.get("growth_ice_mask_until_min")
    if until is not None and np.isfinite(float(until)) and (tmid_sec / 60.0) >= float(until):
        return
    if str(kind).upper() == "Q":
        fl = float(sw_cfg.get("growth_ice_sum_floor_q", 1e-6))
    else:
        fl = float(sw_cfg.get("growth_ice_sum_floor_n", 1e3))
    if not np.isfinite(sum_ice) or sum_ice >= fl:
        return
    arr["D_ice"][it] = float("nan")
    arr["sum_ice"][it] = float("nan")
    arr["ice_ok"][it] = False


def _precompute_growth_overlays(
    *,
    plot_exp_ids: list[int],
    plot_range_keys: list[str],
    station_ids: list[int],
    time_window: list[np.datetime64],
    cfg: dict[str, Any],
    kind: str,
    sw_cfg: dict[str, Any],
    ridge_context: dict[tuple[int, str], dict[int, tuple[int, int, float]]],
    ds: Optional[xr.Dataset] = None,
) -> tuple[
    dict[tuple[int, str, int], dict[int, GrowthOverlay]],
    list[dict[str, Any]],
    dict[tuple[int, str], dict[int, dict[str, np.ndarray]]],
    dict[tuple[int, str], dict[int, float]],
]:
    """Build overlays/CSV rows; optional per-station series for the growth footer strip.

    Raw per-window ridge mean height ``z_m`` (from ``ridge_window_stats``) is post-processed:
    a **pre-descent reference** height is the max raw height before the first sustained
    descent (two consecutive frames strictly below the running max of all prior frames).
    ``z_m`` is pinned to that reference from ``itime=0`` until descent begins, so the
    footer/CSV ``z_ridge_m`` starts at the plateau (not low-level argmax noise). The same
    reference is returned for the figure label ``Ice plume ridge (… m)``.

    Ice ⟨D⟩ / Σ ice in the footer are optionally cleared when ridge-integrated ice mass (Q) or
    number (N) is below YAML ``growth_ice_sum_floor_*`` — see ``_apply_tiny_ice_mask_for_growth``.
    """
    use_ridge = bool(sw_cfg.get("use_plume_ridge", True))
    want_overlay_csv = use_ridge and (
        sw_cfg.get("show_growth_textbox", False) or sw_cfg.get("write_growth_csv", False)
    )
    want_footer = use_ridge and bool(
        sw_cfg.get("show_growth_footer", False) or sw_cfg.get("show_growth_sparkline", False)
    )
    if not (want_overlay_csv or want_footer):
        return {}, [], {}, {}

    floor = float(sw_cfg.get("ridge_mass_floor", 0.0))
    K = int(sw_cfg.get("growth_rate_window_frames", 3))
    K = max(2, min(4, K))
    mode = str(sw_cfg.get("growth_rate_mode", "regression")).lower()
    t0 = time_window[0]
    nfr = len(time_window) - 1
    out: dict[tuple[int, str, int], dict[int, GrowthOverlay]] = {}
    csv_rows: list[dict[str, Any]] = []
    series_cache: dict[tuple[int, str], dict[int, dict[str, np.ndarray]]] = {}
    series_keys = (
        "t_mid",
        "z_m",
        "D_liq",
        "D_ice",
        "ice_ok",
        "sum_liq",
        "sum_ice",
        "d_liq_dt",
        "d_ice_dt",
    )
    ridge_ref_height_m: dict[tuple[int, str], dict[int, float]] = {}

    for eid in plot_exp_ids:
        r = cfg["rates_by_exp"][eid]
        rcf = r.get("spec_conc_Q_F")
        spec_w = r.get(f"spec_conc_{kind}_W")
        spec_f = r.get(f"spec_conc_{kind}_F")
        if rcf is None or spec_w is None or spec_f is None:
            continue

        ds_exp = ds.isel(expname=eid) if ds is not None else None
        env_fields = _growth_env_fields(ds_exp, sw_cfg) if ds_exp is not None else {}

        for range_key in plot_range_keys:
            ctx = ridge_context.get((eid, range_key), {})
            bin_slice = cfg["size_ranges"][range_key]["slice"]
            d = np.asarray(cfg["diameter_um"][bin_slice])

            by_stn: dict[int, dict[str, np.ndarray]] = {
                stn: {
                    "t_mid": np.zeros(nfr, dtype=float),
                    "D_liq": np.full(nfr, np.nan, dtype=float),
                    "D_ice": np.full(nfr, np.nan, dtype=float),
                    "z_m": np.full(nfr, np.nan, dtype=float),
                    "ice_ok": np.zeros(nfr, dtype=bool),
                    "sum_liq": np.full(nfr, np.nan, dtype=float),
                    "sum_ice": np.full(nfr, np.nan, dtype=float),
                    "d_liq_dt": np.full(nfr, np.nan, dtype=float),
                    "d_ice_dt": np.full(nfr, np.nan, dtype=float),
                }
                for stn in station_ids
            }
            for it in range(nfr):
                tw = slice(time_window[it], time_window[it + 1])
                tmid = _window_mid_epoch_sec(t0, time_window[it], time_window[it + 1])
                for stn in station_ids:
                    r_anch = (ctx[stn][0], ctx[stn][1]) if stn in ctx else None
                    z_m, iok = ridge_window_stats(rcf, stn, tw, bin_slice, floor=floor, ridge_anchor=r_anch)
                    cl = ridge_concentration_profile(
                        spec_w, rcf, stn, tw, bin_slice, floor=floor, ridge_anchor=r_anch
                    )
                    ci = ridge_concentration_profile(
                        spec_f, rcf, stn, tw, bin_slice, floor=floor, ridge_anchor=r_anch
                    )
                    arr = by_stn[stn]
                    arr["t_mid"][it] = tmid
                    arr["z_m"][it] = z_m
                    arr["ice_ok"][it] = iok
                    if cl.size == len(d):
                        arr["D_liq"][it] = spectral_mean_diameter(d, cl)
                        arr["sum_liq"][it] = float(np.nansum(cl))
                    if ci.size == len(d):
                        s_ice = float(np.nansum(ci))
                        arr["sum_ice"][it] = s_ice
                        arr["D_ice"][it] = spectral_mean_diameter(d, ci)
                        _apply_tiny_ice_mask_for_growth(
                            kind=kind,
                            sw_cfg=sw_cfg,
                            tmid_sec=float(tmid),
                            sum_ice=s_ice,
                            arr=arr,
                            it=it,
                        )

            per_stn_ref: dict[int, float] = {}
            for stn in station_ids:
                arr = by_stn[stn]
                z_raw = np.asarray(arr["z_m"], dtype=float).copy()
                z_ref, pin_end = _pre_descent_ridge_reference(z_raw)
                if np.isfinite(z_ref):
                    arr["z_m"][:pin_end] = z_ref
                    per_stn_ref[stn] = float(z_ref)
                else:
                    per_stn_ref[stn] = float("nan")

            ridge_ref_height_m[(eid, range_key)] = per_stn_ref

            for stn in station_ids:
                arr = by_stn[stn]
                z_ref = per_stn_ref.get(stn, float("nan"))
                for it in range(nfr):
                    tw = slice(time_window[it], time_window[it + 1])
                    i0 = max(0, it - K + 1)
                    t_win = arr["t_mid"][i0 : it + 1]
                    if mode == "last_interval":
                        if it > 0:
                            dt = arr["t_mid"][it] - arr["t_mid"][it - 1]
                            s_liq = (
                                (arr["D_liq"][it] - arr["D_liq"][it - 1]) / dt
                                if dt > 0 and np.isfinite(dt)
                                else float("nan")
                            )
                            s_ice = (
                                (arr["D_ice"][it] - arr["D_ice"][it - 1]) / dt
                                if dt > 0 and np.isfinite(dt)
                                else float("nan")
                            )
                            n_reg = 2
                        else:
                            s_liq, s_ice, n_reg = float("nan"), float("nan"), 0
                    else:
                        s_liq, n_l = _growth_slope_um_s(t_win, arr["D_liq"][i0 : it + 1])
                        s_ice, _ = _growth_slope_um_s(t_win, arr["D_ice"][i0 : it + 1])
                        n_reg = n_l
                    if not arr["ice_ok"][it]:
                        s_ice = float("nan")
                    arr["d_liq_dt"][it] = float(s_liq)
                    arr["d_ice_dt"][it] = float(s_ice)
                    if not want_overlay_csv:
                        continue
                    key = (eid, range_key, it)
                    if key not in out:
                        out[key] = {}
                    out[key][stn] = GrowthOverlay(
                        z_ridge_m=float(arr["z_m"][it]),
                        z_anchor_m=z_ref if np.isfinite(z_ref) else None,
                        d_liq_um=float(arr["D_liq"][it]),
                        d_ice_um=float(arr["D_ice"][it]),
                        d_liq_um_s=float(s_liq),
                        d_ice_um_s=float(s_ice),
                        ice_ok=bool(arr["ice_ok"][it]),
                        n_regress=int(n_reg),
                    )
                    row_env: dict[str, Any] = {}
                    if env_fields:
                        r_anch = (ctx[stn][0], ctx[stn][1]) if stn in ctx else None
                        for col, da in env_fields.items():
                            row_env[col] = ridge_window_field_mean(
                                da,
                                rcf,
                                stn,
                                tw,
                                bin_slice,
                                floor=floor,
                                ridge_anchor=r_anch,
                            )
                    csv_rows.append(
                        {
                            "exp_id": eid,
                            "range_key": range_key,
                            "itime": it,
                            "t_lo": str(time_window[it]),
                            "t_hi": str(time_window[it + 1]),
                            "t_mid_sec_from_start": arr["t_mid"][it],
                            "station": stn,
                            "z_ridge_m": arr["z_m"][it],
                            "z_anchor_m": z_ref if np.isfinite(z_ref) else "",
                            "D_liq_um": arr["D_liq"][it],
                            "D_ice_um": arr["D_ice"][it],
                            "dD_liq_dt_um_s": s_liq,
                            "dD_ice_dt_um_s": s_ice,
                            "ice_ok": arr["ice_ok"][it],
                            "n_regress_pts": n_reg,
                            **row_env,
                        }
                    )

            if want_footer:
                series_cache[(eid, range_key)] = {
                    stn: {k: by_stn[stn][k].copy() for k in series_keys} for stn in station_ids
                }

    return out, csv_rows, series_cache, ridge_ref_height_m


# ── Plotting ─────────────────────────────────────────────────────────────────

def _panel_height_ratios(cfg_plot: dict[str, Any]) -> tuple[float, float, float]:
    """PSD : liquid : ice row height ratios (identical for every station column)."""
    raw = cfg_plot.get("panel_height_ratios", (1.5, 2.3, 2.3))
    if isinstance(raw, (list, tuple)) and len(raw) == 3:
        a, b, c = (float(x) for x in raw)
        return (a, b, c)
    return (1.5, 2.3, 2.3)


def _apply_spine_tick_style(ax: Any, cfg_plot: dict[str, Any], *, y_label_column: bool) -> None:
    """Shared minimal axis chrome: thin neutral spines, outward ticks, y labels on outer column only."""
    lw = float(cfg_plot.get("axis_spine_linewidth", 0.6))
    edge_c = str(cfg_plot.get("axis_spine_color", "0.35"))
    tpt = float(cfg_plot.get("axis_tick_label_pt", 7.0))
    tlen = float(cfg_plot.get("axis_tick_length", 3.5))
    tw = float(cfg_plot.get("axis_tick_width", 0.55))
    for side in ("left", "right"):
        sp = ax.spines[side]
        sp.set_visible(True)
        sp.set_linewidth(lw)
        sp.set_edgecolor(edge_c)
    ax.tick_params(
        axis="both",
        which="both",
        direction="out",
        length=tlen,
        width=tw,
        labelsize=tpt,
    )
    if not y_label_column:
        ax.tick_params(axis="y", labelleft=False, labelright=False)


def _annotation_box_style(
    cfg_plot: dict[str, Any],
    *,
    alpha: float | None = None,
    edgecolor: str | None = None,
    linewidth: float | None = None,
) -> dict[str, Any]:
    """Shared translucent white annotation box used across PSD, rate, and footer panels."""
    pad = float(cfg_plot.get("panel_annotation_pad", 0.12))
    box_alpha = float(cfg_plot.get("panel_annotation_alpha", 0.55) if alpha is None else alpha)
    return {
        "facecolor": str(cfg_plot.get("annotation_box_facecolor", "white")),
        "edgecolor": str(cfg_plot.get("annotation_box_edgecolor", "none") if edgecolor is None else edgecolor),
        "linewidth": float(cfg_plot.get("annotation_box_linewidth", 0.0) if linewidth is None else linewidth),
        "alpha": box_alpha,
        "boxstyle": f"round,pad={pad}",
    }


def _major_grid(ax: Any, cfg_plot: dict[str, Any]) -> None:
    """One grid recipe for rate panels and footer (color/line from cfg)."""
    ax.grid(
        True,
        which="major",
        linestyle=str(cfg_plot.get("grid_linestyle", "-")),
        linewidth=float(cfg_plot.get("grid_linewidth", 0.2)),
        color=str(cfg_plot.get("grid_color", "0.82")),
        alpha=float(cfg_plot.get("grid_alpha", 0.38)),
    )


def _sparse_major_tick_formatter(ax: Any):
    """Show all major labels when sparse; otherwise every other one to limit collisions."""
    from matplotlib.ticker import FuncFormatter

    def _fmt(y: float, _pos: Any) -> str:
        lo, hi = ax.get_ylim()
        if lo > hi:
            lo, hi = hi, lo
        ticks: list[float] = []
        for tick in ax.get_yticks():
            if not np.isfinite(tick):
                continue
            if ax.get_yscale() == "log" and tick <= 0:
                continue
            if lo <= tick <= hi:
                ticks.append(float(tick))
        if not ticks:
            return ""
        visible = ticks if len(ticks) <= 5 else ticks[::2]
        if len(visible) < 2 and len(ticks) >= 2:
            visible = [ticks[0], ticks[-1]]
        return f"{y:g}" if any(np.isclose(y, t) for t in visible) else ""

    return FuncFormatter(_fmt)


def _legend_style(cfg_plot: dict[str, Any], *, fontsize: float | None = None) -> dict[str, Any]:
    """Compact legend frame styling for a clean scientific layout."""
    font_pt = float(cfg_plot.get("legend_font_pt", 6.4) if fontsize is None else fontsize)
    return {
        "fontsize": font_pt,
        "frameon": True,
        "framealpha": float(cfg_plot.get("legend_frame_alpha", 0.82)),
        "edgecolor": str(cfg_plot.get("legend_edge_color", "0.75")),
    }


def _apply_figure_layout(fig: Any, cfg_plot: dict[str, Any], *, show_footer: bool, use_cmap_labels: bool) -> None:
    """Apply one figure-level margin recipe; inner spacing stays in the GridSpec config."""
    right_key = "figure_margin_right_cmap" if use_cmap_labels else "figure_margin_right"
    bottom_key = "figure_margin_bottom_footer" if show_footer else "figure_margin_bottom"
    fig.subplots_adjust(
        left=float(cfg_plot.get("figure_margin_left", 0.085)),
        right=float(cfg_plot.get(right_key, 0.82 if use_cmap_labels else 0.98)),
        top=float(cfg_plot.get("figure_margin_top", 0.88)),
        bottom=float(cfg_plot.get(bottom_key, 0.17 if show_footer else 0.12)),
    )


def _main_block_labels(
    fig: Any,
    axes_psd: np.ndarray,
    axes_liq: np.ndarray,
    axes_ice: np.ndarray,
    *,
    x_label: str,
    y_label: str,
    cfg_plot: dict[str, Any],
) -> None:
    """Place shared x/y labels relative to the main PSD+rate block, not the footer."""
    axes_all = [ax for arr in (axes_psd, axes_liq, axes_ice) for ax in arr.flat if ax.get_visible()]
    if not axes_all:
        return
    left = min(ax.get_position().x0 for ax in axes_all)
    right = max(ax.get_position().x1 for ax in axes_all)
    bottom = min(ax.get_position().y0 for ax in axes_all)
    top = max(ax.get_position().y1 for ax in axes_all)
    label_pt = float(cfg_plot.get("figure_label_pt", 8.0))
    xlabel_y = max(0.02, bottom - float(cfg_plot.get("main_xlabel_pad", 0.03)))
    ylabel_x = max(0.006, left - float(cfg_plot.get("main_ylabel_pad", 0.08)))
    fig.text(0.5 * (left + right), xlabel_y, x_label, ha="center", va="top", fontsize=label_pt, color="0.25")
    fig.text(ylabel_x, 0.5 * (top + bottom), y_label, rotation=90, ha="center", va="center",
             fontsize=label_pt, color="0.25")


def _draw_bars(ax, d, widths, order, net_merged, cfg_plot):
    """Stacked bar chart: positive up, negative down."""
    n = len(d)
    any_data = False
    bottom_pos, bottom_neg = np.zeros(n), np.zeros(n)
    for p in order:
        c, net_arr = net_merged[p]
        pos_part, neg_part = np.maximum(0.0, net_arr), np.minimum(0.0, net_arr)
        h = proc_hatch(p)
        if np.any(pos_part > 0):
            ax.bar(d, pos_part, width=widths, bottom=bottom_pos, color=c,
                   edgecolor=cfg_plot["bar_edge_color"], linewidth=cfg_plot["bar_edge_linewidth"],
                   alpha=cfg_plot["pos_alpha"], hatch=h)
            bottom_pos += pos_part
            any_data = True
        if np.any(neg_part < 0):
            ax.bar(d, neg_part, width=widths, bottom=bottom_neg, color=c,
                   edgecolor=cfg_plot["bar_edge_color"], linewidth=cfg_plot["bar_edge_linewidth"],
                   alpha=cfg_plot["neg_alpha"], hatch=h)
            bottom_neg += neg_part
            any_data = True
    return any_data


def _draw_lines(ax, d, order, net_merged, cfg_plot):
    """Line + fill_between: white underlay, then fill (alpha 0.25), then colored line (alpha 0.7) on top."""
    any_data = False
    fill_alpha = cfg_plot.get("fill_alpha", 0.25)
    line_alpha = cfg_plot.get("line_alpha", 0.7)
    white_lw = cfg_plot.get("rate_white_linewidth", 2.5)
    color_lw = cfg_plot.get("rate_color_linewidth", 0.8)
    black_lw = cfg_plot.get("rate_black_linewidth", 0.5)
    for p in order:
        c, net_arr = net_merged[p]
        if not np.any(np.abs(net_arr) > 0):
            continue
        any_data = True
        ax.plot(d, net_arr, color="white", linewidth=white_lw, linestyle="-", zorder=1)
        ax.fill_between(d, 0, net_arr, color=c, alpha=fill_alpha, linewidth=color_lw, zorder=2)
        ax.plot(d, net_arr, color="black", linewidth=black_lw, linestyle="-", alpha=line_alpha, zorder=3)
    return any_data


def _draw_steps(ax, d, order, net_merged, cfg_plot):
    """Step + fill_between: white underlay, then fill (alpha 0.25), then colored step line (alpha 0.7) on top."""
    any_data = False
    fill_alpha = cfg_plot.get("fill_alpha", 0.25)
    line_alpha = cfg_plot.get("line_alpha", 0.7)
    white_lw = cfg_plot.get("rate_white_linewidth", 2.5)
    color_lw = cfg_plot.get("rate_color_linewidth", 0.8)
    black_lw = cfg_plot.get("rate_black_linewidth", 0.5)
    for p in order:
        c, net_arr = net_merged[p]
        if not np.any(np.abs(net_arr) > 0):
            continue
        any_data = True
        ax.step(d, net_arr, color="white", where="mid", linewidth=white_lw, zorder=1)
        ax.fill_between(d, 0, net_arr, color=c, alpha=fill_alpha, step="mid", linewidth=color_lw, zorder=2)
        ax.step(d, net_arr, color="black", where="mid", linewidth=black_lw, alpha=line_alpha, zorder=3)
    return any_data


def _draw_phase(ax, plot_style, d, widths, order, net_map, cfg_plot):
    """Dispatch to bars/lines/steps based on plot_style."""
    if plot_style == "lines":
        any_data = _draw_lines(ax, d, order, net_map, cfg_plot)
    elif plot_style == "steps":
        any_data = _draw_steps(ax, d, order, net_map, cfg_plot)
    else:
        any_data = _draw_bars(ax, d, widths, order, net_map, cfg_plot)

    if any_data:
        for p in order:
            c, net_arr = net_map[p]
            if not np.any(np.abs(net_arr) > 0):
                continue
            mean_rate = float(np.nanmean(net_arr))
            if not np.isfinite(mean_rate):
                continue
            y0, y1 = ax.get_ylim()
            if y0 > y1:
                y0, y1 = y1, y0
            if np.isfinite(y0) and np.isfinite(y1):
                mean_rate = float(np.clip(mean_rate, y0, y1))
            # Triangle points left, vertex aligned with right end of y-axis major tick
            ax.plot(
                0.985,
                mean_rate,
                marker=">",
                color=c,
                transform=ax.get_yaxis_transform(),
                clip_on=False,
                markersize=5,
                markeredgecolor="black",
                markeredgewidth=0.4,
                zorder=10,
            )

    return any_data


def _plot_psd_mean_triangle(ax, x, color, *, markersize=6, y_offset_pt=-6.0) -> None:
    """Place an upward triangle below the PSD axis with its tip aligned to the tick end."""
    # Data x (µm), axes y: y=0 is bottom of panel — not log *data* y (log-y + y=0 → overflow in InvertedLogTransform).
    trans = ax.get_xaxis_transform() + matplotlib.transforms.ScaledTranslation(
        0.0,
        y_offset_pt / 72.0,
        ax.figure.dpi_scale_trans,
    )
    ax.plot(
        x,
        0.0,
        marker="^",
        color=color,
        transform=trans,
        clip_on=False,
        markersize=markersize,
        markeredgecolor="black",
        markeredgewidth=0.5,
        zorder=10,
    )


def _plot_psd_max_triangle(ax, y, color, *, markersize=6) -> None:
    """Place a phase-colored triangle on the PSD y-axis spine (use twinx ax); same recipe as rate means (vertex at tick)."""
    y_draw = float(y)
    if ax.get_yscale() == "log" and np.isfinite(y_draw) and y_draw > 0:
        lo, hi = ax.get_ylim()
        if lo > 0 and hi > lo:
            y_draw = float(np.clip(y_draw, lo * 1.0000001, hi * 0.9999999))
    ax.plot(
        0.985,
        y_draw,
        marker=">",
        color=color,
        transform=ax.get_yaxis_transform(),
        clip_on=False,
        markersize=markersize,
        markeredgecolor="black",
        markeredgewidth=0.5,
        zorder=10,
    )
    


def _add_psd_max_textbox(ax, labels, cfg_plot: dict[str, Any]) -> None:
    """Show PSD maxima for liquid and ice as a compact textbox in the upper-left half."""
    if not labels:
        return
    ax.text(
        0.1,
        0.95,
        "\n".join(labels),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize="xx-small",
        color="0.15",
        bbox=_annotation_box_style(
            cfg_plot,
            alpha=float(cfg_plot.get("legend_frame_alpha", 0.82)),
            edgecolor=str(cfg_plot.get("legend_edge_color", "0.75")),
            linewidth=0.6,
        ),
        zorder=11,
    )


def _fmt_growth_um_s(v: float) -> str:
    return "—" if not np.isfinite(v) else f"{v:.2f} µm/s"


def _growth_overlay_text_lines(go: GrowthOverlay) -> list[str]:
    """Ridge height (window vs anchor) and spectral mean-diameter growth rates."""
    zr = f"{go.z_ridge_m:.0f} m" if np.isfinite(go.z_ridge_m) else "—"
    if go.z_anchor_m is not None and np.isfinite(go.z_anchor_m):
        za = f"{go.z_anchor_m:.0f} m"
    else:
        za = "—"
    return [
        # f"z_ridge (window): {zr}",
        # f"z_anchor (1st plume): {za}",
        r"$d\,D_{\mathrm{liq}}/d\,t$: " + _fmt_growth_um_s(go.d_liq_um_s),
        r"$d\,D_{\mathrm{ice}}/d\,t$: " + _fmt_growth_um_s(go.d_ice_um_s),
    ]


def _apply_optional_ylim(ax: Any, raw: Any) -> None:
    """If *raw* is a length-2 numeric pair, set axis limits (overrides autoscale)."""
    if raw is None:
        return
    if isinstance(raw, (list, tuple)) and len(raw) == 2:
        lo, hi = float(raw[0]), float(raw[1])
        if np.isfinite(lo) and np.isfinite(hi) and lo != hi:
            ax.set_ylim(lo, hi)


def _footer_stack_tick_formatter(ax: Any, *, hide_lower: bool, hide_upper: bool):
    """Hide boundary y-tick labels where stacked footer axes touch each other."""
    from matplotlib.ticker import FuncFormatter

    def _fmt(y: float, _pos: Any) -> str:
        lo, hi = ax.get_ylim()
        if lo > hi:
            lo, hi = hi, lo
        ticks = [
            float(t)
            for t in ax.get_yticks()
            if np.isfinite(t) and lo <= t <= hi and (ax.get_yscale() != "log" or t > 0)
        ]
        if hide_lower and len(ticks) > 2:
            ticks = ticks[1:]
        if hide_upper and len(ticks) > 2:
            ticks = ticks[:-1]
        if len(ticks) > 5:
            ticks = ticks[::2]
        return f"{y:g}" if any(np.isclose(y, t) for t in ticks) else ""

    return FuncFormatter(_fmt)


def _plot_growth_footer_strip(
    fig: Any,
    *,
    bottom_cell: Any,
    station_ids: list[int],
    station_labels: dict[int, str],
    series_by_station: dict[int, dict[str, np.ndarray]],
    itime: int,
    cfg_plot: dict[str, Any],
    slice_unit_label: str,
) -> None:
    """Four-row footer per station: z, Σ(conc) in slice, <D>, d<D>/dt; trail ends at *itime* with a cursor line."""
    from matplotlib.ticker import FuncFormatter, LogLocator as _FooterLogLocator

    n_cols = len(station_ids)
    mh = float(cfg_plot.get("main_stack_hspace", 0.001))
    mw = float(cfg_plot.get("main_col_wspace", 0.005))
    foot = bottom_cell.subgridspec(4, n_cols, hspace=mh, wspace=mw)
    c_w = cfg_plot.get("psd_color_W", "steelblue")
    c_f = cfg_plot.get("psd_color_F", "sienna")
    lw = float(cfg_plot.get("growth_footer_linewidth", 0.9))
    title_pt = float(cfg_plot.get("station_title_pt", 8.0))
    cur_c = str(cfg_plot.get("growth_cursor_color", "0.45"))
    glw_m = float(cfg_plot.get("grid_linewidth", 0.2)) * 0.75
    galpha_m = float(cfg_plot.get("grid_alpha", 0.38)) * 0.65
    gc = str(cfg_plot.get("grid_color", "0.82"))

    t_lo, t_hi = np.inf, -np.inf
    for stn in station_ids:
        ser = series_by_station.get(stn)
        if ser is None:
            continue
        tx = np.asarray(ser["t_mid"], dtype=float)
        ok = np.isfinite(tx)
        if np.any(ok):
            t_lo = min(t_lo, float(np.nanmin(tx[ok])))
            t_hi = max(t_hi, float(np.nanmax(tx[ok])))
    if not np.isfinite(t_lo):
        t_lo, t_hi = 0.0, 1.0
    span_min = max((t_hi - t_lo) / 60.0, 1.0 / 60.0)

    def _fmt_sec_axis(val: float, _pos: Any) -> str:
        return format_elapsed_minutes_tick(val / 60.0, span_min, zero_if_close=True)

    xfmt = FuncFormatter(_fmt_sec_axis)

    for c, stn in enumerate(station_ids):
        ser = series_by_station.get(stn)
        show_title = n_cols > 1
        ax0 = fig.add_subplot(foot[0, c])
        ax1 = fig.add_subplot(foot[1, c], sharex=ax0)
        ax2 = fig.add_subplot(foot[2, c], sharex=ax0)
        ax3 = fig.add_subplot(foot[3, c], sharex=ax0)
        for ax in (ax0, ax1, ax2, ax3):
            ax.set_axisbelow(True)
            ax.spines["top"].set_visible(False)
            ax.spines["bottom"].set_visible(False)
            _apply_spine_tick_style(ax, cfg_plot, y_label_column=(c == 0))
        if ser is None or len(ser["t_mid"]) == 0:
            for ax in (ax0, ax1, ax2):
                ax.tick_params(labelbottom=False)
            ax3.tick_params(labelbottom=c == 0)
            _apply_optional_ylim(ax0, cfg_plot.get("growth_footer_z_ylim"))
            _apply_optional_ylim(ax1, cfg_plot.get("growth_footer_sigma_ylim"))
            _apply_optional_ylim(ax2, cfg_plot.get("growth_footer_D_ylim"))
            _apply_optional_ylim(ax3, cfg_plot.get("growth_footer_ddt_ylim"))
            ax0.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax0, hide_lower=True, hide_upper=False))
            ax1.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax1, hide_lower=True, hide_upper=True))
            ax2.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax2, hide_lower=True, hide_upper=True))
            ax3.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax3, hide_lower=False, hide_upper=True))
            continue
        end = max(0, min(int(itime), len(ser["t_mid"]) - 1))
        t_cur = float(ser["t_mid"][end])
        if not np.isfinite(t_cur):
            t_cur = float(np.nanmedian(ser["t_mid"]))

        if show_title:
            ax0.set_title(stn_label(stn, station_labels), fontsize=title_pt, fontweight="medium", color="0.2")
        tx, zx = _growth_footer_trail(ser, "z_m", end)
        if tx.size:
            ax0.plot(tx, zx, color="0.2", lw=lw, clip_on=False)
        ax0.axvline(t_cur, color=cur_c, lw=0.75, ls=":")
        if c == 0:
            ax0.set_ylabel(str(cfg_plot.get("growth_footer_z_ylabel", "z / (m)")), fontsize=title_pt, color="0.25")
        _apply_optional_ylim(ax0, cfg_plot.get("growth_footer_z_ylim"))
        ax0.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax0, hide_lower=True, hide_upper=False))
        _major_grid(ax0, cfg_plot)

        ax1.set_yscale("log")
        sxt, syl = _growth_footer_trail(ser, "sum_liq", end)
        ypos_chunks: list[np.ndarray] = []
        if sxt.size and np.any(syl > 0):
            yy = np.maximum(syl, np.finfo(float).tiny)
            ax1.plot(sxt, yy, color=c_w, lw=lw, label="liq")
            ypos_chunks.append(yy[np.isfinite(yy) & (yy > 0)])
        six, siy = _growth_footer_trail(ser, "sum_ice", end, mask_ok=True)
        if six.size and np.any(siy > 0):
            yyi = np.maximum(siy, np.finfo(float).tiny)
            ax1.plot(six, yyi, color=c_f, lw=lw, label="ice")
            ypos_chunks.append(yyi[np.isfinite(yyi) & (yyi > 0)])
        ax1.axvline(t_cur, color=cur_c, lw=0.75, ls=":")
        if c == 0:
            ax1.set_ylabel(
                str(cfg_plot.get("growth_footer_sigma_ylabel", f"Σ / ({slice_unit_label})")),
                fontsize=title_pt,
                color="0.25",
            )
        # Explicit positive ylim + integer minor subs: avoids degenerate log autoscale / tick overflow on tiny panels.
        _tin = np.finfo(float).tiny
        if ypos_chunks:
            ycat = np.concatenate(ypos_chunks)
            ylo, yhi = float(np.nanmin(ycat)), float(np.nanmax(ycat))
            if ylo > 0 and yhi > 0 and np.isfinite(ylo) and np.isfinite(yhi):
                if yhi <= ylo * 1.0001:
                    ax1.set_ylim(max(ylo * 0.1, _tin * 100), max(ylo * 10, yhi * 10))
                else:
                    ax1.set_ylim(max(ylo * 0.2, _tin * 10), yhi * 5.0)
            else:
                ax1.set_ylim(1e-12, 1.0)
        else:
            ax1.set_ylim(1e-12, 1.0)
        _apply_optional_ylim(ax1, cfg_plot.get("growth_footer_sigma_ylim"))
        ax1.yaxis.set_minor_locator(_FooterLogLocator(base=10.0, subs=tuple(range(2, 10))))
        ax1.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax1, hide_lower=True, hide_upper=True))
        _major_grid(ax1, cfg_plot)
        ax1.grid(True, which="minor", linestyle=":", linewidth=glw_m, color=gc, alpha=galpha_m)
        if c == 0 and ax1.get_lines():
            ax1.legend(loc="upper right", **_legend_style(cfg_plot))

        dx, dyl = _growth_footer_trail(ser, "D_liq", end)
        if dx.size:
            ax2.plot(dx, dyl, color=c_w, lw=lw)
        dxi, dyi = _growth_footer_trail(ser, "D_ice", end, mask_ok=True)
        if dxi.size:
            ax2.plot(dxi, dyi, color=c_f, lw=lw)
        ax2.axvline(t_cur, color=cur_c, lw=0.75, ls=":")
        if c == 0:
            ax2.set_ylabel(
                str(cfg_plot.get("growth_footer_D_ylabel", r"$D$ / ($\mu$m)")),
                fontsize=title_pt,
                color="0.25",
            )
        _apply_optional_ylim(ax2, cfg_plot.get("growth_footer_D_ylim"))
        ax2.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax2, hide_lower=True, hide_upper=True))
        _major_grid(ax2, cfg_plot)

        rx, ryl = _growth_footer_trail(ser, "d_liq_dt", end)
        if rx.size:
            ax3.plot(rx, ryl, color=c_w, lw=lw)
        rxi, ryi = _growth_footer_trail(ser, "d_ice_dt", end, mask_ok=True)
        if rxi.size:
            ax3.plot(rxi, ryi, color=c_f, lw=lw)
        ax3.axvline(t_cur, color=cur_c, lw=0.75, ls=":")
        if c == 0:
            ax3.set_ylabel(
                str(cfg_plot.get("growth_footer_ddt_ylabel", r"$\mathrm{d}D/\mathrm{d}t$ / ($\mu$m $\mathrm{s}^{-1}$)")),
                fontsize=title_pt,
                color="0.25",
            )
            ax3.set_xlabel(
                str(cfg_plot.get("growth_footer_time_xlabel", "time from start / (min)")),
                fontsize=title_pt,
                color="0.25",
            )
        ax3.xaxis.set_major_formatter(xfmt)
        _apply_optional_ylim(ax3, cfg_plot.get("growth_footer_ddt_ylim"))
        ax3.yaxis.set_major_formatter(_footer_stack_tick_formatter(ax3, hide_lower=False, hide_upper=True))
        _major_grid(ax3, cfg_plot)

        for ax in (ax0, ax1, ax2):
            ax.tick_params(labelbottom=False)
        ax3.tick_params(labelbottom=c == 0)


def _format_psd_ax(
    ax,
    *,
    row,
    col,
    n_cols,
    xlim,
    ylim,
    grid_linewidth,
    psd_yscale,
    conc_unit_label,
    station_idx,
    station_labels,
    spec_label,
    cfg_plot: dict[str, Any],
) -> Any:
    """Format the compact shared PSD strip axis above each liquid/ice pair (same spine/tick/grid recipe as rates)."""
    from matplotlib.ticker import FuncFormatter, LogLocator

    ax.set_xscale("log")
    ax.set_xlim(*xlim)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.set_yscale(psd_yscale)
    ax.set_ylim(*ylim)
    if psd_yscale == "log":
        ax.yaxis.set_major_locator(LogLocator(base=10.0))
        ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=tuple(range(2, 10))))

    ax.yaxis.set_major_formatter(_sparse_major_tick_formatter(ax))
    gc = str(cfg_plot.get("grid_color", "0.82"))
    ga = float(cfg_plot.get("grid_alpha", 0.38))
    gls = str(cfg_plot.get("grid_linestyle", "-"))
    ax.grid(True, which="major", linestyle=gls, linewidth=grid_linewidth, color=gc, alpha=ga * 0.95)
    if psd_yscale == "log":
        ax.grid(True, which="minor", linestyle=":", linewidth=grid_linewidth * 0.75, color=gc, alpha=ga * 0.55)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))
    ax.tick_params(which="both", bottom=True, top=False, labelbottom=False, labeltop=False)
    ax.yaxis.set_ticks_position("both")
    ax.yaxis.set_label_position("left")
    tpt = float(cfg_plot.get("axis_tick_label_pt", 7.0))
    ax.set_ylabel(conc_unit_label, fontsize=tpt, color="0.25")
    ann_a = float(cfg_plot.get("panel_annotation_alpha", 0.55))
    ax.text(
        0.02,
        0.92,
        "PSD",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=tpt + 0.5,
        fontweight="medium",
        color="0.2",
        bbox=_annotation_box_style(cfg_plot, alpha=ann_a),
    )
    title_pt = float(cfg_plot.get("station_title_pt", 8.0))
    if row == 0:
        ax.set_title(stn_label(station_idx, station_labels), fontsize=title_pt, fontweight="medium", color="0.2")
    _apply_spine_tick_style(ax, cfg_plot, y_label_column=True)
    ax.spines["right"].set_visible(False)

    ax_r = ax.twinx()
    ax_r.sharey(ax)
    ax_r.set_xlim(ax.get_xlim())
    ax_r.set_xscale(ax.get_xscale())
    for _side in ("top", "bottom"):
        ax_r.spines[_side].set_visible(False)
    ax_r.spines["left"].set_visible(False)
    ax_r.spines["right"].set_visible(True)
    ax_r.patch.set_visible(False)
    ax_r.grid(False)
    ax_r.tick_params(axis="x", bottom=False, top=False, labelbottom=False, labeltop=False)
    ax_r.yaxis.set_major_formatter(_sparse_major_tick_formatter(ax_r))
    if psd_yscale == "log":
        ax_r.yaxis.set_major_locator(LogLocator(base=10.0))
        ax_r.yaxis.set_minor_locator(LogLocator(base=10.0, subs=tuple(range(2, 10))))
    _apply_spine_tick_style(ax_r, cfg_plot, y_label_column=False)
    ax_r.tick_params(axis="y", which="both", left=False, right=True, labelleft=False, labelright=True)
    return ax_r

def _add_psd_legend(ax, cfg_plot) -> None:
    """Add small legend for Liquid/Ice PSD curves on the PSD axis."""
    import matplotlib.patches as mpatches

    c_w = cfg_plot.get("psd_color_W", "steelblue")
    c_f = cfg_plot.get("psd_color_F", "sienna")
    alpha = cfg_plot.get("psd_fill_alpha", 0.4)
    handles = [
        mpatches.Patch(facecolor=c_w, edgecolor=c_w, alpha=alpha, label="Liquid"),
        mpatches.Patch(facecolor=c_f, edgecolor=c_f, alpha=alpha, label="Ice"),
    ]
    ax.legend(
        handles=handles,
        loc="upper right",
        handlelength=1.2,
        handleheight=0.8,
        handletextpad=0.5,
        **_legend_style(cfg_plot),
    )


def _add_process_cmap(fig, process_order, *, show_active_only=False) -> None:
    """Add a discrete process colormap with process-name tick labels on the right."""
    from matplotlib import colors

    labels = [p for p in process_order] if not show_active_only else [p for p in process_order if p]
    cmap = colors.ListedColormap([proc_color(p) for p in labels], name="proc_labels")
    bounds = np.arange(len(labels) + 1)
    norm = colors.BoundaryNorm(bounds, cmap.N)
    sm = matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(
        sm,
        ax=fig.axes,
        location="right",
        fraction=0.045,
        pad=0.02,
        ticks=np.arange(len(labels)) + 0.5,
    )
    cbar.ax.set_yticklabels([p.replace("_", " ").title() for p in labels])
    cbar.ax.tick_params(labelsize="x-small")
    cbar.outline.set_linewidth(0.6)


def plot_spectral_waterfall(
    *,
    spec_rates_w: dict[str, Any],
    spec_rates_f: dict[str, Any],
    size_ranges: dict[str, Any],
    range_key: str,
    diameter_um: np.ndarray,
    station_ids: list[int],
    station_labels: dict[int, str],
    height_sel_m: list[float],
    twindow: slice,
    unit_label: str,
    kind_label: str,
    cfg_plot: dict[str, Any],
    normalize_mode: str = "none",
    plot_style: str = "bars",
    yscale: str = "symlog",
    spec_conc_w: Optional[xr.DataArray] = None,
    spec_conc_f: Optional[xr.DataArray] = None,
    ridge_conc_f: Optional[xr.DataArray] = None,
    conc_unit_label: str = "",
    ridge_anchor_by_station: Optional[dict[int, tuple[int, int]]] = None,
    ridge_height_m_by_station: Optional[dict[int, float]] = None,
    growth_overlay_by_station: Optional[dict[int, GrowthOverlay]] = None,
    growth_footer_series_by_station: Optional[dict[int, dict[str, np.ndarray]]] = None,
    growth_footer_itime: int = 0,
    growth_footer_slice_unit: str = "",
    shared_x_label: str | None = None,
    shared_y_label: str | None = None,
) -> tuple[Any, Any]:
    """Spectral waterfall with one shared PSD strip above liquid and ice rate panels.

    plot_style : 'bars' | 'lines' | 'steps'.
    yscale     : 'symlog' | 'linear' | 'log'.
    """
    bin_slice = size_ranges[range_key]["slice"]
    use_plume_ridge = bool(cfg_plot.get("use_plume_ridge", True))
    ridge_floor = float(cfg_plot.get("ridge_mass_floor", 0.0))
    xlim = (min(cfg_plot["xlim_W"][0], cfg_plot["xlim_F"][0]), max(cfg_plot["xlim_W"][1], cfg_plot["xlim_F"][1]))
    ylim_W, ylim_F = tuple(cfg_plot["ylim_W"]), tuple(cfg_plot["ylim_F"])
    linthresh_W, linthresh_F = cfg_plot["linthresh_W"], cfg_plot["linthresh_F"]
    spec_label = f"Liq / Ice ({range_key})"

    n_hl = 1 if use_plume_ridge else len(height_sel_m) - 1
    n_cols = len(station_ids)
    show_footer = (
        use_plume_ridge
        and bool(cfg_plot.get("show_growth_footer") or cfg_plot.get("show_growth_sparkline"))
        and growth_footer_series_by_station is not None
    )
    show_proc_labels_mode = str(cfg_plot.get("show_proc_labels", "legend")).lower()
    use_fig_legend = show_proc_labels_mode == "legend"
    use_cmap_labels = show_proc_labels_mode == "cmap"
    main_h_in = min(n_hl * 125 * MM, MAX_H_IN)
    foot_ratio = float(cfg_plot.get("footer_outer_height_ratio", 0.72))
    # Extra height / footer ratio: room for suptitle, fig.legend, and 4×n_st footer rows (avoids constrained_layout collapse).
    fig_h_in = main_h_in + (2.85 * foot_ratio / 0.72 if show_footer else 0.0)
    fig_h_in *= float(cfg_plot.get("figure_height_scale", 1.0))
    fig_w_in = SINGLE_COL_IN * max(1, n_cols)
    # One manual layout path keeps PSD, liquid/ice, and footer geometry deterministic across modes.
    fig = plt.figure(figsize=(fig_w_in, fig_h_in), constrained_layout=False)
    outer_gs = None
    mh = float(cfg_plot.get("main_stack_hspace", 0.001))
    mw = float(cfg_plot.get("main_col_wspace", 0.005))
    phr = _panel_height_ratios(cfg_plot)
    if show_footer:
        fb = float(cfg_plot.get("footer_block_hspace", 0.10))
        outer_gs = fig.add_gridspec(2, 1, height_ratios=[1.0, foot_ratio], hspace=fb)
        gs = outer_gs[0].subgridspec(n_hl, n_cols, hspace=mh, wspace=mw)
    else:
        gs = fig.add_gridspec(n_hl, n_cols, hspace=mh, wspace=mw)

    axes_psd = np.empty((n_hl, n_cols), dtype=object)
    axes_psd_r = np.empty((n_hl, n_cols), dtype=object)
    axes_liq = np.empty((n_hl, n_cols), dtype=object)
    axes_ice = np.empty((n_hl, n_cols), dtype=object)
    pch = float(cfg_plot.get("panel_column_hspace", 0.12))
    for r in range(n_hl):
        for c in range(n_cols):
            sub = gs[r, c].subgridspec(3, 1, height_ratios=phr, hspace=pch)
            axes_psd[r, c] = fig.add_subplot(sub[0])
            axes_liq[r, c] = fig.add_subplot(sub[1], sharex=axes_psd[r, c])
            axes_ice[r, c] = fig.add_subplot(sub[2], sharex=axes_psd[r, c])
            for _ax in (axes_psd[r, c], axes_liq[r, c], axes_ice[r, c]):
                _ax.set_facecolor(str(cfg_plot.get("panel_group_bg", "1.0")))

    global_active: set[str] = set()
    d = np.asarray(diameter_um[bin_slice])
    n = len(d)
    diff = np.diff(d) if n > 1 else np.array([max(d[0] * 0.1, 1e-8)])
    bin_width = np.concatenate([diff, [diff[-1]]]) if n > 1 else diff
    widths = cfg_plot["bar_width_frac_merged"] * bin_width

    for row in range(n_hl):
        h0, h1 = (height_sel_m[row], height_sel_m[row + 1]) if not use_plume_ridge else (np.nan, np.nan)
        for col, station_idx in enumerate(station_ids):
            ax_psd = axes_psd[row, col]
            ax_liq = axes_liq[row, col]
            ax_ice = axes_ice[row, col]
            psd_max_labels: list[str] = []
            if use_plume_ridge:
                if ridge_conc_f is None:
                    raise ValueError("ridge_conc_f is required when use_plume_ridge=True")
                r_anch = (
                    ridge_anchor_by_station[station_idx]
                    if ridge_anchor_by_station and station_idx in ridge_anchor_by_station
                    else None
                )
                net_w = ridge_process_values(
                    spec_rates_w,
                    list(spec_rates_w.keys()),
                    ridge_conc_f,
                    station_idx,
                    twindow,
                    bin_slice,
                    floor=ridge_floor,
                    ridge_anchor=r_anch,
                )
                net_f = ridge_process_values(
                    spec_rates_f,
                    list(spec_rates_f.keys()),
                    ridge_conc_f,
                    station_idx,
                    twindow,
                    bin_slice,
                    floor=ridge_floor,
                    ridge_anchor=r_anch,
                )
            else:
                net_w = panel_process_values(spec_rates_w, list(spec_rates_w.keys()), station_idx, h0, h1, twindow, bin_slice)
                net_f = panel_process_values(spec_rates_f, list(spec_rates_f.keys()), station_idx, h0, h1, twindow, bin_slice)
            psd_ylim = (
                min(cfg_plot["psd_ylim_W"][0], cfg_plot["psd_ylim_F"][0]),
                max(cfg_plot["psd_ylim_W"][1], cfg_plot["psd_ylim_F"][1]),
            )
            if cfg_plot.get("show_psd_twin", False):
                axes_psd_r[row, col] = _format_psd_ax(
                    ax_psd,
                    row=row,
                    col=col,
                    n_cols=n_cols,
                    xlim=xlim,
                    ylim=psd_ylim,
                    grid_linewidth=cfg_plot.get("grid_linewidth", 0.15),
                    psd_yscale=cfg_plot.get("psd_yscale", "log"),
                    conc_unit_label=conc_unit_label,
                    station_idx=station_idx,
                    station_labels=station_labels,
                    spec_label=spec_label,
                    cfg_plot=cfg_plot,
                )
            else:
                ax_psd.set_visible(False)
                axes_psd_r[row, col] = None

            for phase, ax_ph, net_src in [
                ("liq", ax_liq, net_w),
                ("ice", ax_ice, net_f),
            ]:
                ylim_ph = ylim_W if phase == "liq" else ylim_F
                linthresh_ph = linthresh_W if phase == "liq" else linthresh_F
                # Set scales/limits *before* drawing data (else log/symlog relayout → scale.py overflow).
                _format_ax(
                    ax_ph,
                    phase=phase,
                    row=row,
                    col=col,
                    n_hl=n_hl,
                    n_cols=n_cols,
                    xlim=xlim,
                    ylim=ylim_ph,
                    linthresh=linthresh_ph,
                    normalize_mode=normalize_mode,
                    yscale=yscale,
                    cfg_plot=cfg_plot,
                    unit_label=unit_label,
                    station_idx=station_idx,
                    station_labels=station_labels,
                    spec_label=spec_label,
                    h0=h0,
                    h1=h1,
                )
                if net_src:
                    nm = merge_liq_ice_net(net_src, {}, n) if phase == "liq" else merge_liq_ice_net({}, net_src, n)
                    nm = normalize_net_stacks(nm, normalize_mode)
                    global_active |= {p for p, (_, arr) in nm.items() if np.any(np.abs(arr) > 0)}
                    weights = {p: float(np.sum(np.abs(arr))) for p, (_, arr) in nm.items()}
                    order = sorted(nm.keys(), key=lambda p: (PROCESS_PLOT_ORDER.index(p) if p in PROCESS_PLOT_ORDER else 999, -weights.get(p, 0.0)))
                    if not _draw_phase(ax_ph, plot_style, d, widths, order, nm, cfg_plot):
                        ax_ph.text(0.5, 0.5, "no signal", transform=ax_ph.transAxes, ha="center", va="center", color="grey")
                else:
                    ax_ph.text(0.5, 0.5, "no signal", transform=ax_ph.transAxes, ha="center", va="center", color="grey")
                
                if cfg_plot.get("show_psd_twin", False):
                    spec_conc = spec_conc_w if phase == "liq" else spec_conc_f
                    if spec_conc is not None:
                        conc_arr = (
                            ridge_concentration_profile(
                                spec_conc,
                                ridge_conc_f,
                                station_idx,
                                twindow,
                                bin_slice,
                                floor=ridge_floor,
                                ridge_anchor=r_anch,
                            )
                            if use_plume_ridge
                            else panel_concentration_profile(spec_conc, station_idx, h0, h1, twindow, bin_slice)
                        )
                        if np.any(conc_arr > 0):
                            psd_color = cfg_plot["psd_color_W"] if phase == "liq" else cfg_plot["psd_color_F"]
                            psd_alpha = cfg_plot.get("psd_fill_alpha", 0.4)
                            psd_floor = np.full_like(d, max(psd_ylim[0], np.finfo(float).tiny), dtype=float)
                            ax_psd.step(d, psd_floor, where="mid", color='white', alpha=psd_alpha, linewidth=cfg_plot.get("psd_white_linewidth", 2.0))
                            ax_psd.fill_between(
                                d,
                                psd_floor,
                                conc_arr,
                                step="mid",
                                color=psd_color,
                                alpha=psd_alpha,
                                linewidth=1.2,
                            )
                            ax_psd.step(d, conc_arr, where="mid", color='black', alpha=1.0, linewidth=cfg_plot.get("psd_black_linewidth", 0.8))
                            
                            max_val = float(np.nanmax(conc_arr))
                            psd_ylim_phase = cfg_plot["psd_ylim_W"] if phase == "liq" else cfg_plot["psd_ylim_F"]
                            within_ylim = (
                                np.isfinite(max_val)
                                and max_val > 0.0
                                and psd_ylim_phase[0] <= max_val <= psd_ylim_phase[1]
                            )
                            if within_ylim:
                                mean_d = spectral_mean_diameter(d, conc_arr)
                                _plot_psd_mean_triangle(ax_psd, mean_d, psd_color)
                                ax_psd_yr = axes_psd_r[row, col]
                                if ax_psd_yr is not None:
                                    _plot_psd_max_triangle(ax_psd_yr, max_val, psd_color)
                                phase_tag = "Liq" if phase == "liq" else "Ice"
                                psd_max_labels.append(f"{phase_tag} max: {max_val:.1e}")
                        
            if cfg_plot.get("show_psd_twin", False):
                if (
                    cfg_plot.get("show_growth_textbox", False)
                    and growth_overlay_by_station is not None
                    and station_idx in growth_overlay_by_station
                ):
                    psd_max_labels.extend(_growth_overlay_text_lines(growth_overlay_by_station[station_idx]))
                _add_psd_max_textbox(ax_psd, psd_max_labels, cfg_plot)
                _add_psd_legend(ax_psd, cfg_plot)

    for r in range(n_hl):
        for c in range(n_cols):
            for ax in (axes_psd[r, c], axes_liq[r, c], axes_ice[r, c]):
                ax.spines["top"].set_visible(False)
                ax.spines["bottom"].set_visible(False)
            ax_pr = axes_psd_r[r, c]
            if ax_pr is not None:
                ax_pr.spines["top"].set_visible(False)
                ax_pr.spines["bottom"].set_visible(False)

    if show_footer and outer_gs is not None:
        slabel = growth_footer_slice_unit or conc_unit_label.replace("m³", "m3").replace("m⁻³", "m-3")
        _plot_growth_footer_strip(
            fig,
            bottom_cell=outer_gs[1],
            station_ids=station_ids,
            station_labels=station_labels,
            series_by_station=growth_footer_series_by_station or {},
            itime=growth_footer_itime,
            cfg_plot=cfg_plot,
            slice_unit_label=slabel,
        )

    if show_proc_labels_mode == "cmap":
        _add_process_cmap(fig, PROCESS_PLOT_ORDER)
    else:
        build_fixed_legend(
            fig, global_active, PROCESS_PLOT_ORDER,
            bbox_y=float(cfg_plot.get("legend_bbox_y", -0.02)),
            handletextpad=cfg_plot.get("legend_handletextpad", 0.8),
            columnspacing=cfg_plot.get("legend_columnspacing", 1.4),
        )

    tw_str = f"{str(twindow.start)[11:19]} - {str(twindow.stop)[11:19]}"
    norm_tag = f" (relative:{normalize_mode})" if normalize_mode != "none" else ""
    ridge_tag = " along ice-plume ridge" if use_plume_ridge else ""
    fig.suptitle(
        f"Spectral waterfall — View D: {kind_label} spectral budget{ridge_tag} [{unit_label}]{norm_tag} — {tw_str}",
        fontweight="semibold",
        fontsize=float(cfg_plot.get("figure_title_pt", 9.0)),
        color="0.18",
    )

    y_lbl = shared_y_label or (
        f"Process rates / ({unit_label})" if normalize_mode == "none" else "Relative process rates / (-)"
    )
    x_lbl = shared_x_label or r"Diameter / ($\mu$m)"
    _apply_figure_layout(fig, cfg_plot, show_footer=show_footer, use_cmap_labels=use_cmap_labels)
    _main_block_labels(fig, axes_psd, axes_liq, axes_ice, x_label=x_lbl, y_label=y_lbl, cfg_plot=cfg_plot)

    return fig, (axes_psd, axes_liq, axes_ice)


def _format_ax(ax, *, phase, row, col, n_hl, n_cols, xlim, ylim, linthresh,
               normalize_mode, yscale, cfg_plot, unit_label, station_idx,
               station_labels, spec_label, h0, h1, panel_label=None) -> None:
    """Apply axis formatting for a liquid (upper) or ice (lower) sub-axis."""
    from matplotlib.ticker import FuncFormatter, SymmetricalLogLocator

    ax.set_xscale("log")
    ax.set_xlim(*xlim)

    if yscale == "symlog":
        ax.set_yscale("symlog", linthresh=linthresh, linscale=cfg_plot["linscale"])
        _sloc = SymmetricalLogLocator(linthresh=linthresh, base=10)
        _sloc.numticks = int(cfg_plot.get("rate_symlog_numticks", 7))
        ax.yaxis.set_major_locator(_sloc)
    elif yscale == "log":
        ax.set_yscale("log")
    else:
        ax.set_yscale("linear")

    ax.set_ylim(*ylim)
    ax.axhline(
        0,
        color=str(cfg_plot.get("zero_line_color", "0.55")),
        linewidth=cfg_plot["zero_linewidth"],
        linestyle="--",
    )
    _major_grid(ax, cfg_plot)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:g}"))

    ax.yaxis.set_major_formatter(_sparse_major_tick_formatter(ax))
    ax.yaxis.set_ticks_position("both")

    tpt = float(cfg_plot.get("axis_tick_label_pt", 7.0))
    ann_a = float(cfg_plot.get("panel_annotation_alpha", 0.55))
    phase_lbl = "Liquid" if phase == "liq" else "Ice"
    ax.text(
        0.02,
        0.92,
        phase_lbl,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=tpt + 0.5,
        fontweight="medium",
        color="0.2",
        bbox=_annotation_box_style(cfg_plot, alpha=ann_a),
    )

    if phase == "liq":
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
    elif row < n_hl - 1:
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
    if phase == "liq":
        panel_txt = panel_label or f"{h1:.0f} – {h0:.0f} m"
        ax.text(
            0.95,
            0.92,
            panel_txt,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=tpt + 0.5,
            fontweight="semibold",
            color="0.2",
            bbox=_annotation_box_style(cfg_plot, alpha=float(cfg_plot.get("panel_bbox_alpha", 0.35))),
        )
    _apply_spine_tick_style(ax, cfg_plot, y_label_column=(col == 0))
    ypad = float(cfg_plot.get("rate_ytick_pad", 3.8))
    ax.tick_params(axis="y", pad=ypad)


# ── I/O helpers ──────────────────────────────────────────────────────────────

def _save_frame(fig: Any, stem: str, out_dir: Path, dpi: int, png_compress: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        out_dir / f"{stem}.png",
        dpi=dpi,
        pil_kwargs={"compress_level": png_compress, "optimize": False},
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_time_window(cfg_yaml: dict[str, Any], cfg_loaded: dict[str, Any]) -> list[np.datetime64]:
    seed_start = cfg_loaded["seed_start"]
    vals = cfg_yaml.get("plotting", {}).get("time_spacing_min")
    if isinstance(vals, list) and len(vals) >= 2:
        return [seed_start + np.timedelta64(int(float(t) * 60), "s") for t in vals]
    return cfg_loaded["time_window"]


def _waterfall_cfg(cfg_yaml: dict[str, Any], *, kind_hint: str | None = None) -> dict[str, Any]:
    """Merge plotting.spectral_waterfall_<N|Q> over legacy plotting.spectral_waterfall (back-compat)."""
    defaults = {
        "kind": "N",
        "linthresh_W": 1e-2,
        "linthresh_F": 1e-2,
        "linscale": 0.1,
        "xlim_W": (0.001, 4e3),
        "xlim_F": (0.001, 4e3),
        "ylim_W": (-1e6, 1e6),
        "ylim_F": (-1e6, 1e6),
        "bar_edge_color": "black",
        "bar_edge_linewidth": 0.35,
        "bar_width_frac_merged": 0.95,
        "pos_alpha": 0.6,
        "neg_alpha": 0.6,
        "grid_linewidth": 0.15,
        "grid_alpha": 0.5,
        "zero_linewidth": 0.4,
        "panel_bbox_alpha": 0.35,
        "normalize_mode": "none",
        "plot_style": "bars",
        "yscale": "symlog",
        "line_linewidth": 0.6,
        "fill_alpha": 0.25,
        "line_alpha": 0.7,
        "rate_white_linewidth": 2.0,
        "rate_color_linewidth": 0.8,
        "rate_black_linewidth": 0.5,
        "show_psd_twin": True,
        "use_plume_ridge": True,
        "psd_color_W": "steelblue",
        "psd_color_F": "sienna",
        "psd_fill_alpha": 0.4,         # 0.25 was too faint; outline uses same alpha
        "psd_color_linewidth": 1.2, # thin step outline at fill alpha to show PSD shape
        "psd_black_linewidth": 0.5,
        "psd_white_linewidth": 2.0,
        "psd_yscale": "log",
        "psd_ylim_W": (1e1, 1e9),
        "psd_ylim_F": (1e1, 1e9),
        "psd_xlim_W": (0.001, 4e3), 
        "psd_xlim_F": (0.001, 4e3),
        "show_proc_labels": "legend",
        "ridge_mass_floor": 0.0,
        # Growth footer: mask ill-conditioned ice ⟨D⟩ when ∑c_ice along ridge is tiny (see _apply_tiny_ice_mask_for_growth).
        "growth_ice_sum_floor_q": 1e-6,
        "growth_ice_sum_floor_n": 1e3,
        "growth_ice_mask_until_min": None,
        "show_growth_textbox": False,
        "growth_rate_window_frames": 3,
        "growth_rate_mode": "regression",
        "write_growth_csv": False,
        "growth_csv_include_environment": True,
        "growth_csv_derive_supersaturation": True,
        "growth_csv_env_field_map": {},
        "show_growth_sparkline": False,
        "show_growth_footer": False,
        "growth_footer_linewidth": 0.9,
        # Shared layout + axis chrome (inherited across spectral_waterfall_N / _Q; see _CROSS_KIND_KEYS).
        "panel_height_ratios": (1.5, 2.3, 2.3),
        "main_stack_hspace": 0.001,
        "panel_column_hspace": 0.12,
        "rate_symlog_numticks": 7,
        "rate_ytick_pad": 3.8,
        "growth_footer_z_ylim": None,
        "growth_footer_sigma_ylim": None,
        "growth_footer_D_ylim": None,
        "growth_footer_ddt_ylim": None,
        "main_col_wspace": 0.005,
        "footer_block_hspace": 0.10,
        "footer_outer_height_ratio": 0.72,
        "axis_tick_label_pt": 7.0,
        "station_title_pt": 8.0,
        "axis_tick_length": 3.5,
        "axis_tick_width": 0.55,
        "axis_spine_linewidth": 0.6,
        "axis_spine_color": "0.35",
        "grid_linestyle": "-",
        "grid_color": "0.82",
        "panel_group_bg": "1.0",
        "panel_annotation_alpha": 0.55,
        "panel_annotation_pad": 0.12,
        "annotation_box_facecolor": "white",
        "annotation_box_edgecolor": "none",
        "annotation_box_linewidth": 0.0,
        "zero_line_color": "0.55",
        "growth_cursor_color": "0.45",
        "legend_font_pt": 6.4,
        "legend_frame_alpha": 0.82,
        "legend_edge_color": "0.75",
        "legend_bbox_y": -0.02,
        "legend_handletextpad": 0.8,
        "legend_columnspacing": 1.4,
        "figure_title_pt": 9.0,
        "figure_label_pt": 8.0,
        "figure_margin_left": 0.085,
        "figure_margin_right": 0.98,
        "figure_margin_right_cmap": 0.82,
        "figure_margin_top": 0.88,
        "figure_margin_bottom": 0.12,
        "figure_margin_bottom_footer": 0.17,
        "figure_height_scale": 1.0,
        "main_xlabel_pad": 0.03,
        "main_ylabel_pad": 0.08,
        "growth_footer_z_ylabel": "z / (m)",
        "growth_footer_sigma_ylabel": None,
        "growth_footer_D_ylabel": r"$D$ / ($\mu$m)",
        "growth_footer_ddt_ylabel": r"$\mathrm{d}D/\mathrm{d}t$ / ($\mu$m $\mathrm{s}^{-1}$)",
        "growth_footer_time_xlabel": "time from start / (min)",
    }
    plotting = cfg_yaml.get("plotting", {}) or {}
    kh = (kind_hint or "").strip().upper()
    if kh not in ("N", "Q"):
        if isinstance(plotting.get("spectral_waterfall_Q"), dict) and plotting["spectral_waterfall_Q"]:
            kh = "Q"
        elif isinstance(plotting.get("spectral_waterfall_N"), dict) and plotting["spectral_waterfall_N"]:
            kh = "N"
        else:
            kh = "Q"
    legacy_sw = plotting.get("spectral_waterfall", {})
    if not isinstance(legacy_sw, dict):
        legacy_sw = {}
    primary = plotting.get(f"spectral_waterfall_{kh}")
    if not isinstance(primary, dict):
        primary = {}
    sib_k = "Q" if kh == "N" else "N"
    sibling = plotting.get(f"spectral_waterfall_{sib_k}")
    if not isinstance(sibling, dict):
        sibling = {}
    # If only spectral_waterfall_Q (or only _N) exists, publication still runs both jobs; inherit
    # ridge/growth options from the sibling block so N and Q stay in sync without duplicating YAML.
    _CROSS_KIND_KEYS = (
        "ridge_mass_floor",
        "growth_ice_sum_floor_q",
        "growth_ice_sum_floor_n",
        "growth_ice_mask_until_min",
        "show_growth_textbox",
        "write_growth_csv",
        "growth_csv_include_environment",
        "growth_csv_derive_supersaturation",
        "growth_csv_env_field_map",
        "show_growth_footer",
        "show_growth_sparkline",
        "growth_rate_window_frames",
        "growth_rate_mode",
        "growth_footer_linewidth",
        "panel_height_ratios",
        "main_stack_hspace",
        "panel_column_hspace",
        "rate_symlog_numticks",
        "rate_ytick_pad",
        "growth_footer_z_ylim",
        "growth_footer_sigma_ylim",
        "growth_footer_D_ylim",
        "growth_footer_ddt_ylim",
        "main_col_wspace",
        "footer_block_hspace",
        "footer_outer_height_ratio",
        "axis_tick_label_pt",
        "station_title_pt",
        "axis_tick_length",
        "axis_tick_width",
        "axis_spine_linewidth",
        "axis_spine_color",
        "grid_linestyle",
        "grid_color",
        "panel_group_bg",
        "panel_annotation_alpha",
        "panel_annotation_pad",
        "annotation_box_facecolor",
        "annotation_box_edgecolor",
        "annotation_box_linewidth",
        "zero_line_color",
        "growth_cursor_color",
        "legend_font_pt",
        "legend_frame_alpha",
        "legend_edge_color",
        "legend_bbox_y",
        "legend_handletextpad",
        "legend_columnspacing",
        "figure_title_pt",
        "figure_label_pt",
        "figure_margin_left",
        "figure_margin_right",
        "figure_margin_right_cmap",
        "figure_margin_top",
        "figure_margin_bottom",
        "figure_margin_bottom_footer",
        "figure_height_scale",
        "main_xlabel_pad",
        "main_ylabel_pad",
        "growth_footer_z_ylabel",
        "growth_footer_sigma_ylabel",
        "growth_footer_D_ylabel",
        "growth_footer_ddt_ylabel",
        "growth_footer_time_xlabel",
    )
    cfg_raw = dict(legacy_sw)
    for gk in _CROSS_KIND_KEYS:
        if gk in primary:
            cfg_raw[gk] = primary[gk]
        elif gk in sibling:
            cfg_raw[gk] = sibling[gk]
    cfg_raw = {**cfg_raw, **primary}
    cfg = {**defaults, **cfg_raw}
    # Back-compat: old configs may use pos_liq_alpha / neg_liq_alpha
    if "pos_liq_alpha" in cfg and "pos_alpha" not in cfg_raw:
        cfg["pos_alpha"] = cfg.pop("pos_liq_alpha")
    if "neg_liq_alpha" in cfg and "neg_alpha" not in cfg_raw:
        cfg["neg_alpha"] = cfg.pop("neg_liq_alpha")
    cfg["kind"] = "Q" if str(cfg["kind"]).upper() == "Q" else "N"
    cfg["normalize_mode"] = str(cfg.get("normalize_mode", "none")).lower()
    cfg["plot_style"] = str(cfg.get("plot_style", "bars")).lower()
    if cfg["plot_style"] not in ("bars", "lines", "steps"):
        raise ValueError(f"plot_style must be 'bars' or 'lines', got '{cfg['plot_style']}'")
    cfg["yscale"] = str(cfg.get("yscale", "symlog")).lower()
    if cfg["yscale"] not in ("symlog", "linear", "log"):
        raise ValueError(f"yscale must be 'symlog', 'linear', or 'log', got '{cfg['yscale']}'")
    cfg["show_proc_labels"] = str(cfg.get("show_proc_labels", "legend")).lower()
    if cfg["show_proc_labels"] not in ("legend", "cmap"):
        raise ValueError(f"show_proc_labels must be 'legend' or 'cmap', got '{cfg['show_proc_labels']}'")
    # Back-compat: old configs may use psd_step_color / psd_step_linewidth
    if "psd_step_color" in cfg and "psd_color_W" not in cfg_raw and "psd_color_F" not in cfg_raw:
        cfg["psd_color_W"] = cfg["psd_color_F"] = cfg.pop("psd_step_color")
    if "psd_step_linewidth" in cfg and "psd_outline_linewidth" not in cfg_raw:
        cfg["psd_outline_linewidth"] = cfg.pop("psd_step_linewidth")
    for k in ("xlim_W", "xlim_F", "ylim_W", "ylim_F", "psd_xlim_W", "psd_xlim_F", "psd_ylim_W", "psd_ylim_F"):
        if k in cfg:
            cfg[k] = tuple(float(v) for v in cfg[k])
    for k in ("linthresh_W", "linthresh_F", "linscale", "ridge_mass_floor"):
        cfg[k] = float(cfg[k])
    cfg["growth_rate_window_frames"] = int(cfg["growth_rate_window_frames"])
    cfg["growth_rate_window_frames"] = max(2, min(4, cfg["growth_rate_window_frames"]))
    cfg["growth_rate_mode"] = str(cfg.get("growth_rate_mode", "regression")).lower()
    if cfg["growth_rate_mode"] not in ("regression", "last_interval"):
        raise ValueError("growth_rate_mode must be 'regression' or 'last_interval'")
    for k in (
        "show_growth_textbox",
        "write_growth_csv",
        "growth_csv_include_environment",
        "growth_csv_derive_supersaturation",
        "show_growth_sparkline",
        "show_growth_footer",
    ):
        cfg[k] = bool(cfg[k])
    if cfg["show_growth_sparkline"]:
        cfg["show_growth_footer"] = True
    cfg["growth_footer_linewidth"] = float(cfg.get("growth_footer_linewidth", 0.9))
    cfg["growth_ice_sum_floor_q"] = float(cfg.get("growth_ice_sum_floor_q", 1e-6))
    cfg["growth_ice_sum_floor_n"] = float(cfg.get("growth_ice_sum_floor_n", 1e3))
    gum = cfg.get("growth_ice_mask_until_min")
    cfg["growth_ice_mask_until_min"] = None if gum is None else float(gum)
    phr = cfg.get("panel_height_ratios", (1.5, 2.3, 2.3))
    if isinstance(phr, (list, tuple)) and len(phr) == 3:
        cfg["panel_height_ratios"] = tuple(float(x) for x in phr)
    else:
        cfg["panel_height_ratios"] = (1.5, 2.3, 2.3)
    for k in (
        "main_stack_hspace",
        "main_col_wspace",
        "footer_block_hspace",
        "footer_outer_height_ratio",
        "axis_tick_label_pt",
        "station_title_pt",
        "axis_tick_length",
        "axis_tick_width",
        "axis_spine_linewidth",
        "panel_annotation_alpha",
        "panel_annotation_pad",
        "annotation_box_linewidth",
        "legend_font_pt",
        "legend_frame_alpha",
        "legend_bbox_y",
        "legend_handletextpad",
        "legend_columnspacing",
        "figure_title_pt",
        "figure_label_pt",
        "figure_margin_left",
        "figure_margin_right",
        "figure_margin_right_cmap",
        "figure_margin_top",
        "figure_margin_bottom",
        "figure_margin_bottom_footer",
        "main_xlabel_pad",
        "main_ylabel_pad",
    ):
        cfg[k] = float(cfg[k])
    for k in (
        "grid_linestyle",
        "grid_color",
        "axis_spine_color",
        "panel_group_bg",
        "annotation_box_facecolor",
        "annotation_box_edgecolor",
        "zero_line_color",
        "growth_cursor_color",
        "legend_edge_color",
    ):
        cfg[k] = str(cfg[k])
    return cfg


def _ffmpeg_path() -> str:
    if is_server():
        return "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    return "ffmpeg"


def _ffmpeg_concat_file_path(path: Path) -> str:
    # Concat `file '…'` lines use single quotes; a literal apostrophe must be `'\''`.
    return str(path.resolve()).replace("'", r"'\''")


def _build_mp4(ffmpeg_cmd: str, frames: list[str], mp4_path: Path, fps: int) -> None:
    # Still PNGs in concat often carry ~zero segment length. We used to force CFR with setpts + -r,
    # which resampled that stream and dropped most frames in playback. Explicit `duration` per file
    # is the supported slideshow pattern: https://ffmpeg.org/ffmpeg-formats.html#concat-1
    if not frames:
        return
    list_file = mp4_path.parent / f"concat_{mp4_path.stem}.txt"
    frame_dur = 1.0 / max(int(fps), 1)
    dur_s = f"{frame_dur:.6f}"
    with list_file.open("w", encoding="utf-8") as f:
        for p in frames:
            q = _ffmpeg_concat_file_path(Path(p))
            f.write(f"file '{q}'\n")
            f.write(f"duration {dur_s}\n")
        f.write(f"file '{_ffmpeg_concat_file_path(Path(frames[-1]))}'\n")
    try:
        subprocess.run(
            [
                ffmpeg_cmd,
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(list_file),
                "-vf",
                "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                str(mp4_path.resolve()),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        print(exc.stderr)
        raise
    finally:
        list_file.unlink(missing_ok=True)


def _station_tag(station_ids: list[int]) -> str:
    if not station_ids:
        return "stn_none"
    if len(station_ids) == 1:
        return f"stn{station_ids[0]}"
    return "stn" + "-".join(str(s) for s in station_ids)


def _parse_csv_ints(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return [int(v) for v in vals] if vals else None


def _parse_csv_strs(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return vals if vals else None


def _group_existing_frames(
    frame_root: Path,
    *,
    kind: str,
    kind_dir: str,
    cs_run_tag: str,
    exp_ids: list[int] | None = None,
    range_keys: list[str] | None = None,
    station_tag: str | None = None,
) -> list[tuple[str, list[str]]]:
    """Collect existing frame groups for MP4-only mode without loading analysis data."""
    pattern = f"exp*/{kind_dir}/spectral_waterfall_{kind}_{cs_run_tag}_exp*_itime*.png"
    grouped: dict[str, list[tuple[int, str]]] = {}

    for png in frame_root.glob(pattern):
        stem = png.stem
        prefix, sep, itime_raw = stem.rpartition("_itime")
        if not sep:
            continue
        try:
            itime = int(itime_raw)
        except ValueError:
            continue
        if exp_ids is not None and not any(f"_exp{eid}_" in prefix for eid in exp_ids):
            continue
        if station_tag is not None and f"_{station_tag}_" not in prefix:
            continue
        if range_keys is not None and not any(prefix.endswith(f"_{rk}") for rk in range_keys):
            continue
        grouped.setdefault(prefix, []).append((itime, str(png)))

    return [
        (prefix, [path for _, path in sorted(items, key=lambda item: item[0])])
        for prefix, items in sorted(grouped.items())
    ]


def _parse_min_max(s: str) -> tuple[float, float]:
    """Parse 'MIN MAX' or 'MIN,MAX' into (float, float). Allows negative MIN without -- prefix issues."""
    s = s.strip()
    if "," in s:
        a, b = s.split(",", 1)
        return (float(a.strip()), float(b.strip()))
    parts = s.split()
    if len(parts) == 2:
        return (float(parts[0]), float(parts[1]))
    raise argparse.ArgumentTypeError("expected MIN MAX or MIN,MAX")


class _MinMaxAction(argparse.Action):
    """Accept 1 value (MIN,MAX) or 2 values (MIN MAX) so --ylim-w -0.01 0.01 works."""

    def __init__(self, option_strings, dest, **kwargs):
        kwargs["nargs"] = "+"
        kwargs["metavar"] = ("MIN", "MAX")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            if len(values) == 1 and "," in values[0]:
                parsed = _parse_min_max(values[0])
            elif len(values) == 2:
                parsed = (float(values[0]), float(values[1]))
            else:
                raise argparse.ArgumentError(self, "expected MIN MAX or MIN,MAX")
            setattr(namespace, self.dest, [parsed])
        except (ValueError, argparse.ArgumentTypeError) as e:
            raise argparse.ArgumentError(self, f"expected MIN MAX or MIN,MAX: {e}") from e


def export_ridge_growth_csv(
    *,
    repo_root: Path,
    config_path: Path,
    kind: str | None = None,
    output_csv: Path | str | None = None,
    exp_ids: list[int] | None = None,
    range_keys: list[str] | None = None,
    station_ids: list[int] | None = None,
) -> Path:
    """Recompute ridge-growth rows and write CSV (same schema as the spectral-waterfall run).

    Does not render PNG frames. Ensures growth overlay precompute runs even if YAML sets
    ``write_growth_csv: false``.
    """
    cfg_yaml = _read_yaml(Path(config_path))
    kh = kind.strip().upper() if kind else None
    sw_cfg = _waterfall_cfg(cfg_yaml, kind_hint=kh)
    if kind is not None:
        sw_cfg["kind"] = kind.strip().upper()
    kind_u = str(sw_cfg["kind"]).upper()
    sw_work = {**sw_cfg, "write_growth_csv": True}

    cfg = load_process_budget_data(repo_root, config_path=config_path)
    plot_station_ids = (
        station_ids
        if station_ids is not None
        else cfg_yaml.get("selection", {}).get("plot_station_ids", cfg["plot_stn_ids"])
    )
    time_window = _build_time_window(cfg_yaml, cfg)
    if len(time_window) < 2:
        raise ValueError("Need at least two time points in plotting.time_spacing_min (or cfg time_window).")

    plot_exp_ids = (
        exp_ids
        if exp_ids is not None
        else cfg_yaml.get("selection", {}).get("plot_experiment_ids", cfg["plot_exp_ids"])
    )
    plot_range_keys = (
        range_keys
        if range_keys is not None
        else cfg_yaml.get("plotting", {}).get("plot_range_keys", cfg["plot_range_keys"])
    )
    bad_ranges = [rk for rk in plot_range_keys if rk not in cfg["size_ranges"]]
    if bad_ranges:
        raise ValueError(f"Unknown range key(s): {bad_ranges}. Valid: {', '.join(cfg['size_ranges'].keys())}")

    stn_tag = _station_tag(plot_station_ids)
    ridge_floor = float(sw_work.get("ridge_mass_floor", 0.0))
    ridge_context: dict[tuple[int, str], dict[int, tuple[int, int, float]]] = {}
    for eid in plot_exp_ids:
        r0 = cfg["rates_by_exp"][eid]
        rcf0 = r0.get("spec_conc_Q_F")
        if rcf0 is None:
            continue
        for rk in plot_range_keys:
            bs0 = cfg["size_ranges"][rk]["slice"]
            per_stn: dict[int, tuple[int, int, float]] = {}
            for stn in plot_station_ids:
                anch0 = first_plume_ridge_anchor(rcf0, stn, bs0, floor=ridge_floor)
                if anch0 is None:
                    continue
                gti0, hi0 = anch0
                hm0 = float(np.asarray(rcf0["height_level"].values)[int(hi0)])
                per_stn[stn] = (gti0, hi0, hm0)
            ridge_context[(eid, rk)] = per_stn

    _, growth_csv_rows, _series, _ref = _precompute_growth_overlays(
        plot_exp_ids=plot_exp_ids,
        plot_range_keys=plot_range_keys,
        station_ids=plot_station_ids,
        time_window=time_window,
        cfg=cfg,
        kind=kind_u,
        sw_cfg=sw_work,
        ridge_context=ridge_context,
        ds=cfg.get("ds"),
    )
    if not growth_csv_rows:
        raise ValueError("No ridge-growth CSV rows (check use_plume_ridge, data, and ridge anchors).")

    cs_run = cfg_yaml.get("ensemble", {}).get("cs_run", "unknown_cs_run")
    cs_run_tag = str(cs_run).replace("/", "_")
    default_paths = spectral_growth_output_paths(cs_run_tag, kind_u, stn_tag, repo_root=repo_root)
    out = Path(output_csv) if output_csv is not None else default_paths["canonical"]
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = list(growth_csv_rows[0].keys())
    with out.open("w", newline="", encoding="utf-8") as cf:
        w = csv.DictWriter(cf, fieldnames=fields)
        w.writeheader()
        w.writerows(growth_csv_rows)
    if output_csv is None:
        sync_file(out, [default_paths["legacy"]])
    print(f"  Ridge growth CSV: {out} ({len(growth_csv_rows)} rows)")
    return out


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate spectral waterfall PNG frames and optional MP4 from YAML/CLI options.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --config config/psd_process_evolution.yaml --mp4\n"
            "  # Number concentration (N); use = or quoted for negative ylim:\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --kind N --ylim-w=-1,1 --ylim-f=-1,1 --linthresh-w 1e-9 --linthresh-f 1e-9 --xlim-w 0.001,4000 --xlim-f 0.001,4000\n"
            "  # Mass concentration (Q):\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --kind Q --ylim-w=-0.01,0.01 --ylim-f=-0.01,0.01 --linthresh-w 1e-6 --linthresh-f 1e-6 --xlim-w 0.001,4000\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --normalize-mode bin --exp-ids 1 --range-keys ALLBB\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --workers 8 --mp4\n"
            "  python scripts/analysis/growth/run_spectral_waterfall.py --mp4-only   # build MP4 from existing PNGs only\n"
        ),
    )
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "config" / "psd_process_evolution.yaml", help="Path to psd_process_evolution.yaml")
    parser.add_argument("--workers", type=int, default=None, help="Thread workers for frame rendering.")
    parser.add_argument("--exp-ids", type=str, default=None, help="Comma-separated experiment indices, e.g. '1,2'.")
    parser.add_argument("--range-keys", type=str, default=None, help="Comma-separated range keys, e.g. 'ALLBB,CRYBB'.")
    parser.add_argument("--station-ids", type=str, default=None, help="Comma-separated station indices, e.g. '0,1,2'.")
    parser.add_argument("--kind", type=str, default=None, choices=["N", "Q", "n", "q"], help="N (number) or Q (mass).")
    parser.add_argument("--normalize-mode", type=str, default=None, choices=["none", "bin", "panel"], help="Relative normalization mode.")
    parser.add_argument("--plot-style", type=str, default=None, choices=["bars", "lines", "steps"], help="'bars' (stacked) or 'lines' (line+fill_between) or 'steps' (step function).")
    parser.add_argument("--yscale", type=str, default=None, choices=["symlog", "linear", "log"], help="Y-axis scale: symlog, linear, or log.")
    parser.add_argument("--linthresh-w", type=float, default=None, help="Symlog linear threshold for liquid axis (e.g. 1e-9).")
    parser.add_argument("--linthresh-f", type=float, default=None, help="Symlog linear threshold for ice axis.")
    parser.add_argument("--linscale", type=float, default=None, help="Symlog linscale (e.g. 0.01).")
    parser.add_argument("--xlim-w", action=_MinMaxAction, default=None, help="X-axis limits liquid: MIN MAX or MIN,MAX (e.g. --xlim-w 0.1 4000 or --xlim-w=0.1,4000).")
    parser.add_argument("--xlim-f", action=_MinMaxAction, default=None, help="X-axis limits ice.")
    parser.add_argument("--ylim-w", action=_MinMaxAction, default=None, help="Y-axis limits liquid: MIN MAX or MIN,MAX (e.g. --ylim-w -0.01 0.01).")
    parser.add_argument("--ylim-f", action=_MinMaxAction, default=None, help="Y-axis limits ice.")
    parser.add_argument("--psd-ylim-w", action=_MinMaxAction, default=None, help="PSD twin Y-axis limits liquid.")
    parser.add_argument("--psd-ylim-f", action=_MinMaxAction, default=None, help="PSD twin Y-axis limits ice.")
    parser.add_argument("--mp4", action="store_true", help="Build MP4 after frame generation.")
    parser.add_argument("--mp4-only", action="store_true", help="Skip PNG generation; only build MP4 from existing frames (implies --mp4).")
    parser.add_argument("--no-psd-twin", action="store_true", help="Disable the PSD twin axis.")
    args = parser.parse_args()
    if args.mp4_only:
        args.mp4 = True

    cfg_yaml = _read_yaml(args.config)
    sw_cfg = _waterfall_cfg(cfg_yaml, kind_hint=args.kind.upper() if args.kind else None)
    if args.kind is not None:
        sw_cfg["kind"] = args.kind.upper()
    if args.normalize_mode is not None:
        sw_cfg["normalize_mode"] = args.normalize_mode.lower()
    if args.plot_style is not None:
        sw_cfg["plot_style"] = args.plot_style.lower()
    if args.yscale is not None:
        sw_cfg["yscale"] = args.yscale.lower()
    if args.linthresh_w is not None:
        sw_cfg["linthresh_W"] = float(args.linthresh_w)
    if args.linthresh_f is not None:
        sw_cfg["linthresh_F"] = float(args.linthresh_f)
    if args.linscale is not None:
        sw_cfg["linscale"] = float(args.linscale)
    if args.xlim_w is not None:
        sw_cfg["xlim_W"] = args.xlim_w[0]
    if args.xlim_f is not None:
        sw_cfg["xlim_F"] = args.xlim_f[0]
    if args.ylim_w is not None:
        sw_cfg["ylim_W"] = args.ylim_w[0]
    if args.ylim_f is not None:
        sw_cfg["ylim_F"] = args.ylim_f[0]
    if args.psd_ylim_w is not None:
        sw_cfg["psd_ylim_W"] = args.psd_ylim_w[0]
    if args.psd_ylim_f is not None:
        sw_cfg["psd_ylim_F"] = args.psd_ylim_f[0]
    if args.no_psd_twin:
        sw_cfg["show_psd_twin"] = False

    cs_run = cfg_yaml.get("ensemble", {}).get("cs_run", "unknown_cs_run")
    frame_root = REPO_ROOT / "output" / "gfx" / "png" / "05" / cs_run
    mp4_root = REPO_ROOT / "output" / "gfx" / "mp4"
    frame_root.mkdir(parents=True, exist_ok=True)
    mp4_root.mkdir(parents=True, exist_ok=True)
    render_cfg = cfg_yaml.get("plotting", {}).get("render", {})
    mp4_fps = int(render_cfg.get("mp4_fps", 1))
    ffmpeg_cmd = _ffmpeg_path()
    kind = sw_cfg["kind"]
    kind_label = "number" if kind == "N" else "mass"
    kind_dir = "N" if kind == "N" else "M"
    cs_run_tag = cs_run.replace("/", "_")

    if args.mp4_only:
        selected_exp_ids = _parse_csv_ints(args.exp_ids) or cfg_yaml.get("selection", {}).get("plot_experiment_ids")
        selected_range_keys = _parse_csv_strs(args.range_keys) or cfg_yaml.get("plotting", {}).get("plot_range_keys")
        selected_station_ids = _parse_csv_ints(args.station_ids) or cfg_yaml.get("selection", {}).get("plot_station_ids")
        selected_station_tag = _station_tag(selected_station_ids) if selected_station_ids else None
        frame_groups = _group_existing_frames(
            frame_root,
            kind=kind,
            kind_dir=kind_dir,
            cs_run_tag=cs_run_tag,
            exp_ids=selected_exp_ids,
            range_keys=selected_range_keys,
            station_tag=selected_station_tag,
        )
        if not frame_groups:
            print(f"No existing {kind_label} frames found under {frame_root}; MP4 skipped.")
            return

        t_start = time.perf_counter()
        for prefix, frames in frame_groups:
            mp4_path = mp4_root / f"{prefix}_evolution_nframes{len(frames)}.mp4"
            print(f"  {len(frames)} existing frames -> {mp4_path.name}")
            _build_mp4(ffmpeg_cmd, frames, mp4_path, mp4_fps)
            print(f"  MP4: {mp4_path}")
            if sys.platform == "darwin":
                subprocess.run(["open", "-R", str(mp4_path)])

        dt_total = time.perf_counter() - t_start
        print(f"\nDone. Total wall time: {dt_total:.1f}s ({dt_total / 60:.1f}min)")
        return

    cfg = load_process_budget_data(REPO_ROOT, config_path=args.config)
    apply_publication_style()
    matplotlib.use("Agg")

    station_ids = _parse_csv_ints(args.station_ids) or cfg_yaml.get("selection", {}).get("plot_station_ids", cfg["plot_stn_ids"])
    height_sel_m = cfg_yaml.get("plotting", {}).get("height_sel_m", cfg["height_sel_m"])
    time_window = _build_time_window(cfg_yaml, cfg)
    if len(time_window) < 2:
        raise ValueError("Need at least two time points in plotting.time_spacing_min.")
    frame_dpi = int(render_cfg.get("frame_dpi", 300))
    frame_png_compress = int(render_cfg.get("frame_png_compress", 1))

    plot_exp_ids = _parse_csv_ints(args.exp_ids) or cfg_yaml.get("selection", {}).get("plot_experiment_ids", cfg["plot_exp_ids"])
    plot_range_keys = _parse_csv_strs(args.range_keys) or cfg_yaml.get("plotting", {}).get("plot_range_keys", cfg["plot_range_keys"])
    bad_ranges = [rk for rk in plot_range_keys if rk not in cfg["size_ranges"]]
    if bad_ranges:
        raise ValueError(f"Unknown range key(s): {bad_ranges}. Valid: {', '.join(cfg['size_ranges'].keys())}")
    workers = max(1, int(args.workers if args.workers is not None else render_cfg.get("workers", 4)))
    stn_tag = _station_tag(station_ids)

    ridge_floor = float(sw_cfg.get("ridge_mass_floor", 0.0))
    ridge_context: dict[tuple[int, str], dict[int, tuple[int, int, float]]] = {}
    for eid in plot_exp_ids:
        r0 = cfg["rates_by_exp"][eid]
        rcf0 = r0.get("spec_conc_Q_F")
        if rcf0 is None:
            continue
        for rk in plot_range_keys:
            bs0 = cfg["size_ranges"][rk]["slice"]
            per_stn: dict[int, tuple[int, int, float]] = {}
            for stn in station_ids:
                anch0 = first_plume_ridge_anchor(rcf0, stn, bs0, floor=ridge_floor)
                if anch0 is None:
                    continue
                gti0, hi0 = anch0
                hm0 = float(np.asarray(rcf0["height_level"].values)[int(hi0)])
                per_stn[stn] = (gti0, hi0, hm0)
            ridge_context[(eid, rk)] = per_stn

    growth_overlays, growth_csv_rows, growth_series, ridge_ref_height_m = _precompute_growth_overlays(
        plot_exp_ids=plot_exp_ids,
        plot_range_keys=plot_range_keys,
        station_ids=station_ids,
        time_window=time_window,
        cfg=cfg,
        kind=kind,
        sw_cfg=sw_cfg,
        ridge_context=ridge_context,
        ds=cfg.get("ds"),
    )
    if sw_cfg.get("write_growth_csv", False) and growth_csv_rows:
        csv_paths = spectral_growth_output_paths(cs_run_tag, kind, stn_tag, repo_root=REPO_ROOT)
        csv_path = csv_paths["canonical"]
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        fields = list(growth_csv_rows[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as cf:
            w = csv.DictWriter(cf, fieldnames=fields)
            w.writeheader()
            w.writerows(growth_csv_rows)
        sync_file(csv_path, [csv_paths["legacy"]])
        print(f"  Ridge growth CSV: {csv_path} ({len(growth_csv_rows)} rows)")

    def render_task(task: tuple[int, str, int]) -> str:
        eid, range_key, itime = task
        r = cfg["rates_by_exp"][eid]
        tw = slice(time_window[itime], time_window[itime + 1])
        ctx = ridge_context.get((eid, range_key), {})
        anch_map = {s: (g, h) for s, (g, h, _) in ctx.items()}
        ref_hm = ridge_ref_height_m.get((eid, range_key), {})
        fallback_hm = {s: hm for s, (_, _, hm) in ctx.items()}
        hm_map = {
            s: (float(ref_hm[s]) if s in ref_hm and np.isfinite(ref_hm[s]) else fallback_hm[s])
            for s in station_ids
            if (s in ref_hm and np.isfinite(ref_hm[s])) or s in fallback_hm
        }

        spec_conc_w = r.get(f"spec_conc_{kind}_W") if sw_cfg.get("show_psd_twin") else None
        spec_conc_f = r.get(f"spec_conc_{kind}_F") if sw_cfg.get("show_psd_twin") else None
        conc_unit_label = r"#/m³" if kind == "N" else r"g/m³"
        
        gmap = growth_overlays.get((eid, range_key, itime))
        if sw_cfg.get("show_growth_footer") or sw_cfg.get("show_growth_sparkline"):
            fser = growth_series.get((eid, range_key)) or {}
        else:
            fser = None
        slice_lbl = "g/m3" if kind == "Q" else "#/m3"
        fig, _ = plot_spectral_waterfall(
            spec_rates_w=r[f"spec_rates_{kind}_W"],
            spec_rates_f=r[f"spec_rates_{kind}_F"],
            size_ranges=cfg["size_ranges"],
            range_key=range_key,
            diameter_um=cfg["diameter_um"],
            station_ids=station_ids,
            station_labels=cfg["station_labels"],
            height_sel_m=height_sel_m,
            twindow=tw,
            unit_label=r[f"unit_{kind}"],
            kind_label=kind_label,
            cfg_plot=sw_cfg,
            normalize_mode=sw_cfg["normalize_mode"],
            plot_style=sw_cfg["plot_style"],
            yscale=sw_cfg["yscale"],
            spec_conc_w=spec_conc_w,
            spec_conc_f=spec_conc_f,
            ridge_conc_f=r.get("spec_conc_Q_F"),
            conc_unit_label=conc_unit_label,
            ridge_anchor_by_station=anch_map if anch_map else None,
            ridge_height_m_by_station=hm_map if hm_map else None,
            growth_overlay_by_station=gmap,
            growth_footer_series_by_station=fser,
            growth_footer_itime=itime,
            growth_footer_slice_unit=slice_lbl,
        )
        stem = f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime{itime}"
        _save_frame(fig, stem, frame_root / f"exp{eid}" / kind_dir, frame_dpi, frame_png_compress)
        plt.close(fig)
        return stem

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    t_start = time.perf_counter()
    dt_batch = 0.0
    for eid in plot_exp_ids:
        for range_key in plot_range_keys:
            frame_dir = frame_root / f"exp{eid}" / kind_dir
            pattern = str(frame_dir / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_itime*.png")
            expected_frames = len(time_window) - 1

            if not args.mp4_only:
                _remove_existing_frames(pattern)
                tasks = [(eid, range_key, i) for i in range(len(time_window) - 1)]
                desc = f"exp={eid} {range_key}"
                t_batch = time.perf_counter()
                with ThreadPoolExecutor(max_workers=min(workers, len(tasks))) as ex:
                    futures = ex.map(render_task, tasks)
                    if tqdm is not None:
                        list(
                            tqdm(
                                futures,
                                total=len(tasks),
                                desc=desc,
                                unit="frame",
                                file=sys.stderr,
                                dynamic_ncols=True,
                            )
                        )
                    else:
                        for i, _ in enumerate(futures, 1):
                            elapsed = time.perf_counter() - t_batch
                            print(f"\r  {desc}: {i}/{len(tasks)} frames  [{elapsed:.1f}s]", end="", flush=True)
                        print()
                dt_batch = time.perf_counter() - t_batch

            frames = _sorted_frame_paths(pattern, expected_count=expected_frames)
            n_frames = len(frames)
            if args.mp4_only:
                print(f"  exp={eid} {range_key}: {n_frames} existing frames in {frame_dir}")
            else:
                print(f"  {n_frames} frames in {frame_dir}  ({dt_batch:.1f}s, {dt_batch / max(n_frames, 1):.2f}s/frame)")

            if args.mp4 and n_frames > 0:
                mp4_path = mp4_root / f"spectral_waterfall_{kind}_{cs_run_tag}_exp{eid}_{stn_tag}_{range_key}_evolution_nframes{n_frames}.mp4"
                _build_mp4(ffmpeg_cmd, frames, mp4_path, mp4_fps)
                print(f"  MP4: {mp4_path}")
                if sys.platform == "darwin":
                    subprocess.run(["open", "-R", str(mp4_path)])
            elif args.mp4 and n_frames == 0:
                print(f"  No frames found for {pattern}; MP4 skipped.")
            elif not args.mp4:
                print("  MP4 generation skipped (pass --mp4 to enable).")

    dt_total = time.perf_counter() - t_start
    print(f"\nDone. Total wall time: {dt_total:.1f}s ({dt_total / 60:.1f}min)")


if __name__ == "__main__":
    main()
