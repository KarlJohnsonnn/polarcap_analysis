#!/usr/bin/env python3
"""Plot open-system compartment diagnostics for vapor/liquid/ice.

This script mirrors the data-loading path of `run_spectral_waterfall.py` and
creates one publication-style figure with:
1) stacked compartment fractions [%],
2) compartment gain/loss time series [g m^-3 s^-1],
3) total-water diagnostics f(t) and df/dt.

Method note for comparability with CLOUDLAB/Omanovic diagnostics:
the focus is on trajectory-style phase evolution and residual tendencies in an
open control volume, not strict local conservation.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import xarray as xr
import yaml

matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities import apply_publication_style, load_process_budget_data, stn_label  # noqa: E402


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_time_window(cfg_yaml: dict[str, Any], cfg_loaded: dict[str, Any]) -> list[np.datetime64]:
    seed_start = cfg_loaded["seed_start"]
    vals = cfg_yaml.get("plotting", {}).get("time_spacing_min")
    if isinstance(vals, list) and len(vals) >= 2:
        return [seed_start + np.timedelta64(int(float(t) * 60), "s") for t in vals]
    return cfg_loaded["time_window"]


def _pick_var(ds: xr.Dataset, name: str) -> xr.DataArray:
    for cand in (name, name.upper(), name.lower()):
        if cand in ds.data_vars:
            return ds[cand]
    raise KeyError(f"Variable '{name}' not found in dataset.")


def _height_slice(da: xr.DataArray, h_low: float, h_high: float) -> xr.DataArray:
    if "height_level" not in da.dims:
        return da
    z = da["height_level"].values
    if z.size < 2:
        return da.sel(height_level=slice(h_low, h_high))
    ascending = bool(z[-1] > z[0])
    if ascending:
        return da.sel(height_level=slice(min(h_low, h_high), max(h_low, h_high)))
    return da.sel(height_level=slice(max(h_low, h_high), min(h_low, h_high)))


def _reduce_to_time_series(
    da: xr.DataArray,
    station_idx: int,
    h_low: float,
    h_high: float,
    twindow: slice,
) -> xr.DataArray:
    out = da
    if "station" in out.dims:
        out = out.isel(station=station_idx)
    out = _height_slice(out, h_low, h_high)
    if "height_level" in out.dims:
        out = out.mean(dim="height_level")
    out = out.sel(time=twindow)
    if "bins" in out.dims:
        out = out.sum(dim="bins")
    return out


def _default_height_band(cfg_yaml: dict[str, Any], cfg_loaded: dict[str, Any]) -> tuple[float, float]:
    hsel = cfg_yaml.get("plotting", {}).get("height_sel_m", cfg_loaded.get("height_sel_m", [1400, 1200]))
    if not isinstance(hsel, list) or len(hsel) < 2:
        return (1200.0, 1400.0)
    return (float(min(hsel[0], hsel[1])), float(max(hsel[0], hsel[1])))


def _compute_time_derivative(y: np.ndarray, t_s: np.ndarray) -> np.ndarray:
    if y.size < 2:
        return np.zeros_like(y)
    return np.gradient(y, t_s, edge_order=1)


def extract_absolute_compartments(
    ds_exp: xr.Dataset,
    station_idx: int,
    h_low: float,
    h_high: float,
    twindow: slice,
) -> dict[str, Any]:
    rho = _pick_var(ds_exp, "RHO")
    qv = _pick_var(ds_exp, "QV")
    qw = _pick_var(ds_exp, "QW")
    qf = _pick_var(ds_exp, "QF")
    qfw = _pick_var(ds_exp, "QFW")

    rho_t = _reduce_to_time_series(rho, station_idx, h_low, h_high, twindow)
    qv_t = _reduce_to_time_series(qv, station_idx, h_low, h_high, twindow)
    qw_t = _reduce_to_time_series(qw, station_idx, h_low, h_high, twindow)
    qf_t = _reduce_to_time_series(qf, station_idx, h_low, h_high, twindow)
    qfw_t = _reduce_to_time_series(qfw, station_idx, h_low, h_high, twindow)

    # Mixing ratio [kg kg^-1] * rho [kg m^-3] * 1000 -> [g m^-3]
    vapor = (qv_t * rho_t * 1.0e3).values
    ice = (qf_t * rho_t * 1.0e3).values
    liquid_shell = ((qfw_t - qf_t) * rho_t * 1.0e3).values
    liquid = ((qw_t + (qfw_t - qf_t)) * rho_t * 1.0e3).values

    time_vals = qv_t["time"].values
    t_s = (time_vals - time_vals[0]).astype("timedelta64[s]").astype(float)
    t_min = t_s / 60.0

    f_total = vapor + liquid + ice
    
    return {
        "vapor": vapor,
        "liquid": liquid,
        "ice": ice,
        "liquid_shell": liquid_shell,
        "f_total": f_total,
        "time_vals": time_vals,
        "t_s": t_s,
        "t_min": t_min,
    }

def compute_anomaly_diagnostics(
    ds: xr.Dataset,
    seed_exp_id: int,
    ref_exp_id: int,
    station_idx: int,
    h_low: float,
    h_high: float,
    twindow: slice,
) -> dict[str, Any]:
    from scipy.integrate import cumulative_trapezoid

    ds_seed = ds.isel(expname=seed_exp_id)
    if "HMLd" in ds_seed.coords:
        ds_seed = ds_seed.assign_coords(height_level=ds_seed["HMLd"])
    
    ds_ref = ds.isel(expname=ref_exp_id)
    if "HMLd" in ds_ref.coords:
        ds_ref = ds_ref.assign_coords(height_level=ds_ref["HMLd"])

    seed_abs = extract_absolute_compartments(ds_seed, station_idx, h_low, h_high, twindow)
    ref_abs = extract_absolute_compartments(ds_ref, station_idx, h_low, h_high, twindow)

    t_s = seed_abs["t_s"]
    t_min = seed_abs["t_min"]

    d_vap = seed_abs["vapor"] - ref_abs["vapor"]
    d_liq = seed_abs["liquid"] - ref_abs["liquid"]
    d_ice = seed_abs["ice"] - ref_abs["ice"]
    d_tot = seed_abs["f_total"] - ref_abs["f_total"]

    dt_vap = _compute_time_derivative(d_vap, t_s)
    dt_liq = _compute_time_derivative(d_liq, t_s)
    dt_ice = _compute_time_derivative(d_ice, t_s)
    dt_tot = _compute_time_derivative(d_tot, t_s)

    int_vap = cumulative_trapezoid(d_vap, x=t_s, initial=0.0)
    int_liq = cumulative_trapezoid(d_liq, x=t_s, initial=0.0)
    int_ice = cumulative_trapezoid(d_ice, x=t_s, initial=0.0)
    int_tot = cumulative_trapezoid(d_tot, x=t_s, initial=0.0)

    return {
        "time": seed_abs["time_vals"],
        "time_min": t_min,
        "abs_seed": seed_abs,
        "abs_ref": ref_abs,
        "d_vap": d_vap,
        "d_liq": d_liq,
        "d_ice": d_ice,
        "d_tot": d_tot,
        "dt_vap": dt_vap,
        "dt_liq": dt_liq,
        "dt_ice": dt_ice,
        "dt_tot": dt_tot,
        "int_vap": int_vap,
        "int_liq": int_liq,
        "int_ice": int_ice,
        "int_tot": int_tot,
    }


def plot_anomaly_diagnostics(
    diag: dict[str, Any],
    *,
    seed_exp_id: int,
    ref_exp_id: int,
    station_label: str,
    h_low: float,
    h_high: float,
) -> plt.Figure:
    fig, axes = plt.subplots(
        4,
        1,
        figsize=(8.4, 11.0),
        sharex=True,
        constrained_layout=True,
        gridspec_kw={"height_ratios": [1.0, 1.0, 1.0, 1.0]},
    )
    axA, axB, axC, axD = axes

    t = diag["time_min"]
    c_vap = "#4C78A8"
    c_liq = "#54A24B"
    c_ice = "#B279A2"
    c_tot = "#F58518"

    # Panel A: Absolute seeded series
    abs_seed = diag["abs_seed"]
    axA.plot(t, abs_seed["vapor"], color=c_vap, linewidth=1.5, label="Vapor")
    axA.plot(t, abs_seed["liquid"], color=c_liq, linewidth=1.5, label="Liquid")
    axA.plot(t, abs_seed["ice"], color=c_ice, linewidth=1.5, label="Ice")
    axA.plot(t, abs_seed["f_total"], color=c_tot, linewidth=1.5, linestyle="--", label="Total")
    axA.set_ylabel(r"Mass [g m$^{-3}$]")
    axA.set_title("A: Absolute compartments (seeded)", loc="left", fontsize="medium", fontweight="bold")
    axA.grid(True, alpha=0.25, linewidth=0.5)
    axA.legend(ncol=4, loc="upper right", frameon=False, fontsize="small")

    # Panel B: Anomalies (Seed - Ref)
    axB.axhline(0.0, color="0.25", linewidth=0.8, linestyle="-")
    axB.plot(t, diag["d_vap"], color=c_vap, linewidth=1.5, label=r"$\Delta$ Vapor")
    axB.plot(t, diag["d_liq"], color=c_liq, linewidth=1.5, label=r"$\Delta$ Liquid")
    axB.plot(t, diag["d_ice"], color=c_ice, linewidth=1.5, label=r"$\Delta$ Ice")
    axB.plot(t, diag["d_tot"], color=c_tot, linewidth=1.5, linestyle="--", label=r"$\Delta$ Total")
    axB.set_ylabel(r"Anomaly [g m$^{-3}$]")
    axB.set_title("B: Seeded minus Reference anomaly", loc="left", fontsize="medium", fontweight="bold")
    axB.grid(True, alpha=0.25, linewidth=0.5)
    axB.legend(ncol=4, loc="upper right", frameon=False, fontsize="small")

    # Panel C: Tendencies of anomalies
    axC.axhline(0.0, color="0.25", linewidth=0.8, linestyle="-")
    axC.plot(t, diag["dt_vap"], color=c_vap, linewidth=1.2, label=r"d($\Delta$V)/dt")
    axC.plot(t, diag["dt_liq"], color=c_liq, linewidth=1.2, label=r"d($\Delta$L)/dt")
    axC.plot(t, diag["dt_ice"], color=c_ice, linewidth=1.2, label=r"d($\Delta$I)/dt")
    axC.plot(t, diag["dt_tot"], color=c_tot, linewidth=1.2, linestyle="--", label=r"d($\Delta$Tot)/dt")
    axC.set_ylabel(r"Tendency [g m$^{-3}$ s$^{-1}$]")
    axC.set_title("C: Anomaly tendencies", loc="left", fontsize="medium", fontweight="bold")
    axC.grid(True, alpha=0.25, linewidth=0.5)
    axC.legend(ncol=4, loc="upper right", frameon=False, fontsize="small")

    # Panel D: Cumulative integral of anomalies
    axD.axhline(0.0, color="0.25", linewidth=0.8, linestyle="-")
    axD.plot(t, diag["int_vap"], color=c_vap, linewidth=1.5, label=r"$\int \Delta$V dt")
    axD.plot(t, diag["int_liq"], color=c_liq, linewidth=1.5, label=r"$\int \Delta$L dt")
    axD.plot(t, diag["int_ice"], color=c_ice, linewidth=1.5, label=r"$\int \Delta$I dt")
    axD.plot(t, diag["int_tot"], color=c_tot, linewidth=1.5, linestyle="--", label=r"$\int \Delta$Tot dt")
    axD.set_ylabel(r"Cumulative [g m$^{-3}$ s]")
    axD.set_title("D: Integrated cumulative change", loc="left", fontsize="medium", fontweight="bold")
    axD.grid(True, alpha=0.25, linewidth=0.5)
    axD.legend(ncol=4, loc="upper right", frameon=False, fontsize="small")

    axD.set_xlabel("Elapsed time [min]")

    t0 = str(diag["time"][0])[11:19] if diag["time"].size else "n/a"
    t1 = str(diag["time"][-1])[11:19] if diag["time"].size else "n/a"
    fig.suptitle(
        (
            f"Seeding Impact Diagnostics — Seed: exp {seed_exp_id} | Ref: exp {ref_exp_id}\n"
            f"{station_label}, "
            f"{h_low:.0f}-{h_high:.0f} m ({t0}-{t1})\n"
            "Mapping: vapor=QV, liquid=QW+(QFW-QF), ice=QF"
        ),
        fontweight="semibold",
    )

    return fig


def _print_summary(diag: dict[str, np.ndarray]) -> None:
    d_tot = diag["d_tot"]
    dt_tot = diag["dt_tot"]
    int_tot = diag["int_tot"]
    print(
        "Impact diagnostics: "
        f"max|ΔTot|={np.nanmax(np.abs(d_tot)):.3e} g m^-3, "
        f"max|d(ΔTot)/dt|={np.nanmax(np.abs(dt_tot)):.3e} g m^-3 s^-1, "
        f"final ∫ΔTot dt={int_tot[-1] if int_tot.size else np.nan:.3e} g m^-3 s"
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate multi-panel seeding impact diagnostics (Seed - Ref).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--config", type=Path, default=REPO_ROOT / "config" / "psd_process_evolution.yaml")
    p.add_argument("--seed-exp-id", type=int, default=None, help="Seeded experiment index.")
    p.add_argument("--ref-exp-id", type=int, default=None, help="Reference experiment index.")
    p.add_argument("--station-id", type=int, default=None, help="Station index.")
    p.add_argument("--h-low", type=float, default=None, help="Lower height bound [m].")
    p.add_argument("--h-high", type=float, default=None, help="Upper height bound [m].")
    p.add_argument("--time-start", type=int, default=0, help="Start index in plotting time-spacing list.")
    p.add_argument("--time-stop", type=int, default=-1, help="Stop index (inclusive). -1 uses last.")
    p.add_argument("--out-dir", type=Path, default=None, help="Optional output directory.")
    p.add_argument("--filename", type=str, default=None, help="Optional output PNG name.")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    cfg_yaml = _read_yaml(args.config)
    cfg = load_process_budget_data(REPO_ROOT, config_path=args.config)
    apply_publication_style()

    ds = cfg["ds"]
    seed_exp_id = int(args.seed_exp_id if args.seed_exp_id is not None else cfg["plot_exp_ids"][0])
    # Default ref to exp 0 if available, else first exp
    ref_exp_id = int(args.ref_exp_id if args.ref_exp_id is not None else (0 if 0 in ds.expname else cfg["plot_exp_ids"][0]))
    
    station_id = int(args.station_id if args.station_id is not None else cfg["plot_stn_ids"][0])
    station_label = stn_label(station_id, cfg["station_labels"])

    h_default_low, h_default_high = _default_height_band(cfg_yaml, cfg)
    h_low = float(args.h_low) if args.h_low is not None else h_default_low
    h_high = float(args.h_high) if args.h_high is not None else h_default_high
    if h_low > h_high:
        h_low, h_high = h_high, h_low

    tw = _build_time_window(cfg_yaml, cfg)
    i0 = max(0, int(args.time_start))
    i1 = len(tw) - 1 if args.time_stop < 0 else min(len(tw) - 1, int(args.time_stop))
    if i1 <= i0:
        raise ValueError(f"Invalid time window indices: start={i0}, stop={i1}")
    twindow = slice(tw[i0], tw[i1])

    diag = compute_anomaly_diagnostics(ds, seed_exp_id, ref_exp_id, station_id, h_low, h_high, twindow)
    fig = plot_anomaly_diagnostics(
        diag,
        seed_exp_id=seed_exp_id,
        ref_exp_id=ref_exp_id,
        station_label=station_label,
        h_low=h_low,
        h_high=h_high,
    )
    _print_summary(diag)

    cs_run = cfg_yaml.get("ensemble", {}).get("cs_run", "unknown_cs_run")
    if args.out_dir is None:
        out_dir = REPO_ROOT / "output" / "gfx" / "png" / "06" / cs_run / f"exp{seed_exp_id}"
    else:
        out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.filename:
        out_name = args.filename
    else:
        cs_tag = cs_run.replace("/", "_")
        out_name = f"compartment_impact_{cs_tag}_seed{seed_exp_id}_ref{ref_exp_id}_stn{station_id}_{h_low:.0f}-{h_high:.0f}m.png"
    out_path = out_dir / out_name
    fig.savefig(out_path, dpi=300)
    plt.close(fig)
    print(f"Saved figure: '{out_path}'")


if __name__ == "__main__":
    main()
