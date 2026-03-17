#!/usr/bin/env python3
"""Build LV1-backed ridge metrics from integrated plume-path `nf(path, diameter)` files."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_DIR = REPO_ROOT / "data" / "registry"
REGISTRY_CSV = REGISTRY_DIR / "analysis_registry.csv"
PROCESSED_ROOT = REPO_ROOT / "scripts" / "processing_chain" / "processed"
OUT_CSV = REGISTRY_DIR / "ridge_metrics.csv"
OUT_TS_CSV = REGISTRY_DIR / "ridge_timeseries.csv"

ANCHOR_THRESHOLD_L = 1.0
RIDGE_MIN_UM = 5.0
ALPHA_T_MIN = 0.5
ALPHA_T_MAX = 10.0
MIN_ALPHA_POINTS = 4
TIME_BIN_DECIMALS = 3


def _paper_lv1_subset(registry_csv: Path) -> pd.DataFrame:
    reg = pd.read_csv(registry_csv, dtype=str)
    keep = reg[
        (reg["is_reference"].astype(str).str.upper() == "FALSE")
        & (reg["include_in_paper"].astype(str).str.upper() == "TRUE")
        & (reg["local_lv1_available"].astype(str).str.upper() == "TRUE")
    ].copy()
    cols = ["cs_run", "exp_id", "expname", "ref_exp_id", "ref_expname", "pair_method"]
    return keep[cols].drop_duplicates().sort_values(["cs_run", "exp_id"]).reset_index(drop=True)


def _path_files(cs_run: str, expname: str, processed_root: Path) -> list[Path]:
    pattern = f"data_{cs_run}_{expname}_integrated_plume_path_nf_cell*.nc"
    return sorted((processed_root / cs_run / "lv1_paths").glob(pattern))


def _safe_total_nf(nf: np.ndarray) -> np.ndarray:
    return np.nansum(np.where(np.isfinite(nf), np.maximum(nf, 0.0), 0.0), axis=1)


def _ridge_diameters(nf: np.ndarray, diameter_um: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    peak = np.full(nf.shape[0], np.nan, dtype=float)
    mean = np.full(nf.shape[0], np.nan, dtype=float)
    for idx, row in enumerate(nf):
        good = np.isfinite(row) & (row > 0.0) & np.isfinite(diameter_um) & (diameter_um >= RIDGE_MIN_UM)
        if not np.any(good):
            continue
        weights = row[good]
        diam = diameter_um[good]
        peak[idx] = float(diam[np.argmax(weights)])
        mean[idx] = float(np.sum(weights * diam) / np.sum(weights))
    return peak, mean


def _time_minutes(time_values: np.ndarray, anchor_idx: int) -> np.ndarray:
    anchor = np.datetime64(time_values[anchor_idx], "ns")
    vals = np.asarray(time_values, dtype="datetime64[ns]")
    return np.asarray((vals - anchor) / np.timedelta64(1, "m"), dtype=float)


def _nan_quantile(values: pd.Series | np.ndarray, q: float) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return np.nan
    return float(np.quantile(arr, q))


def _alpha_fit(t_min: np.ndarray, ridge_um: np.ndarray) -> tuple[float, float]:
    mask = (
        np.isfinite(t_min)
        & np.isfinite(ridge_um)
        & (t_min >= ALPHA_T_MIN)
        & (t_min <= ALPHA_T_MAX)
        & (ridge_um > 0.0)
    )
    if int(mask.sum()) < MIN_ALPHA_POINTS:
        return np.nan, np.nan
    slope, intercept = np.polyfit(np.log(t_min[mask]), np.log(ridge_um[mask]), 1)
    return float(slope), float(intercept)


def _open_cell(path: Path) -> tuple[pd.DataFrame, dict[str, object]] | None:
    ds = xr.open_dataset(path)
    if "nf" not in ds or "time" not in ds or "diameter" not in ds:
        return None
    nf = np.asarray(ds["nf"].values, dtype=float)
    if nf.ndim != 2:
        return None
    time_values = np.asarray(ds["time"].values)
    diameter_um = np.asarray(ds["diameter"].values, dtype=float)
    total_nf = _safe_total_nf(nf)
    anchor_hits = np.where(total_nf >= ANCHOR_THRESHOLD_L)[0]
    if anchor_hits.size == 0:
        return None
    anchor_idx = int(anchor_hits[0])
    t_min = _time_minutes(time_values, anchor_idx)
    peak_um, mean_um = _ridge_diameters(nf, diameter_um)
    alpha_peak, intercept_peak = _alpha_fit(t_min, peak_um)
    alpha_mean, intercept_mean = _alpha_fit(t_min, mean_um)
    df = pd.DataFrame(
        {
            "time": pd.to_datetime(time_values),
            "time_rel_min": np.round(t_min, TIME_BIN_DECIMALS),
            "total_nf_l": total_nf,
            "ridge_peak_um": peak_um,
            "ridge_mean_um": mean_um,
        }
    )
    df["valid_ridge"] = np.isfinite(df["ridge_peak_um"]).astype(int)
    summary = {
        "path_start_time": str(np.datetime64(time_values[0], "s")),
        "anchor_time": str(np.datetime64(time_values[anchor_idx], "s")),
        "anchor_idx": anchor_idx,
        "anchor_total_nf_l": float(total_nf[anchor_idx]),
        "alpha_peak": alpha_peak,
        "alpha_peak_intercept": intercept_peak,
        "alpha_mean": alpha_mean,
        "alpha_mean_intercept": intercept_mean,
    }
    return df, summary


def _quantile_rows(ts: pd.DataFrame, meta: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for t_rel, grp in ts.groupby("time_rel_min", dropna=True):
        peak = grp["ridge_peak_um"].astype(float)
        mean = grp["ridge_mean_um"].astype(float)
        total = grp["total_nf_l"].astype(float)
        rows.append(
            {
                **meta,
                "time_rel_min": float(t_rel),
                "n_cells": int(len(grp)),
                "ridge_peak_um_p25": _nan_quantile(peak, 0.25),
                "ridge_peak_um_p50": _nan_quantile(peak, 0.50),
                "ridge_peak_um_p75": _nan_quantile(peak, 0.75),
                "ridge_mean_um_p25": _nan_quantile(mean, 0.25),
                "ridge_mean_um_p50": _nan_quantile(mean, 0.50),
                "ridge_mean_um_p75": _nan_quantile(mean, 0.75),
                "total_nf_l_p25": _nan_quantile(total, 0.25),
                "total_nf_l_p50": _nan_quantile(total, 0.50),
                "total_nf_l_p75": _nan_quantile(total, 0.75),
            }
        )
    return pd.DataFrame(rows)


def _run_summary(cell_rows: pd.DataFrame, ts_rows: pd.DataFrame, meta: dict[str, object]) -> dict[str, object]:
    alpha_peak = pd.to_numeric(cell_rows["alpha_peak"], errors="coerce")
    alpha_mean = pd.to_numeric(cell_rows["alpha_mean"], errors="coerce")
    peak_p50 = pd.to_numeric(ts_rows["ridge_peak_um_p50"], errors="coerce")
    mean_p50 = pd.to_numeric(ts_rows["ridge_mean_um_p50"], errors="coerce")
    time_rel = pd.to_numeric(ts_rows["time_rel_min"], errors="coerce")
    late = ts_rows.loc[time_rel >= 5.0]
    return {
        **meta,
        "data_source": "lv1_paths",
        "anchor_threshold_l": ANCHOR_THRESHOLD_L,
        "n_cells": int(len(cell_rows)),
        "n_times": int(len(ts_rows)),
        "alpha_peak_median": float(np.nanmedian(alpha_peak)) if alpha_peak.notna().any() else np.nan,
        "alpha_peak_p25": _nan_quantile(alpha_peak, 0.25),
        "alpha_peak_p75": _nan_quantile(alpha_peak, 0.75),
        "alpha_mean_median": float(np.nanmedian(alpha_mean)) if alpha_mean.notna().any() else np.nan,
        "alpha_mean_p25": _nan_quantile(alpha_mean, 0.25),
        "alpha_mean_p75": _nan_quantile(alpha_mean, 0.75),
        "ridge_peak_start_um": float(peak_p50.iloc[0]) if len(peak_p50) else np.nan,
        "ridge_peak_end_um": float(peak_p50.iloc[-1]) if len(peak_p50) else np.nan,
        "ridge_peak_max_um": float(np.nanmax(peak_p50)) if len(peak_p50) else np.nan,
        "ridge_mean_start_um": float(mean_p50.iloc[0]) if len(mean_p50) else np.nan,
        "ridge_mean_end_um": float(mean_p50.iloc[-1]) if len(mean_p50) else np.nan,
        "ridge_mean_max_um": float(np.nanmax(mean_p50)) if len(mean_p50) else np.nan,
        "ridge_peak_late_um": float(pd.to_numeric(late["ridge_peak_um_p50"], errors="coerce").iloc[-1]) if len(late) else np.nan,
        "time_end_min": float(time_rel.max()) if len(time_rel) else np.nan,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build LV1 plume-ridge metrics and uncertainty summaries.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_CSV, help="Merged analysis registry CSV.")
    parser.add_argument("--processed-root", type=Path, default=PROCESSED_ROOT, help="Processed output root with lv1_paths.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Run-summary CSV path.")
    parser.add_argument("--timeseries-output", type=Path, default=OUT_TS_CSV, help="Per-run ridge uncertainty time series CSV.")
    args = parser.parse_args()

    subset = _paper_lv1_subset(args.registry)
    if subset.empty:
        raise SystemExit("No paper-subset flare runs with local LV1 paths.")

    summary_rows: list[dict[str, object]] = []
    ts_frames: list[pd.DataFrame] = []
    cell_frames: list[pd.DataFrame] = []

    for row in subset.itertuples(index=False):
        files = _path_files(row.cs_run, row.expname, args.processed_root)
        if not files:
            continue
        cell_rows: list[dict[str, object]] = []
        ts_rows: list[pd.DataFrame] = []
        base_meta = {
            "cs_run": row.cs_run,
            "exp_id": int(row.exp_id),
            "expname": row.expname,
            "ref_exp_id": int(row.ref_exp_id) if str(row.ref_exp_id).strip() else "",
            "ref_expname": row.ref_expname,
            "pair_method": row.pair_method,
        }
        for path in files:
            opened = _open_cell(path)
            if opened is None:
                continue
            ts, summary = opened
            cell_id = int(path.stem.rsplit("cell", 1)[-1])
            cell_meta = {**base_meta, "cell_id": cell_id, **summary}
            ts_rows.append(ts.assign(cell_id=cell_id, **base_meta))
            cell_rows.append(cell_meta)
        if not cell_rows:
            continue
        cell_df = pd.DataFrame(cell_rows).sort_values("cell_id").reset_index(drop=True)
        ts_df = pd.concat(ts_rows, ignore_index=True)
        ts_q = _quantile_rows(ts_df, base_meta).sort_values("time_rel_min").reset_index(drop=True)
        summary_rows.append(_run_summary(cell_df, ts_q, base_meta))
        ts_frames.append(ts_q)
        cell_frames.append(cell_df)

    if not summary_rows:
        raise SystemExit("No LV1 ridge metrics could be computed from the available plume-path files.")

    out = pd.DataFrame(summary_rows).sort_values(["cs_run", "exp_id"]).reset_index(drop=True)
    out_ts = pd.concat(ts_frames, ignore_index=True).sort_values(["cs_run", "exp_id", "time_rel_min"]).reset_index(drop=True)
    out_cells = pd.concat(cell_frames, ignore_index=True).sort_values(["cs_run", "exp_id", "cell_id"]).reset_index(drop=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(out.to_csv(index=False), encoding="utf-8")
    args.timeseries_output.write_text(out_ts.to_csv(index=False), encoding="utf-8")
    cell_output = args.output.with_name("ridge_cell_metrics.csv")
    cell_output.write_text(out_cells.to_csv(index=False), encoding="utf-8")
    print(f"Wrote {len(out)} rows to {args.output}")
    print(f"Wrote {len(out_ts)} rows to {args.timeseries_output}")
    print(f"Wrote {len(out_cells)} rows to {cell_output}")


if __name__ == "__main__":
    main()
