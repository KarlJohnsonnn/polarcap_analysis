#!/usr/bin/env python3
"""Join ridge metrics with PSD waterfall window statistics into one growth summary."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_DIR = REPO_ROOT / "data" / "registry"
RIDGE_CSV = REGISTRY_DIR / "ridge_metrics.csv"
PSD_CSV = REGISTRY_DIR / "psd_stats.csv"
OUT_CSV = REGISTRY_DIR / "growth_summary.csv"


def _psd_variant_summary(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values(["t_hi_min", "t_lo_min"]).reset_index(drop=True)
    last = df.iloc[-1]
    liq_mean = pd.to_numeric(df["liq_mean_diam_um"], errors="coerce")
    ice_mean = pd.to_numeric(df["ice_mean_diam_um"], errors="coerce")
    liq_tendency = pd.to_numeric(df["liq_net_tendency_per_min"], errors="coerce").abs()
    ice_tendency = pd.to_numeric(df["ice_net_tendency_per_min"], errors="coerce").abs()
    alpha = pd.to_numeric(df["alpha_ice_mean_diam"], errors="coerce")
    return pd.Series(
        {
            "psd_windows_n": int(len(df)),
            "psd_t_end_min": float(last["t_hi_min"]),
            "psd_liq_mean_diam_start_um": float(df.iloc[0]["liq_mean_diam_um"]),
            "psd_liq_mean_diam_end_um": float(last["liq_mean_diam_um"]),
            "psd_liq_mean_diam_max_um": float(liq_mean.max(skipna=True)),
            "psd_ice_mean_diam_start_um": float(df.iloc[0]["ice_mean_diam_um"]),
            "psd_ice_mean_diam_end_um": float(last["ice_mean_diam_um"]),
            "psd_ice_mean_diam_max_um": float(ice_mean.max(skipna=True)),
            "psd_liq_net_tendency_abs_max_per_min": float(liq_tendency.max(skipna=True)),
            "psd_ice_net_tendency_abs_max_per_min": float(ice_tendency.max(skipna=True)),
            "psd_alpha_ice_mean_diam_max": float(alpha.max(skipna=True)) if alpha.notna().any() else np.nan,
        }
    )


def build_growth_summary_from_dataframes(ridge: pd.DataFrame, psd: pd.DataFrame) -> pd.DataFrame:
    """Join ridge metrics rows with PSD window aggregates (same logic as file-based ``build_growth_summary``)."""
    if ridge.empty or psd.empty:
        return pd.DataFrame()
    psd = psd.copy()
    ridge = ridge.copy()
    for col in ("cs_run", "exp_id", "expname", "variant"):
        if col in psd.columns:
            psd[col] = psd[col].astype(str)
    for col in ("cs_run", "exp_id", "expname"):
        if col in ridge.columns:
            ridge[col] = ridge[col].astype(str)

    psd_grp = (
        psd.groupby(["cs_run", "exp_id", "variant"], dropna=False)
        .apply(_psd_variant_summary, include_groups=False)
        .reset_index()
    )
    psd_wide = psd_grp.pivot(index=["cs_run", "exp_id"], columns="variant")
    psd_wide.columns = [f"{col}_{variant}" for col, variant in psd_wide.columns]
    psd_wide = psd_wide.reset_index()

    out = ridge.copy()
    out["exp_id"] = out["exp_id"].astype(str)
    return out.merge(psd_wide, on=["cs_run", "exp_id"], how="left").sort_values(["cs_run", "exp_id"]).reset_index(drop=True)


def build_growth_summary(ridge_csv: Path = RIDGE_CSV, psd_csv: Path = PSD_CSV) -> pd.DataFrame:
    ridge = pd.read_csv(ridge_csv, dtype={"cs_run": str, "exp_id": str, "expname": str})
    psd = pd.read_csv(psd_csv, dtype={"cs_run": str, "exp_id": str, "expname": str, "variant": str})
    return build_growth_summary_from_dataframes(ridge, psd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Join ridge metrics and PSD waterfall stats into one growth summary CSV.")
    parser.add_argument("--ridge", type=Path, default=RIDGE_CSV, help="Ridge metrics CSV path.")
    parser.add_argument("--psd", type=Path, default=PSD_CSV, help="PSD stats CSV path.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output growth summary CSV path.")
    args = parser.parse_args()

    df = build_growth_summary(args.ridge, args.psd)
    if df.empty:
        raise SystemExit("Could not build growth summary; ridge metrics or PSD stats are empty.")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
