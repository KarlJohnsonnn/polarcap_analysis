#!/usr/bin/env python3
"""Summarize plume ridge growth metrics from the promoted PSD-stats product."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_DIR = REPO_ROOT / "data" / "registry"
PSD_STATS_CSV = REGISTRY_DIR / "psd_stats.csv"
OUT_CSV = REGISTRY_DIR / "ridge_metrics.csv"


def _load_psd_stats(psd_stats_csv: Path) -> pd.DataFrame:
    if psd_stats_csv.is_file():
        return pd.read_csv(psd_stats_csv)
    from run_psd_stats import collect_psd_stats

    return collect_psd_stats()


def _finite(series: pd.Series) -> pd.Series:
    return series[np.isfinite(series.astype(float))]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ridge growth summary metrics from PSD stats.")
    parser.add_argument("--psd-stats", type=Path, default=PSD_STATS_CSV, help="Input PSD stats CSV.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output CSV path.")
    args = parser.parse_args()

    df = _load_psd_stats(args.psd_stats)
    if df.empty:
        raise SystemExit("No PSD stats available.")

    rows: list[dict[str, object]] = []
    group_cols = ["run_id", "variant", "cs_run", "exp_id"]
    for keys, grp in df.groupby(group_cols, dropna=False):
        run_id, variant, cs_run, exp_id = keys
        grp = grp.sort_values("t_mid_min")
        alpha = _finite(grp["alpha"])
        icnc = _finite(grp["icnc_mean_um"])
        rows.append(
            {
                "run_id": run_id,
                "variant": variant,
                "cs_run": cs_run if pd.notna(cs_run) else "",
                "exp_id": int(exp_id) if pd.notna(exp_id) else "",
                "n_frames": int(len(grp)),
                "t_start_min": float(grp["t_lo_min"].min()),
                "t_end_min": float(grp["t_hi_min"].max()),
                "icnc_mean_start_um": float(grp["icnc_mean_um"].iloc[0]),
                "icnc_mean_end_um": float(grp["icnc_mean_um"].iloc[-1]),
                "icnc_mean_peak_um": float(grp["icnc_mean_um"].max()),
                "alpha_mean": float(alpha.mean()) if not alpha.empty else np.nan,
                "alpha_median": float(alpha.median()) if not alpha.empty else np.nan,
                "alpha_min": float(alpha.min()) if not alpha.empty else np.nan,
                "alpha_max": float(alpha.max()) if not alpha.empty else np.nan,
                "has_registry_match": "TRUE" if pd.notna(cs_run) else "FALSE",
            }
        )
    out = pd.DataFrame(rows).sort_values(["cs_run", "run_id", "variant"], na_position="last")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
