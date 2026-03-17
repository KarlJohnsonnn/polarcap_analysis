#!/usr/bin/env python3
"""Collect PSD statistics from existing figure13-style LaTeX tables into one CSV."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
TABLE_DIR = REPO_ROOT / "notebooks" / "output" / "tables"
REGISTRY_CSV = REPO_ROOT / "data" / "registry" / "experiment_registry.csv"
OUT_CSV = REPO_ROOT / "data" / "registry" / "psd_stats.csv"

KNOWN_VARIANTS = ("mass_small", "number_small", "mass", "number")
RAW_COLS = [
    "time_frame_min",
    "cdnc_mean_um",
    "cdnc_std_um",
    "cdnc_growth_rate",
    "icnc_mean_um",
    "icnc_std_um",
    "icnc_growth_rate",
    "alpha",
]


def _parse_file_name(path: Path) -> tuple[str, str]:
    stem = path.stem.replace("figure13_psd_stats_", "", 1)
    for variant in KNOWN_VARIANTS:
        prefix = f"{variant}_"
        if stem.startswith(prefix):
            return variant, stem[len(prefix) :]
    return "combined", stem


def _parse_numeric(cell: str) -> float:
    cell = cell.replace(r"\textbf{", "").replace("}", "")
    if "/" in cell:
        cell = cell.split("/", 1)[0]
    if "—" in cell or cell.strip() in {"", "-"}:
        return np.nan
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", cell)
    return float(match.group(0)) if match else np.nan


def _parse_time_bounds(label: str) -> tuple[float, float]:
    label = label.split("/", 1)[0].strip()
    lo, hi = label.split("-", 1)
    return float(lo), float(hi)


def collect_psd_stats(table_dir: Path = TABLE_DIR) -> pd.DataFrame:
    exp = pd.read_csv(REGISTRY_CSV, dtype={"expname": str})
    exp_map = exp[["expname", "cs_run", "exp_id"]].rename(columns={"expname": "run_id"})
    exp_map["run_id"] = exp_map["run_id"].astype(str)
    rows: list[dict[str, object]] = []
    for path in sorted(table_dir.glob("figure13_psd_stats_*.tex")):
        variant, run_id = _parse_file_name(path)
        lines = path.read_text(encoding="utf-8").splitlines()
        in_body = False
        for line in lines:
            line = line.strip()
            if line.startswith(r"\midrule"):
                in_body = True
                continue
            if line.startswith(r"\bottomrule"):
                break
            if not in_body or "&" not in line:
                continue
            parts = [p.strip().replace(r"\\", "") for p in line.split("&")]
            if len(parts) != 8:
                continue
            t_lo, t_hi = _parse_time_bounds(parts[0])
            rows.append(
                {
                    "variant": variant,
                    "run_id": str(run_id),
                    "time_frame_min": parts[0],
                    "t_lo_min": t_lo,
                    "t_hi_min": t_hi,
                    "t_mid_min": 0.5 * (t_lo + t_hi),
                    RAW_COLS[1]: _parse_numeric(parts[1]),
                    RAW_COLS[2]: _parse_numeric(parts[2]),
                    RAW_COLS[3]: _parse_numeric(parts[3]),
                    RAW_COLS[4]: _parse_numeric(parts[4]),
                    RAW_COLS[5]: _parse_numeric(parts[5]),
                    RAW_COLS[6]: _parse_numeric(parts[6]),
                    RAW_COLS[7]: _parse_numeric(parts[7]),
                    "source_table": str(path.relative_to(REPO_ROOT)),
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.merge(exp_map, on="run_id", how="left")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect figure13-style PSD stats tables into one CSV.")
    parser.add_argument("--table-dir", type=Path, default=TABLE_DIR, help="Directory with figure13_psd_stats_*.tex files.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output CSV path.")
    args = parser.parse_args()

    df = collect_psd_stats(args.table_dir)
    if df.empty:
        raise SystemExit(f"No PSD stats tables found in {args.table_dir}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
