#!/usr/bin/env python3
"""Collect structured PSD waterfall statistics into one registry CSV."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
STATS_DIR = REPO_ROOT / "scripts" / "processing_chain" / "output" / "04" / "stats"
LEGACY_TABLE_DIR = REPO_ROOT / "scripts" / "processing_chain" / "output" / "04" / "tables"
REGISTRY_CSV = REPO_ROOT / "data" / "registry" / "experiment_registry.csv"
OUT_CSV = REPO_ROOT / "data" / "registry" / "psd_stats.csv"

KNOWN_VARIANTS = ("mass_small", "number_small", "mass", "number")
NUMERIC_COLS = [
    "t_lo_min",
    "t_hi_min",
    "t_mid_min",
    "liq_mean_diam_um",
    "liq_std_diam_um",
    "liq_net_tendency_per_min",
    "ice_mean_diam_um",
    "ice_std_diam_um",
    "ice_net_tendency_per_min",
    "obs_ice_mean_diam_um",
    "obs_ice_std_diam_um",
    "alpha_ice_mean_diam",
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


def _registry_lookup(registry_csv: Path) -> pd.DataFrame:
    reg = pd.read_csv(registry_csv, dtype=str)
    rows = []
    for row in reg.itertuples(index=False):
        for key in {str(getattr(row, "expname", "")).strip(), str(getattr(row, "exp_id", "")).strip()}:
            if not key:
                continue
            rows.append(
                {
                    "run_id": key,
                    "cs_run": getattr(row, "cs_run", ""),
                    "exp_id": getattr(row, "exp_id", ""),
                    "expname": getattr(row, "expname", ""),
                }
            )
    return pd.DataFrame(rows).drop_duplicates(subset=["run_id"])


def _collect_structured_stats(stats_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(stats_dir.glob("figure13_psd_stats_*.csv")):
        df = pd.read_csv(path, dtype={"run_id": str, "run_label": str, "variant": str, "basis": str})
        variant, run_id = _parse_file_name(path)
        if "variant" not in df.columns:
            df["variant"] = variant
        if "run_id" not in df.columns:
            df["run_id"] = str(run_id)
        if "basis" not in df.columns:
            df["basis"] = "mass" if str(df["variant"].iloc[0]).startswith("mass") else "number"
        df["source_stats_csv"] = str(path.relative_to(REPO_ROOT))
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _collect_legacy_latex(table_dir: Path) -> pd.DataFrame:
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
                    "basis": "mass" if variant.startswith("mass") else "number",
                    "run_id": str(run_id),
                    "time_frame_min": parts[0],
                    "t_lo_min": t_lo,
                    "t_hi_min": t_hi,
                    "t_mid_min": 0.5 * (t_lo + t_hi),
                    "liq_mean_diam_um": _parse_numeric(parts[1]),
                    "liq_std_diam_um": _parse_numeric(parts[2]),
                    "liq_net_tendency_per_min": _parse_numeric(parts[3]),
                    "ice_mean_diam_um": _parse_numeric(parts[4]),
                    "ice_std_diam_um": _parse_numeric(parts[5]),
                    "ice_net_tendency_per_min": _parse_numeric(parts[6]),
                    "alpha_ice_mean_diam": _parse_numeric(parts[7]),
                    "obs_match_ids": "",
                    "obs_ice_mean_diam_um": np.nan,
                    "obs_ice_std_diam_um": np.nan,
                    "source_table": str(path.relative_to(REPO_ROOT)),
                }
            )
    return pd.DataFrame(rows)


def collect_psd_stats(
    stats_dir: Path = STATS_DIR,
    legacy_table_dir: Path = LEGACY_TABLE_DIR,
    registry_csv: Path = REGISTRY_CSV,
) -> pd.DataFrame:
    df = _collect_structured_stats(stats_dir)
    if df.empty:
        df = _collect_legacy_latex(legacy_table_dir)
    if df.empty:
        return df

    if "run_id" in df.columns:
        df["run_id"] = df["run_id"].astype(str)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    lookup = _registry_lookup(registry_csv)
    if not lookup.empty:
        df = df.merge(lookup, on="run_id", how="left")
    return df.sort_values(["variant", "run_id", "t_lo_min"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect PSD waterfall window statistics into one CSV.")
    parser.add_argument(
        "--stats-dir",
        type=Path,
        default=STATS_DIR,
        help="Directory with structured figure13_psd_stats_*.csv files.",
    )
    parser.add_argument(
        "--legacy-table-dir",
        type=Path,
        default=LEGACY_TABLE_DIR,
        help="Fallback directory with legacy figure13_psd_stats_*.tex files.",
    )
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output CSV path.")
    args = parser.parse_args()

    df = collect_psd_stats(args.stats_dir, args.legacy_table_dir, REGISTRY_CSV)
    if df.empty:
        raise SystemExit(f"No PSD stats found in {args.stats_dir} or {args.legacy_table_dir}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
