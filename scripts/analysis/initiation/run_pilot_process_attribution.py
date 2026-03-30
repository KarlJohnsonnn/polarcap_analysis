#!/usr/bin/env python3
"""Build a first pilot process-attribution matrix from the promoted initiation fractions table."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.table_paths import registry_output_paths, resolve_registry_input, sync_file  # noqa: E402

INPUT_CSV = resolve_registry_input("initiation_process_fractions.csv", repo_root=REPO_ROOT)
OUT_PATHS = registry_output_paths("pilot_process_attribution_matrix.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]
DEFAULT_CS_RUNS = (
    "cs-eriswil__20260304_110254",
    "cs-eriswil__20260210_113944",
    "cs-eriswil__20260211_194236",
)


def _slug(text: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(text).strip().lower())
    return text.strip("_")


def _dominance_rows(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    group_cols = [
        "cs_run",
        "exp_id",
        "expname",
        "ref_exp_id",
        "ref_expname",
        "pair_method",
        "station_idx",
        "window_start",
        "window_end",
        "phase_rate",
    ]
    for keys, grp in df.groupby(group_cols, dropna=False):
        grp = grp.sort_values("positive_fraction", ascending=False).reset_index(drop=True)
        first = grp.iloc[0]
        second = grp.iloc[1] if len(grp) > 1 else None
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "dominant_process": first["process_label"],
                "dominant_fraction": float(first["positive_fraction"]),
                "runner_up_process": second["process_label"] if second is not None else "",
                "runner_up_fraction": float(second["positive_fraction"]) if second is not None else 0.0,
                "dominance_margin": float(first["positive_fraction"] - (second["positive_fraction"] if second is not None else 0.0)),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a wide pilot process-attribution matrix for the first paper subset.")
    parser.add_argument("--input", type=Path, default=INPUT_CSV, help="Input initiation process fractions CSV.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output pilot matrix CSV.")
    parser.add_argument(
        "--cs-runs",
        nargs="+",
        default=list(DEFAULT_CS_RUNS),
        help="Priority pilot cs_run values to include. Default uses the PI workplan pilot subset.",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input, dtype=str)
    df["positive_fraction"] = pd.to_numeric(df["positive_fraction"], errors="coerce").fillna(0.0)
    df["mean_rate_diff"] = pd.to_numeric(df["mean_rate_diff"], errors="coerce")
    pilot = df[df["cs_run"].isin(args.cs_runs)].copy()
    if pilot.empty:
        raise SystemExit(f"No initiation rows found for pilot runs: {', '.join(args.cs_runs)}")

    pilot["process_key"] = pilot["phase_rate"].astype(str).str.lower() + "__" + pilot["process_label"].map(_slug)
    idx_cols = [
        "cs_run",
        "exp_id",
        "expname",
        "ref_exp_id",
        "ref_expname",
        "pair_method",
        "station_idx",
        "window_start",
        "window_end",
    ]
    frac = (
        pilot.pivot_table(index=idx_cols, columns="process_key", values="positive_fraction", aggfunc="first")
        .reset_index()
        .rename_axis(columns=None)
    )
    diff = (
        pilot.assign(process_key=pilot["process_key"] + "__diff")
        .pivot_table(index=idx_cols, columns="process_key", values="mean_rate_diff", aggfunc="first")
        .reset_index()
        .rename_axis(columns=None)
    )
    dom = _dominance_rows(pilot)
    dom["phase_key"] = dom["phase_rate"].astype(str).str.lower()

    dom_wide = dom[idx_cols].drop_duplicates().copy()
    for phase_key in sorted(dom["phase_key"].unique()):
        sub = dom[dom["phase_key"] == phase_key][idx_cols + ["dominant_process", "dominant_fraction", "runner_up_process", "runner_up_fraction", "dominance_margin"]]
        rename = {
            "dominant_process": f"{phase_key}__dominant_process",
            "dominant_fraction": f"{phase_key}__dominant_fraction",
            "runner_up_process": f"{phase_key}__runner_up_process",
            "runner_up_fraction": f"{phase_key}__runner_up_fraction",
            "dominance_margin": f"{phase_key}__dominance_margin",
        }
        dom_wide = dom_wide.merge(sub.rename(columns=rename), on=idx_cols, how="left")

    out = frac.merge(diff, on=idx_cols, how="left").merge(dom_wide, on=idx_cols, how="left")
    out = out.sort_values(["cs_run", "exp_id", "station_idx"]).reset_index(drop=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    if args.output.resolve() == OUT_CSV.resolve():
        sync_file(args.output, [OUT_PATHS["legacy"]])
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
