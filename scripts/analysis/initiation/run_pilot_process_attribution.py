#!/usr/bin/env python3
"""Build a first pilot process-attribution matrix from the promoted initiation fractions table."""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"


def _load_src_module(module_name: str, relative_path: str):
    path = SRC_DIR / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Load without importing utilities/__init__.py (avoids pulling xarray et al.).
_tp = _load_src_module("run_pilot_table_paths", "utilities/table_paths.py")
registry_output_paths = _tp.registry_output_paths
repo_relative = _tp.repo_relative
resolve_registry_input = _tp.resolve_registry_input
sync_file = _tp.sync_file

_paper_mod = _load_src_module("run_pilot_paper_tables", "utilities/paper_tables.py")
render_table_environment = _paper_mod.render_table_environment

INPUT_CSV = resolve_registry_input("initiation_process_fractions.csv", repo_root=REPO_ROOT)
OUT_PATHS = registry_output_paths("pilot_process_attribution_matrix.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]
OUT_TEX = OUT_PATHS["canonical"].with_suffix(".tex")

# Same column layout as paper_tables.yaml (process_attribution); label distinct from manuscript tab:process_attribution.
PILOT_ATTRIBUTION_TEX_SPEC: dict[str, object] = {
    "display_columns": [
        {"name": "cs_run", "label": "CS run"},
        {"name": "expname", "label": "Expname"},
        {"name": "station", "label": "Station"},
        {"name": "ice_dominant_process", "label": "Ice dominant"},
        {"name": "ice_dominant_fraction", "label": "Ice frac.", "fmt": "percent1"},
        {"name": "ice_runner_up_process", "label": "Ice runner-up"},
        {"name": "liq_dominant_process", "label": "Liq dominant"},
        {"name": "liq_dominant_fraction", "label": "Liq frac.", "fmt": "percent1"},
        {"name": "liq_runner_up_process", "label": "Liq runner-up"},
        {"name": "window_minutes", "label": "Window min", "fmt": "float2"},
    ],
    "latex_label": "tab:pilot_process_attribution",
    "caption": (
        "Early-window pilot process-attribution (selected cs\\_run rows). "
        "Dominant and runner-up pathways for frozen-number and liquid-number tendencies "
        "in the initial post-seeding window."
    ),
    "tex_environment": "table*",
    "tex_position": "t",
    "tex_resize_to_textwidth": True,
    "tex_align": "lrlclclclc",
}
DEFAULT_CS_RUNS = (
    "cs-eriswil__20260304_110254",
    # "cs-eriswil__20260210_113944",
    # "cs-eriswil__20260211_194236",
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


def _dominance_display_df(wide: pd.DataFrame) -> pd.DataFrame:
    """Compact dominance columns for TeX (matches run_paper_tables.build_process_attribution layout)."""
    df = wide.copy()
    num_cols = [
        "station_idx",
        "rate_n_f__dominant_fraction",
        "rate_n_w__dominant_fraction",
        "rate_n_f__runner_up_fraction",
        "rate_n_w__runner_up_fraction",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return pd.DataFrame(
        {
            "cs_run": df["cs_run"],
            "expname": df["expname"],
            "station": df["station_idx"].fillna(0).astype(int).map(lambda idx: f"S{idx + 1}"),
            "ice_dominant_process": df.get("rate_n_f__dominant_process", pd.Series("", index=df.index)).fillna(""),
            "ice_dominant_fraction": df.get("rate_n_f__dominant_fraction", pd.Series(dtype=float)),
            "ice_runner_up_process": df.get("rate_n_f__runner_up_process", pd.Series("", index=df.index)).fillna(""),
            "liq_dominant_process": df.get("rate_n_w__dominant_process", pd.Series("", index=df.index)).fillna(""),
            "liq_dominant_fraction": df.get("rate_n_w__dominant_fraction", pd.Series(dtype=float)),
            "liq_runner_up_process": df.get("rate_n_w__runner_up_process", pd.Series("", index=df.index)).fillna(""),
            "window_minutes": (pd.to_datetime(df["window_end"]) - pd.to_datetime(df["window_start"])).dt.total_seconds()
            / 60.0,
        }
    ).sort_values(["cs_run", "expname", "station"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a wide pilot process-attribution matrix for the first paper subset.")
    parser.add_argument("--input", type=Path, default=INPUT_CSV, help="Input initiation process fractions CSV.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output pilot matrix CSV.")
    parser.add_argument(
        "--output-tex",
        type=Path,
        default=None,
        help="Output LaTeX table (.tex). Default: same stem as --output with .tex.",
    )
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

    tex_path = args.output_tex if args.output_tex is not None else args.output.with_suffix(".tex")
    display_df = _dominance_display_df(out)
    source_csvs = [repo_relative(args.input.resolve()), repo_relative(args.output.resolve())]
    tex_body = render_table_environment(
        display_df,
        PILOT_ATTRIBUTION_TEX_SPEC,
        source_csvs,
        generator=repo_relative(Path(__file__).resolve()),
    )
    tex_path.parent.mkdir(parents=True, exist_ok=True)
    tex_path.write_text(tex_body, encoding="utf-8")
    if tex_path.resolve() == OUT_TEX.resolve():
        sync_file(tex_path, [OUT_PATHS["legacy"].with_suffix(".tex")])

    print(f"Wrote {len(out)} rows to {args.output}")
    print(f"Wrote LaTeX ({len(display_df)} rows) to {tex_path}")


if __name__ == "__main__":
    main()
