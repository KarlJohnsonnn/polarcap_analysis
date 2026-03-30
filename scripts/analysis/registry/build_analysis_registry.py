#!/usr/bin/env python3
"""Build one merged analysis-ready registry table and the first paper subset."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.processing_paths import default_local_processed_root  # noqa: E402
from utilities.table_paths import registry_output_paths, resolve_registry_input, sync_file  # noqa: E402

REGISTRY_DIR = REPO_ROOT / "output" / "tables" / "registry"
PROCESSED_ROOT = Path(default_local_processed_root())

AVAIL_CSV = resolve_registry_input("availability_check.csv", repo_root=REPO_ROOT)
EXP_CSV = resolve_registry_input("experiment_registry.csv", repo_root=REPO_ROOT)
PAIR_CSV = resolve_registry_input("flare_reference_pairs.csv", repo_root=REPO_ROOT)
OUT_PATHS = registry_output_paths("analysis_registry.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]
OUT_PAPER_PATHS = registry_output_paths("paper_core_subset.csv", repo_root=REPO_ROOT)
OUT_PAPER = OUT_PAPER_PATHS["canonical"]


def _bool_flag(value: bool) -> str:
    return "TRUE" if bool(value) else "FALSE"


def _local_outputs(cs_run: str) -> dict[str, str]:
    run_root = PROCESSED_ROOT / cs_run
    lv2 = any((run_root / "lv2_meteogram").glob("Meteogram_*.zarr"))
    lv3 = any((run_root / "lv3_rates").glob("process_rates_exp*.nc"))
    lv1 = any((run_root / "lv1_paths").glob("*plume_path*.nc"))
    return {
        "local_lv1_available": _bool_flag(lv1),
        "local_lv2_available": _bool_flag(lv2),
        "local_lv3_available": _bool_flag(lv3),
    }


def main() -> None:
    avail = pd.read_csv(AVAIL_CSV, dtype={"run_id": str}).rename(columns={"run_id": "cs_run"})
    exp = pd.read_csv(EXP_CSV, dtype={"cs_run": str, "expname": str, "is_reference": str, "domain": str})
    pair = pd.read_csv(
        PAIR_CSV,
        dtype={
            "cs_run": str,
            "flare_expname": str,
            "ref_expname": str,
            "pair_method": str,
            "pair_status": str,
        },
    )

    merged = exp.merge(avail, on="cs_run", how="left")
    merged = merged.merge(
        pair,
        left_on=["cs_run", "exp_id", "expname"],
        right_on=["cs_run", "flare_exp_id", "flare_expname"],
        how="left",
    )

    merged["has_reference_pair"] = merged["pair_status"].fillna("UNMATCHED").eq("MATCHED")
    merged["is_reference"] = merged["is_reference"].astype(str).str.upper()
    merged["is_reference_bool"] = merged["is_reference"].eq("TRUE")

    local_rows = [{**{"cs_run": cs_run}, **_local_outputs(cs_run)} for cs_run in sorted(merged["cs_run"].unique())]
    merged = merged.merge(pd.DataFrame(local_rows), on="cs_run", how="left")

    ref_used = (
        merged.loc[merged["has_reference_pair"], ["cs_run", "ref_expname"]]
        .dropna()
        .assign(ref_used_by_flare=True)
        .rename(columns={"ref_expname": "expname"})
        .drop_duplicates()
    )
    merged = merged.merge(ref_used, on=["cs_run", "expname"], how="left")
    merged["ref_used_by_flare"] = merged["ref_used_by_flare"] == True

    merged["usable_for_lv1"] = merged["lv1_ready"].fillna("NO").eq("YES")
    merged["usable_for_lv2"] = merged["lv2_ready"].fillna("NO").eq("YES")
    merged["usable_for_plume_tracking"] = merged["usable_for_lv1"] & (
        merged["has_reference_pair"] | merged["is_reference_bool"] & merged["ref_used_by_flare"]
    )
    merged["usable_for_process_budget"] = merged["usable_for_lv2"] & (
        merged["has_reference_pair"] | merged["is_reference_bool"] & merged["ref_used_by_flare"]
    )
    merged["include_in_paper"] = merged["usable_for_plume_tracking"] | merged["usable_for_process_budget"]

    bool_cols = [
        "has_reference_pair",
        "ref_used_by_flare",
        "usable_for_lv1",
        "usable_for_lv2",
        "usable_for_plume_tracking",
        "usable_for_process_budget",
        "include_in_paper",
    ]
    for col in bool_cols:
        merged[col] = merged[col].map(_bool_flag)
    merged["ref_exp_id"] = merged["ref_exp_id"].apply(lambda v: "" if pd.isna(v) else str(int(v)))
    merged["ref_expname"] = merged["ref_expname"].fillna("").astype(str)

    cols = [
        "cs_run",
        "exp_id",
        "expname",
        "is_reference",
        "flare_emission",
        "ishape",
        "ikeis",
        "domain",
        "remote_run_dir",
        "remote_json",
        "meteogram_count",
        "three_d_count",
        "lv1_ready",
        "lv2_ready",
        "local_lv1_available",
        "local_lv2_available",
        "local_lv3_available",
        "ref_exp_id",
        "ref_expname",
        "pair_method",
        "pair_status",
        "has_reference_pair",
        "ref_used_by_flare",
        "usable_for_lv1",
        "usable_for_lv2",
        "usable_for_plume_tracking",
        "usable_for_process_budget",
        "include_in_paper",
    ]
    merged = merged[cols].sort_values(["cs_run", "exp_id"]).reset_index(drop=True)
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)
    merged.loc[merged["include_in_paper"] == "TRUE"].to_csv(OUT_PAPER, index=False)
    sync_file(OUT_CSV, [OUT_PATHS["legacy"]])
    sync_file(OUT_PAPER, [OUT_PAPER_PATHS["legacy"]])
    print(f"Wrote {len(merged)} rows to {OUT_CSV}")
    print(f"Wrote paper subset to {OUT_PAPER}")


if __name__ == "__main__":
    main()
