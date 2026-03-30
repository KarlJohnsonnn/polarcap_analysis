#!/usr/bin/env python3
"""Build flare_reference_pairs.csv from data/plan_pc run JSON files."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from utilities.tracking_pipeline import _is_flare, find_matching_reference
from utilities.table_paths import registry_output_paths, sync_file

PLAN_PC = REPO_ROOT / "data" / "plan_pc"
OUT_PATHS = registry_output_paths("flare_reference_pairs.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]


def _exp_meta(entry: dict) -> dict[str, object]:
    io = entry.get("INPUT_ORG", {})
    sbm = io.get("sbm_par", {})
    return {
        "ishape": int(sbm.get("ishape", -1)),
        "ikeis": int(sbm.get("ikeis", -1)),
        "domain": str(entry.get("domain", "")),
    }


def _fallback_reference(meta: dict, flare_exp: str, ref_names: list[str]) -> tuple[str, str]:
    flare_meta = _exp_meta(meta.get(flare_exp, {}))
    same_shape = [r for r in ref_names if _exp_meta(meta.get(r, {})) == flare_meta]
    if len(same_shape) == 1:
        return same_shape[0], "same_shape"
    if len(ref_names) == 1:
        return ref_names[0], "single_reference"
    return "", "none"


def main() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for jpath in sorted(PLAN_PC.glob("cs-eriswil__*.json")):
        cs_run = jpath.stem
        with jpath.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        exp_names = [k for k, v in meta.items() if isinstance(v, dict) and "INPUT_ORG" in v]
        exp_name_to_id = {name: idx for idx, name in enumerate(sorted(exp_names))}
        flare_names = [e for e in exp_names if _is_flare(meta, e)]
        ref_names = [e for e in exp_names if not _is_flare(meta, e)]
        if not ref_names:
            continue
        for flare_exp in sorted(flare_names):
            pair_method = "signature"
            ref_exp = find_matching_reference(meta, flare_exp, ref_names)
            if ref_exp is None:
                ref_exp, pair_method = _fallback_reference(meta, flare_exp, ref_names)
            rows.append(
                {
                    "cs_run": cs_run,
                    "flare_exp_id": exp_name_to_id[flare_exp],
                    "flare_expname": flare_exp,
                    "ref_exp_id": exp_name_to_id.get(ref_exp, ""),
                    "ref_expname": ref_exp,
                    "pair_method": pair_method,
                    "pair_status": "MATCHED" if ref_exp else "UNMATCHED",
                }
            )
    cols = [
        "cs_run",
        "flare_exp_id",
        "flare_expname",
        "ref_exp_id",
        "ref_expname",
        "pair_method",
        "pair_status",
    ]
    with OUT_CSV.open("w", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n")
        for row in rows:
            f.write(",".join(str(row[c]) for c in cols) + "\n")
    sync_file(OUT_CSV, [OUT_PATHS["legacy"]])
    print(f"Wrote {len(rows)} pairs to {OUT_CSV}")


if __name__ == "__main__":
    main()
