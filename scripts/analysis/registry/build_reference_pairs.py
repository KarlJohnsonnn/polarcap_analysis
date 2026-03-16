#!/usr/bin/env python3
"""Build flare_reference_pairs.csv from data/plan_pc run JSONs using tracking_pipeline pairing logic."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from utilities.tracking_pipeline import find_matching_reference, _is_flare

PLAN_PC = REPO_ROOT / "data" / "plan_pc"
OUT_CSV = REPO_ROOT / "data" / "registry" / "flare_reference_pairs.csv"


def main() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for jpath in sorted(PLAN_PC.glob("cs-eriswil__*.json")):
        cs_run = jpath.stem
        with jpath.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        exp_names = [k for k in meta if isinstance(meta.get(k), dict) and "INPUT_ORG" in meta.get(k, {})]
        flare_names = [e for e in exp_names if _is_flare(meta, e)]
        ref_names = [e for e in exp_names if not _is_flare(meta, e)]
        if not ref_names:
            continue
        for fi, flare_exp in enumerate(flare_names):
            ref_exp = find_matching_reference(meta, flare_exp, ref_names)
            if ref_exp is None:
                ref_exp = ""
            rows.append({
                "cs_run": cs_run,
                "flare_exp_id": fi,
                "flare_expname": flare_exp,
                "ref_expname": ref_exp,
            })
    cols = ["cs_run", "flare_exp_id", "flare_expname", "ref_expname"]
    with OUT_CSV.open("w", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(str(r[c]) for c in cols) + "\n")
    print(f"Wrote {len(rows)} pairs to {OUT_CSV}")


if __name__ == "__main__":
    main()
