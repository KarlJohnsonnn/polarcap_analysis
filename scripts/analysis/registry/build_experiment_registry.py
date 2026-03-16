#!/usr/bin/env python3
"""Build experiment_registry.csv from data/plan_pc run JSON files. One row per (cs_run, exp_id)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PLAN_PC = REPO_ROOT / "data" / "plan_pc"
OUT_CSV = REPO_ROOT / "data" / "registry" / "experiment_registry.csv"


def _exp_meta(entry: dict) -> dict:
    sbm = entry.get("INPUT_ORG", {}).get("sbm_par", {})
    flare = entry.get("INPUT_ORG", {}).get("flare_sbm", {})
    lflare = sbm.get("lflare", False)
    emission = float(flare.get("flare_emission", 0.0))
    ishape = sbm.get("ishape", -1)
    ikeis = sbm.get("ikeis", -1)
    domain = entry.get("domain", "")
    is_ref = (not lflare) or emission == 0
    return {
        "lflare": lflare,
        "flare_emission": emission,
        "ishape": ishape,
        "ikeis": ikeis,
        "domain": domain,
        "is_reference": is_ref,
    }


def main() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for jpath in sorted(PLAN_PC.glob("cs-eriswil__*.json")):
        cs_run = jpath.stem
        with jpath.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        exp_names = [k for k in meta if isinstance(meta.get(k), dict) and "INPUT_ORG" in meta.get(k, {})]
        for exp_idx, expname in enumerate(sorted(exp_names)):
            m = _exp_meta(meta[expname])
            rows.append({
                "cs_run": cs_run,
                "exp_id": exp_idx,
                "expname": expname,
                "is_reference": "TRUE" if m["is_reference"] else "FALSE",
                "flare_emission": m["flare_emission"],
                "ishape": m["ishape"],
                "ikeis": m["ikeis"],
                "domain": m["domain"],
            })
    if not rows:
        print("No experiments found in data/plan_pc", file=sys.stderr)
        sys.exit(1)
    cols = ["cs_run", "exp_id", "expname", "is_reference", "flare_emission", "ishape", "ikeis", "domain"]
    with OUT_CSV.open("w", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(str(r[c]) for c in cols) + "\n")
    print(f"Wrote {len(rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
