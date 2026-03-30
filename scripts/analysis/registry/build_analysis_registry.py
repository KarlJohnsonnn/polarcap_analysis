#!/usr/bin/env python3
"""Build the unified analysis registry from run metadata and availability checks."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]

PLAN_PC = REPO_ROOT / "data" / "plan_pc"
PROCESSED_ROOT = (REPO_ROOT / "data" / "processed").resolve()
AVAIL_CSV = (
    next(
        (
            path
            for path in [
                REPO_ROOT / "output" / "tables" / "registry" / "availability_check.csv",
                REPO_ROOT / "data" / "registry" / "availability_check.csv",
            ]
            if path.exists()
        ),
        REPO_ROOT / "output" / "tables" / "registry" / "availability_check.csv",
    )
)
OUT_PATHS = {
    "canonical": REPO_ROOT / "output" / "tables" / "registry" / "analysis_registry.csv",
    "legacy": REPO_ROOT / "data" / "registry" / "analysis_registry.csv",
}
OUT_CSV = OUT_PATHS["canonical"]
FLARE_ONLY_KEYS = {
    "lflare",
    "flare_emission",
    "lflare_inp",
    "lflare_ccn",
    "flare_dn",
    "flare_dp",
    "flare_sig",
}

OUTPUT_COLUMNS = [
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


def _bool_flag(value: bool) -> str:
    return "TRUE" if bool(value) else "FALSE"


def _copy_file_if_changed(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    content = src.read_bytes()
    if dst.exists() and dst.read_bytes() == content:
        return
    dst.write_bytes(content)


def _is_flare(meta: dict, exp_name: str) -> bool:
    entry = meta.get(exp_name, {})
    return bool(entry.get("INPUT_ORG", {}).get("sbm_par", {}).get("lflare", False))


def _non_emission_signature(meta: dict, exp_name: str) -> tuple[object, ...]:
    entry = meta.get(exp_name, {})
    input_org = entry.get("INPUT_ORG", {})
    sbm = input_org.get("sbm_par", {})
    flare = input_org.get("flare_sbm", {})
    sbm_sub = {key: value for key, value in sbm.items() if key not in FLARE_ONLY_KEYS}
    flare_sub = {key: value for key, value in flare.items() if key not in FLARE_ONLY_KEYS}
    return (
        input_org.get("domain"),
        tuple(sorted(sbm_sub.items())),
        tuple(sorted(flare_sub.items())),
        tuple(sorted(input_org.get("runctl", {}).items())),
    )


def _find_matching_reference(
    meta: dict,
    flare_expname: str,
    ref_names: list[str],
) -> str | None:
    flare_sig = _non_emission_signature(meta, flare_expname)
    for ref_name in ref_names:
        if _non_emission_signature(meta, ref_name) == flare_sig:
            return ref_name
    return None


def _experiment_names(meta: dict) -> list[str]:
    return sorted(
        name
        for name, entry in meta.items()
        if isinstance(entry, dict) and "INPUT_ORG" in entry
    )


def _exp_meta(entry: dict) -> dict[str, object]:
    input_org = entry.get("INPUT_ORG", {})
    sbm = input_org.get("sbm_par", {})
    flare = input_org.get("flare_sbm", {})
    lflare = bool(sbm.get("lflare", False))
    flare_emission = float(flare.get("flare_emission", 0.0))
    return {
        "is_reference": (not lflare) or flare_emission == 0.0,
        "flare_emission": flare_emission,
        "ishape": int(sbm.get("ishape", -1)),
        "ikeis": int(sbm.get("ikeis", -1)),
        "domain": str(entry.get("domain", "")),
    }


def _pair_fallback(
    meta: dict,
    flare_expname: str,
    ref_names: list[str],
) -> tuple[str, str]:
    # If the full signature does not match, fall back to the minimal shape/domain
    # signature used by the earlier split registry scripts.
    flare_entry = meta.get(flare_expname, {})
    flare_sbm = flare_entry.get("INPUT_ORG", {}).get("sbm_par", {})
    flare_meta = (
        int(flare_sbm.get("ishape", -1)),
        int(flare_sbm.get("ikeis", -1)),
        str(flare_entry.get("domain", "")),
    )
    same_shape = [
        ref_name
        for ref_name in ref_names
        if (
            int(meta.get(ref_name, {}).get("INPUT_ORG", {}).get("sbm_par", {}).get("ishape", -1)),
            int(meta.get(ref_name, {}).get("INPUT_ORG", {}).get("sbm_par", {}).get("ikeis", -1)),
            str(meta.get(ref_name, {}).get("domain", "")),
        )
        == flare_meta
    ]
    if len(same_shape) == 1:
        return same_shape[0], "same_shape"
    if len(ref_names) == 1:
        return ref_names[0], "single_reference"
    return "", "none"


def _build_run_rows(cs_run: str, meta: dict) -> list[dict[str, object]]:
    exp_names = _experiment_names(meta)
    exp_name_to_id = {name: idx for idx, name in enumerate(exp_names)}
    flare_names = [name for name in exp_names if _is_flare(meta, name)]
    ref_names = [name for name in exp_names if not _is_flare(meta, name)]
    pair_by_flare: dict[str, dict[str, object]] = {}

    for flare_expname in flare_names:
        pair_method = "signature"
        ref_expname = _find_matching_reference(meta, flare_expname, ref_names) if ref_names else None
        if ref_expname is None:
            ref_expname, pair_method = _pair_fallback(meta, flare_expname, ref_names)
        pair_by_flare[flare_expname] = {
            "ref_exp_id": exp_name_to_id.get(ref_expname, ""),
            "ref_expname": ref_expname,
            "pair_method": pair_method if ref_expname or pair_method == "none" else "",
            "pair_status": "MATCHED" if ref_expname else "UNMATCHED",
            "has_reference_pair": bool(ref_expname),
        }

    refs_used = {
        str(pair["ref_expname"])
        for pair in pair_by_flare.values()
        if pair["has_reference_pair"] and pair["ref_expname"]
    }

    rows: list[dict[str, object]] = []
    for expname in exp_names:
        pair = pair_by_flare.get(
            expname,
            {
                "ref_exp_id": "",
                "ref_expname": "",
                "pair_method": "",
                "pair_status": "",
                "has_reference_pair": False,
            },
        )
        rows.append(
            {
                "cs_run": cs_run,
                "exp_id": exp_name_to_id[expname],
                "expname": expname,
                **_exp_meta(meta.get(expname, {})),
                **pair,
                "ref_used_by_flare": expname in refs_used,
            }
        )
    return rows


def _local_outputs(cs_run: str) -> dict[str, object]:
    run_root = PROCESSED_ROOT / cs_run
    return {
        "cs_run": cs_run,
        "local_lv1_available": any((run_root / "lv1_paths").glob("*plume_path*.nc")),
        "local_lv2_available": any((run_root / "lv2_meteogram").glob("Meteogram_*.zarr")),
        "local_lv3_available": any((run_root / "lv3_rates").glob("process_rates_exp*.nc")),
    }


def _load_availability() -> pd.DataFrame:
    if not AVAIL_CSV.exists():
        return pd.DataFrame({"cs_run": pd.Series(dtype=str)})
    return pd.read_csv(AVAIL_CSV, dtype={"run_id": str}).rename(columns={"run_id": "cs_run"})


def _registry_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for jpath in sorted(PLAN_PC.glob("cs-eriswil__*.json")):
        with jpath.open("r", encoding="utf-8") as handle:
            meta = json.load(handle)
        rows.extend(_build_run_rows(jpath.stem, meta))
    return rows


def main() -> None:
    rows = _registry_rows()
    if not rows:
        print("No experiments found in data/plan_pc", file=sys.stderr)
        sys.exit(1)

    registry = pd.DataFrame(rows)
    availability = _load_availability()
    local_outputs = pd.DataFrame(
        [_local_outputs(cs_run) for cs_run in sorted(registry["cs_run"].astype(str).unique())]
    )

    registry = registry.merge(availability, on="cs_run", how="left")
    registry = registry.merge(local_outputs, on="cs_run", how="left")

    registry["usable_for_lv1"] = registry["lv1_ready"].fillna("NO").eq("YES")
    registry["usable_for_lv2"] = registry["lv2_ready"].fillna("NO").eq("YES")
    registry["usable_for_plume_tracking"] = registry["usable_for_lv1"] & (
        registry["has_reference_pair"] | (registry["is_reference"] & registry["ref_used_by_flare"])
    )
    registry["usable_for_process_budget"] = registry["usable_for_lv2"] & (
        registry["has_reference_pair"] | (registry["is_reference"] & registry["ref_used_by_flare"])
    )
    registry["include_in_paper"] = (
        registry["usable_for_plume_tracking"] | registry["usable_for_process_budget"]
    )

    bool_cols = [
        "is_reference",
        "local_lv1_available",
        "local_lv2_available",
        "local_lv3_available",
        "has_reference_pair",
        "ref_used_by_flare",
        "usable_for_lv1",
        "usable_for_lv2",
        "usable_for_plume_tracking",
        "usable_for_process_budget",
        "include_in_paper",
    ]
    for col in bool_cols:
        registry[col] = registry[col].map(_bool_flag)

    registry["ref_exp_id"] = registry["ref_exp_id"].apply(
        lambda value: "" if pd.isna(value) or value == "" else str(int(value))
    )
    registry["ref_expname"] = registry["ref_expname"].fillna("").astype(str)
    registry["pair_method"] = registry["pair_method"].fillna("").astype(str)
    registry["pair_status"] = registry["pair_status"].fillna("").astype(str)

    registry = registry.reindex(columns=OUTPUT_COLUMNS, fill_value="")
    registry = registry.sort_values(["cs_run", "exp_id"]).reset_index(drop=True)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    registry.to_csv(OUT_CSV, index=False)
    _copy_file_if_changed(OUT_CSV, OUT_PATHS["legacy"])
    print(f"Wrote {len(registry)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
