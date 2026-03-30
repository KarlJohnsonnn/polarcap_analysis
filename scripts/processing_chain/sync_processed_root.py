#!/usr/bin/env python3
"""Unify processed artifacts under the canonical data/processed tree."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.processing_paths import default_local_processed_root  # noqa: E402
from utilities.table_paths import registry_output_paths, sync_file  # noqa: E402

DEFAULT_SOURCE_ROOTS = [
    Path("/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output"),
    Path("/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_200x160x100/ensemble_output"),
    Path("/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output"),
]
PLAN_PC_ROOT = REPO_ROOT / "data" / "plan_pc"
TARGET_ROOT = Path(default_local_processed_root())

STAGES: dict[str, dict[str, tuple[str, ...]]] = {
    "lv1_tracking": {"dirs": ("lv1_tracking", "processed/lv1_tracking"), "globs": ()},
    "lv1_paths": {"dirs": ("lv1_paths", "processed/lv1_paths"), "globs": ()},
    "lv2_meteogram": {"dirs": ("lv2_meteogram", "processed/lv2_meteogram"), "globs": ("Meteogram_*.zarr",)},
    "lv2_3d": {"dirs": ("lv2_3d", "processed/lv2_3d"), "globs": ()},
    "lv3_rates": {"dirs": ("lv3_rates", "processed/lv3_rates"), "globs": ()},
    "manifests": {"dirs": ("manifests", "processed/manifests"), "globs": ()},
}
STAGE_ORDER = ("lv1_tracking", "lv1_paths", "lv2_meteogram", "lv2_3d", "lv3_rates", "manifests")


def _run_ids_from_plan_pc() -> set[str]:
    return {path.stem for path in PLAN_PC_ROOT.glob("cs-eriswil__*.json")}


def _run_ids_under(root: Path, *, require_processed: bool = False) -> set[str]:
    if not root.is_dir():
        return set()
    out = set()
    for path in root.iterdir():
        if not path.is_dir() or not path.name.startswith("cs-eriswil__"):
            continue
        if require_processed and not any((path / stage).is_dir() and any((path / stage).iterdir()) for stage in STAGE_ORDER):
            continue
        out.add(path.name)
    return out


def _has_any_processed_stage(run_dir: Path) -> bool:
    return any(_source_items(run_dir, stage) is not None for stage in STAGE_ORDER)


def _target_items(stage_dir: Path) -> set[str]:
    if not stage_dir.is_dir():
        return set()
    return {path.name for path in stage_dir.iterdir()}


def _source_items(run_dir: Path, stage: str) -> tuple[Path, list[Path]] | None:
    spec = STAGES[stage]
    for rel in spec["dirs"]:
        stage_dir = run_dir / rel
        if not stage_dir.is_dir():
            continue
        items = sorted(stage_dir.iterdir(), key=lambda path: path.name)
        if items:
            return stage_dir, items
    for pattern in spec["globs"]:
        items = sorted(run_dir.glob(pattern), key=lambda path: path.name)
        if items:
            return run_dir, items
    return None


def _raw_counts(run_dirs: list[Path]) -> dict[str, int]:
    counts = {"json": 0, "m": 0, "3d": 0, "y": 0}
    for run_dir in run_dirs:
        counts["json"] = max(counts["json"], len(list(run_dir.glob("*.json"))))
        counts["m"] = max(counts["m"], len(list(run_dir.glob("M_*.nc"))))
        counts["3d"] = max(counts["3d"], len(list(run_dir.glob("3D_*.nc"))))
        counts["y"] = max(counts["y"], len(list(run_dir.glob("Y*"))))
    return counts


def _missing_note(stage: str, raw: dict[str, int]) -> str:
    if stage in {"lv1_tracking", "lv1_paths"} and raw["json"] > 0 and raw["3d"] > 0:
        return "missing_processed_but_raw_3d_present"
    if stage == "lv2_meteogram" and raw["json"] > 0 and raw["m"] > 0:
        return "missing_processed_but_meteograms_present"
    if stage == "lv3_rates" and (raw["m"] > 0 or raw["json"] > 0):
        return "missing_processed_but_lv2_inputs_may_exist"
    return "no_source_found"


def _write_inventory(rows: list[dict[str, object]]) -> tuple[Path, Path]:
    csv_paths = registry_output_paths("processed_sync_inventory.csv", repo_root=REPO_ROOT)
    md_paths = registry_output_paths("processed_sync_report.md", repo_root=REPO_ROOT)

    csv_paths["canonical"].parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "cs_run",
        "stage",
        "status",
        "linked_items",
        "kept_items",
        "source_root",
        "source_dir",
        "target_dir",
        "raw_json_count",
        "raw_m_count",
        "raw_3d_count",
        "raw_y_count",
        "note",
    ]
    with csv_paths["canonical"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    status_counts = Counter(str(row["status"]) for row in rows)
    lines = [
        "# Processed Root Sync Report",
        "",
        f"- Canonical target: `{TARGET_ROOT}`",
        f"- Source roots checked: {', '.join(f'`{root}`' for root in DEFAULT_SOURCE_ROOTS)}",
        f"- Runs scanned: `{len(sorted({str(row['cs_run']) for row in rows}))}`",
        f"- Stage rows: `{len(rows)}`",
        "",
        "## Status Summary",
        "",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f"- `{status}`: {count}")

    lines.extend(["", "## Missing Or Rebuild Candidates", ""])
    missing_rows = [row for row in rows if str(row["status"]) == "missing"]
    if not missing_rows:
        lines.append("- None.")
    else:
        for row in missing_rows:
            lines.append(
                f"- `{row['cs_run']}` / `{row['stage']}`: {row['note']} "
                f"(3D={row['raw_3d_count']}, M={row['raw_m_count']}, json={row['raw_json_count']})"
            )

    md_paths["canonical"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    sync_file(csv_paths["canonical"], [csv_paths["legacy"]])
    sync_file(md_paths["canonical"], [md_paths["legacy"]])
    return csv_paths["canonical"], md_paths["canonical"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate data/processed using symlinks to existing processed artifacts.")
    parser.add_argument("--target-root", type=Path, default=TARGET_ROOT)
    parser.add_argument("--source-roots", nargs="*", type=Path, default=DEFAULT_SOURCE_ROOTS)
    parser.add_argument("--dry-run", action="store_true", help="Inspect and report without creating symlinks.")
    args = parser.parse_args()

    target_root = args.target_root.resolve()
    source_roots = [root.resolve() for root in args.source_roots if root.exists()]
    target_root.mkdir(parents=True, exist_ok=True)

    plan_runs = _run_ids_from_plan_pc()
    run_ids = set(plan_runs)
    run_ids |= _run_ids_under(target_root, require_processed=True)
    for root in source_roots:
        for run_id in _run_ids_under(root):
            run_dir = root / run_id
            if _has_any_processed_stage(run_dir):
                run_ids.add(run_id)
    rows: list[dict[str, object]] = []

    for cs_run in sorted(run_ids):
        target_run = target_root / cs_run
        if not args.dry_run and (cs_run in plan_runs or target_run.exists()):
            target_run.mkdir(parents=True, exist_ok=True)
        run_dirs = [root / cs_run for root in source_roots if (root / cs_run).is_dir()]
        raw = _raw_counts(run_dirs)

        for stage in STAGE_ORDER:
            target_stage = target_run / stage
            existing_items = _target_items(target_stage)
            source_choice: tuple[Path, list[Path], Path] | None = None
            for run_dir in run_dirs:
                located = _source_items(run_dir, stage)
                if located is None:
                    continue
                source_dir, items = located
                source_choice = (source_dir, items, run_dir)
                break

            if source_choice is None:
                if existing_items:
                    rows.append(
                        {
                            "cs_run": cs_run,
                            "stage": stage,
                            "status": "already_present",
                            "linked_items": 0,
                            "kept_items": len(existing_items),
                            "source_root": "",
                            "source_dir": "",
                            "target_dir": str(target_stage),
                            "raw_json_count": raw["json"],
                            "raw_m_count": raw["m"],
                            "raw_3d_count": raw["3d"],
                            "raw_y_count": raw["y"],
                            "note": "local_only",
                        }
                    )
                    continue
                if cs_run not in plan_runs:
                    continue
                rows.append(
                    {
                        "cs_run": cs_run,
                        "stage": stage,
                        "status": "missing",
                        "linked_items": 0,
                        "kept_items": len(existing_items),
                        "source_root": "",
                        "source_dir": "",
                        "target_dir": str(target_stage),
                        "raw_json_count": raw["json"],
                        "raw_m_count": raw["m"],
                        "raw_3d_count": raw["3d"],
                        "raw_y_count": raw["y"],
                        "note": _missing_note(stage, raw),
                    }
                )
                continue

            source_dir, items, source_run = source_choice
            target_stage.mkdir(parents=True, exist_ok=True)
            linked = 0
            kept = 0
            for src_item in items:
                dst_item = target_stage / src_item.name
                if dst_item.exists() or dst_item.is_symlink():
                    kept += 1
                    continue
                if not args.dry_run:
                    os.symlink(src_item, dst_item)
                linked += 1
            status = "linked" if linked > 0 else "already_present"
            rows.append(
                {
                    "cs_run": cs_run,
                    "stage": stage,
                    "status": status,
                    "linked_items": linked,
                    "kept_items": kept,
                    "source_root": str(source_run.parent),
                    "source_dir": str(source_dir),
                    "target_dir": str(target_stage),
                    "raw_json_count": raw["json"],
                    "raw_m_count": raw["m"],
                    "raw_3d_count": raw["3d"],
                    "raw_y_count": raw["y"],
                    "note": "",
                }
            )

    csv_path, md_path = _write_inventory(rows)
    print(f"Wrote inventory CSV -> {csv_path}")
    print(f"Wrote inventory report -> {md_path}")
    if args.dry_run:
        print("Dry run only; no symlinks created.")


if __name__ == "__main__":
    main()
