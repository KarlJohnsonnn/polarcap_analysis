#!/usr/bin/env python3
"""Produce a lightweight figure/table inventory for the manuscript draft."""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.paper_tables import load_manifest  # noqa: E402
from utilities.table_paths import registry_output_paths, sync_file  # noqa: E402

MANIFEST = REPO_ROOT / "scripts" / "analysis" / "synthesis" / "paper_tables.yaml"
OUT_PATHS = registry_output_paths("figure_inventory.csv", repo_root=REPO_ROOT)
OUT_CSV = OUT_PATHS["canonical"]


def _load_script_caption(script_relpath: str) -> str:
    """Read `FIGURE_CAPTION` without importing the plotting stack during registry refresh."""
    script_path = REPO_ROOT / script_relpath
    if not script_path.is_file():
        return ""
    try:
        module = ast.parse(script_path.read_text(encoding="utf-8"), filename=str(script_path))
    except SyntaxError:
        return ""
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "FIGURE_CAPTION":
                return " ".join(str(ast.literal_eval(node.value)).split())
    return ""


def build_inventory(manifest_path: Path = MANIFEST) -> pd.DataFrame:
    manifest = load_manifest(manifest_path)
    rows: list[dict[str, object]] = [
        {
            "artifact_id": "fig_cloud_field_overview",
            "artifact_type": "figure",
            "section": "results",
            "script": "scripts/analysis/forcing/run_cloud_field_overview.py",
            "config_or_args": "config/psd_process_evolution.yaml",
            "output_path": "output/gfx/png/01/cloud_field_overview_mass_profiles_steps_symlog_<exp>_<range>.png",
            "caption": _load_script_caption("scripts/analysis/forcing/run_cloud_field_overview.py"),
            "draft_target": "manual figure include in article_draft/PolarCAP",
        },
        {
            "artifact_id": "fig_plume_lagrangian",
            "artifact_type": "figure",
            "section": "results",
            "script": "scripts/analysis/growth/run_plume_lagrangian_evolution.py",
            "config_or_args": "FigCfg in script (utilities.plume_lagrangian.DEFAULT_CFG)",
            "output_path": "output/gfx/png/03/figure12_ensemble_mean_plume_path.png",
            "caption": _load_script_caption("scripts/analysis/growth/run_plume_lagrangian_evolution.py"),
            "draft_target": "manual figure include in article_draft/PolarCAP",
        },
        {
            "artifact_id": "fig_psd_waterfall",
            "artifact_type": "figure",
            "section": "results",
            "script": "scripts/analysis/growth/run_psd_waterfall.py",
            "config_or_args": "CLI args for processed root, plot kinds, run labels",
            "output_path": "output/gfx/png/04/figure13_psd_alt_time_<kind>_<run_id>.png",
            "caption": _load_script_caption("scripts/analysis/growth/run_psd_waterfall.py"),
            "draft_target": "manual figure include in article_draft/PolarCAP",
        },
        {
            "artifact_id": "fig_spectral_waterfall",
            "artifact_type": "figure",
            "section": "results",
            "script": "scripts/analysis/growth/run_spectral_waterfall.py",
            "config_or_args": "process-budget config + publication wrapper",
            "output_path": "output/gfx/mp4/05/<cs_run>/... or output/gallery/*.mp4",
            "caption": _load_script_caption("scripts/analysis/growth/run_spectral_waterfall.py"),
            "draft_target": "gallery / future manuscript figure",
        },
    ]

    for spec in manifest.get("tables", []):
        rows.append(
            {
                "artifact_id": spec["id"],
                "artifact_type": "table",
                "section": spec.get("section", ""),
                "script": "scripts/analysis/synthesis/run_paper_tables.py",
                "config_or_args": "scripts/analysis/synthesis/paper_tables.yaml",
                "output_path": spec["csv_output"],
                "caption": spec.get("caption", ""),
                "draft_target": spec.get("draft_output", ""),
            }
        )

    return pd.DataFrame(rows).sort_values(["artifact_type", "artifact_id"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a manuscript figure/table inventory CSV.")
    parser.add_argument("--manifest", type=Path, default=MANIFEST, help="Paper-table manifest path.")
    parser.add_argument("--output", type=Path, default=OUT_CSV, help="Output inventory CSV path.")
    args = parser.parse_args()

    out = build_inventory(args.manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    sync_file(args.output, [OUT_PATHS["legacy"]])
    print(f"Wrote {len(out)} rows to {args.output}")


if __name__ == "__main__":
    main()
