#!/usr/bin/env python3
"""
LV1a + LV1b: Tobac cloud tracking and plume-path extraction.

Reads 3D NetCDFs and run JSON from ensemble_output/<cs_run>/, writes:
  - lv1_tracking/: features CSV, tracks CSV, features_mask CSV, segmentation_mask NetCDF
  - lv1_paths/: per-cell plume path NetCDFs (integrated, extreme, vertical)

Usage:
  python run_tracking.py --root /path/to/cosmo-specs-runs --cs-run cs-eriswil__20260123_180947
  python run_tracking.py --root /path/to/runs --cs-run cs-eriswil__20260123_180947 --domain 200x160 --out processed
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from _paths import get_runs_root
from utilities.tracking_pipeline import (
    DEFAULT_EXTRACTION_TYPES,
    DEFAULT_TRACER_SPECS,
    discover_3d_runs,
    run_plume_path_extraction,
    run_tobac_tracking,
)


def parse_args():
    p = argparse.ArgumentParser(description="LV1a/LV1b: Tobac tracking + plume path extraction")
    p.add_argument("--root", default=None,
                   help="Model data root (default: $CS_RUNS_DIR; should contain RUN_ERISWILL_*/)")
    p.add_argument("--cs-run", required=True, help="Run ID, e.g. cs-eriswil__20260123_180947")
    p.add_argument("--domain", default="200x160", help="Domain e.g. 50x40 or 200x160")
    p.add_argument("--flare-idx", type=int, default=0, help="Flare experiment index")
    p.add_argument("--ref-idx", type=int, default=0, help="Reference experiment index")
    p.add_argument("--threshold", type=float, default=1e-6, help="Tobac detection threshold")
    p.add_argument("--out", default="processed", help="Output root; <out>/<cs_run>/lv1_*")
    p.add_argument("--skip-tracking", action="store_true", help="Skip LV1a, only run LV1b path extraction")
    p.add_argument("--skip-paths", action="store_true", help="Skip LV1b, only run LV1a tracking")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    return p.parse_args()


def main():
    args = parse_args()
    root = get_runs_root(args.root)
    if not root:
        print("Set CS_RUNS_DIR or pass --root")
        sys.exit(1)
    ctx = discover_3d_runs(
        root, args.domain, args.cs_run,
        flare_idx=args.flare_idx, ref_idx=args.ref_idx, threshold=args.threshold,
    )
    if ctx is None:
        print("No flare/ref pair found; check --root, --cs-run, --domain")
        sys.exit(1)
    out_root = Path(args.out) / args.cs_run
    lv1_tracking_dir = out_root / "lv1_tracking"
    lv1_paths_dir = out_root / "lv1_paths"
    lv1_tracking_dir.mkdir(parents=True, exist_ok=True)
    lv1_paths_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_tracking:
        print("[LV1a] Tobac tracking...")
        written = run_tobac_tracking(
            ctx, output_dir=lv1_tracking_dir,
            tracer_specs=DEFAULT_TRACER_SPECS,
            overwrite=args.overwrite,
        )
        print(f"  Wrote {len(written)} tracer outputs under {lv1_tracking_dir}")
    else:
        print("[LV1a] Skipped (--skip-tracking)")

    if not args.skip_paths:
        print("[LV1b] Plume path extraction...")
        written_paths = run_plume_path_extraction(
            ctx, lv1_tracking_dir, lv1_paths_dir,
            tracer_specs=DEFAULT_TRACER_SPECS,
            extraction_types=DEFAULT_EXTRACTION_TYPES,
            overwrite=args.overwrite,
        )
        print(f"  Wrote {len(written_paths)} path NetCDFs under {lv1_paths_dir}")
    else:
        print("[LV1b] Skipped (--skip-paths)")

    print("Done.")


if __name__ == "__main__":
    main()
