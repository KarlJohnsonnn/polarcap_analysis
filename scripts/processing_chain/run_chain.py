#!/usr/bin/env python3
"""
Top-level orchestrator for LV0 → LV3 processing chain.

Given a run ID (cs_run), runs stages in order:
  LV1a  Tobac tracking (run_tracking)
  LV1b  Plume path extraction (run_tracking)
  LV2   Meteogram Zarr (run_meteogram_zarr)
  LV3   Process rates (run_lv3)

Outputs go under <output_root>/<cs_run>/lv1_tracking|lv1_paths|lv2_meteogram|lv3_rates.
Use --skip-* to skip stages; --overwrite to recompute existing outputs.

Usage:
  python run_chain.py --cs-run cs-eriswil__20260123_180947
  python run_chain.py --cs-run cs-eriswil__20260123_180947 --skip-tracking --out processed
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from _paths import get_runs_root


def _load_config(path: Path) -> dict:
    """Load YAML or JSON config. Returns dict of overrides."""
    if not path or not path.exists():
        return {}
    with open(path) as f:
        if path.suffix in (".yaml", ".yml"):
            try:
                import yaml
                return yaml.safe_load(f) or {}
            except ImportError:
                return {}
        return json.load(f)


def parse_args():
    p = argparse.ArgumentParser(description="Run full LV0→LV3 processing chain")
    p.add_argument("--cs-run", required=True, help="Run ID (e.g. cs-eriswil__YYYYMMDD_HHMMSS)")
    p.add_argument("--config", type=Path, default=None, help="YAML/JSON config (model_data_root, output_root, domain, run_lv*, overwrite, debug)")
    p.add_argument("--root", default=os.environ.get("CS_RUNS_DIR"),
                   help="Model data root for LV1/LV2 (default: $CS_RUNS_DIR if set)")
    p.add_argument("--out", default="processed", help="Output root")
    p.add_argument("--domain", default="200x160", help="Domain for LV1 (e.g. 50x40)")
    p.add_argument("--skip-tracking", action="store_true", help="Skip LV1a and LV1b")
    p.add_argument("--skip-meteogram", action="store_true", help="Skip LV2")
    p.add_argument("--skip-lv3", action="store_true", help="Skip LV3")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    p.add_argument("--debug", action="store_true", help="Debug mode for LV2")
    return p.parse_args()


def run_cmd(cmd: list[str], env=None) -> int:
    """Run command; return exit code."""
    return subprocess.run(cmd, env=env or None).returncode


def main():
    args = parse_args()
    cfg = _load_config(args.config)
    cfg_root = cfg.get("model_data_root")
    if cfg_root and "${" in str(cfg_root):
        cfg_root = os.path.expandvars(str(cfg_root)) or None
    root = get_runs_root(cfg_root or args.root)
    out = cfg.get("output_root") or args.out
    domain = cfg.get("domain") or args.domain
    skip_tracking = args.skip_tracking or (
        cfg.get("run_lv1a_tracking", True) is False and cfg.get("run_lv1b_paths", True) is False
    )
    skip_meteogram = args.skip_meteogram or cfg.get("run_lv2_meteogram", True) is False
    skip_lv3 = args.skip_lv3 or cfg.get("run_lv3_rates", True) is False
    overwrite = cfg.get("overwrite", False) or args.overwrite
    debug = cfg.get("debug", False) or args.debug

    if not skip_tracking and not root:
        print("Set CS_RUNS_DIR, pass --root, or set model_data_root in config")
        sys.exit(1)

    out_root = Path(out) / args.cs_run
    manifest = {
        "cs_run": args.cs_run,
        "output_root": str(out_root),
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stages": {},
    }
    if not skip_tracking:
        cmd = [sys.executable, str(_script_dir / "run_tracking.py"), "--cs-run", args.cs_run, "--out", out,
               "--domain", domain]
        if overwrite:
            cmd.append("--overwrite")
        if root:
            cmd.extend(["--root", root])
        manifest["stages"]["lv1"] = "run_tracking"
        if run_cmd(cmd) != 0:
            print("run_tracking failed")
            sys.exit(1)
    else:
        manifest["stages"]["lv1"] = "skipped"

    if not skip_meteogram:
        cmd = [sys.executable, str(_script_dir / "run_meteogram_zarr.py"), "-r", args.cs_run, "--out", out]
        if debug:
            cmd.append("--debug")
        if overwrite:
            cmd.append("--overwrite")
        if root:
            cmd.extend(["--root", root])
        manifest["stages"]["lv2"] = "run_meteogram_zarr"
        if run_cmd(cmd) != 0:
            print("run_meteogram_zarr failed")
            sys.exit(1)
    else:
        manifest["stages"]["lv2"] = "skipped"

    if not skip_lv3:
        cmd = [sys.executable, str(_script_dir / "run_lv3.py"), "--cs-run", args.cs_run, "--out", out]
        if overwrite:
            cmd.append("--overwrite")
        manifest["stages"]["lv3"] = "run_lv3"
        if run_cmd(cmd) != 0:
            print("run_lv3 failed")
            sys.exit(1)
    else:
        manifest["stages"]["lv3"] = "skipped"

    manifest["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifests_dir = out_root / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifests_dir / "run_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Manifest: {manifest_path}")
    print("Done.")


if __name__ == "__main__":
    main()
