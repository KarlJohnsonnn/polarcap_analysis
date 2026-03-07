#!/usr/bin/env python3
"""
Top-level orchestrator for LV0 → LV3 processing chain.

Given a run ID (cs_run), runs stages in order:
  LV1a  Tobac tracking (run_lv1_tracking)
  LV1b  Plume path extraction (run_lv1_tracking)
  LV2   Meteogram Zarr (run_lv2_meteogram_zarr)
  LV3   Process rates (run_lv3_analysis)

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
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from utilities.processing_paths import get_runs_root

CS_RUN_PATTERN = re.compile(r"^cs-eriswil__\d{8}_\d{6}$")


def _validate_cs_run(cs_run: str) -> None:
    if not CS_RUN_PATTERN.match(cs_run):
        raise ValueError(
            f"Invalid cs_run format: {cs_run!r} (expected e.g. cs-eriswil__YYYYMMDD_HHMMSS)"
        )


def _expand_path(p: str) -> str:
    if not p or not isinstance(p, str):
        return ""
    return (os.path.expandvars(os.path.expanduser(str(p))) or "").strip()


def _load_config(path: Path | None) -> dict:
    """Load YAML or JSON config. Returns dict of overrides."""
    if path is None or not path.exists():
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
    p.add_argument("--dry-run", action="store_true", help="Print commands only, do not run")
    return p.parse_args()


def run_cmd(cmd: list[str], env=None) -> int:
    """Run command; return exit code."""
    return subprocess.run(cmd, env=env or None).returncode


def _write_manifest(out_root: Path, manifest: dict) -> None:
    manifests_dir = out_root / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    with open(manifests_dir / "run_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    args = parse_args()
    try:
        _validate_cs_run(args.cs_run)
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    cfg = _load_config(args.config)
    cfg_root = _expand_path(str(cfg.get("model_data_root") or ""))
    root = get_runs_root(cfg_root or args.root)
    out = _expand_path(str(cfg.get("output_root") or args.out)) or "processed"
    domain = str(cfg.get("domain") or args.domain)
    skip_tracking = args.skip_tracking or (
        cfg.get("run_lv1a_tracking", True) is False and cfg.get("run_lv1b_paths", True) is False
    )
    skip_meteogram = args.skip_meteogram or cfg.get("run_lv2_meteogram", True) is False
    skip_lv3 = args.skip_lv3 or cfg.get("run_lv3_rates", True) is False
    overwrite = cfg.get("overwrite", False) or args.overwrite
    debug = cfg.get("debug", False) or args.debug
    dry_run = args.dry_run

    if (not skip_tracking or not skip_meteogram) and not root:
        print("Set CS_RUNS_DIR, pass --root, or set model_data_root in config", file=sys.stderr)
        sys.exit(1)

    out_root = Path(out) / args.cs_run
    out_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "cs_run": args.cs_run,
        "output_root": str(out_root),
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stages": {},
    }
    if dry_run:
        print("Dry-run (commands not executed):")
    if not skip_tracking:
        cmd = [sys.executable, str(_script_dir / "run_lv1_tracking.py"), "--cs-run", args.cs_run,
               "--out", out, "--domain", domain]
        if overwrite:
            cmd.append("--overwrite")
        if root:
            cmd.extend(["--root", root])
        manifest["stages"]["lv1"] = "run_lv1_tracking"
        print(" ", " ".join(cmd))
        if not dry_run and run_cmd(cmd) != 0:
            print("run_lv1_tracking failed", file=sys.stderr)
            _write_manifest(out_root, manifest)
            sys.exit(1)
    else:
        manifest["stages"]["lv1"] = "skipped"

    if not skip_meteogram:
        cmd = [sys.executable, str(_script_dir / "run_lv2_meteogram_zarr.py"), "-r", args.cs_run, "--out", out]
        if debug:
            cmd.append("--debug")
        if overwrite:
            cmd.append("--overwrite")
        if root:
            cmd.extend(["--root", root])
        manifest["stages"]["lv2"] = "run_lv2_meteogram_zarr"
        print(" ", " ".join(cmd))
        if not dry_run and run_cmd(cmd) != 0:
            print("run_lv2_meteogram_zarr failed", file=sys.stderr)
            _write_manifest(out_root, manifest)
            sys.exit(1)
    else:
        manifest["stages"]["lv2"] = "skipped"

    if not skip_lv3:
        cmd = [sys.executable, str(_script_dir / "run_lv3_analysis.py"), "--cs-run", args.cs_run, "--out", out]
        if overwrite:
            cmd.append("--overwrite")
        manifest["stages"]["lv3"] = "run_lv3_analysis"
        print(" ", " ".join(cmd))
        if not dry_run and run_cmd(cmd) != 0:
            print("run_lv3_analysis failed", file=sys.stderr)
            _write_manifest(out_root, manifest)
            sys.exit(1)
    else:
        manifest["stages"]["lv3"] = "skipped"

    manifest["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if not dry_run:
        _write_manifest(out_root, manifest)
        print(f"Manifest: {out_root / 'manifests' / 'run_manifest.json'}")
    print("Done.")


if __name__ == "__main__":
    main()
