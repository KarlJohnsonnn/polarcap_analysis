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
  python run_chain.py --cs-run cs-eriswil__20260123_180947 --skip-tracking
  python run_chain.py --root $CS_RUNS_DIR --cs-run cs-eriswil__20260314_103039 --skip-tracking
  POLARCAP_OUTPUT_ROOT=/work/.../RUN_ERISWILL_200x160x100/ensemble_output python run_chain.py --cs-run cs-eriswil__20260314_103039
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

CS_RUN_PATTERN = re.compile(r"^cs-eriswil__\d{8}_\d{6}$")  # default when not in config


def _get_cs_run_pattern(cfg: dict[str, Any]) -> re.Pattern[str]:
    """Compile cs_run validation regex from config or return default.

    Args:
        cfg: Config dict from _load_config (may contain "cs_run_pattern" string).

    Returns:
        Compiled regex pattern for validating run IDs.
    """
    raw = cfg.get("cs_run_pattern") if isinstance(cfg.get("cs_run_pattern"), str) else None
    return re.compile(raw) if raw else CS_RUN_PATTERN


def _validate_cs_run(cs_run: str, pattern: re.Pattern[str] | None = None) -> None:
    """Ensure cs_run matches expected format.

    Args:
        cs_run: Run ID string (e.g. cs-eriswil__YYYYMMDD_HHMMSS).
        pattern: Regex to match. If None, uses default CS_RUN_PATTERN.

    Raises:
        ValueError: If cs_run does not match the pattern.
    """
    pat = pattern if pattern is not None else CS_RUN_PATTERN
    if not pat.match(cs_run):
        raise ValueError(
            f"Invalid cs_run format: {cs_run!r} (expected pattern {pat.pattern!r})"
        )


def _load_config(path: Path | None) -> dict[str, Any]:
    """Load YAML or JSON config file into a dict.

    Args:
        path: Path to config file, or None to return empty dict.

    Returns:
        Config key-value overrides. Empty dict if path is None, missing, or YAML
        is not available (ImportError). JSON is used for non-.yaml/.yml suffixes.
    """
    if path is None or not path.exists():
        return {}
    with open(path) as f:
        if path.suffix in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore[import-untyped]
                return yaml.safe_load(f) or {}
            except ImportError:
                return {}
        return json.load(f)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments and return namespace.

    Returns:
        Namespace with cs_run, config, root, out, domain, skip_* flags,
        overwrite, debug, dry_run. Exits with help message if --help is passed.
    """
    epilog = """
Examples:
  # Full chain (set CS_RUNS_DIR or use --root):
  %(prog)s --cs-run cs-eriswil__20260304_110254
  %(prog)s --root /path/to/runs --cs-run cs-eriswil__20260304_110254

  # Use config file (model_data_root, output_root, domain, etc.):
  %(prog)s --config config.yaml --cs-run cs-eriswil__20260304_110254

  # Skip stages and control output:
  %(prog)s --cs-run cs-eriswil__20260304_110254 --skip-tracking
  %(prog)s --cs-run cs-eriswil__20260304_110254 --out /path/to/processed
  %(prog)s --cs-run cs-eriswil__20260304_110254 --skip-lv3 --overwrite

  # Dry-run (print commands only):
  %(prog)s --cs-run cs-eriswil__20260304_110254 --dry-run
"""
    p = argparse.ArgumentParser(
        description="Run full LV0→LV3 processing chain (LV1a/LV1b tracking, LV2 meteogram Zarr, LV3 process rates).",
        epilog=epilog.strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--cs-run", required=True, help="Run ID (e.g. cs-eriswil__YYYYMMDD_HHMMSS)")
    p.add_argument("--config", type=Path, default=None, help="YAML/JSON config: model_data_root, output_root, domain, run_lv*, overwrite, debug")
    from utilities.processing_paths import get_runs_root

    p.add_argument("--root", default=get_runs_root(), help="Model data root for LV1/LV2 (default: $CS_RUNS_DIR)")
    p.add_argument(
        "--out",
        default=None,
        help=(
            "Output root; writes <out>/<cs_run>/lv1_tracking|lv1_paths|lv2_meteogram|lv3_rates "
            "(default: --out, else $POLARCAP_OUTPUT_ROOT, else matching RUN_ERISWILL_*x100/ensemble_output, else repo scripts/data/processed)"
        ),
    )
    p.add_argument("--domain", default="200x160", help="Domain for LV1 (e.g. 50x40 or 200x160)")
    p.add_argument("--skip-tracking", action="store_true", help="Skip LV1a and LV1b")
    p.add_argument("--skip-meteogram", action="store_true", help="Skip LV2 (meteogram Zarr)")
    p.add_argument("--skip-lv3", action="store_true", help="Skip LV3 (process rates)")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing stage outputs")
    p.add_argument("--debug", action="store_true", help="Debug mode for LV2 (small subset)")
    p.add_argument("--dry-run", action="store_true", help="Print commands only, do not run")
    p.add_argument("--flare-idx", type=int, default=0, help="Index of the flare experiment (0-based). Experiments are split into two lists by metadata (lflare): flare_idx indexes the flare-only list, ref_idx the ref-only list.")
    p.add_argument("--ref-idx", type=int, default=-1, help="Index of the reference experiment in the ref-only list. -1 = auto-select ref that matches flare in non-emission params.")
    p.add_argument("--threshold", type=float, default=1.0, help="Tobac detection threshold in 1/L (default: 1.0 = 1 per liter)")
    return p.parse_args()


def run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> int:
    """Run a command in a subprocess.

    Args:
        cmd: List of program and arguments.
        env: Optional environment dict for the subprocess. None uses current env.

    Returns:
        Return code of the process.
    """
    return subprocess.run(cmd, env=env or None).returncode


def _write_manifest(out_root: Path, manifest: dict[str, Any]) -> None:
    """Write run manifest JSON under out_root/manifests/.

    Args:
        out_root: Base path for the run (e.g. processed/<cs_run>).
        manifest: Dict with cs_run, output_root, started_utc, stages, etc.
    """
    manifests_dir = out_root / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    with open(manifests_dir / "run_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main() -> None:
    """Run the LV0→LV3 chain: validate args, load config, run stages, write manifest.

    Exits with code 1 on validation failure or stage failure. Writes partial
    manifest if a stage fails. Requires --root or CS_RUNS_DIR when running LV1 or LV2.
    """
    args = parse_args()
    cfg = _load_config(args.config)
    try:
        _validate_cs_run(args.cs_run, _get_cs_run_pattern(cfg))
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    from utilities.processing_paths import expand_path, get_output_root, get_runs_root

    cfg_root = expand_path(str(cfg.get("model_data_root") or ""))
    root = get_runs_root(cfg_root or args.root)
    out = get_output_root(cfg.get("output_root") or args.out, runs_root=root, cs_run=args.cs_run)
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
    # LV1 uses flare_idx/ref_idx from config (defaults 0, -1). They index two separate lists:
    # flare experiments vs reference experiments (split by lflare in run JSON). ref_idx=-1 in
    # run_lv1_tracking means auto-match; run_chain passes config values (default 0) unless set in YAML.
    if not skip_tracking:
        cmd = [
            sys.executable,
            str(_script_dir / "run_lv1_tracking.py"),
            "--cs-run", args.cs_run,
            "--out", out,
            "--domain", domain,
            "--flare-idx", str(args.flare_idx),
            "--ref-idx", str(args.ref_idx),
            "--threshold", str(args.threshold),
        ]
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
