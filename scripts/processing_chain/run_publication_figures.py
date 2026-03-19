#!/usr/bin/env python3
"""
Run the curated publication products from `scripts/analysis`.

This wrapper lives in the processing chain so the reproducible LV1-LV3 products,
paper figures, supporting tables, and registry metrics can be generated from one
place. After a successful run, plots and MP4s under output/gfx are copied into
output/gallery (single-frame PNGs such as spectral_waterfall *_itime*.png are
excluded). Extend `PUBLICATION_PRODUCTS` when new promoted analysis drivers are
added in the future.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
CONFIG_DIR = SCRIPT_DIR / "config"
DEFAULT_CONFIG = CONFIG_DIR / "cfg_publication_figures.yaml"
GALLERY_DIR = REPO_ROOT / "output" / "gallery"
GFX_PNG_ROOT = REPO_ROOT / "output" / "gfx" / "png"
GFX_MP4_DIR = REPO_ROOT / "output" / "gfx" / "mp4"
DEFAULT_MIRA_DIR = Path("/Users/schimmel/data/observations/eriswil/mira35/20230125")
PAMTRA_MISSIONS = ("composite", "SM059", "SM058", "SM060")

# Filename substring that marks single-frame outputs (e.g. spectral waterfall frames); exclude from gallery.
SINGLE_FRAME_MARKER = "_itime"

ArgBuilder = Callable[[argparse.Namespace], tuple[str, ...]]


def _load_publication_config(path: Path) -> dict[str, list[str]]:
    """Load default_args per job from YAML. Returns job_key -> list of argv strings.
    Each list item may be one token (e.g. '--mp4') or 'option value' (e.g. '--kind N');
    items are split on whitespace so one line per argument is supported."""
    if not path.is_file():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    raw = data.get("default_args") or data
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, list):
            tokens: list[str] = []
            for x in v:
                tokens.extend(str(x).split())
            out[str(k)] = tokens
        else:
            out[str(k)] = []
    return out


@dataclass(frozen=True)
class FigureJob:
    key: str
    rel_script: str
    description: str
    default_args: tuple[str, ...] = ()
    arg_builder: ArgBuilder | None = None

    @property
    def script_path(self) -> Path:
        return REPO_ROOT / self.rel_script

    def resolve_args(
        self, cli_args: argparse.Namespace, config_args: tuple[str, ...] = ()
    ) -> tuple[str, ...]:
        if self.arg_builder is not None:
            base = self.arg_builder(cli_args)
            return base + config_args
        return config_args if config_args else self.default_args


def _pamtra_quicklook_args(args: argparse.Namespace) -> tuple[str, ...]:
    if args.pamtra_file is None:
        raise ValueError(
            "The pamtra_mira35_quicklook job requires --pamtra-file <path-to-*_pamtra.nc>."
        )
    pamtra_file = args.pamtra_file.expanduser().resolve()
    mira_dir = args.mira_dir.expanduser().resolve()
    out: list[str] = [
        str(pamtra_file),
        "--mira-dir",
        str(mira_dir),
        "--observation-mission",
        args.pamtra_observation_mission,
    ]
    if args.pamtra_output is not None:
        out.extend(["--output", str(args.pamtra_output.expanduser().resolve())])
    return tuple(out)


PUBLICATION_PRODUCTS: tuple[FigureJob, ...] = (
    FigureJob(
        key="analysis_registry",
        rel_script="scripts/analysis/registry/build_analysis_registry.py",
        description="Merged paper-ready registry plus the paper-core subset CSV.",
    ),
    FigureJob(
        key="cloud_field_overview",
        rel_script="scripts/analysis/forcing/run_cloud_field_overview.py",
        description="Notebook 01 cloud-field overview figure.",
    ),
    FigureJob(
        key="cloud_phase_budget_table",
        rel_script="scripts/analysis/forcing/run_cloud_phase_budget_table.py",
        description="Phase-integrated budget summary tables for the overview figure.",
    ),
    FigureJob(
        key="plume_lagrangian",
        rel_script="scripts/analysis/growth/run_plume_lagrangian_evolution.py",
        description="Notebook 03 plume-lagrangian evolution figure.",
    ),
    FigureJob(
        key="ridge_metrics",
        rel_script="scripts/analysis/growth/run_ridge_metrics.py",
        description="LV1 plume-ridge metrics plus uncertainty time series CSVs.",
    ),
    FigureJob(
        key="psd_waterfall",
        rel_script="scripts/analysis/growth/run_psd_waterfall.py",
        description="Notebook 04 PSD waterfall figure suite with stats and LaTeX tables.",
    ),
    FigureJob(
        key="psd_stats",
        rel_script="scripts/analysis/growth/run_psd_stats.py",
        description="Registry CSV assembled from PSD waterfall window statistics.",
    ),
    FigureJob(
        key="growth_summary",
        rel_script="scripts/analysis/growth/run_growth_summary.py",
        description="Joined growth-summary registry built from ridge and PSD metrics.",
    ),
    FigureJob(
        key="spectral_waterfall_N",
        rel_script="scripts/analysis/growth/run_spectral_waterfall.py",
        description="Spectral waterfall (number concentration N); args from cfg_publication_figures.",
    ),
    FigureJob(
        key="spectral_waterfall_Q",
        rel_script="scripts/analysis/growth/run_spectral_waterfall.py",
        description="Spectral waterfall (mass concentration Q); args from cfg_publication_figures.",
    ),
    FigureJob(
        key="first_ice_metrics",
        rel_script="scripts/analysis/initiation/run_first_ice_metrics.py",
        description="First excess-ice onset metrics from local LV2 meteogram outputs.",
    ),
    FigureJob(
        key="initiation_process_summary",
        rel_script="scripts/analysis/initiation/run_initiation_process_summary.py",
        description="Early-window initiation process fractions from local LV3 rates.",
    ),
    FigureJob(
        key="pamtra_mira35_quicklook",
        rel_script="scripts/analysis/impacts/run_pamtra_mira35_quicklook.py",
        description="Publication PAMTRA vs MIRA35 quicklook example.",
        arg_builder=_pamtra_quicklook_args,
    ),
)

JOB_MAP = {job.key: job for job in PUBLICATION_PRODUCTS}


def _parse_csv(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [item.strip() for item in raw.split(",") if item.strip()]
    return vals or None


def _resolve_jobs(selected: list[str] | None, skipped: list[str] | None) -> list[FigureJob]:
    jobs = list(PUBLICATION_PRODUCTS)
    if selected:
        # Allow "spectral_waterfall" to select both N and Q
        expanded = []
        for name in selected:
            if name == "spectral_waterfall":
                expanded.extend(["spectral_waterfall_N", "spectral_waterfall_Q"])
            else:
                expanded.append(name)
        selected = expanded
        unknown = [name for name in selected if name not in JOB_MAP]
        if unknown:
            valid = ", ".join(JOB_MAP)
            raise ValueError(f"Unknown figure key(s): {', '.join(unknown)}. Valid: {valid}")
        jobs = [JOB_MAP[name] for name in selected]
    if skipped:
        # --skip spectral_waterfall skips both N and Q
        skip_set = set(skipped)
        if "spectral_waterfall" in skip_set:
            skip_set.update(("spectral_waterfall_N", "spectral_waterfall_Q"))
        unknown = [name for name in skipped if name not in JOB_MAP and name != "spectral_waterfall"]
        if unknown:
            valid = ", ".join(JOB_MAP)
            raise ValueError(f"Unknown skip key(s): {', '.join(unknown)}. Valid: {valid}")
        jobs = [job for job in jobs if job.key not in skip_set]
    return jobs


def _print_jobs(jobs: list[FigureJob]) -> None:
    for job in jobs:
        print(f"{job.key:22s} {job.rel_script}")
        print(f"  {job.description}")
        if job.key == "pamtra_mira35_quicklook":
            print("  requires: --pamtra-file /path/to/*_pamtra.nc")


def parse_args() -> argparse.Namespace:
    epilog = """
Examples:
  python run_publication_figures.py
  python run_publication_figures.py --list
  python run_publication_figures.py --figures cloud_field_overview,plume_lagrangian
  python run_publication_figures.py --skip psd_waterfall --dry-run
  python run_publication_figures.py --figures pamtra_mira35_quicklook --pamtra-file /path/to/example_pamtra.nc
"""
    parser = argparse.ArgumentParser(
        description="Run the promoted publication figure, table, and metric scripts under scripts/analysis.",
        epilog=epilog.strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--figures",
        type=str,
        default=None,
        help="Comma-separated subset of publication job keys to run. Default: all curated jobs.",
    )
    parser.add_argument(
        "--skip",
        type=str,
        default=None,
        help="Comma-separated figure keys to skip.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the curated publication figure jobs and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep running later figure jobs if one job fails.",
    )
    parser.add_argument(
        "--pamtra-file",
        type=Path,
        default=None,
        help="Example PAMTRA NetCDF used by the pamtra_mira35_quicklook job.",
    )
    parser.add_argument(
        "--mira-dir",
        type=Path,
        default=DEFAULT_MIRA_DIR,
        help="Directory with raw MIRA35 *.mmclx files for the PAMTRA quicklook example.",
    )
    parser.add_argument(
        "--pamtra-observation-mission",
        type=str,
        default="composite",
        choices=PAMTRA_MISSIONS,
        help="Observation mission passed to the PAMTRA quicklook example.",
    )
    parser.add_argument(
        "--pamtra-output",
        type=Path,
        default=None,
        help="Optional explicit PNG output path for the PAMTRA quicklook example.",
    )
    parser.add_argument(
        "--no-gallery-sync",
        action="store_true",
        help="Do not copy output/gfx plots and MP4s into output/gallery after a successful run.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to cfg_publication_figures.yaml (default: config/cfg_publication_figures.yaml).",
    )
    return parser.parse_args()


def _run_job(
    job: FigureJob,
    cli_args: argparse.Namespace,
    config: dict[str, list[str]],
    *,
    dry_run: bool = False,
) -> int:
    if not job.script_path.is_file():
        raise FileNotFoundError(f"Figure script not found: {job.script_path}")
    config_args = tuple(config.get(job.key, ()))
    cmd = [sys.executable, str(job.script_path), *job.resolve_args(cli_args, config_args)]
    print(f"[figure] {job.key}")
    print(" ", " ".join(cmd))
    if dry_run:
        return 0
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def _sync_gallery() -> None:
    """Copy all plots and MP4s from output/gfx into output/gallery, excluding single-frame PNGs."""
    GALLERY_DIR.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []

    if GFX_PNG_ROOT.is_dir():
        for p in sorted(GFX_PNG_ROOT.rglob("*.png")):
            if SINGLE_FRAME_MARKER in p.name:
                continue
            dest = GALLERY_DIR / p.name
            shutil.copy2(p, dest)
            copied.append(p.name)

    if GFX_MP4_DIR.is_dir():
        for p in sorted(GFX_MP4_DIR.glob("*.mp4")):
            dest = GALLERY_DIR / p.name
            shutil.copy2(p, dest)
            copied.append(p.name)

    # Canonical names for docs/gallery.html; captions are in figure_captions.yaml (one per figure type).
    canonical = [
        ("fig01_cloud_field_overview.png", GFX_PNG_ROOT / "01", "cloud_field_overview_*_ALLBB.png"),
        ("fig12_plume_path.png", GFX_PNG_ROOT / "03", "figure12_ensemble_mean_plume_path_foo.png"),
        ("fig13_psd_mass.png", GFX_PNG_ROOT / "04", "figure13_psd_alt_time_mass_*.png"),
        ("fig13_psd_number.png", GFX_PNG_ROOT / "04", "figure13_psd_alt_time_number_*.png"),
    ]
    for gal_name, src_dir, pattern in canonical:
        if not src_dir.is_dir():
            continue
        matches = sorted(src_dir.glob(pattern))
        if matches:
            shutil.copy2(matches[0], GALLERY_DIR / gal_name)
            if gal_name not in copied:
                copied.append(gal_name)

    if copied:
        print(f"[gallery] Synced {len(copied)} file(s) to {GALLERY_DIR}")
    else:
        print("[gallery] No PNG/MP4 outputs found under output/gfx; nothing to sync.")


def main() -> None:
    args = parse_args()
    try:
        jobs = _resolve_jobs(_parse_csv(args.figures), _parse_csv(args.skip))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        sys.exit(2)

    if args.list:
        _print_jobs(jobs)
        return

    if not jobs:
        print("No publication figure jobs selected.")
        return

    config = _load_publication_config(args.config.expanduser().resolve())
    failures: list[str] = []
    for job in jobs:
        if job.key == "pamtra_mira35_quicklook" and args.pamtra_file is None:
            print(f"[figure] {job.key} (skipped: no --pamtra-file)")
            continue
        try:
            rc = _run_job(job, args, config, dry_run=args.dry_run)
        except ValueError as exc:
            failures.append(job.key)
            print(f"{job.key} failed: {exc}", file=sys.stderr)
            if not args.continue_on_error:
                break
            continue
        if rc != 0:
            failures.append(job.key)
            print(f"{job.key} failed with exit code {rc}", file=sys.stderr)
            if not args.continue_on_error:
                break

    if failures:
        print(f"Figure run finished with failures: {', '.join(failures)}", file=sys.stderr)
        sys.exit(1)

    if not args.dry_run and not args.no_gallery_sync:
        _sync_gallery()

    print("Publication figure run complete.")


if __name__ == "__main__":
    main()
