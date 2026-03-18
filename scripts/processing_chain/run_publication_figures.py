#!/usr/bin/env python3
"""
Run the curated publication-ready figure scripts from `scripts/analysis`.

This wrapper lives in the processing chain so the reproducible LV1-LV3 products
and the promoted paper figures can be generated from one place. Extend
`PUBLICATION_FIGURES` when new figure drivers are added in the future.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent


@dataclass(frozen=True)
class FigureJob:
    key: str
    rel_script: str
    description: str
    default_args: tuple[str, ...] = ()

    @property
    def script_path(self) -> Path:
        return REPO_ROOT / self.rel_script


PUBLICATION_FIGURES: tuple[FigureJob, ...] = (
    FigureJob(
        key="cloud_field_overview",
        rel_script="scripts/analysis/forcing/run_cloud_field_overview.py",
        description="Notebook 01 cloud-field overview figure.",
    ),
    FigureJob(
        key="plume_lagrangian",
        rel_script="scripts/analysis/growth/run_plume_lagrangian_evolution.py",
        description="Notebook 03 plume-lagrangian evolution figure.",
    ),
    FigureJob(
        key="psd_waterfall",
        rel_script="scripts/analysis/growth/run_psd_waterfall.py",
        description="Notebook 04 PSD waterfall figure suite.",
    ),
)

FIGURE_MAP = {job.key: job for job in PUBLICATION_FIGURES}


def _parse_csv(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    vals = [item.strip() for item in raw.split(",") if item.strip()]
    return vals or None


def _resolve_jobs(selected: list[str] | None, skipped: list[str] | None) -> list[FigureJob]:
    jobs = list(PUBLICATION_FIGURES)
    if selected:
        unknown = [name for name in selected if name not in FIGURE_MAP]
        if unknown:
            valid = ", ".join(FIGURE_MAP)
            raise ValueError(f"Unknown figure key(s): {', '.join(unknown)}. Valid: {valid}")
        jobs = [FIGURE_MAP[name] for name in selected]
    if skipped:
        unknown = [name for name in skipped if name not in FIGURE_MAP]
        if unknown:
            valid = ", ".join(FIGURE_MAP)
            raise ValueError(f"Unknown skip key(s): {', '.join(unknown)}. Valid: {valid}")
        jobs = [job for job in jobs if job.key not in set(skipped)]
    return jobs


def _print_jobs(jobs: list[FigureJob]) -> None:
    for job in jobs:
        print(f"{job.key:22s} {job.rel_script}")
        print(f"  {job.description}")


def parse_args() -> argparse.Namespace:
    epilog = """
Examples:
  python run_publication_figures.py
  python run_publication_figures.py --list
  python run_publication_figures.py --figures cloud_field_overview,plume_lagrangian
  python run_publication_figures.py --skip psd_waterfall --dry-run
"""
    parser = argparse.ArgumentParser(
        description="Run the promoted publication-ready figure scripts under scripts/analysis.",
        epilog=epilog.strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--figures",
        type=str,
        default=None,
        help="Comma-separated subset of figure keys to run. Default: all curated figure scripts.",
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
    return parser.parse_args()


def _run_job(job: FigureJob, *, dry_run: bool = False) -> int:
    if not job.script_path.is_file():
        raise FileNotFoundError(f"Figure script not found: {job.script_path}")
    cmd = [sys.executable, str(job.script_path), *job.default_args]
    print(f"[figure] {job.key}")
    print(" ", " ".join(cmd))
    if dry_run:
        return 0
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


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

    failures: list[str] = []
    for job in jobs:
        rc = _run_job(job, dry_run=args.dry_run)
        if rc != 0:
            failures.append(job.key)
            print(f"{job.key} failed with exit code {rc}", file=sys.stderr)
            if not args.continue_on_error:
                break

    if failures:
        print(f"Figure run finished with failures: {', '.join(failures)}", file=sys.stderr)
        sys.exit(1)

    print("Publication figure run complete.")


if __name__ == "__main__":
    main()
