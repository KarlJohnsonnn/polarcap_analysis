"""Canonical and legacy paths for manuscript-facing table outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]

CANONICAL_TABLES_ROOT = REPO_ROOT / "output" / "tables"
CANONICAL_REGISTRY_DIR = CANONICAL_TABLES_ROOT / "registry"
CANONICAL_PAPER_TABLES_DIR = CANONICAL_TABLES_ROOT / "paper"
CANONICAL_PHASE_BUDGET_DIR = CANONICAL_TABLES_ROOT / "phase_budget"
CANONICAL_PSD_DIR = CANONICAL_TABLES_ROOT / "psd"
CANONICAL_SPECTRAL_GROWTH_DIR = CANONICAL_TABLES_ROOT / "spectral_growth"
CANONICAL_GROWTH_BUNDLE_DIR = CANONICAL_TABLES_ROOT / "growth_bundle"

LEGACY_REGISTRY_DIR = REPO_ROOT / "data" / "registry"
LEGACY_PAPER_TABLES_DIR = LEGACY_REGISTRY_DIR / "paper_tables"
LEGACY_GFX_ROOT = REPO_ROOT / "output" / "gfx"
LEGACY_GROWTH_BUNDLE_DIR = REPO_ROOT / "output" / "growth_bundle"


def ensure_parent(path: Path) -> None:
    """Create a parent directory when needed."""
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file_if_changed(src: Path, dst: Path) -> None:
    """Mirror one file without rewriting identical content."""
    ensure_parent(dst)
    content = src.read_bytes()
    if dst.exists() and dst.read_bytes() == content:
        return
    dst.write_bytes(content)


def sync_file(src: Path, mirrors: Iterable[Path]) -> None:
    """Copy one rendered file to each compatibility location."""
    for dst in mirrors:
        copy_file_if_changed(src, dst)


def sync_tree(src_dir: Path, dst_dir: Path) -> None:
    """Recursively mirror all files from one directory tree into another."""
    if not src_dir.exists():
        return
    for path in sorted(src_dir.rglob("*")):
        rel = path.relative_to(src_dir)
        target = dst_dir / rel
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        copy_file_if_changed(path, target)


def first_existing(paths: Iterable[Path]) -> Path | None:
    """Return the first existing path from a preference-ordered iterable."""
    for path in paths:
        if path.exists():
            return path
    return None


def repo_relative(path: Path) -> str:
    """Return a repo-relative string when possible."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def registry_output_paths(filename: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Path]:
    """Canonical plus compatibility paths for one registry CSV or TXT file."""
    return {
        "canonical": repo_root / "output" / "tables" / "registry" / filename,
        "legacy": repo_root / "data" / "registry" / filename,
    }


def resolve_registry_input(filename: str, *, repo_root: Path = REPO_ROOT) -> Path:
    """Prefer canonical registry files while accepting legacy copies."""
    paths = registry_output_paths(filename, repo_root=repo_root)
    return first_existing([paths["canonical"], paths["legacy"]]) or paths["canonical"]


def paper_table_output_paths(stem: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Path]:
    """Canonical plus compatibility paths for one compiled paper table stem."""
    return {
        "canonical_csv": repo_root / "output" / "tables" / "paper" / f"{stem}.csv",
        "canonical_tex": repo_root / "output" / "tables" / "paper" / f"{stem}.tex",
        "legacy_csv": repo_root / "data" / "registry" / "paper_tables" / f"{stem}.csv",
        "legacy_tex": repo_root / "data" / "registry" / "paper_tables" / f"{stem}.tex",
    }


def resolve_paper_table_input(stem: str, suffix: str = ".csv", *, repo_root: Path = REPO_ROOT) -> Path:
    """Prefer canonical paper tables while accepting legacy copies."""
    paths = paper_table_output_paths(stem, repo_root=repo_root)
    key = "canonical_csv" if suffix == ".csv" else "canonical_tex"
    legacy_key = "legacy_csv" if suffix == ".csv" else "legacy_tex"
    return first_existing([paths[key], paths[legacy_key]]) or paths[key]


def phase_budget_output_paths(
    exp_label: str,
    active_range_key: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Path]:
    """Canonical plus compatibility paths for phase-budget summary products."""
    stem = f"cloud_phase_budget_summary_{exp_label}_{active_range_key}"
    return {
        "canonical_long_csv": repo_root / "output" / "tables" / "phase_budget" / f"{stem}_long.csv",
        "canonical_summary_csv": repo_root / "output" / "tables" / "phase_budget" / f"{stem}.csv",
        "canonical_summary_tex": repo_root / "output" / "tables" / "phase_budget" / f"{stem}.tex",
        "legacy_long_csv": repo_root / "output" / "gfx" / "csv" / "01" / f"{stem}_long.csv",
        "legacy_summary_csv": repo_root / "output" / "gfx" / "csv" / "01" / f"{stem}.csv",
        "legacy_summary_tex": repo_root / "output" / "gfx" / "tex" / "01" / f"{stem}.tex",
    }


def resolve_phase_budget_input(
    exp_label: str,
    active_range_key: str,
    *,
    long_form: bool = False,
    repo_root: Path = REPO_ROOT,
) -> Path:
    """Prefer canonical phase-budget CSVs while accepting legacy copies."""
    paths = phase_budget_output_paths(exp_label, active_range_key, repo_root=repo_root)
    if long_form:
        return first_existing([paths["canonical_long_csv"], paths["legacy_long_csv"]]) or paths["canonical_long_csv"]
    return first_existing([paths["canonical_summary_csv"], paths["legacy_summary_csv"]]) or paths["canonical_summary_csv"]


def psd_stats_output_paths(var_kind: str, run_id: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Path]:
    """Canonical plus compatibility paths for figure13 PSD statistics tables."""
    stem = f"figure13_psd_stats_{var_kind}_{run_id}"
    return {
        "canonical_csv": repo_root / "output" / "tables" / "psd" / f"{stem}.csv",
        "canonical_tex": repo_root / "output" / "tables" / "psd" / f"{stem}.tex",
        "legacy_csv": repo_root / "output" / "gfx" / "csv" / "04" / f"{stem}.csv",
        "legacy_tex": repo_root / "output" / "gfx" / "tex" / "04" / f"{stem}.tex",
    }


def spectral_growth_output_paths(
    cs_run: str,
    kind: str,
    station_tag: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Path]:
    """Canonical plus compatibility paths for ridge-growth CSV exports."""
    filename = f"ridge_growth_{kind}_{station_tag}.csv"
    return {
        "canonical": repo_root / "output" / "tables" / "spectral_growth" / cs_run / filename,
        "legacy": repo_root / "output" / "gfx" / "csv" / "05" / cs_run / filename,
    }


def growth_bundle_output_dir(cs_run: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Path]:
    """Canonical plus compatibility roots for growth-bundle outputs."""
    return {
        "canonical": repo_root / "output" / "tables" / "growth_bundle" / cs_run,
        "legacy": repo_root / "output" / "growth_bundle" / cs_run,
    }
