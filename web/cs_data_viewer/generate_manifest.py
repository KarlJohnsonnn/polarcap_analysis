#!/usr/bin/env python3
"""
Generate manifest.json for QuicklookBrowser.

Default mode scans the repository output trees and writes a combined manifest to
`web/cs_data_viewer/manifest.json`:

    python web/cs_data_viewer/generate_manifest.py

Single-root mode preserves the older behavior and writes `manifest.json` in the
target directory:

    python web/cs_data_viewer/generate_manifest.py /path/to/media_root
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SUPPORTED = {".png", ".gif", ".mp4"}
VIDEO_EXT = {".mp4"}
# Match run_publication_figures.SINGLE_FRAME_MARKER: skip per-frame PNGs, keep MP4 in viewer.
SKIP_PNG_SUBSTR = "_itime"
REPO_OUTPUT_DIRS = (
    Path("output/"),
    Path("notebooks/output"),
    Path("scripts/processing_chain/output"),
)


def get_default_repo_root(script_path: Path | None = None) -> Path:
    script_path = script_path or Path(__file__).resolve()
    return script_path.resolve().parent.parents[1]


def get_manifest_path(repo_root: Path) -> Path:
    return repo_root / "web/cs_data_viewer/manifest.json"


def build_entry(path: Path, relative_to: Path) -> dict[str, str | int]:
    rel = path.relative_to(relative_to)
    ext = path.suffix[1:].lower()
    folder = str(rel.parent) if str(rel.parent) != "." else ""
    return {
        "path": str(rel),
        "name": path.name,
        "folder": folder,
        "type": "video" if path.suffix.lower() in VIDEO_EXT else "image",
        "ext": ext,
        "mtime": int(path.stat().st_mtime * 1000),
    }


def scan_root(scan_root: Path, relative_to: Path) -> list[dict[str, str | int]]:
    if not scan_root.exists():
        return []

    files: list[dict[str, str | int]] = []
    for path in sorted(scan_root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED:
            continue
        if path.suffix.lower() == ".png" and SKIP_PNG_SUBSTR in path.name:
            continue
        files.append(build_entry(path, relative_to))
    return files


def write_manifest(files: list[dict[str, str | int]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"files": files}, indent=1) + "\n")


def print_summary(files: list[dict[str, str | int]], out_path: Path, scanned_roots: list[Path]) -> None:
    print(f"Indexed {len(files)} files -> {out_path}")
    if scanned_roots:
        print("Scanned:")
        for root in scanned_roots:
            print(f"  - {root}")

    folders = sorted({f["folder"] for f in files if f["folder"]})
    print(f"Folders: {len(folders)}")
    for folder in folders[:20]:
        count = sum(1 for f in files if f["folder"] == folder)
        print(f"  {folder}/ ({count})")
    if len(folders) > 20:
        print(f"  ... and {len(folders) - 20} more")


def latest_output_mtime(repo_root: Path) -> int:
    latest = 0
    for rel_root in REPO_OUTPUT_DIRS:
        abs_root = repo_root / rel_root
        if not abs_root.exists():
            continue
        for path in abs_root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in SUPPORTED:
                continue
            if path.suffix.lower() == ".png" and SKIP_PNG_SUBSTR in path.name:
                continue
            latest = max(latest, int(path.stat().st_mtime * 1000))
    return latest


def get_repo_status(repo_root: Path) -> dict[str, object]:
    repo_root = repo_root.resolve()
    manifest_path = get_manifest_path(repo_root)
    manifest_exists = manifest_path.exists()
    manifest_mtime = int(manifest_path.stat().st_mtime * 1000) if manifest_exists else 0
    latest_output = latest_output_mtime(repo_root)
    return {
        "repoRoot": str(repo_root),
        "manifestPath": str(manifest_path),
        "manifestExists": manifest_exists,
        "manifestMtime": manifest_mtime,
        "latestOutputMtime": latest_output,
        "manifestNeedsUpdate": latest_output > manifest_mtime,
    }


def generate_repo_manifest(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    files: list[dict[str, str | int]] = []
    scanned_roots: list[Path] = []

    for rel_root in REPO_OUTPUT_DIRS:
        abs_root = repo_root / rel_root
        scanned_roots.append(abs_root)
        files.extend(scan_root(abs_root, repo_root))

    files.sort(key=lambda item: item["path"])
    out_path = get_manifest_path(repo_root)
    write_manifest(files, out_path)
    print_summary(files, out_path, scanned_roots)
    return out_path


def generate_single_root_manifest(root: Path) -> Path:
    root = root.resolve()
    files = scan_root(root, root)
    files.sort(key=lambda item: item["path"])
    out_path = root / "manifest.json"
    write_manifest(files, out_path)
    print_summary(files, out_path, [root])
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        help="Optional single directory to index. If omitted, scan repository output folders.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Explicit repository root for repo mode.",
    )
    parser.add_argument(
        "--repo",
        action="store_true",
        help="Force repository mode even if a root argument is present.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = (args.repo_root or get_default_repo_root()).resolve()

    if args.repo or not args.root:
        generate_repo_manifest(repo_root)
        return

    generate_single_root_manifest(Path(args.root))


if __name__ == "__main__":
    main()
