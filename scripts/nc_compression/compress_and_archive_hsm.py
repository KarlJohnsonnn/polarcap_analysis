#!/usr/bin/env python3
"""
Compress M_*.nc and 3D_*.nc to a single tar.zst, then archive it to the DKRZ HSM tape system via pyslk.

Requires: pyslk (and slk/slk_helpers on Levante: `module load slk`).
See: https://docs.dkrz.de/doc/datastorage/hsm/ and https://hsm-tools.gitlab-pages.dkrz.de/pyslk/
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _find_compress_script(script_dir: Path | None) -> Path:
    """Locate compress_nc.sh next to this script or in script_dir."""
    base = script_dir or Path(__file__).resolve().parent
    candidate = base / "compress_nc.sh"
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(f"compress_nc.sh not found in {base}")


def run_compress(
    data_dir: Path,
    archive_path: Path,
    compress_script: Path,
    overwrite: bool,
) -> None:
    """Run compress_nc.sh to create the tar.zst archive."""
    env = os.environ.copy()
    if overwrite:
        env["OVERWRITE"] = "1"
    env.setdefault("PV_INTERVAL", "1")
    cmd = [str(compress_script), "compress", str(data_dir), str(archive_path)]
    if overwrite:
        cmd = [str(compress_script), "--overwrite", "compress", str(data_dir), str(archive_path)]
    subprocess.run(cmd, env=env, check=True)


def archive_to_hsm(local_path: Path, dst_gns: str, retry: bool) -> None:
    """Upload a single file to HSM using pyslk.archive."""
    import pyslk

    if not pyslk.login_valid(quiet=True, rise_error_on_invalid=True):
        raise RuntimeError(
            "No valid slk login token. Run 'slk login' (e.g. after 'module load slk') and retry."
        )
    archived, _ = pyslk.archive(str(local_path), dst_gns, recursive=False, retry=retry)
    if not archived:
        raise RuntimeError(f"pyslk.archive reported no archived files for {local_path} -> {dst_gns}")
    print(f"Archived to HSM: {dst_gns.rstrip('/')}/{local_path.name}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compress model NC files to tar.zst and archive to DKRZ HSM (pyslk).",
        epilog="Requires pyslk and slk (e.g. module load slk on Levante).",
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Directory containing M_*.nc and 3D_*.nc to compress.",
    )
    parser.add_argument(
        "hsm_dest",
        type=str,
        help="HSM destination namespace (e.g. /arch/<project>/polarcap/run01/).",
    )
    parser.add_argument(
        "--archive",
        "-a",
        type=str,
        default=None,
        help="Archive basename (default: nc_YYYYMMDD_HHMMSS.tar.zst).",
    )
    parser.add_argument(
        "--outdir",
        "-o",
        type=Path,
        default=None,
        help="Directory for the .tar.zst file (default: data_dir).",
    )
    parser.add_argument(
        "--compress-script",
        type=Path,
        default=None,
        help="Path to compress_nc.sh (default: next to this script).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing local archive before compressing.",
    )
    parser.add_argument(
        "--skip-compress",
        action="store_true",
        help="Skip compression; use existing archive (--archive or default name in --outdir).",
    )
    parser.add_argument(
        "--skip-archive",
        action="store_true",
        help="Only compress; do not upload to HSM.",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="Retry HSM archive on failure (pyslk, requires tenacity).",
    )
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    if not data_dir.is_dir():
        print(f"Error: data_dir is not a directory: {data_dir}", file=sys.stderr)
        return 1

    outdir = (args.outdir or data_dir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    if args.archive:
        archive_name = args.archive if args.archive.endswith(".tar.zst") else f"{args.archive}.tar.zst"
    else:
        from datetime import datetime

        archive_name = f"nc_{datetime.now():%Y%m%d_%H%M%S}.tar.zst"
    archive_path = outdir / archive_name

    compress_script = _find_compress_script(args.compress_script and args.compress_script.resolve().parent)

    if not args.skip_compress:
        run_compress(data_dir, archive_path, compress_script, args.overwrite)
    elif not archive_path.is_file():
        print(f"Error: --skip-compress but archive not found: {archive_path}", file=sys.stderr)
        return 1

    if args.skip_archive:
        print(f"Compression done. Archive: {archive_path}")
        return 0

    dst_gns = args.hsm_dest.rstrip("/") + "/"
    try:
        archive_to_hsm(archive_path, dst_gns, args.retry)
    except Exception as e:
        print(f"HSM archival failed: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
