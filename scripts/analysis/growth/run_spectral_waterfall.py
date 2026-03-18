#!/usr/bin/env python3
"""Preferred analysis entry point for the spectral waterfall renderer."""
from __future__ import annotations

import runpy
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
LEGACY_ENTRYPOINT = REPO_ROOT / "scripts" / "processing_chain" / "run_spectral_waterfall.py"


def main() -> None:
    runpy.run_path(str(LEGACY_ENTRYPOINT), run_name="__main__")


if __name__ == "__main__":
    main()
