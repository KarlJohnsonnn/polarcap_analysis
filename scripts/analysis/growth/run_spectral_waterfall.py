#!/usr/bin/env python3
"""Preferred entry: spectral waterfall PNG frames and optional MP4 (see ``--help``)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC = REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from utilities.spectral_waterfall import main  # noqa: E402

if __name__ == "__main__":
    main()
