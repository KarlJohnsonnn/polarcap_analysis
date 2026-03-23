#!/usr/bin/env python3
"""Preferred entry: spectral waterfall PNG frames and optional MP4 (see ``--help``)."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC = REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Manuscript-facing caption; keep this wording aligned with gallery and registry copies.
FIGURE_CAPTION = """Animated ridge-following spectral budget for the seeded plume. Each frame
combines the liquid and frozen particle size distributions with diameter-resolved microphysical tendencies at
the selected stations, either in number-concentration space (N) or mass-concentration space (Q). The
animation separates the processes that first generate excess ice shortly after seeding from those that later
grow, redistribute, and remove condensate across the spectrum."""

from utilities.spectral_waterfall import main  # noqa: E402

if __name__ == "__main__":
    main()
