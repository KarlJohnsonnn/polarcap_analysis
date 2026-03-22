#!/usr/bin/env python3
"""Spectral waterfall CLI — thin entry; logic lives in ``utilities.spectral_waterfall``.

Preferred launcher (same behavior): ``python scripts/analysis/growth/run_spectral_waterfall.py``

Examples::

    python scripts/processing_chain/run_spectral_waterfall.py --kind Q --mp4
    python scripts/processing_chain/run_spectral_waterfall.py --mp4-only --kind Q

Full CLI: ``python .../run_spectral_waterfall.py --help``
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC = REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from utilities.spectral_waterfall import main  # noqa: E402

if __name__ == "__main__":
    main()
