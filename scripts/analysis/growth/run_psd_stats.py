#!/usr/bin/env python3
"""Produce PSD statistics table (e.g. figure13-style). Thin driver."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
# See notebooks/plume_path output/tables/figure13_psd_stats_*.tex

def main() -> None:
    print("Placeholder: promote PSD stats from notebooks/plume_path or processing_chain outputs.")
    sys.exit(0)


if __name__ == "__main__":
    main()
