#!/usr/bin/env python3
"""Produce figure inventory linking paper figures to script and output path."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
# Output: data/registry/figure_inventory.csv

def main() -> None:
    print("Placeholder: list figure label, script, config, output path for article_draft.")
    sys.exit(0)


if __name__ == "__main__":
    main()
