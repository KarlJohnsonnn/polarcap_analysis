#!/usr/bin/env python3
"""Produce radar-facing proxy metrics (if available). Thin driver."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

def main() -> None:
    print("Placeholder: implement when radar/PAMTRA pipeline or proxy is available.")
    sys.exit(0)


if __name__ == "__main__":
    main()
