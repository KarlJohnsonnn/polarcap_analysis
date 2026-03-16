#!/usr/bin/env python3
"""Produce forcing/setup summary table and figures. Thin driver: reads data/registry, calls src/utilities."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
# Reads experiment_registry.csv; optional: model_helpers for flare emission. Output: data/registry/forcing_summary.csv

def main() -> None:
    print("Placeholder: implement forcing summary using experiment_registry and src/utilities/model_helpers.")
    sys.exit(0)


if __name__ == "__main__":
    main()
