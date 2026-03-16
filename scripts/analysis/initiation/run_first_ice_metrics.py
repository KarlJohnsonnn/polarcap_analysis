#!/usr/bin/env python3
"""Produce first-ice onset and initiation process metrics. Thin driver: uses process_budget_data, process_rates."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
# Output: first_ice_onset_metrics.csv, initiation_process_fractions.csv

def main() -> None:
    print("Placeholder: implement using load_process_budget_data and initiation-pathway aggregation.")
    sys.exit(0)


if __name__ == "__main__":
    main()
