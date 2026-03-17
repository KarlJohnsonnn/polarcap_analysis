#!/usr/bin/env bash
set -euo pipefail

# Run from repo root so relative paths to script and data work
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Mass concentration (Q)
python "$SCRIPT_DIR/run_spectral_waterfall.py" \
    --config "$REPO_ROOT/scripts/processing_chain/config/process_budget_NQall.yaml" \
    --kind Q \
    --linthresh-w 1e-8 \
    --linthresh-f 1e-8 \
    --xlim-w=1e-1,4e3 \
    --xlim-f=1e-1,4e3 \
    --ylim-w=-1e-2,1e-2 \
    --ylim-f=-1e-2,1e-2 \
    --psd-ylim-w=1e-6,1e0 \
    --psd-ylim-f=1e-6,1e0 \
    --mp4 &


# Number concentration (N)
python "$SCRIPT_DIR/run_spectral_waterfall.py" \
    --config "$REPO_ROOT/scripts/processing_chain/config/process_budget_NQall.yaml" \
    --kind N \
    --linthresh-w 1e-2 \
    --linthresh-f 1e-2 \
    --xlim-w=1e-1,4e3 \
    --xlim-f=1e-1,4e3 \
    --ylim-w=-1e6,1e6 \
    --ylim-f=-1e6,1e6 \
    --psd-ylim-w=1e1,1e9 \
    --psd-ylim-f=1e1,1e9 \
    --mp4 &


wait
