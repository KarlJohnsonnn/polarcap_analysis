#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<'EOF'
Background launcher for spectral waterfall rendering.

Usage:
  scripts/processing_chain/run_spectral_waterfall_bg.sh [CONFIG_PATH] [EXTRA_ARGS...]
  scripts/processing_chain/run_spectral_waterfall_bg.sh [EXTRA_ARGS...]

Notes:
  - If first argument does not start with "--", it is treated as CONFIG_PATH.
  - All extra args are forwarded to run_spectral_waterfall.py.
  - Use env vars WORKERS, PYTHON_BIN, LOG_DIR to customize runtime.

Examples:
  # Frames only with default config
  scripts/processing_chain/run_spectral_waterfall_bg.sh

  # Frames + MP4 with explicit config
  scripts/processing_chain/run_spectral_waterfall_bg.sh \
    notebooks/config/process_budget.yaml --mp4

  # Subset run: experiment/range/station
  scripts/processing_chain/run_spectral_waterfall_bg.sh \
    --exp-ids 1 --range-keys ALLBB --station-ids 0 --kind N

  # Relative mode + MP4
  scripts/processing_chain/run_spectral_waterfall_bg.sh \
    --normalize-mode bin --exp-ids 1 --range-keys ALLBB --mp4

  # Custom workers / python binary
  WORKERS=8 PYTHON_BIN=python scripts/processing_chain/run_spectral_waterfall_bg.sh --mp4
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  show_help
  exit 0
fi

WORKERS="${WORKERS:-4}"
PYTHON_BIN="${PYTHON_BIN:-python}"
LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "$LOG_DIR"

CONFIG_PATH="notebooks/config/process_budget.yaml"
EXTRA_ARGS=()
if [[ $# -gt 0 ]]; then
  if [[ "$1" == --* ]]; then
    EXTRA_ARGS=("$@")
  else
    CONFIG_PATH="$1"
    shift
    EXTRA_ARGS=("$@")
  fi
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/spectral_waterfall_${STAMP}.log"

CMD=(
  "$PYTHON_BIN"
  "scripts/processing_chain/run_spectral_waterfall.py"
  "--config" "$CONFIG_PATH"
  "--workers" "$WORKERS"
  "${EXTRA_ARGS[@]}"
)

nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
PID=$!

echo "Started spectral waterfall renderer in background."
echo "PID: ${PID}"
echo "Log: ${LOG_FILE}"
echo "Monitor: tail -f ${LOG_FILE}"
echo "Command: ${CMD[*]}"
