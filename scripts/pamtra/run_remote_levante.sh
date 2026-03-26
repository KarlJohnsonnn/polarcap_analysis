#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:?Usage: run_remote_levante.sh ROOT_DIR INPUT... [--output-dir DIR] [extra args]}"
shift

if [[ $# -lt 1 ]]; then
    echo "Usage: run_remote_levante.sh ROOT_DIR INPUT... [--output-dir DIR] [extra args]" >&2
    exit 1
fi

module load python3/2023.01-gcc-11.2.0
spack load /bcn7mbu
spack load /l2ulgpu
spack load /fnfhvr6
spack load /tpmfvw

unset PYTHONPATH
export PYTHONNOUSERSITE=1
export PAMTRA_DATADIR="${ROOT_DIR}/pamtra_data"
export OPENBLAS_NUM_THREADS=1
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/sw/spack-levante/openblas-0.3.18-tpmfvw/lib/:/sw/spack-levante/fftw-3.3.10-fnfhvr/lib/"

"${ROOT_DIR}/.venv/bin/python" 
    "${ROOT_DIR}/scripts/pamtra/run_pamtra_plume_paths.py" \
    "$@"
