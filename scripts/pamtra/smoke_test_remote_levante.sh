#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-$HOME/code/radar_forward/pamtra}"
OUT_DIR="${2:-$ROOT_DIR/test_output}"

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

mkdir -p "${ROOT_DIR}/testdata" "${OUT_DIR}"

"${ROOT_DIR}/.venv/bin/python" - <<'PY'
from pathlib import Path

import numpy as np
import xarray as xr

root = Path.home() / "code" / "radar_forward" / "pamtra" / "testdata"
root.mkdir(parents=True, exist_ok=True)
t = np.array(["2023-01-25T10:30:00", "2023-01-25T10:30:10"], dtype="datetime64[s]")
z = np.array([1000.0, 1100.0, 1200.0])
d = np.array([20.0, 50.0, 100.0])
de = np.array([10.0, 35.0, 75.0, 125.0])
shape = (t.size, z.size, d.size)
ds = xr.Dataset(
    {
        "temperature": (("time", "altitude"), np.full((t.size, z.size), 268.0)),
        "nw": (("time", "altitude", "diameter"), np.full(shape, 0.1)),
        "nf": (("time", "altitude", "diameter"), np.full(shape, 5.0)),
    },
    coords={
        "time": t,
        "altitude": z,
        "diameter": d,
        "diameter_edges": ("diameter_edges", de),
    },
    attrs={"kind": "vertical"},
)
ds["nw"].attrs["units"] = "cm-3"
ds["nf"].attrs["units"] = "L-1"
path = root / "data_smoke_vertical_plume_path_nf_cell0.nc"
ds.to_netcdf(path)
print(path)
PY

"${ROOT_DIR}/.venv/bin/python" \
    "${ROOT_DIR}/scripts/pamtra/run_pamtra_plume_paths.py" \
    "${ROOT_DIR}/testdata/data_smoke_vertical_plume_path_nf_cell0.nc" \
    --output-dir "${OUT_DIR}" \
    --limit-times 2 \
    --overwrite

ls -la "${OUT_DIR}"
