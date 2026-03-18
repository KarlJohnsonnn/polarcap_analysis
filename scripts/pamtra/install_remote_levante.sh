#!/usr/bin/env bash
set -euo pipefail

# Install PAMTRA on Levante without editing shell startup files.

ROOT_DIR="${1:-$HOME/code/radar_forward/pamtra}"
DATA_DIR="${ROOT_DIR}/pamtra_data"
VENV_DIR="${ROOT_DIR}/.venv"

module load python3/2023.01-gcc-11.2.0
spack load /bcn7mbu
spack load /l2ulgpu
spack load /fnfhvr6
spack load /tpmfvw

export PYTHONNOUSERSITE=1
export PAMTRA_DATADIR="${DATA_DIR}"
export OPENBLAS_NUM_THREADS=1
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/sw/spack-levante/openblas-0.3.18-tpmfvw/lib/:/sw/spack-levante/fftw-3.3.10-fnfhvr/lib/"

cd "${ROOT_DIR}"
python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip "setuptools<70" wheel
"${VENV_DIR}/bin/pip" install "numpy<2" scipy netCDF4 xarray pandas matplotlib

mkdir -p "${DATA_DIR}"
if [[ ! -f "${DATA_DIR}/.data_ready" ]]; then
    wget -q -O data.tar.bz2 https://uni-koeln.sciebo.de/s/As5fqDdPCOx4JbS/download
    wget -q -O example_data.tar.bz2 https://uni-koeln.sciebo.de/s/28700CuFssmin8q/download
    tar -xjf example_data.tar.bz2 -C "${DATA_DIR}"
    tar -xjf data.tar.bz2 -C "${DATA_DIR}"
    rm -f example_data.tar.bz2 data.tar.bz2
    touch "${DATA_DIR}/.data_ready"
fi

sed "s/-llapack//g" Makefile > Makefile.levante
sed -i "s%-lblas% -L/sw/spack-levante/openblas-0.3.18-tpmfvw/lib/ -L/sw/spack-levante/fftw-3.3.10-fnfhvr/lib/ -lopenblas%g" Makefile.levante

PYINSTDIR="$("${VENV_DIR}/bin/python" -c 'import site; print(site.getsitepackages()[0] + "/")')"
make -f Makefile.levante clean all F2PY="${VENV_DIR}/bin/f2py" PYINSTDIR="${PYINSTDIR}"
make -f Makefile.levante pyinstall F2PY="${VENV_DIR}/bin/f2py" PYINSTDIR="${PYINSTDIR}"

unset PYTHONPATH
"${VENV_DIR}/bin/python" -c "import pyPamtra; print(pyPamtra.__file__)"
