# PAMTRA workflow

This directory contains the vendored upstream PAMTRA source tree in `pamtra/`
and the maintained PolarCAP wrapper `run_pamtra_plume_paths.py`.

## Current maintained path

- Install and build PAMTRA on `lev` in `~/code/radar_forward/pamtra`
- Keep PAMTRA in its own virtual environment to avoid the `numpy` / `netCDF4`
  ABI mismatches present in mixed user-site installs
- Run the maintained wrapper on `vertical` LV1 plume-path files
- Write output NetCDF files into a run-local `pamtra/` folder

The first maintained input target is `vertical` plume-path NetCDF output from
`processed/lv1_paths/`. `integrated` plume paths collapse the altitude axis and
are therefore not suitable as a radar-profile default.

## Remote lev environment

The working remote install now lives at:

- code: `/home/b/b382237/code/radar_forward/pamtra`
- data: `/home/b/b382237/code/radar_forward/pamtra/pamtra_data`
- venv: `/home/b/b382237/code/radar_forward/pamtra/.venv`

Use a clean environment when running the wrapper so an older `~/lib/python`
install does not override the dedicated venv:

```bash
module load python3/2023.01-gcc-11.2.0
spack load /bcn7mbu
spack load /l2ulgpu
spack load /fnfhvr6
spack load /tpmfvw
unset PYTHONPATH
export PYTHONNOUSERSITE=1
export PAMTRA_DATADIR=/home/b/b382237/code/radar_forward/pamtra/pamtra_data
export OPENBLAS_NUM_THREADS=1
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/sw/spack-levante/openblas-0.3.18-tpmfvw/lib/:/sw/spack-levante/fftw-3.3.10-fnfhvr/lib/
```

## Build notes

The upstream `install_levante_readmefirst.sh` was used as the reference, but
the maintained install avoids editing shell startup files and keeps dependencies
isolated inside the PAMTRA venv.

The successful remote build sequence was:

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip "setuptools<70" wheel
.venv/bin/pip install "numpy<2" scipy netCDF4 xarray pandas matplotlib
sed "s/-llapack//g" Makefile > Makefile.levante
sed -i "s%-lblas% -L/sw/spack-levante/openblas-0.3.18-tpmfvw/lib/ -L/sw/spack-levante/fftw-3.3.10-fnfhvr/lib/ -lopenblas%g" Makefile.levante
PYINSTDIR=$(.venv/bin/python -c "import site; print(site.getsitepackages()[0] + '/')")
make -f Makefile.levante clean all F2PY=$(pwd)/.venv/bin/f2py PYINSTDIR=$PYINSTDIR
make -f Makefile.levante pyinstall F2PY=$(pwd)/.venv/bin/f2py PYINSTDIR=$PYINSTDIR
```

## Running the wrapper

Example on `lev`:

```bash
/home/b/b382237/code/radar_forward/pamtra/.venv/bin/python \
  /path/to/polarcap_analysis/scripts/pamtra/run_pamtra_plume_paths.py \
  /work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x42x100/ensemble_output/<cs-run-dir>/processed/lv1_paths \
  --output-dir /work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x42x100/ensemble_output/<cs-run-dir>/pamtra
```

For a quick smoke test, add `--limit-times 2`.

## Output format

Each output file is written as:

- `<plume_path_stem>_pamtra.nc`

and contains:

- `Ze(time, height, frequency)`
- `radar_moments(time, height, frequency, moment)`
- `radar_spectra(time, height, frequency, velocity_bin)`
- `radar_velocity(frequency, velocity_bin)`
- convenience moment variables:
  `mean_doppler_velocity`, `spectrum_width`, `skewness`, `kurtosis`
