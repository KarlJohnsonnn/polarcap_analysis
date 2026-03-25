# NetCDF compression and HSM archive

- `compress.sh`: list/compress/extract `M_*.nc` and `3D_*.nc`
- `archive.sh`: list `*.nc.zst` or archive one file via `slk archive -vv`
- `run_compess_and_archive.sh`: submits compression+archive Slurm arrays
- `archive2tape`: short wrapper command

Before archiving on Levante, run `module load slk` and `slk login`.

DKRZ docs:
- [Archivals to tape](https://docs.dkrz.de/doc/datastorage/hsm/archivals.html#)
- [Getting Started with slk](https://docs.dkrz.de/doc/datastorage/hsm/getting_started.html)

## Enable `archive2tape` from anywhere

```bash
export POLARCAP_ROOT=/path/to/polarcap_analysis
export PATH="$POLARCAP_ROOT/scripts/nc_compression:$PATH"
```

## Command

```bash
archive2tape [source_dir] <compressed_name>
```

Example:

```bash
cd /path/to/ensemble_output
archive2tape ./cs-eriswil__20260318_153631 cs-eriswil__20260318_153631.tar.zst
```

Behavior:

0. Create run dir in `$GRAVEYARD`: `cs-eriswil__YYYYMMDD_HHMMSS`
1. Compress NetCDF files directly into `$GRAVEYARD/<run_name>/`
2. Archive compressed files to `$HSM_ROOT/<run_name>/`

## Key env vars

- `GRAVEYARD` (default: `/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output`)
- `HSM_ROOT` (default: `/arch/bb1262/cosmo-specs/ensemble_output`)
- `COMPRESS_JOBS` (default: `8`)
- `ARCHIVE_JOBS` (default: `2`)
- `OVERWRITE=1`
- `RETRY=1` and `RETRY_DELAY=60`

## Printed outputs

- `RUN_NAME=...`
- `COMPRESSED_DIR=...`
- `HSM_NAMESPACE=...`
- `COMPRESS_JOB_ID=...`
- `ARCHIVE_JOB_ID=...`
