# NetCDF compression and HSM archive

- `compress.sh`: list, compress, or extract `M_*.nc` and `3D_*.nc`
- `archive.sh`: list `*.nc.zst` files or archive one file with `slk archive -vv`
- `run_compess_and_archive.sh`: build manifests and submit both Slurm arrays inline

## Manual use

```bash
./compress.sh list /path/to/ensemble_output
./compress.sh compress /path/to/M_file.nc /scratch/compressed_run
./compress.sh extract /scratch/compressed_run /tmp/unpacked

./archive.sh list /scratch/compressed_run
./archive.sh archive /scratch/compressed_run/M_file.nc.zst /arch/bb1262/cosmo-specs/ensemble_output/run01
```

## Full Slurm workflow

Before archiving on Levante, run `module load slk` and `slk login`. DKRZ usage notes are here:

- [Archivals to tape](https://docs.dkrz.de/doc/datastorage/hsm/archivals.html#)
- [Getting Started with slk](https://docs.dkrz.de/doc/datastorage/hsm/getting_started.html)

```bash
./run_compess_and_archive.sh /path/to/ensemble_output /scratch/compressed_run run01
OVERWRITE=1 RETRY=1 COMPRESS_JOBS=8 ARCHIVE_JOBS=2 ./run_compess_and_archive.sh /path/to/ensemble_output
```

The runner prints:

- `SOURCE_MANIFEST=...`
- `COMPRESSED_MANIFEST=...`
- `COMPRESS_JOB_ID=...`
- `ARCHIVE_JOB_ID=...`

Defaults:

- compressed files: `/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output/<run_name>`
- HSM namespace: `/arch/bb1262/cosmo-specs/ensemble_output/<run_name>`
