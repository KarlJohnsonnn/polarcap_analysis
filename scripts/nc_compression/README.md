# NetCDF compression (tar.zst)

Scripts to compress and extract `M_*.nc` and `3D_*.nc` into a single tar.zst archive (zstd level 9). Useful for large ensemble output on the server before transfer or archival.

## Usage

**Interactive (local or login node):**
```bash
./compress_nc.sh compress /path/to/ensemble_output nc_run.tar.zst
PV_INTERVAL=1 ./compress_nc.sh compress .   # progress updates every 1s

./compress_nc.sh extract my_run.tar.zst .    # extract to current dir
./compress_nc.sh -f extract my_run.tar.zst /tmp/out   # overwrite existing files
```

**SLURM on Levante (queue compression for large datasets):**

Submit from `scripts/nc_compression/` so both `compress_nc.sh` and `compress_nc_slurm.sh` are found:
```bash
cd scripts/nc_compression && sbatch compress_nc_slurm.sh /path/to/ensemble_output nc_run.tar.zst
cd scripts/nc_compression && sbatch compress_nc_slurm.sh .   # dir=., auto archive name
COMPRESS_NC_OUTDIR=/path/to/archive_dir sbatch compress_nc_slurm.sh . my_run.tar.zst
COMPRESS_NC_OVERWRITE=1 sbatch compress_nc_slurm.sh . my_run.tar.zst
```

Logs: `log_compress_nc<jobid>.out`, `log_compress_nc<jobid>.err` (in the directory where you ran `sbatch`). Edit `#SBATCH` in `compress_nc_slurm.sh` for account, time, memory.

## Compress and archive to DKRZ HSM (tape)

To compress model output and upload it to the [DKRZ HSM tape archive](https://docs.dkrz.de/doc/datastorage/hsm/), use the Python script with [pyslk](https://hsm-tools.gitlab-pages.dkrz.de/pyslk/). On Levante load the slk module and ensure a valid token (`slk login`).

**Interactive (after `module load slk`, `slk login`):**
```bash
python3 compress_and_archive_hsm.py /path/to/ensemble_01 /arch/bb1262/polarcap/run01
python3 compress_and_archive_hsm.py /path/to/run /arch/bb1262/polarcap/run --archive nc_run.tar.zst --outdir /tmp
```

**SLURM (compress + archive in one job):**
```bash
cd scripts/nc_compression && sbatch compress_archive_hsm_slurm.sh /path/to/ensemble_01 /arch/bb1262/polarcap/run01 nc_run01.tar.zst
HSM_SKIP_COMPRESS=1 sbatch compress_archive_hsm_slurm.sh /path/to/run /arch/bb1262/polarcap/run nc_existing.tar.zst   # upload only
```

Logs: `log_compress_archive_hsm_<jobid>.out` / `.err`. HSM destination is a namespace path (e.g. `/arch/<project>/polarcap/run01/`). Recommended archive size for tape: 10–200 GB.

## Options

- `-f` / `--overwrite`: overwrite existing archive (compress) or existing files (extract).
- Env: `PV_INTERVAL=1` — pv progress update interval (seconds); use 1 for steady progress on big archives.
- Env (SLURM): `COMPRESS_NC_DIR`, `COMPRESS_NC_ARCHIVE`, `COMPRESS_NC_OUTDIR`, `COMPRESS_NC_OVERWRITE` — override positional args.
- Env (HSM SLURM): `HSM_DEST`, `HSM_SKIP_COMPRESS`, `HSM_SKIP_ARCHIVE`, `HSM_OVERWRITE`, `HSM_RETRY` — see script header.
