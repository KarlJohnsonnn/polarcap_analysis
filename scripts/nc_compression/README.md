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
COMPRESS_NC_OVERWRITE=1 sbatch compress_nc_slurm.sh . my_run.tar.zst
```

Logs: `log_compress_nc<jobid>.out`, `log_compress_nc<jobid>.err` (in the directory where you ran `sbatch`). Edit `#SBATCH` in `compress_nc_slurm.sh` for account, time, memory.

## Options

- `-f` / `--overwrite`: overwrite existing archive (compress) or existing files (extract).
- Env: `PV_INTERVAL=1` — pv progress update interval (seconds); use 1 for steady progress on big archives.
- Env (SLURM): `COMPRESS_NC_DIR`, `COMPRESS_NC_ARCHIVE`, `COMPRESS_NC_OVERWRITE` — override positional args.
