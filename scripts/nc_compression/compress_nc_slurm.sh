#!/usr/bin/env bash
# SLURM job script to run compress_nc.sh on Levante (or similar).
# Submit from the server where data lives; use for large datasets.
#
# Usage (submit from scripts/nc_compression/ so compress_nc.sh is found):
#   cd scripts/nc_compression && sbatch compress_nc_slurm.sh [dir] [archive] [outdir]
#   sbatch compress_nc_slurm.sh /path/to/ensemble_output/ensemble_01 nc_run01.tar.zst
#   sbatch compress_nc_slurm.sh /path/to/ensemble_output run01.tar.zst /path/to/archive_dir
#   sbatch compress_nc_slurm.sh .    # default dir=., archive=nc_YYYYMMDD_HHMMSS.tar.zst
#
# Options (edit below or override via env before sbatch):
#   COMPRESS_NC_DIR        data dir (default: .)
#   COMPRESS_NC_ARCHIVE    archive name (default: auto)
#   COMPRESS_NC_OUTDIR     output dir for archive (default: data dir)
#   COMPRESS_NC_OVERWRITE  1 to overwrite existing archive

#-----------------------------------------------------------------------------
#SBATCH --job-name=compress_nc
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=08:00:00
#SBATCH --account=bb1262
#SBATCH --output=log_compress_nc%j.out
#SBATCH --error=log_compress_nc%j.err
#SBATCH --mem=128G

#-----------------------------------------------------------------------------
# SLURM copies the script to spool; use SLURM_SUBMIT_DIR (cwd when sbatch ran) to find compress_nc.sh.
# Submit from scripts/nc_compression/:  cd scripts/nc_compression && sbatch compress_nc_slurm.sh ...
SCRIPT_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
DATA_DIR="${COMPRESS_NC_DIR:-${1:-.}}"
ARCHIVE="${COMPRESS_NC_ARCHIVE:-${2:-}}"
OUTDIR="${COMPRESS_NC_OUTDIR:-${3:-}}"
[[ -n "${COMPRESS_NC_OVERWRITE:-}" ]] && OVERWRITE=1
export OVERWRITE
# Progress bar: update every 1s for large data (avoids slow % on big archives)
export PV_INTERVAL=1

mkdir -p "$SCRIPT_DIR/logs_compress_nc"
DATA_DIR_ABS="$(cd "$DATA_DIR" && pwd)" || { echo "Cannot cd to $DATA_DIR" >&2; exit 1; }

ARCHIVE_ARG="$ARCHIVE"
OUTDIR_ABS=""
if [[ -n "$OUTDIR" ]]; then
    mkdir -p "$OUTDIR"
    OUTDIR_ABS="$(cd "$OUTDIR" && pwd)" || { echo "Cannot cd to $OUTDIR" >&2; exit 1; }
    ARCHIVE_NAME="${ARCHIVE:-nc_$(date +%Y%m%d_%H%M%S).tar.zst}"
    ARCHIVE_ARG="$OUTDIR_ABS/$(basename "$ARCHIVE_NAME")"
fi

echo "Compress NC SLURM job: $(date)"
echo "  Data dir:   $DATA_DIR_ABS"
echo "  Archive:    ${ARCHIVE_ARG:-<auto in data dir>}"
[[ -n "$OUTDIR_ABS" ]] && echo "  Output dir: $OUTDIR_ABS"
echo "  Job ID:     ${SLURM_JOB_ID:-N/A}"
echo "  CPUs:      ${SLURM_CPUS_PER_TASK:-1}"
echo "---"

"$SCRIPT_DIR/compress_nc.sh" ${OVERWRITE:+--overwrite} compress "$DATA_DIR" "$ARCHIVE_ARG"

echo "---"
echo "Done: $(date)"
