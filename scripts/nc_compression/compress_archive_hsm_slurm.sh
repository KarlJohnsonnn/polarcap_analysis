#!/usr/bin/env bash
# SLURM job: compress M_*.nc / 3D_*.nc to tar.zst and archive to DKRZ HSM (tape) via pyslk.
# Run on Levante (or similar) where slk/HSM and data are available.
#
# Usage (submit from scripts/nc_compression/):
#   cd scripts/nc_compression && sbatch compress_archive_hsm_slurm.sh <data_dir> <hsm_dest> [archive_name]
#   sbatch compress_archive_hsm_slurm.sh /path/to/ensemble_01 /arch/bb1262/polarcap/run01 nc_run01.tar.zst
#   sbatch compress_archive_hsm_slurm.sh /path/to/run /arch/bb1262/polarcap/run   # archive name auto
#
# Env overrides:
#   COMPRESS_NC_DIR      data dir (default: $1)
#   HSM_DEST             HSM namespace (default: $2), e.g. /arch/<project>/polarcap/run01/
#   COMPRESS_NC_ARCHIVE  archive basename (default: $3 or auto)
#   COMPRESS_NC_OUTDIR   dir for .tar.zst before upload (default: data dir)
#   HSM_SKIP_COMPRESS    set to 1 to use existing .tar.zst and only upload
#   HSM_SKIP_ARCHIVE     set to 1 to only compress, do not upload to HSM
#   HSM_OVERWRITE        1 to overwrite existing local archive
#   HSM_RETRY            1 to retry HSM upload on failure (pyslk)

#-----------------------------------------------------------------------------
#SBATCH --job-name=compress_archive_hsm
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00
#SBATCH --account=bb1262
#SBATCH --output=log_compress_archive_hsm_%j.out
#SBATCH --error=log_compress_archive_hsm_%j.err
#SBATCH --mem=128G

#-----------------------------------------------------------------------------
SCRIPT_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
DATA_DIR="${COMPRESS_NC_DIR:-${1:?Usage: sbatch compress_archive_hsm_slurm.sh <data_dir> <hsm_dest> [archive_name]}}"
HSM_DEST="${HSM_DEST:-${2:?Provide HSM destination namespace as 2nd arg or HSM_DEST}}"
ARCHIVE="${COMPRESS_NC_ARCHIVE:-${3:-}}"
OUTDIR="${COMPRESS_NC_OUTDIR:-}"

# Load slk (provides slk, slk_helpers; pyslk may be in the same env or separate pip)
if command -v module &>/dev/null; then
    module load slk 2>/dev/null || true
fi

mkdir -p "$SCRIPT_DIR/logs_compress_nc"
cd "$SCRIPT_DIR" || exit 1

ARGS=("$DATA_DIR" "$HSM_DEST")
[[ -n "$ARCHIVE" ]] && ARGS+=(--archive "$ARCHIVE")
[[ -n "$OUTDIR" ]] && ARGS+=(--outdir "$OUTDIR")
[[ -n "${HSM_OVERWRITE:-}" ]] && ARGS+=(--overwrite)
[[ -n "${HSM_SKIP_COMPRESS:-}" ]] && ARGS+=(--skip-compress)
[[ -n "${HSM_SKIP_ARCHIVE:-}" ]] && ARGS+=(--skip-archive)
[[ -n "${HSM_RETRY:-}" ]] && ARGS+=(--retry)

echo "Compress + HSM archive job: $(date)"
echo "  Data dir:   $DATA_DIR"
echo "  HSM dest:   $HSM_DEST"
echo "  Archive:    ${ARCHIVE:-<auto>}"
echo "  Job ID:     ${SLURM_JOB_ID:-N/A}"
echo "---"

python3 "$SCRIPT_DIR/compress_and_archive_hsm.py" "${ARGS[@]}"
EXIT=$?

echo "---"
echo "Done: $(date) (exit $EXIT)"
exit $EXIT
