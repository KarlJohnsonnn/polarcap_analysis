#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
CALL_DIR="$(pwd -P)"
GRAVEYARD="${GRAVEYARD:-/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output}"
HSM_ROOT="${HSM_ROOT:-/arch/bb1262/cosmo-specs/ensemble_output}"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [source_dir] <compressed_name>

Env:
  GRAVEYARD=${GRAVEYARD}
  HSM_ROOT=${HSM_ROOT}
  LOG_DIR
  OVERWRITE=1, RETRY=1, RETRY_DELAY=60
  COMPRESS_JOBS=8, ARCHIVE_JOBS=2, PV_INTERVAL=1

Example:
  $(basename "$0") ./cs-eriswil__20260318_153631 cs-eriswil__20260318_153631.tar.zst
EOF
}

dir_abs() { cd "$1" && pwd -P; }
line_count() { awk 'END{print NR+0}' "$1"; }
run_name_from_archive() {
    local name="$1"
    name="${name%.tar.zst}"
    name="${name%.zst}"
    printf '%s\n' "$name"
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }
[[ $# -eq 2 ]] || { usage; exit 1; }

source_dir="$1"
source_dir="$(dir_abs "$source_dir")"

compressed_name="$2"
run_name="$(run_name_from_archive "$compressed_name")"
[[ -n "$run_name" ]] || { echo "Could not derive run name from: $compressed_name" >&2; exit 1; }

compressed_dir="${GRAVEYARD%/}/${run_name}"
mkdir -p "$compressed_dir"
compressed_dir="$(dir_abs "$compressed_dir")"

LOG_DIR="${LOG_DIR:-$CALL_DIR/.slurm/${run_name}_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$LOG_DIR"

source_manifest="$LOG_DIR/source.txt"
compressed_manifest="$LOG_DIR/compressed.txt"
"$SCRIPT_DIR/compress.sh" list "$source_dir" > "$source_manifest"
file_count="$(line_count "$source_manifest")"
[[ "$file_count" -gt 0 ]] || { echo "No M_*.nc or 3D_*.nc files found in $source_dir" >&2; exit 1; }
while IFS= read -r file; do printf '%s/%s.zst\n' "$compressed_dir" "$(basename "$file")"; done < "$source_manifest" > "$compressed_manifest"

hsm_namespace="${HSM_ROOT%/}/${run_name}"

compress_job_id="$(
    sbatch --parsable \
        --job-name=compress_nc \
        --partition=compute \
        --nodes=1 \
        --ntasks=1 \
        --cpus-per-task=8 \
        --time=02:00:00 \
        --account=bb1262 \
        --mem=16G \
        --output="$LOG_DIR/compress_%A_%a.out" \
        --error="$LOG_DIR/compress_%A_%a.err" \
        --array="0-$((file_count - 1))%${COMPRESS_JOBS:-8}" \
        --export="ALL,SCRIPT_DIR=$SCRIPT_DIR,MANIFEST=$source_manifest,OUTDIR=$compressed_dir,OVERWRITE=${OVERWRITE:-0},PV_INTERVAL=${PV_INTERVAL:-1}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
file="$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$MANIFEST")"
cmd=("$SCRIPT_DIR/compress.sh")
[[ "$OVERWRITE" == 1 ]] && cmd+=(--overwrite)
cmd+=(compress "$file" "$OUTDIR")
"${cmd[@]}"
EOF
)"
compress_job_id="${compress_job_id%%;*}"

archive_job_id="$(
    sbatch --parsable \
        --dependency="afterok:$compress_job_id" \
        --job-name=archive_hsm \
        --partition=shared \
        --nodes=1 \
        --ntasks=1 \
        --cpus-per-task=1 \
        --time=04:00:00 \
        --account=bb1262 \
        --mem=8G \
        --output="$LOG_DIR/archive_%A_%a.out" \
        --error="$LOG_DIR/archive_%A_%a.err" \
        --array="0-$((file_count - 1))%${ARCHIVE_JOBS:-2}" \
        --export="ALL,SCRIPT_DIR=$SCRIPT_DIR,MANIFEST=$compressed_manifest,HSM_NAMESPACE=$hsm_namespace,RETRY=${RETRY:-0},RETRY_DELAY=${RETRY_DELAY:-60}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
file="$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$MANIFEST")"
RETRY="$RETRY" RETRY_DELAY="$RETRY_DELAY" "$SCRIPT_DIR/archive.sh" archive "$file" "$HSM_NAMESPACE"
EOF
)"
archive_job_id="${archive_job_id%%;*}"

printf 'SOURCE_DIR=%s\n' "$source_dir"
printf 'COMPRESSED_NAME=%s\n' "$compressed_name"
printf 'RUN_NAME=%s\n' "$run_name"
printf 'COMPRESSED_DIR=%s\n' "$compressed_dir"
printf 'HSM_NAMESPACE=%s\n' "$hsm_namespace"
printf 'SOURCE_MANIFEST=%s\n' "$source_manifest"
printf 'COMPRESSED_MANIFEST=%s\n' "$compressed_manifest"
printf 'COMPRESS_JOB_ID=%s\n' "$compress_job_id"
printf 'ARCHIVE_JOB_ID=%s\n' "$archive_job_id"
