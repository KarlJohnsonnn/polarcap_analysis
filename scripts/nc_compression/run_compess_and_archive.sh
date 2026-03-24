#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DOMAIN="${DOMAIN:-200x160x100}"
RUN="${RUN:-cs-eriswil__20260318_153631}"
COMPRESSED_ROOT="${COMPRESSED_ROOT:-/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output}"
HSM_ROOT="${HSM_ROOT:-/arch/bb1262/cosmo-specs/ensemble_output}"

usage() {
    cat <<EOF
Usage:
  run_compess_and_archive.sh [source_dir] [compressed_dir] [namespace_name]

Env:
  SOURCE_DIR, COMPRESSED_DIR, RUN_NAME, WORK_DIR
  OVERWRITE=1, RETRY=1, RETRY_DELAY=60
  COMPRESS_JOBS=8, ARCHIVE_JOBS=2, PV_INTERVAL=1
  DOMAIN=${DOMAIN}, RUN=${RUN}
  COMPRESSED_ROOT=${COMPRESSED_ROOT}
  HSM_ROOT=${HSM_ROOT}
EOF
}

dir_abs() { cd "$1" && pwd -P; }
line_count() { awk 'END{print NR+0}' "$1"; }
default_source_dir() { [[ -n "${CS_RUNS_DIR:-}" ]] && printf '%s\n' "${CS_RUNS_DIR}/RUN_ERISWILL_${DOMAIN}/ensemble_output/${RUN}"; }

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }

source_dir="${1:-${SOURCE_DIR:-$(default_source_dir)}}"
[[ -n "$source_dir" ]] || { usage; exit 1; }
source_dir="$(dir_abs "$source_dir")"

run_name="${3:-${RUN_NAME:-$(basename "$source_dir")}}"
compressed_dir="${2:-${COMPRESSED_DIR:-${COMPRESSED_ROOT%/}/${run_name}}}"
mkdir -p "$compressed_dir"
compressed_dir="$(dir_abs "$compressed_dir")"

work_dir="${WORK_DIR:-$SCRIPT_DIR/.slurm/${run_name}_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$work_dir"

source_manifest="$work_dir/source.txt"
compressed_manifest="$work_dir/compressed.txt"
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
        --output="$work_dir/compress_%A_%a.out" \
        --error="$work_dir/compress_%A_%a.err" \
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
        --output="$work_dir/archive_%A_%a.out" \
        --error="$work_dir/archive_%A_%a.err" \
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
printf 'COMPRESSED_DIR=%s\n' "$compressed_dir"
printf 'HSM_NAMESPACE=%s\n' "$hsm_namespace"
printf 'SOURCE_MANIFEST=%s\n' "$source_manifest"
printf 'COMPRESSED_MANIFEST=%s\n' "$compressed_manifest"
printf 'COMPRESS_JOB_ID=%s\n' "$compress_job_id"
printf 'ARCHIVE_JOB_ID=%s\n' "$archive_job_id"
