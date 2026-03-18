#!/usr/bin/env bash
set -euo pipefail

# Sync the maintained local PAMTRA workflow to Levante, build it remotely,
# run the existing smoke test from the remote tree, and optionally process
# one real vertical plume-path file.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_REMOTE_HOST="lev"
DEFAULT_REMOTE_ROOT="/home/b/b382237/code/radar_forward/pamtra"

REMOTE_HOST="${DEFAULT_REMOTE_HOST}"
REMOTE_ROOT="${DEFAULT_REMOTE_ROOT}"
SMOKE_OUT_DIR=""
REAL_INPUT=""
REAL_OUTPUT_DIR=""
SUBMIT_SLURM="F"
SLURM_CPUS=4
SLURM_MEM="16G"
SLURM_TIME="01:00:00"
SLURM_LOG_DIR=""
SLURM_JOB_ID=""

PAMTRA_SRC_DIR="${SCRIPT_DIR}/pamtra"

usage() {
    cat <<'EOF'
Usage:
  enable_remote_levante.sh [remote_host] [remote_root] [smoke_out_dir] [options]

Options:
  --real-input PATH         Run PAMTRA on one real vertical plume-path file after the smoke test.
  --real-output-dir PATH    Output directory for the real PAMTRA run.
  --submit-slurm           Submit the real PAMTRA run via sbatch instead of running it on the login node.
  --slurm-cpus N           CPUs per task for PAMTRA sbatch jobs. Default: 4
  --slurm-mem MEM          Memory request for PAMTRA sbatch jobs. Default: 16G
  --slurm-time HH:MM:SS    Walltime for PAMTRA sbatch jobs. Default: 01:00:00
  --slurm-log-dir PATH     Remote directory for SLURM stdout/stderr. Default: <remote_root>/slurm_logs
  -h, --help                Show this help.
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --real-input)
            REAL_INPUT="${2:?Missing value for --real-input}"
            shift 2
            ;;
        --real-output-dir)
            REAL_OUTPUT_DIR="${2:?Missing value for --real-output-dir}"
            shift 2
            ;;
        --submit-slurm)
            SUBMIT_SLURM="T"
            shift
            ;;
        --slurm-cpus)
            SLURM_CPUS="${2:?Missing value for --slurm-cpus}"
            shift 2
            ;;
        --slurm-mem)
            SLURM_MEM="${2:?Missing value for --slurm-mem}"
            shift 2
            ;;
        --slurm-time)
            SLURM_TIME="${2:?Missing value for --slurm-time}"
            shift 2
            ;;
        --slurm-log-dir)
            SLURM_LOG_DIR="${2:?Missing value for --slurm-log-dir}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

if (( ${#POSITIONAL[@]} > 0 )); then
    REMOTE_HOST="${POSITIONAL[0]}"
fi
if (( ${#POSITIONAL[@]} > 1 )); then
    REMOTE_ROOT="${POSITIONAL[1]}"
fi
if (( ${#POSITIONAL[@]} > 2 )); then
    SMOKE_OUT_DIR="${POSITIONAL[2]}"
fi
if (( ${#POSITIONAL[@]} > 3 )); then
    usage >&2
    exit 1
fi

SMOKE_OUT_DIR="${SMOKE_OUT_DIR:-${REMOTE_ROOT}/test_output}"
SLURM_LOG_DIR="${SLURM_LOG_DIR:-${REMOTE_ROOT}/slurm_logs}"

REMOTE_SCRIPTS_DIR="${REMOTE_ROOT}/scripts/pamtra"
REMOTE_UTILS_DIR="${REMOTE_ROOT}/src/utilities"

remote_bash() {
    local cmd="$1"
    ssh "${REMOTE_HOST}" "bash -lc $(printf '%q' "${cmd}")"
}

sync_tree() {
    local src="$1"
    local dst="$2"
    rsync -a \
        --exclude '.git/' \
        --exclude '.venv/' \
        --exclude '.venv*/' \
        --exclude '__pycache__/' \
        --exclude '.DS_Store' \
        --exclude '._*' \
        "${src}" "${REMOTE_HOST}:${dst}"
}

[[ -d "${PAMTRA_SRC_DIR}" ]] || {
    echo "Missing vendored PAMTRA tree at ${PAMTRA_SRC_DIR}" >&2
    exit 1
}

[[ -f "${SCRIPT_DIR}/install_remote_levante.sh" ]] || {
    echo "Missing install_remote_levante.sh in ${SCRIPT_DIR}" >&2
    exit 1
}

[[ -f "${SCRIPT_DIR}/smoke_test_remote_levante.sh" ]] || {
    echo "Missing smoke_test_remote_levante.sh in ${SCRIPT_DIR}" >&2
    exit 1
}

[[ -f "${SCRIPT_DIR}/run_pamtra_plume_paths.py" ]] || {
    echo "Missing run_pamtra_plume_paths.py in ${SCRIPT_DIR}" >&2
    exit 1
}

[[ -f "${SCRIPT_DIR}/run_remote_levante.sh" ]] || {
    echo "Missing run_remote_levante.sh in ${SCRIPT_DIR}" >&2
    exit 1
}

[[ -f "${REPO_ROOT}/src/utilities/pamtra_forward.py" ]] || {
    echo "Missing src/utilities/pamtra_forward.py in ${REPO_ROOT}" >&2
    exit 1
}

if [[ "${SUBMIT_SLURM}" == "T" && -z "${REAL_INPUT}" ]]; then
    echo "--submit-slurm requires --real-input PATH" >&2
    exit 1
fi

echo "[1/4] Prepare remote directories on ${REMOTE_HOST}:${REMOTE_ROOT}"
remote_bash "mkdir -p '${REMOTE_ROOT}' '${REMOTE_SCRIPTS_DIR}' '${REMOTE_UTILS_DIR}' '${SLURM_LOG_DIR}'; find '${REMOTE_ROOT}' -name '._*' -delete 2>/dev/null || true"

echo "[2/4] Sync vendored PAMTRA source tree"
sync_tree "${PAMTRA_SRC_DIR}/" "${REMOTE_ROOT}/"

echo "[3/4] Sync maintained wrapper files"
sync_tree "${SCRIPT_DIR}/README.md" "${REMOTE_SCRIPTS_DIR}/README.md"
sync_tree "${SCRIPT_DIR}/install_remote_levante.sh" "${REMOTE_SCRIPTS_DIR}/install_remote_levante.sh"
sync_tree "${SCRIPT_DIR}/smoke_test_remote_levante.sh" "${REMOTE_SCRIPTS_DIR}/smoke_test_remote_levante.sh"
sync_tree "${SCRIPT_DIR}/run_pamtra_plume_paths.py" "${REMOTE_SCRIPTS_DIR}/run_pamtra_plume_paths.py"
sync_tree "${SCRIPT_DIR}/run_remote_levante.sh" "${REMOTE_SCRIPTS_DIR}/run_remote_levante.sh"
sync_tree "${REPO_ROOT}/src/utilities/pamtra_forward.py" "${REMOTE_UTILS_DIR}/pamtra_forward.py"
remote_bash "chmod +x '${REMOTE_SCRIPTS_DIR}/install_remote_levante.sh' '${REMOTE_SCRIPTS_DIR}/smoke_test_remote_levante.sh' '${REMOTE_SCRIPTS_DIR}/run_remote_levante.sh'"

echo "[4/4] Build remotely and run smoke test"
remote_bash "cd '${REMOTE_ROOT}' && '${REMOTE_SCRIPTS_DIR}/install_remote_levante.sh' '${REMOTE_ROOT}'"
remote_bash "cd '${REMOTE_ROOT}' && '${REMOTE_SCRIPTS_DIR}/smoke_test_remote_levante.sh' '${REMOTE_ROOT}' '${SMOKE_OUT_DIR}'"

if [[ -n "${REAL_INPUT}" ]]; then
    real_args=("${REMOTE_ROOT}" "${REAL_INPUT}" "--overwrite")
    if [[ -n "${REAL_OUTPUT_DIR}" ]]; then
        real_args+=("--output-dir" "${REAL_OUTPUT_DIR}")
    fi
    printf -v real_args_quoted "%q " "${real_args[@]}"

    if [[ "${SUBMIT_SLURM}" == "T" ]]; then
        echo "[5/5] Submit PAMTRA SLURM job for real plume-path input"
        printf -v wrap_cmd "%q " "${REMOTE_SCRIPTS_DIR}/run_remote_levante.sh" "${real_args[@]}"
        SLURM_JOB_ID="$(
            remote_bash "set -euo pipefail; sbatch --parsable --job-name=pamtra_remote --partition=compute --account=bb1262 --nodes=1 --ntasks=1 --cpus-per-task='${SLURM_CPUS}' --mem='${SLURM_MEM}' --time='${SLURM_TIME}' --output='${SLURM_LOG_DIR}/%j.out' --error='${SLURM_LOG_DIR}/%j.err' --wrap $(printf '%q' "${wrap_cmd}")"
        )"
    else
        echo "[5/5] Run PAMTRA on real plume-path input"
        remote_bash "set -euo pipefail; '${REMOTE_SCRIPTS_DIR}/run_remote_levante.sh' ${real_args_quoted}"
    fi
fi

echo
echo "Remote PAMTRA is ready on ${REMOTE_HOST}:${REMOTE_ROOT}"
echo "Smoke test output: ${REMOTE_HOST}:${SMOKE_OUT_DIR}"
if [[ -n "${REAL_INPUT}" ]]; then
    if [[ -n "${REAL_OUTPUT_DIR}" ]]; then
        echo "Real input output: ${REMOTE_HOST}:${REAL_OUTPUT_DIR}"
    else
        echo "Real input output: next to ${REMOTE_HOST}:${REAL_INPUT}"
    fi
fi
if [[ -n "${SLURM_JOB_ID}" ]]; then
    echo "SLURM job: ${REMOTE_HOST}:${SLURM_JOB_ID}"
    echo "SLURM logs: ${REMOTE_HOST}:${SLURM_LOG_DIR}"
fi
