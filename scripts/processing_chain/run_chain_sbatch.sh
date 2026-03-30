#!/bin/bash
#SBATCH --job-name=polarcap-chain
#SBATCH --partition=compute
#SBATCH --time=08:00:00
#SBATCH --mem=0
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --exclusive
#SBATCH --account=bb1262
#SBATCH --output=logs/chain-%j.out
#SBATCH --error=logs/chain-%j.err
#SBATCH --mail-user=schimmel@tropos.de

# ----- config for a 200x160 domain -----
# DOMAIN="200x160"
# CS_RUN="cs-eriswil__20260318_153631"
# FLARE_IDX_LIST=(0 1 3 4)
# REF_IDX_LIST=(0 1 1 1)

# ----- config for a 50x42 domain -----
cd "$POLARCAP_ROOT/scripts/processing_chain"
source ~/.bashrc
set +u
conda activate pcpaper_env
set -euo pipefail

DOMAIN="50x40"
CS_RUN="cs-eriswil__20260328_205320"
FLARE_IDX_LIST=(0 1 2 3 0 1 2 3 0 1 2 3)
REF_IDX_LIST=(0 0 0 0 1 1 1 1 2 2 2 2)


DATA_ROOT="$CS_RUNS_DIR"
CONFIG="${POLARCAP_ROOT}/config/processing_chain.yaml"
OUTPUT_ROOT="${DATA_ROOT}/ensemble_output/${CS_RUN}/"
MAX_PARALLEL=1
THREADS_PER_RUN=1

mkdir -p logs

N=${#FLARE_IDX_LIST[@]}
P="$MAX_PARALLEL"
if [ "$P" -gt "$N" ]; then
  P="$N"
fi
fail=0
STATUS_FILE="logs/chain-${SLURM_JOB_ID:-local}.status"
: > "$STATUS_FILE"

pids=()
active=0
launch_stopped=0
for i in $(seq 0 $((N - 1))); do
  f="${FLARE_IDX_LIST[$i]}"; r="${REF_IDX_LIST[$i]}"
  log="logs/chain-${SLURM_JOB_ID:-local}-f${f}_r${r}.log"
  (
    export OMP_NUM_THREADS="$THREADS_PER_RUN" MKL_NUM_THREADS="$THREADS_PER_RUN" OPENBLAS_NUM_THREADS="$THREADS_PER_RUN" NUMEXPR_NUM_THREADS="$THREADS_PER_RUN" 
    python run_chain.py --config "$CONFIG" --root "$DATA_ROOT" --cs-run "$CS_RUN" --domain "$DOMAIN" --flare-idx "$f" --ref-idx "$r" --out "$OUTPUT_ROOT" --skip-tracking
    rc=$?
    echo "${BASHPID}|${f}|${r}|${rc}|${log}" >> "$STATUS_FILE"
    exit "$rc"
  ) >"$log" 2>&1 &
  pid="$!"
  pids+=("$pid")
  active=$((active + 1))

  if [ "$active" -ge "$MAX_PARALLEL" ]; then
    if ! wait -n; then
      fail=1
      launch_stopped=1
      for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          kill "$pid" 2>/dev/null || true
        fi
      done
      break
    fi
    active=$((active - 1))
  fi
done

if [ "$launch_stopped" -eq 0 ]; then
  while [ "$active" -gt 0 ]; do
    if ! wait -n; then
      fail=1
      for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          kill "$pid" 2>/dev/null || true
        fi
      done
      break
    fi
    active=$((active - 1))
  done
fi

for pid in "${pids[@]}"; do wait "$pid" || true; done

if [ ! -s "$STATUS_FILE" ]; then
  if rg -q "oom_kill event|OUT_OF_MEMORY|OOM Killed" "logs/chain-${SLURM_JOB_ID:-local}.err" 2>/dev/null; then
    echo "FAILED: OOM detected before child status write (job=${SLURM_JOB_ID:-local})." >&2
    fail=1
  fi
fi

while IFS='|' read -r pid f r rc log; do
  [ -z "${pid:-}" ] && continue
  if [ "${rc}" -ne 0 ]; then
    echo "FAILED: flare_idx=${f} ref_idx=${r} (pid=${pid}, log: ${log})" >&2
    tail -n 40 "${log}" >&2 || true
    fail=1
  fi
done < "$STATUS_FILE"

exit "$fail"
