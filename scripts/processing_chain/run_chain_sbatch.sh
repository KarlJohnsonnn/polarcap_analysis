#!/bin/bash
#SBATCH --job-name=polarcap-chain
#SBATCH --partition=compute
#SBATCH --time=04:00:00
#SBATCH --mem=0
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --exclusive
#SBATCH --account=bb1262
#SBATCH --output=logs/chain-%j.out
#SBATCH --error=logs/chain-%j.err

# ----- config for a 200x160 domain -----
# DOMAIN="200x160"
# CS_RUN="cs-eriswil__20260318_153631"
# FLARE_IDX_LIST=(0 1 3 4)
# REF_IDX_LIST=(0 1 1 1)

# ----- config for a 50x42 domain -----
DOMAIN="50x42"
CS_RUN="cs-eriswil__20260313_111441"
FLARE_IDX_LIST=(0 1)
REF_IDX_LIST=(0 1)


DATA_ROOT="$CS_RUNS_DIR"
CONFIG="${POLARCAP_ROOT}/config/processing_chain.yaml"
OUTPUT_ROOT="${POLARCAP_ROOT}/data/processed"
DEBUG_LOG="${POLARCAP_ROOT}/.cursor/debug-f85260.log"
export DEBUG_LOG
MAX_PARALLEL=1
THREADS_PER_RUN=1

cd "$POLARCAP_ROOT/scripts/processing_chain"
source ~/.bashrc
set +u
conda activate pcpaper_env
set -euo pipefail

mkdir -p logs

N=${#FLARE_IDX_LIST[@]}
P="$MAX_PARALLEL"
if [ "$P" -gt "$N" ]; then
  P="$N"
fi
T=$(( (${SLURM_CPUS_PER_TASK:-1} + P - 1) / P ))
fail=0
STATUS_FILE="logs/chain-${SLURM_JOB_ID:-local}.status"
: > "$STATUS_FILE"

dbg_log() {
  local hypothesis="$1" location="$2" message="$3" data="$4"
  python - "$hypothesis" "$location" "$message" "$data" "$DEBUG_LOG" <<'PY'
import json
import sys
import time
payload = {
    "sessionId": "f85260",
    "runId": __import__("os").environ.get("SLURM_JOB_ID", "local"),
    "hypothesisId": sys.argv[1],
    "location": sys.argv[2],
    "message": sys.argv[3],
    "data": json.loads(sys.argv[4]),
    "timestamp": int(time.time() * 1000),
}
with open(sys.argv[5], "a", encoding="utf-8") as f:
    f.write(json.dumps(payload) + "\n")
PY
}

#region agent log
dbg_log "H0" "run_chain_sbatch.sh:setup" "batch_start" "{\"n_runs\":${N},\"threads_per_run\":${THREADS_PER_RUN},\"threads_ceiling\":${T},\"max_parallel\":${MAX_PARALLEL},\"effective_parallel\":${P},\"domain\":\"${DOMAIN}\",\"cs_run\":\"${CS_RUN}\"}"
#endregion

pids=()
active=0
launch_stopped=0
for i in $(seq 0 $((N - 1))); do
  f="${FLARE_IDX_LIST[$i]}"; r="${REF_IDX_LIST[$i]}"
  log="logs/chain-${SLURM_JOB_ID:-local}-f${f}_r${r}.log"
  (
    export OMP_NUM_THREADS="$THREADS_PER_RUN" MKL_NUM_THREADS="$THREADS_PER_RUN" OPENBLAS_NUM_THREADS="$THREADS_PER_RUN" NUMEXPR_NUM_THREADS="$THREADS_PER_RUN"
    python run_chain.py --config "$CONFIG" --root "$DATA_ROOT" --cs-run "$CS_RUN" --domain "$DOMAIN" --flare-idx "$f" --ref-idx "$r" --out "$OUTPUT_ROOT" --skip-lv3 --skip-meteogram
    rc=$?
    echo "${BASHPID}|${f}|${r}|${rc}|${log}" >> "$STATUS_FILE"
    exit "$rc"
  ) >"$log" 2>&1 &
  pid="$!"
  pids+=("$pid")
  active=$((active + 1))
  #region agent log
  dbg_log "H1" "run_chain_sbatch.sh:launch_loop" "child_started" "{\"pid\":${pid},\"flare_idx\":${f},\"ref_idx\":${r},\"log\":\"${log}\"}"
  #endregion

  if [ "$active" -ge "$MAX_PARALLEL" ]; then
    if ! wait -n; then
      fail=1
      launch_stopped=1
      #region agent log
      dbg_log "H2" "run_chain_sbatch.sh:launch_loop" "failure_during_launch_killing_remaining" "{\"active\":${active}}"
      #endregion
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
      #region agent log
      dbg_log "H2" "run_chain_sbatch.sh:wait_loop" "first_failure_detected_killing_remaining" "{\"active\":${active}}"
      #endregion
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
    #region agent log
    dbg_log "H2" "run_chain_sbatch.sh:status_summary" "oom_detected_no_child_status" "{\"job\":\"${SLURM_JOB_ID:-local}\"}"
    #endregion
    fail=1
  fi
fi

while IFS='|' read -r pid f r rc log; do
  [ -z "${pid:-}" ] && continue
  if [ "${rc}" -ne 0 ]; then
    echo "FAILED: flare_idx=${f} ref_idx=${r} (pid=${pid}, log: ${log})" >&2
    tail -n 40 "${log}" >&2 || true
    #region agent log
    dbg_log "H3" "run_chain_sbatch.sh:status_summary" "child_failed" "{\"pid\":${pid},\"flare_idx\":${f},\"ref_idx\":${r},\"rc\":${rc},\"log\":\"${log}\"}"
    #endregion
    fail=1
  else
    #region agent log
    dbg_log "H4" "run_chain_sbatch.sh:status_summary" "child_succeeded" "{\"pid\":${pid},\"flare_idx\":${f},\"ref_idx\":${r},\"rc\":${rc}}"
    #endregion
  fi
done < "$STATUS_FILE"

#region agent log
if [ "$fail" -ne 0 ]; then
  dbg_log "H0" "run_chain_sbatch.sh:exit" "batch_failed" "{\"status_file\":\"${STATUS_FILE}\"}"
else
  dbg_log "H0" "run_chain_sbatch.sh:exit" "batch_succeeded" "{\"status_file\":\"${STATUS_FILE}\"}"
fi
#endregion

exit "$fail"