#!/usr/bin/env bash
# Check local plan_pc metadata and remote run data availability. Writes CSV + text summary to data/registry/.
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
PLAN_PC="${PLAN_PC:-$REPO_ROOT/data/plan_pc}"
REMOTE_BASE="${REMOTE_BASE:-/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output}"
REMOTE_SSH="${REMOTE_SSH:-lev}"
# Disable X11 forwarding to avoid "untrusted X11 forwarding setup failed" warnings when no display
SSH_OPTS="-o ForwardX11=no"

OUT_DIR="${OUT_DIR:-$REPO_ROOT/data/registry}"
OUT_CSV="$OUT_DIR/availability_check.csv"
OUT_TXT="$OUT_DIR/availability_check.txt"

mkdir -p "$OUT_DIR"

LOCAL_REQUIRED=(
  "INPUT_FILES"
  "INPUT_FILES/INPUT_ASS_50x40xZ"
  "INPUT_FILES/INPUT_DIA_50x40xZ"
  "INPUT_FILES/INPUT_DYN_50x40xZ"
  "INPUT_FILES/INPUT_IDEAL_50x40xZ"
  "INPUT_FILES/INPUT_IO_50x40xZ"
  "INPUT_FILES/INPUT_ORG_50x40xZ"
  "INPUT_FILES/INPUT_PHY_50x40xZ"
  "run_COSMO-SPECS_levante_v3"
  "start_ensemble_simulation3.sh"
)

{
  echo "== LOCAL plan_pc files =="
  for rel in "${LOCAL_REQUIRED[@]}"; do
    if [[ -e "$PLAN_PC/$rel" ]]; then
      printf "OK      %s\n" "$rel"
    else
      printf "MISSING %s\n" "$rel"
    fi
  done
  echo
} | tee "$OUT_TXT"

shopt -s nullglob
RUNS=()
for f in "$PLAN_PC"/cs-eriswil__*.json; do
  RUNS+=("$(basename "$f" .json)")
done
RUNS=($(printf '%s\n' "${RUNS[@]}" | sort -u))

if [[ ${#RUNS[@]} -eq 0 ]]; then
  echo "No local cs-eriswil__*.json files found in $PLAN_PC" | tee -a "$OUT_TXT"
  exit 1
fi

{
  echo "== LOCAL cs_run json files =="
  for run in "${RUNS[@]}"; do
    printf "OK      %s.json\n" "$run"
  done
  echo
} | tee -a "$OUT_TXT"

echo "run_id,local_json,remote_run_dir,remote_json,meteogram_count,three_d_count,y_count,mp_par,lv1_ready,lv2_ready" > "$OUT_CSV"

{
  echo "== REMOTE availability via: ssh $REMOTE_SSH =="
  echo "run_id|local_json|remote_run_dir|remote_json|M_count|3D_count|Y_count|mp_par|lv1_ready|lv2_ready"
} | tee -a "$OUT_TXT"

for run in "${RUNS[@]}"; do
  remote_line=$(ssh $SSH_OPTS "$REMOTE_SSH" "bash -c '
    shopt -s nullglob
    run_dir=\"$REMOTE_BASE/$run\"
    json_file=\"\$run_dir/$run.json\"
    m=(\"\$run_dir\"/M_??_??_*.nc)
    d3=(\"\$run_dir\"/3D_*.nc)
    y=(\"\$run_dir\"/Y*)
    run_ok=MISSING
    json_ok=MISSING
    mp=MISSING
    [[ -d \"\$run_dir\" ]] && run_ok=OK
    [[ -f \"\$json_file\" ]] && json_ok=OK
    [[ -f \"\$run_dir/mp_par.dat\" ]] && mp=OK
    lv1_ready=NO
    lv2_ready=NO
    [[ \"\$json_ok\" = OK && \${#d3[@]} -gt 0 ]] && lv1_ready=YES
    [[ \"\$json_ok\" = OK && \${#m[@]} -gt 0 ]] && lv2_ready=YES
    echo \"\$run_ok,\$json_ok,\${#m[@]},\${#d3[@]},\${#y[@]},\$mp,\$lv1_ready,\$lv2_ready\"
  '")
  local_json=MISSING
  [[ -f "$PLAN_PC/$run.json" ]] && local_json=OK
  echo "$run,$local_json,$remote_line" >> "$OUT_CSV"
  printf "%s|%s|%s\n" "$run" "$local_json" "$(echo "$remote_line" | tr ',' '|')" | tee -a "$OUT_TXT"
done

echo "" | tee -a "$OUT_TXT"
echo "Wrote: $OUT_CSV and $OUT_TXT"
