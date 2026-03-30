#!/usr/bin/env bash
# Check local plan_pc metadata and run data availability. Writes canonical outputs under output/tables/registry/.
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
PLAN_PC="${PLAN_PC:-$REPO_ROOT/data/plan_pc}"
REMOTE_BASES="${REMOTE_BASES:-/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output:/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_200x160x100/ensemble_output:/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output}"
IFS=':' read -r -a REMOTE_ROOTS <<< "$REMOTE_BASES"

OUT_DIR="${OUT_DIR:-$REPO_ROOT/output/tables/registry}"
OUT_CSV="$OUT_DIR/availability_check.csv"
OUT_TXT="$OUT_DIR/availability_check.txt"
LEGACY_DIR="$REPO_ROOT/data/registry"
LEGACY_CSV="$LEGACY_DIR/availability_check.csv"
LEGACY_TXT="$LEGACY_DIR/availability_check.txt"

mkdir -p "$OUT_DIR" "$LEGACY_DIR"

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

probe_run() {
  local run="$1"
  local local_json="$2"
  local base run_dir json_file
  local run_ok=MISSING
  local json_ok=MISSING
  local mp=MISSING
  local lv1_ready=NO
  local lv2_ready=NO
  local m_count=0
  local d3_count=0
  local y_count=0
  local resolved_root=""

  for base in "${REMOTE_ROOTS[@]}"; do
    [[ -z "${base:-}" ]] && continue
    run_dir="$base/$run"
    [[ -d "$run_dir" ]] || continue
    resolved_root="$base"
    json_file="$run_dir/$run.json"
    m=("$run_dir"/M_??_??_*.nc)
    d3=("$run_dir"/3D_*.nc)
    y=("$run_dir"/Y*)
    run_ok=OK
    [[ -f "$json_file" ]] && json_ok=OK
    [[ -f "$run_dir/mp_par.dat" ]] && mp=OK
    m_count=${#m[@]}
    d3_count=${#d3[@]}
    y_count=${#y[@]}
    [[ "$json_ok" = OK && "$d3_count" -gt 0 ]] && lv1_ready=YES
    [[ "$json_ok" = OK && "$m_count" -gt 0 ]] && lv2_ready=YES
    echo "$resolved_root,$local_json,$run_ok,$json_ok,$m_count,$d3_count,$y_count,$mp,$lv1_ready,$lv2_ready"
    return
  done

  echo ",$local_json,MISSING,MISSING,0,0,0,MISSING,NO,NO"
}

echo "run_id,resolved_root,local_json,remote_run_dir,remote_json,meteogram_count,three_d_count,y_count,mp_par,lv1_ready,lv2_ready" > "$OUT_CSV"

{
  echo "== Availability across ensemble roots =="
  echo "run_id|resolved_root|local_json|remote_run_dir|remote_json|M_count|3D_count|Y_count|mp_par|lv1_ready|lv2_ready"
} | tee -a "$OUT_TXT"

for run in "${RUNS[@]}"; do
  local_json=MISSING
  [[ -f "$PLAN_PC/$run.json" ]] && local_json=OK
  remote_line="$(probe_run "$run" "$local_json")"
  echo "$run,$remote_line" >> "$OUT_CSV"
  printf "%s|%s\n" "$run" "$(echo "$remote_line" | tr ',' '|')" | tee -a "$OUT_TXT"
done

echo "" | tee -a "$OUT_TXT"
cp "$OUT_CSV" "$LEGACY_CSV"
cp "$OUT_TXT" "$LEGACY_TXT"
echo "Wrote: $OUT_CSV and $OUT_TXT"
