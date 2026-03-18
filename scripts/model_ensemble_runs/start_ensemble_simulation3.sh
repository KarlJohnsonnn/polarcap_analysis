#!/bin/bash
set -e
set -x

prev_job_id=""
RUN_COUNTER=0
DRY_RUN="F"
METEOGRAM_STATION_FILE="${METEOGRAM_STATION_FILE:-}"
NC_OUTPUT_HCOMB="${NC_OUTPUT_HCOMB:-}"


# Find factor pair
find_factors() {
    local num=$1
    local sqrt=$(echo "sqrt($num)" | bc)
    for ((i = sqrt; i > 0; i--)); do
        if (( num % i == 0 )); then
        echo "$((num / i)) $i"
        return
        fi
    done
}

apply_meteogram_stationlist() {
    local input_dia_path="$1"
    local station_file="${2:-$METEOGRAM_STATION_FILE}"
    [[ -z "$station_file" ]] && return
    [[ -f "$station_file" ]] || { echo "Missing meteogram station file: $station_file" >&2; exit 1; }

    python3 - "$input_dia_path" "$station_file" <<'PYEOF'
from pathlib import Path
import sys

input_path = Path(sys.argv[1])
station_path = Path(sys.argv[2])

stations = [
    line.strip()
    for line in station_path.read_text().splitlines()
    if line.strip() and not line.lstrip().startswith("#")
]
if not stations:
    raise SystemExit(f"No stations found in {station_path}")

lines = input_path.read_text().splitlines()
out = []
inserted_station_block = False
skip_station_rows = False
for line in lines:
    stripped = line.strip()
    if stripped.startswith("lmeteogram_specs"):
        out.append("    lmeteogram_specs=.TRUE.,")
        continue
    if stripped.startswith("stationlist_tot"):
        out.append(f"    stationlist_tot = {stations[0]}")
        out.extend(f"                        {station}" for station in stations[1:])
        inserted_station_block = True
        skip_station_rows = True
        continue
    if skip_station_rows:
        if stripped == "/":
            skip_station_rows = False
            out.append(line)
        continue
    if stripped == "/" and not inserted_station_block:
        out.append(f"    stationlist_tot = {stations[0]}")
        out.extend(f"                        {station}" for station in stations[1:])
        inserted_station_block = True
    out.append(line)

input_path.write_text("\n".join(out) + "\n")
PYEOF
}

# Processes an input FORTRAN namelist, extracts its content, and appends it
# to an existing JSON file as a new "run entry". Namelist variables are organized
# under their respective sections and converted into JSON format.
append_to_json() {
    local input_files=("INPUT_ORG" "INPUT_DIA" "INPUT_IO" "INPUT_ASS" "INPUT_DYN" "INPUT_IDEAL" "INPUT_PHY")
    local run_start_time="$1"
    local job_id="$2"
    local nodeX="$3"
    local input_dir="${4:-$RUN_SCRIPT_DIR}"
    local run_start_datetime=$(date -d "@$run_start_time" +"%Y%m%d%H%M%S")

    sed -i '$ d' "$JSON_FILE"
    echo -e ",\n  \"$run_start_datetime\": {" >> "$JSON_FILE"
    echo -e "    \"run_start_time\": \"$run_start_time\"," >> "$JSON_FILE"
    echo -e "    \"job_id\": \"$job_id\",\n    \"nodeX\": \"$nodeX\",\n    \"domain\": \"$DOMAIN\",\n" >> "$JSON_FILE"

    # Assuming the namelists are plain text files
    for input_file in "${input_files[@]}"; do
        json_content=$(python3 $PYTHON_UTILITIES/utilities/namelist_converter.py "$input_dir/$input_file")
        echo -e "    \"$input_file\": $json_content," >> "$JSON_FILE"
    done

    # Remove the last comma in the file
    tac "$JSON_FILE" | awk '!p && sub(/,$/, "") {p=1} 1' | tac > temp.txt && mv temp.txt "$JSON_FILE"
    # Finalize the JSON (add closing brackets)
    #echo "}}" >> "$JSON_FILE"
    echo "}" >> "$JSON_FILE"
    echo "}" >> "$JSON_FILE"

}

# Function appends bash commands to a file
append_to_file() {
    run_start_datetime=$2
    local files_to_copy=("$1.out" "$1.err" "M_*_*_$run_start_datetime.nc" "3D_$run_start_datetime.nc")  # List of files to copy
    for file in "${files_to_copy[@]}"; do
        echo -e "mv $RUN_SCRIPT_DIR/$file  ensemble_output/${JSON_FILE%.json}/" >> "$COPY_FILE"
    done
}

# Initialize files and directories for the ensemble run
initialize_run() {
    JSON_FILE="cs-eriswil__$SETUP_TIME.json"
    touch "$JSON_FILE"
    echo -e "{\n" >> "$JSON_FILE"
    echo -e "," >> "$JSON_FILE"

    # Initialize copy file
    COPY_FILE="copy2_${JSON_FILE%.json}.sh"
    touch "$COPY_FILE"
    chmod +x "$COPY_FILE"
    echo -e "echo 'mkdir ensemble_output/${JSON_FILE%.json}'" >> "$COPY_FILE"
    echo -e "mkdir -p ensemble_output/${JSON_FILE%.json}" >> "$COPY_FILE"
    echo -e "mv $RUN_SCRIPT_DIR/$JSON_FILE  ensemble_output/${JSON_FILE%.json}/" >> "$COPY_FILE"
    echo -e "mv $RUN_SCRIPT_DIR/Y*  ensemble_output/${JSON_FILE%.json}/" >> "$COPY_FILE"
    echo -e "mv $RUN_SCRIPT_DIR/mp_par.dat  ensemble_output/${JSON_FILE%.json}/" >> "$COPY_FILE"
}

# Print initial run information
print_run_info() {
    echo -e "\nsubmit jobs and wait unitl a node is assigned to read the INPUT* files"
    echo -e "ensemble run date: $SETUP_TIME UTC"
    echo -e "domain: $DOMAIN"
}


# Cleanup and finalize the run files
finalize_run() {
    # Clean up JSON file by removing extra lines
    echo "Clean up $JSON_FILE file"
    sed -i '2,3d' "$JSON_FILE"

    # Add cleanup command to copy file
    echo -e "echo 'remove $COPY_FILE'" >> "$COPY_FILE"
    echo -e "rm $COPY_FILE" >> "$COPY_FILE"
}

# Verify that all runs in the JSON file match the expected parameter sets.
# Reads VERIFY_FILE (pipe-delimited expected values per run) and cross-checks
# against the actual INPUT_ORG namelist stored in JSON_FILE.
verify_ensemble() {
    echo ""
    echo "================================================================================"
    echo "  Ensemble Parameter Verification  ($JSON_FILE)"
    echo "================================================================================"
    python3 - "$JSON_FILE" "$VERIFY_FILE" <<'PYEOF'
import json, sys, os

def fbool(s):
    """Fortran boolean string -> Python bool."""
    return s.strip().strip('.').lower() == 'true'

def fnum(s):
    try: return float(s)
    except ValueError: return s

json_file, verify_file = sys.argv[1], sys.argv[2]

with open(json_file) as f:
    data = json.load(f)
with open(verify_file) as f:
    rows = [line.strip().split('|') for line in f if line.strip()]

HEADER = ['datetime','ishape','iimfr','dnap_init',
          'lflare','lflare_inp','flare_emission',
          'lflare_ccn','flare_dn','flare_dp','flare_sig']

n_pass = n_fail = 0
for row in rows:
    exp = dict(zip(HEADER, row))
    dt = exp['datetime']

    if dt not in data:
        print(f"  FAIL  Run {dt} — not found in JSON")
        n_fail += 1
        continue

    run = data[dt]
    sbm   = run.get('INPUT_ORG', {}).get('sbm_par', {})
    flare = run.get('INPUT_ORG', {}).get('flare_sbm', {})
    job   = run.get('job_id', '?')

    checks = [
        ('ishape',         int(exp['ishape']),          sbm.get('ishape')),
        ('iimfr',          int(exp['iimfr']),           sbm.get('iimfr')),
        ('dnap_init',      fnum(exp['dnap_init']),      sbm.get('dnap_init')),
        ('lflare',         fbool(exp['lflare']),        sbm.get('lflare')),
        ('lflare_inp',     fbool(exp['lflare_inp']),    flare.get('lflare_inp')),
        ('flare_emission', fnum(exp['flare_emission']), flare.get('flare_emission')),
        ('lflare_ccn',     fbool(exp['lflare_ccn']),    flare.get('lflare_ccn')),
        ('flare_dn',       fnum(exp['flare_dn']),       flare.get('flare_dn')),
        ('flare_dp',       fnum(exp['flare_dp']),       flare.get('flare_dp')),
        ('flare_sig',      fnum(exp['flare_sig']),      flare.get('flare_sig')),
    ]

    mismatches = []
    for name, want, got in checks:
        if isinstance(want, float) and isinstance(got, (int, float)):
            if abs(want - got) > 1e-6 * max(abs(want), abs(got), 1.0):
                mismatches.append(f"{name}: expected {want}, got {got}")
        elif want != got:
            mismatches.append(f"{name}: expected {want}, got {got}")

    if mismatches:
        print(f"  FAIL  Run {dt}  (job {job})")
        for m in mismatches:
            print(f"        - {m}")
        n_fail += 1
    else:
        tag = f"ishape={sbm.get('ishape')}, fe={flare.get('flare_emission')}, lflare={sbm.get('lflare')}"
        print(f"  PASS  Run {dt}  (job {job})  [{tag}]")
        n_pass += 1

total = n_pass + n_fail
print(f"\n  Result: {n_pass}/{total} passed, {n_fail}/{total} failed")
if n_fail:
    print("  *** WARNING: parameter mismatches detected! ***")
    sys.exit(1)
else:
    print("  All runs verified successfully.")
PYEOF
    local rc=$?
    rm -f "$VERIFY_FILE"
    return $rc
}

run_simulation() {
    local flare_emission=$1
    local background_inp=$2
    local ccn_dn_in=$3
    local ccn_dp_in=$4
    local ccn_sig_in=$5
    local ishape=$6
    local iimfr=$7
    local ccn_dn_flare=$8
    local ccn_dp_flare=$9
    local ccn_sig_flare=${10}
    
    local run_start_time=$(( $(date +%s) + RUN_COUNTER ))
    local run_start_datetime=$(date -d "@$run_start_time" +"%Y%m%d%H%M%S")
    local outputname_3D="3D_$run_start_datetime"
    local outputname_MG="_$(printf "%02d" $RUN_COUNTER)""_$run_start_datetime"
    
    # Stage per-run INPUT copies (each job gets its own copy, eliminating race conditions)
    local STAGED_DIR="${STAGED_BASE}/run_${RUN_COUNTER}"
    mkdir -p "$STAGED_DIR"
    cp -p INPUT_FILES/*_${DOMAIN} "$STAGED_DIR/"

    # Set flare flags
    local vflare_inp=$flare_emission vflare_dn=$ccn_dn_flare vflare_dp=$ccn_dp_flare vflare_sig=$ccn_sig_flare
    local lflare_inp=".true." lflare_ccn=".true." lflare=".true."
    [[ $flare_emission == "no" ]] && { lflare_inp=".false."; vflare_inp=0.0; }
    [[ $ccn_dn_flare == "no" ]] && { lflare_ccn=".false."; vflare_dn=0.0; vflare_dp=0.0; vflare_sig=0.0; }
    [[ $flare_emission == "no" && $ccn_dn_flare == "no" ]] && lflare=".false."
    
    # Update staged INPUT copies (not the shared originals)
    sed -i -E \
        -e "s/(outputname *=).*/\1 $outputname_3D/" \
        -e "s/(dnap_init *=).*/\1 $background_inp/" \
        -e "s/(dn_in *=).*/\1 $ccn_dn_in/" \
        -e "s/(dp_in *=).*/\1 $ccn_dp_in/" \
        -e "s/(sig_in *=).*/\1 $ccn_sig_in/" \
        -e "s/(ishape *=).*/\1 $ishape/" \
        -e "s/(iimfr *=).*/\1 $iimfr/" \
        -e "s/(lflare *=).*/\1 $lflare/" \
        -e "s/(lflare_inp *=).*/\1 $lflare_inp/" \
        -e "s/(lflare_ccn *=).*/\1 $lflare_ccn/" \
        -e "s/(flare_emission *=).*/\1 $vflare_inp/" \
        -e "s/(flare_dn *=).*/\1 $vflare_dn/" \
        -e "s/(flare_dp *=).*/\1 $vflare_dp/" \
        -e "s/(flare_sig *=).*/\1 $vflare_sig/" \
        "$STAGED_DIR/INPUT_ORG_$DOMAIN"

    if [[ -n "$NC_OUTPUT_HCOMB" ]]; then
        sed -i -E \
            -e "s/(nc_output_hcomb *=).*/\1 $NC_OUTPUT_HCOMB/" \
            "$STAGED_DIR/INPUT_ORG_$DOMAIN"
    fi
    
    apply_meteogram_stationlist "$STAGED_DIR/INPUT_DIA_$DOMAIN" "$METEOGRAM_STATION_FILE"
    sed -i -E "s/_[0-9]{2}_[0-9]{14}/$outputname_MG/g" "$STAGED_DIR/INPUT_DIA_$DOMAIN"
    
    sed -i -E \
        -e "s/^#SBATCH --nodes=[0-9]+/#SBATCH --nodes=$NNODES/" \
        -e "s/^#SBATCH --time=[0-9]{2}:[0-9]{2}:[0-9]{2}+/#SBATCH --time=$CPU_TIME/" \
        -e "s/^NTASKS=[0-9]+/NTASKS=$NTASKS/" \
        "$RUN_SCRIPT"
    
    local nprocall=$(( NNODES * NTASKS ))
    local nprocio=0
    read -r nprocx nprocy <<< "$(find_factors $nprocall)"
    sed -i -E "s/^([[:space:]]*nprocx[[:space:]]*=[[:space:]]*)[0-9]+([[:space:]]*,?)/\1$nprocx\2/" "$STAGED_DIR/INPUT_ORG_$DOMAIN"
    sed -i -E "s/^([[:space:]]*nprocy[[:space:]]*=[[:space:]]*)[0-9]+([[:space:]]*,?)/\1$nprocy\2/" "$STAGED_DIR/INPUT_ORG_$DOMAIN"
    sed -i -E "s/^([[:space:]]*nprocio[[:space:]]*=[[:space:]]*)[0-9]+([[:space:]]*,?)/\1$nprocio\2/" "$STAGED_DIR/INPUT_ORG_$DOMAIN"
    
    # Create plain-name copies for JSON logging
    for f in "$STAGED_DIR"/INPUT_*_${DOMAIN}; do
        cp -p "$f" "$STAGED_DIR/$(basename "${f%_${DOMAIN}}")"
    done

    # Submit job with staged dir passed via environment (each job reads its own private copy)
    local job_id="--no-job--"
    local nodeX="--pending--"
    if [ "$DRY_RUN" == "T" ]; then
        job_id="--dry-run--"
    elif [ -z "$prev_job_id" ] || [ "$prev_job_id" == "--no-job--" ] || [ "$prev_job_id" == "--dry-run--" ]; then
        job_id=$(sbatch --parsable --export=ALL,STAGED_INPUT_DIR="$STAGED_DIR" $RUN_SCRIPT $DOMAIN)
    else
        job_id=$(sbatch --parsable --dependency=after:$prev_job_id --export=ALL,STAGED_INPUT_DIR="$STAGED_DIR" $RUN_SCRIPT $DOMAIN)
    fi
    prev_job_id=$job_id

    if [ "$DRY_RUN" == "T" ]; then
        nodeX="--dry-run--"
    else
        nodeX=$(squeue -h -j "$job_id" -o "%N" 2>/dev/null | awk 'NF{print; exit}' || true)
        nodeX=${nodeX:---pending--}
    fi

    append_to_json "$run_start_time" "$job_id" "$nodeX" "$STAGED_DIR"
    if [ "$DRY_RUN" != "T" ]; then
        append_to_file "$job_id" "$run_start_datetime"
    fi
    
    bg_params=$(printf "%-20s %-20s %-15s %-8s %-8s %-8s" "$ccn_dn_in" "$ccn_dp_in" "$ccn_sig_in" "$background_inp" "$ishape" "$iimfr")
    flare_params=$(printf "%-12s %-10s %-10s %-10s" "$ccn_dn_flare" "$ccn_dp_flare" "$ccn_sig_flare" "$flare_emission")
    printf "             %s |          %s|          %d    (%s)\n" "$bg_params" "$flare_params" "$run_start_datetime" "$job_id"
    
    # Record expected parameters for post-submission verification
    echo "${run_start_datetime}|${ishape}|${iimfr}|${background_inp}|${lflare}|${lflare_inp}|${vflare_inp}|${lflare_ccn}|${vflare_dn}|${vflare_dp}|${vflare_sig}" >> "$VERIFY_FILE"

    RUN_COUNTER=$((RUN_COUNTER + 1))
}

################################################################################
# Main execution
# check for dry run argument
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && { DRY_RUN="T"; break; }; done

# setup run parameter
SETUP_TIME=$(date +%Y%m%d_%H%M%S)
RUN_SCRIPT="${RUN_SCRIPT:-run_COSMO-SPECS_levante_v3}"
RUN_SCRIPT_DIR=$(pwd)
STAGED_BASE="$RUN_SCRIPT_DIR/STAGED_INPUTS"
mkdir -p "$STAGED_BASE"
VERIFY_FILE=$(mktemp "${RUN_SCRIPT_DIR}/verify_XXXXXX.txt")
NNODES="${NNODES:-1}"
NTASKS="${NTASKS:-256}"
CPU_TIME="${CPU_TIME:-08:00:00}"
DOMAIN="${DOMAIN:-50x40xZ}"

initialize_run
printf "Background:  %-20s %-20s %-15s %-8s %-8s %-8s |    Flare:  %-10s %-10s %-10s %-10s |    Identifyer:  %-20s %-10s\n" \
       "DN" "DP" "SIG" "BINP" "ISHAPE" "IIMFR" "DN" "DP" "SIG" "FE_inp" "run_start_time" "(job_id)"
printf "%s\n" "$(printf -- '-%.0s' {1..200})"

# Simulation runs (flare_emission, background_inp, ccn_dn_in, ccn_dp_in, ccn_sig_in, ishape, iimfr, ccn_dn_flare, ccn_dp_flare, ccn_sig_flare)

run_simulation "no"     "400"    "200.0, 200.5," "100.0e-9, 350.0e-9," "1.5, 2.45,"    "2"    "13"    "no" "no" "no"
run_simulation "1e6"    "400"    "200.0, 200.5," "100.0e-9, 350.0e-9," "1.5, 2.45,"    "2"    "13"    "no" "no" "no"
run_simulation "no"     "400"    "200.0, 200.5," "100.0e-9, 350.0e-9," "1.5, 2.45,"    "4"    "13"    "no" "no" "no"
run_simulation "1e6"    "400"    "200.0, 200.5," "100.0e-9, 350.0e-9," "1.5, 2.45,"    "4"    "13"    "no" "no" "no"

finalize_run
verify_ensemble
