#!/bin/bash
set -e
set -x

prev_job_id=""
RUN_COUNTER=0
# Function: replace_params_and_run
# It replaces the parameters in your input files, submits the job, waits for a node to be assigned, and prints out a message with the job details.
replace_params_and_run() {
    local run_start_time=$(date +%s)
    local run_start_datetime=$(date -d "@$run_start_time" +"%Y%m%d%H%M%S")
    local outputname_3D="3D_$run_start_datetime"
    local outputname_MG="_$(printf "%02d" $RUN_COUNTER)""_$run_start_datetime"    

    # check if flare is active and set flags and parameters accordingly
    local vflare_inp=$1
    local vflare_dn=$8
    local vflare_dp=$9
    local vflare_sig=${10}
    local lflare_inp=".true."
    local lflare_ccn=".true."
    local lflare=".true."
    [[ $1 == "no" ]] && { lflare_inp=".false."; vflare_inp=0.0; }
    [[ $8 == "no" ]] && { lflare_ccn=".false."; vflare_dn=0.0; vflare_dp=0.0; vflare_sig=0.0; }
    [[ $1 == "no" && $8 == "no" ]] && { lflare=".false."; }


    # Replace parameters in INPUT_ORG_$DOMAIN using consistent -E syntax
    sed -i -E \
        -e "s/(outputname *=).*/\1 $outputname_3D/" \
        -e "s/(dnap_init *=).*/\1 $2/" \
        -e "s/(dn_in *=).*/\1 $3/" \
        -e "s/(dp_in *=).*/\1 $4/" \
        -e "s/(sig_in *=).*/\1 $5/" \
        -e "s/(ishape *=).*/\1 $6/" \
        -e "s/(iimfr *=).*/\1 $7/" \
        -e "s/(lflare *=).*/\1 $lflare/" \
        -e "s/(lflare_inp *=).*/\1 $lflare_inp/" \
        -e "s/(lflare_ccn *=).*/\1 $lflare_ccn/" \
        -e "s/(flare_emission *=).*/\1 $vflare_inp/" \
        -e "s/(flare_dn *=).*/\1 $vflare_dn/" \
        -e "s/(flare_dp *=).*/\1 $vflare_dp/" \
        -e "s/(flare_sig *=).*/\1 $vflare_sig/" \
        "INPUT_FILES/INPUT_ORG_$DOMAIN"

    # Update output name of the Meteograms in INPUT_DIA
    sed -i -E "s/_[0-9]{2}_[0-9]{14}/$outputname_MG/g" "INPUT_FILES/INPUT_DIA_$DOMAIN"

    # Update SBATCH parameters in run script
    sed -i -E \
        -e "s/^#SBATCH --nodes=[0-9]+/#SBATCH --nodes=$NNODES/" \
        -e "s/^#SBATCH --time=[0-9]{2}:[0-9]{2}:[0-9]{2}+/#SBATCH --time=$CPU_TIME/" \
        -e "s/^NTASKS=[0-9]+/NTASKS=$NTASKS/" \
        "$RUN_SCRIPT"

    local nprocall=$(( NNODES * NTASKS ))
    local nprocio=0
    read -r nprocx nprocy <<< "$(find_factors $nprocall)"
    sed -i "s/^ *nprocx *= *[0-9]\+,/   nprocx = $nprocx,/" "INPUT_FILES/INPUT_ORG_$DOMAIN"
    sed -i "s/^ *nprocy *= *[0-9]\+,/   nprocy = $nprocy,/" "INPUT_FILES/INPUT_ORG_$DOMAIN"
    sed -i "s/^ *nprocio *= *[0-9]\+,/   nprocio = $nprocio,/" "INPUT_FILES/INPUT_ORG_$DOMAIN"

    # Submit the job and wait for a node to be assigned
    job_id=$(submit_and_wait_for_job "$run_start_time")
    
    # Get the node name from the job ID
    nodeX=$(squeue -j "$job_id" -o "%N" | tail -n +2)
    
    # Call the function with the input Fortran namelist file and desired JSON output file
    append_to_json "$run_start_time" "$job_id" "$nodeX"
    
    # add files to copy
    append_to_file "$job_id" "$run_start_datetime"

    # Format and display parameters
    bg_params=$(printf "%-20s %-20s %-15s %-8s %-8s %-8s" "$3" "$4" "$5" "$2" "$6" "$7")
    flare_params=$(printf "%-12s %-10s %-10s %-10s" "$8" "$9" "${10}" "$1")
    printf "             %s |          %s|          %d    (%d)\n" "$bg_params" "$flare_params" "$run_start_datetime" "$job_id"
    
}

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

# Processes an input FORTRAN namelist, extracts its content, and appends it
# to an existing JSON file as a new "run entry". Namelist variables are organized
# under their respective sections and converted into JSON format.
append_to_json() {
    local input_files=("INPUT_ORG" "INPUT_DIA" "INPUT_IO" "INPUT_ASS" "INPUT_DYN" "INPUT_IDEAL" "INPUT_PHY")
    local run_start_time="$1"
    local job_id="$2"
    local nodeX="$3"
    local run_start_datetime=$(date -d "@$run_start_time" +"%Y%m%d%H%M%S")
    
    sed -i '$ d' "$JSON_FILE"
    echo -e ",\n  \"$run_start_datetime\": {" >> "$JSON_FILE"
    echo -e "    \"run_start_time\": \"$run_start_time\"," >> "$JSON_FILE"
    echo -e "    \"job_id\": \"$job_id\",\n    \"nodeX\": \"$nodeX\",\n    \"domain\": \"$DOMAIN\",\n" >> "$JSON_FILE"

    # Assuming the namelists are plain text files
    for input_file in "${input_files[@]}"; do
        json_content=$(python3 $PYTHON_UTILITIES/utilities/namelist_converter.py "$RUN_SCRIPT_DIR/$input_file")
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

    # Initialize metadata file
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

# Execute the ensemble runs with different parameter combinations
run_ensemble() {
    printf "Background:  %-20s %-20s %-15s %-8s %-8s %-8s |    Flare:  %-10s %-10s %-10s %-10s |    Identifyer:  %-20s %-10s\n" \
            "DN" "DP" "SIG" "BINP" "ISHAPE" "IIMFR" "DN" "DP" "SIG" "FE_inp" "run_start_time" "(job_id)"
    printf "%s\n" "$(printf -- '-%.0s' {1..200})"
    
    for ishape in "${ishape_values[@]}"; do
        for iimfr in "${iimfr_values[@]}"; do
            for background_inp in "${background_inp_values[@]}"; do
                for flare_emission in "${flare_emission_values[@]}"; do
                    for i in "${!ccn_dn_flare[@]}"; do
                        for j in "${!ccn_dn_in[@]}"; do
                            # Run with parameters
                            replace_params_and_run \
                                "$flare_emission" \
                                "$background_inp" \
                                "${ccn_dn_in[$j]}" \
                                "${ccn_dp_in[$j]}" \
                                "${ccn_sig_in[$j]}" \
                                "$ishape" \
                                "$iimfr" \
                                "${ccn_dn_flare[$i]}" \
                                "${ccn_dp_flare[$i]}" \
                                "${ccn_sig_flare[$i]}" \
                                "$RUN_COUNTER"
                            ((RUN_COUNTER++))
                        done
                    done
                done
            done
        done
    done
}


submit_and_wait_for_job() {
    local run_start_time=$1
    local max_wait=300  # Maximum wait time in seconds
    local job_id

    # If dry run, return placeholder values
    if [[ "$DRY_RUN" == "T" ]]; then
        echo "--no-job--" "--no-node--"
        return
    fi

    # Submit the job
    job_id=$(sbatch --parsable $RUN_SCRIPT $DOMAIN)
    # Validate job ID
    if [[ $job_id =~ ^[0-9]+$ ]]; then
        prev_job_id=$job_id
    else
        echo "Error: Failed to get valid job ID"
        exit 1
    fi
    
    # Wait for node assignment and SPECS to start
    # Loop continues until max_wait time is reached
    while [ $(($(date +%s) - run_start_time)) -le $max_wait ]; do
        sleep 2
        
        # Skip if output file doesn't exist yet (continue checking every 5 seconds)
        [[ ! -f "${job_id}.out" ]] && { continue; }
        
        # Check if SPECS has started computing (indicated by specific log message 
        # in .out file). **Important: Set `idbg_level = 1` (or `> 1`) in runctl namelist (INPUT_ORG file)
        grep -q "\[SPECS\]  computing mp" "${job_id}.out" && { break; }
                
    done
    #sleep 30 # wait for SPECS to start computing
    echo "$job_id"
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

#################################################################
#                 __  __          _____ _   _ 
#                |  \/  |   /\   |_   _| \ | |
#                | \  / |  /  \    | | |  \| |
#                | |\/| | / /\ \   | | | . ` |
#                | |  | |/ ____ \ _| |_| |\  |
#                |_|  |_/_/    \_\_____|_| \_|
#                              
#################################################################                              
# Check for --dry-run argument
DRY_RUN="F"
for arg in "$@"; do
    if [ "$arg" = "--dry-run" ]; then
        DRY_RUN="T"
        break
    fi
done
#
SETUP_TIME=$(date +%Y%m%d_%H%M%S)

# Define constants
RUN_SCRIPT=run_COSMO-SPECS_levante
RUN_SCRIPT_DIR=$(pwd)

NNODES=20
NTASKS=256
CPU_TIME="08:00:00"

# DOMAIN="50x40xZ"
# DOMAIN="50x40xZ_bulk"
DOMAIN="200x160xZ"



# background aerosol
ccn_dn_in=( "200.0, 200.5,"  )
ccn_dp_in=( "100.0e-9, 350.0e-9," )
ccn_sig_in=( "1.5, 2.45," )
background_inp_values=( "400" )

# flare aerosol
ccn_dn_flare=( "no" "400.0e0" )
ccn_dp_flare=( "no" "50.0e-9"  )
ccn_sig_flare=( "no" "1.5")
flare_emission_values=( "no" "1e6" )

# model parameter ice shape, immersion freezing
ishape_values=( "1" "4" ) 
iimfr_values=( "13" )

# Run the ensemble, initialize counter and prev_job_id as global variables
initialize_run
print_run_info
run_ensemble
finalize_run


#################################################################################################################
# More Information below:
#

# imode_in = 0 required inf sbm_par namelist (INPUT_ORG), default: imode_in = 2
#
# for literature values see: 
#   - Miller et al. 2025, ACP, https://acp.copernicus.org/articles/25/5387/2025/#top 
#       + CDNC~450cm-3 , Dp~100nm, sig~1.5, 
#       + specifically: Table 1: https://acp.copernicus.org/articles/25/5387/2025/acp-25-5387-2025-t01.png
##


##################################################################################################################################################################################################################################
