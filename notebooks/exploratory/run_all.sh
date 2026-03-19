#!/bin/bash

echo "====================================================================="
echo "  PolarCAP Analysis: Parallel Notebook Execution"
echo "====================================================================="
echo "IMPORTANT: This requires the intermediate data to be generated first."
echo "Please ensure you have run the data processing pipeline:"
echo "  -> scripts/processing_chain/run_chain.py"
echo "====================================================================="
echo ""

# Move to the notebooks directory if not already there
cd "$(dirname "$0")"

# Create log directory
mkdir -p logs

# Find all numbered notebooks
NOTEBOOKS=( [0-9]*.ipynb )

echo "Starting execution of ${#NOTEBOOKS[@]} notebooks in parallel (7 workers)..."
echo "Logs will be saved to notebooks/logs/"
echo ""

# Execute using xargs for better process control (limits to 7 parallel jobs)
printf "%s\n" "${NOTEBOOKS[@]}" | xargs -n 1 -P 7 -I {} bash -c '
    NOTEBOOK=$1
    LOGFILE="logs/${NOTEBOOK%.ipynb}.log"
    echo "[$NOTEBOOK] Starting execution..."
    
    # Run notebook and capture output
    jupyter nbconvert --to notebook --execute --inplace "$NOTEBOOK" > "$LOGFILE" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "[$NOTEBOOK] ✅ SUCCESS"
    else
        echo "[$NOTEBOOK] ❌ FAILED - check notebooks/$LOGFILE"
    fi
' _ {}

echo ""
echo "Done! All successfully generated plots are saved in their respective output cells and the notebooks/output/ directory."
