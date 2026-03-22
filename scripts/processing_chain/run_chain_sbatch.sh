#!/bin/bash
#SBATCH --job-name=polarcap-chain
#SBATCH --partition=compute
#SBATCH --time=04:00:00
#SBATCH --mem=64G
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --account=bb1262
#SBATCH --output=logs/chain-%j.out
#SBATCH --error=logs/chain-%j.err

set -euo pipefail


CS_RUN="cs-eriswil__20260318_153631"
CONFIG="${POLARCAP_ANALYSIS_ROOT}/config/processing_chain.yaml"
OUTPUT_ROOT="${POLARCAP_ANALYSIS_ROOT}/scripts/data/processed"

cd $POLARCAP_ANALYSIS_ROOT/scripts/processing_chain
source ~/.bashrc
conda activate pcpaper_env

python run_chain.py \
  --config $CONFIG \
  --cs-run $CS_RUN \
  --out $OUTPUT_ROOT