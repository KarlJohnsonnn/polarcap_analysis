# Analysis Storyline

This directory contains the core scientific analysis for the project, arranged in a seven-chapter sequence that tells the story of ice formation and growth in AgI-seeded stratiform clouds. 

All notebooks are designed to be run sequentially after the automated data pipeline has generated the level 1-3 intermediate files. For pipeline details, see [scripts/processing_chain/README.md](../scripts/processing_chain/README.md).

For legacy analysis and prior versions, see the [archive](archive/README.md).

## Step 0 -- The Processing Chain

Before running these notebooks, you must generate the intermediate data products (Level 1-3). The analysis depends on these aggregated files rather than raw 5D model output. 
👉 **[Go to the Processing Chain](../scripts/processing_chain/README.md)** to generate the required `LV1` and `LV2` data.

---

## Act 1 -- Setting the Stage

**01 - Cloud Field Overview**  
*What does the simulated domain and cloud look like?* Time-height cross-sections of NW/NF at each station.  
**(Input: LV2 meteogram Zarr)**

**02 - HOLIMO Observation Baseline**  
*What do the observations actually contain?* HOLIMO water/ice PSD variables, normalisations, and consistency checks to establish the baseline for model comparison.  
**(Input: HOLIMO NetCDF)**

## Act 2 -- Following the Plume

**03 - Plume Lagrangian Evolution**  
*How does the seeded plume evolve?* Lagrangian tracking of ICNC, CDNC, and frozen water fraction vs residence time on a symlog axis. Includes HOLIMO overlays for model-obs closure.  
**(Input: LV1 paths + HOLIMO)**

**04 - Spectral Waterfall**  
*What happens spectrally?* PSD waterfall plots by altitude band and elapsed time. Generates quantitative model-obs LaTeX tables (CDNC/ICNC mean, std, growth rate).  
**(Input: LV1 paths + HOLIMO)**

## Act 3 -- Understanding the Processes

**05 - Process Budget**  
*Which processes matter?* Stacked-area budgets (sources/sinks) and dominance maps. Shows that IMMERN dominates after seeding, CONDNFROD is redistribution, and deposition and WBF control mass.  
**(Input: LV2 Zarr)**

## Act 4 -- The SBM Advantage

**06 - Growth Rate Benchmark**  
*Does spectral-bin pay off?* Compares COSMO-SPECS growth rates vs Omanovic 2025 (bulk) vs Ramelli 2024 (HOLIMO). Demonstrates that SBM captures size-dependent growth without ventilation tuning.  
**(Input: LV1 paths + HOLIMO)**

**07 - WBF Regime Analysis**  
*When and why does WBF dominate?* Korolev-Mazin critical updraft classification. Analyzes WBF, mixed-phase growth, and evaporation regime frequencies vs time.  
**(Input: LV1 paths)**