# Ensemble Script v3 — Per-Run Input Staging

## Problem (v2)

Race condition: `INPUT_FILES/` is shared across all runs. If a SLURM job hasn't started
when the next run overwrites `INPUT_FILES/`, the first job picks up wrong parameters.
`sleep 100` and squeue-polling were fragile workarounds.

## Solution (v3)

Each run gets its own **private copy** of INPUT files in `STAGED_INPUTS/run_<N>/`.
No shared state, no sleep, no polling.

```
INPUT_FILES/  --cp-->  STAGED_INPUTS/run_N/  --sed-->  modified copy
                                                  |
                                    sbatch --export=STAGED_INPUT_DIR=...
                                                  |
                                    SLURM job copies from STAGED_INPUT_DIR
                                    then cleans up STAGED_INPUTS/run_N/
```

## Files

| File | Role |
|---|---|
| `start_ensemble_simulation2.sh` | Original (unchanged) |
| `start_ensemble_simulation3.sh` | **New** — per-run staging + auto-verification |
| `run_COSMO-SPECS_levante` | Original SLURM script (unchanged) |
| `run_COSMO-SPECS_levante_v3` | **New** — reads from `$STAGED_INPUT_DIR`, self-cleanup |

---

## Execution Plan

### Prerequisites

- `INPUT_FILES/` contains all `INPUT_*_<DOMAIN>` template files
- `psbm_fd4_levante` binary is present in the working directory
- `$PYTHON_UTILITIES` environment variable points to the utilities package
- SLURM scheduler is accessible (`sbatch`, `squeue`)

### Step-by-step

```
1.  Parse --dry-run flag (if present, no jobs submitted)

2.  Set global parameters:
      NNODES, NTASKS, CPU_TIME, DOMAIN
      Create STAGED_INPUTS/ base dir
      Create VERIFY_FILE (temp file for expected params)

3.  initialize_run:
      Create JSON log file (cs-eriswil__<timestamp>.json)
      Create copy script (copy2_*.sh)

4.  For each run_simulation call:

    4a. Create staging dir:  STAGED_INPUTS/run_<N>/
        Copy INPUT_FILES/*_<DOMAIN>  -->  staging dir

    4b. Derive flare flags from arguments:
        flare_emission="no"  -->  lflare_inp=.false., emission=0.0
        ccn_dn_flare="no"   -->  lflare_ccn=.false., dn/dp/sig=0.0
        both "no"            -->  lflare=.false.

    4c. sed modifications on STAGED copies (never INPUT_FILES/):
        - INPUT_ORG: ishape, iimfr, dnap_init, dn_in, dp_in, sig_in,
                     lflare, lflare_inp, lflare_ccn, flare_emission,
                     flare_dn, flare_dp, flare_sig, outputname,
                     nprocx, nprocy, nprocio
        - INPUT_DIA: station name timestamps

    4d. sed on run_COSMO-SPECS_levante_v3:
        #SBATCH --nodes, --time, NTASKS
        (sbatch reads script at submission time — safe to reuse)

    4e. Create plain-name copies in staging dir for JSON logging

    4f. sbatch --export=ALL,STAGED_INPUT_DIR=<staging_dir>
        (with --dependency=after:<prev_job> for orderly scheduling)

    4g. Record params to JSON log + verification file

    4h. Increment RUN_COUNTER

5.  finalize_run:
      Clean up JSON formatting
      Append cleanup commands to copy script

6.  verify_ensemble (automatic):
      Python cross-checks JSON against expected params
      Checks: ishape, iimfr, dnap_init, lflare, lflare_inp,
              flare_emission, lflare_ccn, flare_dn, flare_dp, flare_sig
      Prints PASS/FAIL per run, exits 1 on mismatch
```

### What happens on the SLURM node (run_COSMO-SPECS_levante_v3)

```
1.  INPUT_SRC = $STAGED_INPUT_DIR  (or INPUT_FILES/ as fallback)
2.  cp  INPUT_SRC/INPUT_{DYN,IO,ORG,DIA}_<CASE>  -->  working dir
3.  rm -rf $STAGED_INPUT_DIR  (cleanup)
4.  cat INPUT_* to job log  (for debugging)
5.  srun psbm_fd4_levante
```

---

## Important Notes

- **INPUT_FILES/ is never modified.** All sed targets are staged copies.
  This is the core change that eliminates the race condition.

- **`--dependency=after:<prev_job>`** is kept for orderly scheduling
  but is no longer required for correctness. Each job has isolated inputs.

- **Staging dir cleanup** happens inside the SLURM job (after `cp`).
  If a job fails before the `cp`, the staging dir persists — useful for debugging.

- **Verification exits with code 1** on mismatch. Since `set -e` is active,
  this will cause the script to abort if verification fails.

- **The run script (`run_COSMO-SPECS_levante_v3`) is modified in-place** by `sed`
  for `#SBATCH` directives. This is safe because `sbatch` reads and stores the
  script content at submission time — later modifications don't affect queued jobs.

- **`append_to_json` reads from the staging dir** (4th argument), so the JSON
  log reflects the actual parameters each run will use, not stale top-level files.

## Key Differences: v2 vs v3

| Aspect | v2 | v3 |
|---|---|---|
| INPUT_FILES/ | Modified in-place | Never touched |
| Race protection | `sleep 100` + squeue polling | Per-run staging (structural) |
| Time between runs | ~100-200s | ~1-2s |
| Parameter verification | Manual | Automatic (verify_ensemble) |
| SLURM input source | `INPUT_FILES/` | `$STAGED_INPUT_DIR` |
| Cleanup | None | SLURM job self-cleans staging dir |
