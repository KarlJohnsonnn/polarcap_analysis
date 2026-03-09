# Changelog

All notable changes to the processing chain (LV0 → LV3) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- **Compression** – Moved to `scripts/nc_compression/`: `compress_nc.sh`, `compress_nc_slurm.sh`. Processing chain README now points to that directory.
- **`_paths.py`** → `src/utilities/processing_paths.py` – Path resolution moved into utilities for reuse by notebooks and other scripts.

## [0.1.1] - 2025-03-06

### Removed
- **`INVENTORY.md`** – Migration planning doc; pipeline migration complete
- Unused `flare_idx`, `ref_idx`, `threshold` from `config_example.yaml` (run_chain does not pass them)
- Unused `xarray` import from `run_meteogram_zarr.py`

## [0.1.0] - 2025-03-06

### Added

- **`src/utilities/processing_paths.py`** – Shared path resolution
  - `get_runs_root(root)` – Resolves model data root from `--root` or `$CS_RUNS_DIR`
  - `resolve_ensemble_output(runs_root, cs_run)` – Finds `RUN_ERISWILL_*x100/ensemble_output` under root, preferring one containing `cs_run`

- **`run_tracking.py`** – LV1a + LV1b
  - Tobac cloud tracking (LV1a): features/tracks CSVs, segmentation mask NetCDF
  - Plume path extraction (LV1b): per-cell integrated/extreme/vertical path NetCDFs
  - Options: `--root`, `--cs-run`, `--domain`, `--flare-idx`, `--ref-idx`, `--threshold`, `--skip-tracking`, `--skip-paths`, `--overwrite`
  - Exits with clear message when root missing or no flare/ref pair found

- **`run_meteogram_zarr.py`** – LV2
  - Meteogram NetCDFs → single Zarr store
  - Uses `resolve_ensemble_output` for dynamic `RUN_ERISWILL_*` discovery; legacy fallback when `--root` unset
  - Options: `-r`/`--cs-run`, `--root`, `--out`, `--debug`, `--slurm`, `--overwrite`
  - Exits with clear message when no meteogram files found

- **`run_lv3.py`** – LV3
  - Process-rate datasets from meteogram Zarr
  - Options: `--cs-run`, `--zarr`, `--out`, `--exp-ids`, `--overwrite`

- **`run_chain.py`** – Top-level orchestrator
  - Runs LV1 → LV2 → LV3 in order; `--skip-tracking`, `--skip-meteogram`, `--skip-lv3` for partial runs
  - YAML/JSON config via `--config`: `model_data_root`, `output_root`, `domain`, `run_lv1a_tracking`, `run_lv1b_paths`, `run_lv2_meteogram`, `run_lv3_rates`, `overwrite`, `debug`
  - `model_data_root` supports `${CS_RUNS_DIR}` expansion; CLI flags override config
  - Early exit when root missing and LV1 not skipped
  - Writes `manifests/run_manifest.json` with stage status and timestamps

- **`config_example.yaml`** – Example config with all supported keys
- **`README.md`** – Usage, layout, config, library modules

