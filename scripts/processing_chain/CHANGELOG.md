# Changelog

All notable changes to the processing chain (LV0 → LV3) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **Meteogram Zarr Rechunking on Open** – Added optional in-memory rechunking for meteogram Zarrs when opening (`src/utilities/process_budget_data.py`) to optimize compute for the current machine (server vs laptop). Controlled via `zarr.rechunk_on_open` in the config file.
- **Publication figure runner** – Added `scripts/processing_chain/run_publication_figures.py` to dispatch the curated publication-ready figure scripts from `scripts/analysis/` from one processing-chain entry point.
- **PSD structured stats export** – `scripts/analysis/growth/run_psd_waterfall.py` now writes machine-readable window statistics CSVs alongside the figure13 LaTeX tables, and `run_growth_summary.py` joins PSD and ridge metrics into one registry product.

### Changed
- **Meteogram Zarr Chunk Defaults** – Updated `build_meteogram_zarr` (`src/utilities/meteogram_io.py`) default chunk sizes to be optimal for time-frame and single-station analysis: `time=100`, `station=1`, `bins=30` (added `target_bins_chunk` parameter).
- **Compression** – Simplified `scripts/nc_compression/` to `compress.sh`, `archive.sh`, and `run_compess_and_archive.sh`.
- **`_paths.py`** → `src/utilities/processing_paths.py` – Path resolution moved into utilities for reuse by notebooks and other scripts.
- **Output root defaults** – `run_chain.py`, `run_lv1_tracking.py`, `run_lv2_meteogram_zarr.py`, and `run_lv3_analysis.py` now resolve output roots via explicit `--out`, then `$POLARCAP_OUTPUT_ROOT`, then the matching `RUN_ERISWILL_*x100/ensemble_output` tree for the run, else local `processed`.
- **Spectral waterfall entrypoint** – The preferred launcher is now `scripts/analysis/growth/run_spectral_waterfall.py`; the processing-chain script stays in place for compatibility.

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

