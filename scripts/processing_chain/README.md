# Processing chain (LV0 → LV3)

Scripts under `scripts/processing_chain` run the full pipeline from raw COSMO-SPECS output to analysis-ready datasets. Outputs use a staged layout under `processed/<cs_run>/` with provenance on all NetCDF/Zarr artifacts.

## Stages

| Stage | Script | Inputs | Outputs |
|-------|--------|--------|---------|
| **LV1a** | `run_tracking.py` | 3D NetCDFs, run JSON | `lv1_tracking/`: tobac features/tracks CSVs, segmentation mask NetCDF |
| **LV1b** | `run_tracking.py` | LV1a + 3D fields | `lv1_paths/`: per-cell plume path NetCDFs (integrated, extreme, vertical) |
| **LV2** | `run_meteogram_zarr.py` | Meteogram NetCDFs `M_??_??_*.nc` | `lv2_meteogram/`: single Zarr store |
| **LV3** | `run_lv3.py` | LV2 Zarr | `lv3_rates/`: process-budget rate NetCDFs per experiment |

## Layout

```
processed/
  <cs_run>/
    lv1_tracking/    # Tobac CSVs + *_tobac_mask_xarray.nc
    lv1_paths/       # data_<cs_run>_<exp>_<integrated|extreme|vertical>_plume_path_<qi|qs>_cell<N>.nc
    lv2_meteogram/   # Meteogram_<cs_run>_*.zarr
    lv3_rates/       # process_rates_exp<N>.nc
    manifests/       # run_manifest.json
```

## Usage

Set `CS_RUNS_DIR` to the directory containing run subdirs (e.g. `RUN_ERISWILL_200x160x100/`, `RUN_ERISWILL_50x42x100/`). You can set it in the environment or have `scripts/ipython_startup/install.sh` prompt you and embed it in the IPython startup file.

**Full chain (from repo root or `scripts/processing_chain`):**
```bash
python run_chain.py --cs-run cs-eriswil__20260123_180947
python run_chain.py --cs-run cs-eriswil__20260123_180947 --out /path/to/processed --overwrite
```

**Single stages:**
```bash
# LV1 only (--root defaults to $CS_RUNS_DIR if set)
python run_tracking.py --cs-run cs-eriswil__20260123_180947 --out processed
python run_tracking.py --cs-run cs-eriswil__20260123_180947 --root /path/to/cosmo-specs-runs --out processed

# LV2 only (--root defaults to $CS_RUNS_DIR; scripts resolve RUN_ERISWILL_*/ensemble_output/)
python run_meteogram_zarr.py -r cs-eriswil__20260123_180947 --out processed

# LV3 only (needs LV2 Zarr)
python run_lv3.py --cs-run cs-eriswil__20260123_180947 --out processed
```

**Restart / skip:**
- By default, existing outputs are **not** overwritten. Use `--overwrite` to recompute.
- Use `--skip-tracking`, `--skip-meteogram`, `--skip-lv3` in `run_chain.py` to run only later stages.

## Metadata and provenance

All written datasets receive global attributes: `stage`, `processing_level`, `created_utc`, `git_commit`, `git_commit_short`, `cs_run`, `exp_label` (where applicable), `input_files` (or summary). Variable and coordinate attributes (units, long_name, description) are set from `metadata_config.json` and pipeline conventions.

## Config

Optional YAML/JSON config via `--config` (e.g. `config_example.yaml`). Keys: `model_data_root`, `output_root`, `domain`, `run_lv1a_tracking`, `run_lv1b_paths`, `run_lv2_meteogram`, `run_lv3_rates`, `overwrite`, `debug`. `model_data_root` supports `${CS_RUNS_DIR}`; CLI flags override config.

## Library modules

- `src/utilities/processing_metadata.py`: git commit, provenance_attrs, Zarr-safe attr normalization.
- `src/utilities/tracking_pipeline.py`: RunContext, discover_3d_runs, prep_tobac_input, run_tobac_tracking, extract_segmented_tracks_paths, run_plume_path_extraction.
- `src/utilities/process_rates.py`: PHYSICS_GROUPS, build_proc_vars, build_rates, build_spectral_rates, build_rates_dataset, build_rates_for_experiments.
- `src/utilities/meteogram_io.py`: build_meteogram_zarr (accepts optional `global_attrs` for provenance).
