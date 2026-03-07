# Processing chain (LV0 → LV3)

Scripts under `scripts/processing_chain` run the full pipeline from raw COSMO-SPECS output to analysis-ready datasets. Outputs use a staged layout under `processed/<cs_run>/` with provenance on all NetCDF/Zarr artifacts.

## Stages

| Stage | Script | Inputs | Outputs |
|-------|--------|--------|---------|
| **LV1a** | `run_lv1_tracking.py` | 3D NetCDFs, run JSON | `lv1_tracking/`: tobac features/tracks CSVs, segmentation mask NetCDF |
| **LV1b** | `run_lv1_tracking.py` | LV1a + 3D fields | `lv1_paths/`: per-cell plume path NetCDFs (integrated, extreme, vertical) |
| **LV2** | `run_lv2_meteogram_zarr.py` | Meteogram NetCDFs `M_??_??_*.nc` | `lv2_meteogram/`: single Zarr store |
| **LV3** | `run_lv3_analysis.py` | LV2 Zarr | `lv3_rates/`: process-budget rate NetCDFs per experiment |

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
python run_lv1_tracking.py --cs-run cs-eriswil__20260123_180947 --out processed
python run_lv1_tracking.py --cs-run cs-eriswil__20260123_180947 --root /path/to/cosmo-specs-runs --out processed

# LV2 only (--root defaults to $CS_RUNS_DIR; scripts resolve RUN_ERISWILL_*/ensemble_output/)
python run_lv2_meteogram_zarr.py -r cs-eriswil__20260123_180947 --out processed

# LV3 only (needs LV2 Zarr)
python run_lv3_analysis.py --cs-run cs-eriswil__20260123_180947 --out processed
```

**Restart / skip:**
- By default, existing outputs are **not** overwritten. Use `--overwrite` to recompute.
- Use `--skip-tracking`, `--skip-meteogram`, `--skip-lv3` in `run_chain.py` to run only later stages.
- Use `--dry-run` to print commands without executing (useful to check config and paths).

**Compress M_*.nc and 3D_*.nc (large data on server):**
```bash
# Interactive (local or login node)
./compress_nc.sh compress /path/to/ensemble_output nc_run.tar.zst
PV_INTERVAL=1 ./compress_nc.sh compress .   # progress updates every 1s

# SLURM job on Levante (queue compression for large datasets)
# Submit from scripts/processing_chain/ so both compress_nc.sh and compress_nc_slurm.sh are found
cd scripts/processing_chain && sbatch compress_nc_slurm.sh /path/to/ensemble_output nc_run.tar.zst
cd scripts/processing_chain && sbatch compress_nc_slurm.sh .   # dir=., auto archive name
COMPRESS_NC_OVERWRITE=1 sbatch compress_nc_slurm.sh . my_run.tar.zst
```
Logs: `logs_compress_nc/<jobid>.out`, `logs_compress_nc/<jobid>.err`. Edit `#SBATCH` in `compress_nc_slurm.sh` for account, time, memory.

## Metadata and provenance

All written datasets receive global attributes: `stage`, `processing_level`, `created_utc`, `git_commit`, `git_commit_short`, `cs_run`, `exp_label` (where applicable), `input_files` (or summary). Variable and coordinate attributes (units, long_name, description) are set from `metadata_config.json` and pipeline conventions.

## Config

Optional YAML/JSON config via `--config` (e.g. `config_example.yaml`). Keys: `model_data_root`, `output_root`, `domain`, `run_lv1a_tracking`, `run_lv1b_paths`, `run_lv2_meteogram`, `run_lv3_rates`, `overwrite`, `debug`. `model_data_root` supports `${CS_RUNS_DIR}`; CLI flags override config.

## Library modules

- `src/utilities/processing_paths.py`: get_runs_root, resolve_ensemble_output.
- `src/utilities/processing_metadata.py`: git commit, provenance_attrs, Zarr-safe attr normalization.
- `src/utilities/tracking_pipeline.py`: RunContext, discover_3d_runs, prep_tobac_input, run_tobac_tracking, extract_segmented_tracks_paths, run_plume_path_extraction.
- `src/utilities/process_rates.py`: PHYSICS_GROUPS, build_proc_vars, build_rates, build_spectral_rates, build_rates_dataset, build_rates_for_experiments.
- `src/utilities/meteogram_io.py`: build_meteogram_zarr (accepts optional `global_attrs` for provenance).

## Edge cases and robustness

- **cs_run format**: All stages expect `cs-eriswil__YYYYMMDD_HHMMSS`. `run_chain.py` validates once; LV2 also validates. Use consistent IDs.
- **Domain vs directory name**: LV1 builds paths as `RUN_ERISWILL_{domain}x100` (e.g. `50x40` → `50x40x100`). If your run lives under `RUN_ERISWILL_50x42x100`, pass `--domain 50x42`. LV2 finds any `RUN_ERISWILL_*x100` that contains the run.
- **Flare/ref indices**: If you have multiple flare or reference experiments, use `--flare-idx` and `--ref-idx` in `run_lv1_tracking.py`. Out-of-range indices cause discovery to return no context (clear error).
- **Missing run JSON**: LV1 needs `*.json` and `3D_*.nc` in the run dir. LV2 needs `{cs_run}.json` in the run dir and meteogram NetCDFs. Missing metadata yields an explicit error.
- **Empty Zarr / no experiments**: LV3 exits with a clear message if the Zarr has no `expname` dimension or `--exp-ids` is empty.
- **Multiple Zarrs**: LV3 prefers a non-`_dbg` Zarr when both exist under `lv2_meteogram/`.
- **Config paths**: `model_data_root` and `output_root` in YAML support `$VAR` and `~`; they are expanded before use. Missing `RUN_ERISWILL_*x100/ensemble_output` under the data root is reported instead of using a non-existent fallback path.
- **Failure and manifest**: If a stage fails, `run_chain.py` writes a partial manifest so you can see which stage ran.
- **Root required for LV1 and LV2**: Set `CS_RUNS_DIR` or pass `--root` (or `model_data_root` in config); otherwise the script exits. LV2 accepts two layouts: (1) model data root with `RUN_ERISWILL_*x100/ensemble_output/<cs_run>/`, or (2) flat meteogram root with `<cs_run>/` dirs containing the run JSON and M_*.nc.
