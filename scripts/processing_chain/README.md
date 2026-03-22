# Processing chain (LV0 → LV3)

Scripts under `scripts/processing_chain` run the full pipeline from raw COSMO-SPECS output to analysis-ready datasets. Outputs use a staged layout under `scripts/data/processed/<cs_run>/` with provenance on all NetCDF/Zarr artifacts.

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
    lv1_tracking/    # Tobac CSVs + *_nf_tobac_mask_xarray.nc (tracer: size-integrated ice number)
    lv1_paths/       # data_<cs_run>_<exp>_<integrated|extreme|vertical>_plume_path_nf_cell<N>.nc
    lv2_meteogram/   # Meteogram_<cs_run>_*.zarr
    lv3_rates/       # process_rates_exp<N>.nc
    manifests/       # run_manifest.json
```

Figure and table outputs from scripts and notebooks use a shared layout under **`output/gfx/`** (relative to repo root): PNGs in `output/gfx/png/<nn>/` (e.g. 01, 03, 04, 05, 06), MP4s flat in `output/gfx/mp4/`, LaTeX in `output/gfx/tex/<nn>/`, CSV in `output/gfx/csv/<nn>/`.

## LV1 tracking (Tobac)

Plume tracking follows the approach in Omanovic et al. 2024 and the archive notebook `01-Generate_Tobac_Tracking_from_3D_output.ipynb`:

- **Tracking field**: Difference of size-integrated ice number concentration, \(n_i^{\mathrm{feature}} = n_f^{\mathrm{flare}} - n_f^{\mathrm{ref}}\), over all diameter bins (variable `nf`). Units 1/L (per liter).
- **Detection threshold**: Values \(\geq 1\,\mathrm{L}^{-1}\) are considered for tracking (default `--threshold 1.0`). Multi-threshold list used by Tobac: 1, 10, 100.
- **Domain**: Flare-centred region extending two grid cells beyond the flare location; altitudes above 1500 m a.m.s.l.
- **Tobac**: Multi-threshold feature detection (`position_threshold="extreme"`, `target="maximum"`), linking with `v_max=100` m/s, segmentation for plume voxels. Outputs: features CSV, tracks CSV, features mask CSV, segmentation mask NetCDF. Path extraction produces integrated, extreme, and vertical per-cell plume path NetCDFs.

**Reference pairing (flare vs reference):** Flare and reference runs must differ **only** in emission-related parameters: `lflare`, `flare_emission`, `lflare_inp`, `lflare_ccn`, and optionally `flare_dn`, `flare_dp`, `flare_sig` (see run JSON `INPUT_ORG.sbm_par` and `INPUT_ORG.flare_sbm`). All other settings (e.g. `ishape`, domain, `runctl`) must match.

- **`--ref-idx -1` (default)**: Automatically select the reference experiment that matches the chosen flare in all non-emission parameters. If no such reference exists (e.g. flare has different `ishape` than any ref), discovery fails with a clear error.
- **`--ref-idx N`**: Use the N-th reference experiment by index (previous behaviour). Use when you have multiple refs and want to force a specific one.

## Levante / NumPy

On DKRZ Levante, the base mambaforge + user-site stack may have NumPy 2.x while `numexpr`, `bottleneck`, and `cftime` are built for NumPy 1.x, leading to `AttributeError: _ARRAY_API not found` or `ValueError: numpy.dtype size changed`. Use a NumPy 1.x–compatible environment:

- **Option A:** Create a dedicated env and pin NumPy:
  ```bash
  conda create -n polarcap python=3.10 -y && conda activate polarcap
  pip install -r requirements.txt
  pip install xarray pandas netCDF4 dask tobac iris
  ```
- **Option B:** In the existing env, downgrade NumPy and reinstall dependents:
  ```bash
  pip install 'numpy>=1.26,<2'
  pip install --force-reinstall numexpr bottleneck cftime
  ```

Then run `run_chain.py` (and other processing scripts) in that environment.

## Usage

Set `CS_RUNS_DIR` to the directory containing run subdirs (e.g. `RUN_ERISWILL_200x160x100/`, `RUN_ERISWILL_50x40x100/`). You can set it in the environment or have `scripts/ipython_startup/install.sh` prompt you and embed it in the IPython startup file.

For generated outputs, the launchers resolve the default output root in this order: explicit `--out`, then `$POLARCAP_OUTPUT_ROOT`, then the matching `RUN_ERISWILL_*x100/ensemble_output` path for the active run, else repo `scripts/data/processed` (absolute path, cwd-independent).

**Full chain (from repo root or `scripts/processing_chain`):**
```bash
python run_chain.py --cs-run cs-eriswil__20260123_180947
python run_chain.py --cs-run cs-eriswil__20260123_180947 --out /path/to/processed --overwrite
POLARCAP_OUTPUT_ROOT=/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_200x160x100/ensemble_output \
  python run_chain.py --cs-run cs-eriswil__20260123_180947
```

**Single stages:**
```bash
# LV1 only (--root defaults to $CS_RUNS_DIR if set)
python run_lv1_tracking.py --cs-run cs-eriswil__20260123_180947
python run_lv1_tracking.py --cs-run cs-eriswil__20260123_180947 --root /path/to/cosmo-specs-runs --out /path/to/processed

# LV2 only (--root defaults to $CS_RUNS_DIR; scripts resolve RUN_ERISWILL_*/ensemble_output/)
python run_lv2_meteogram_zarr.py -r cs-eriswil__20260123_180947

# LV3 only (needs LV2 Zarr)
python run_lv3_analysis.py --cs-run cs-eriswil__20260123_180947
```

**Publication-ready figures:**
```bash
# Run all curated paper figures from scripts/analysis/
python run_publication_figures.py

# Show available figure jobs
python run_publication_figures.py --list

# Run only a subset
python run_publication_figures.py --figures cloud_field_overview,plume_lagrangian
```

The runner currently dispatches the promoted figure scripts in `scripts/analysis/forcing/` and
`scripts/analysis/growth/`. Add future figure drivers by appending a new entry to
`PUBLICATION_FIGURES` in `run_publication_figures.py`.

The preferred spectral waterfall launcher now lives in `scripts/analysis/growth/run_spectral_waterfall.py`.
`scripts/processing_chain/run_spectral_waterfall.py` remains available as a compatibility entry point.

**Restart / skip:**
- By default, existing outputs are **not** overwritten. Use `--overwrite` to recompute.
- Use `--skip-tracking`, `--skip-meteogram`, `--skip-lv3` in `run_chain.py` to run only later stages.
- Use `--dry-run` to print commands without executing (useful to check config and paths).

**Compress M_*.nc and 3D_*.nc:** See `scripts/nc_compression/` for `compress_nc.sh` and SLURM usage.

## Metadata and provenance

All written datasets receive global attributes: `stage`, `processing_level`, `created_utc`, `git_commit`, `git_commit_short`, `cs_run`, `exp_label` (where applicable), `input_files` (or summary). Variable and coordinate attributes (units, long_name, description) are set from `metadata_config.json` and pipeline conventions.

## Config

Optional YAML/JSON config via `--config` (e.g. `config/processing_chain.yaml`). Keys: `model_data_root`, `output_root`, `domain`, `run_lv1a_tracking`, `run_lv1b_paths`, `run_lv2_meteogram`, `run_lv3_rates`, `overwrite`, `debug`. `model_data_root` supports `${CS_RUNS_DIR}` and should usually point to the parent containing multiple `RUN_ERISWILL_*x100/` trees; CLI flags override config. Leave `output_root` blank to inherit the launcher default.

## Library modules

- `src/utilities/processing_paths.py`: expand_path, get_runs_root, get_output_root, resolve_ensemble_output.
- `src/utilities/processing_metadata.py`: git commit, provenance_attrs, Zarr-safe attr normalization.
- `src/utilities/tracking_pipeline.py`: RunContext, discover_3d_runs, find_matching_reference, prep_tobac_input, run_tobac_tracking, extract_segmented_tracks_paths, run_plume_path_extraction; DEFAULT_TRACER_SPECS (nf), DEFAULT_THRESHOLD (1 L⁻¹), FLARE_REF_DIFF_KEYS.
- `src/utilities/process_rates.py`: PHYSICS_GROUPS, build_proc_vars, build_rates, build_spectral_rates, build_rates_dataset, build_rates_for_experiments.
- `src/utilities/meteogram_io.py`: build_meteogram_zarr (accepts optional `global_attrs` for provenance).

## Edge cases and robustness

- **cs_run format**: All stages expect `cs-eriswil__YYYYMMDD_HHMMSS`. `run_chain.py` validates once; LV2 also validates. Use consistent IDs.
- **Domain vs directory name**: LV1 builds paths as `RUN_ERISWILL_{domain}x100` (e.g. `50x40` → `50x40x100`). If your run lives under `RUN_ERISWILL_50x42x100`, pass `--domain 50x42`. LV2 finds any `RUN_ERISWILL_*x100` that contains the run.
- **Flare/ref indices**: Use `--flare-idx` to pick which flare experiment to track. Use `--ref-idx -1` (default) to auto-select the reference that matches the flare in all non-emission params; use `--ref-idx N` to force the N-th reference. If auto fails (no matching ref, e.g. different `ishape`), discovery returns no context with a clear error.
- **Missing run JSON**: LV1 needs `*.json` and `3D_*.nc` in the run dir. LV2 needs `{cs_run}.json` in the run dir and meteogram NetCDFs. Missing metadata yields an explicit error.
- **Empty Zarr / no experiments**: LV3 exits with a clear message if the Zarr has no `expname` dimension or `--exp-ids` is empty.
- **Multiple Zarrs**: LV3 prefers a non-`_dbg` Zarr when both exist under `lv2_meteogram/`.
- **Config paths**: `model_data_root` and `output_root` in YAML support `$VAR` and `~`; they are expanded before use. Missing `RUN_ERISWILL_*x100/ensemble_output` under the data root is reported instead of using a non-existent fallback path.
- **Remote output policy**: On lev, new generated products should go to `/work/.../ensemble_output`, not `/home/.../processed`. The default resolver supports both low-resolution and high-resolution ensemble trees; set `$POLARCAP_OUTPUT_ROOT` explicitly when you want to force a specific output tree.
- **Failure and manifest**: If a stage fails, `run_chain.py` writes a partial manifest so you can see which stage ran.
- **Root required for LV1 and LV2**: Set `CS_RUNS_DIR` or pass `--root` (or `model_data_root` in config); otherwise the script exits. LV2 accepts two layouts: (1) model data root with `RUN_ERISWILL_*x100/ensemble_output/<cs_run>/`, or (2) flat meteogram root with `<cs_run>/` dirs containing the run JSON and M_*.nc.
- **Spectral waterfall** (`run_spectral_waterfall.py`): Time window and first frame use `time.seed_start` from the process_budget YAML (fallback `time.seed_start_utc`); ensure that key is set for the desired start time. PSD strip y-axis uses configured `psd_ylim_W`/`psd_ylim_F` with log-spaced major/minor ticks and grid. Numeric CLI args (e.g. `--xlim-w`, `--ylim-w`, `--psd-ylim-w`, `--psd-ylim-f`) take MIN MAX; for exponents use no decimal in mantissa: `--psd-ylim-w 1e-2 1e8` (avoid `1.0e-2` in shell).

Publication-facing diagnostics (registry, forcing, initiation, growth, impacts, synthesis) live in `scripts/analysis/`; see that directory’s README and `docs/methods/figure_script_traceability.md`.
