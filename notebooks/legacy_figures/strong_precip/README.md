# Strong-Precip Legacy Figure Workflow

This folder contains a cleaned workflow migrated from `notebooks/05-Visualise-strong-precip.ipynb`.
It keeps only manuscript-relevant plotting paths for strong-precip diagnostics and plume-path panels.

## Contents

- `strong_precip_plot_workflow.ipynb`: reproducible plotting notebook.
- `figures/`: output directory for exported plots.

## Inputs

- COSMO-SPECS run root (`run_root`) with:
  - `ensemble_output/<run_id>/3D_*.nc`
  - `ensemble_output/<run_id>/*.json` metadata
  - `COS_in/extPar_Eriswil_<resolution>.nc` extpar file
- Internal utilities from `src/utilities`:
  - `utilities.model_helpers.fetch_3d_data`
  - `utilities.model_helpers.convert_units_3d`
  - `utilities.namelist_metadata.update_dataset_metadata`

## Run Order

1. Open `strong_precip_plot_workflow.ipynb`.
2. Ensure `REMOTE_COSMO_SPECS_RUN_OUTPUT` is exported in your shell rc file (`~/.zshrc` local or `~/.bashrc` server).
3. Update the run-config cell (`EXPERIMENTS`, `OUTPUT_DIR`, time windows) if needed.
4. Run cells top-to-bottom.
5. Verify figures are written to `figures/`.

## Output Naming

The notebook writes standardized names:

- `strong_precip_meteogram_<location>.png`
- `strong_precip_qfw_logtime_triptych.png`
- `strong_precip_plume_path_<variable>.png`

## Notes

- The workflow uses local relative output paths only.
- Heavy data loading is lazy with dask-backed xarray operations.

## Performance and Compute Diagnostics

For reusable Dask/SLURM diagnostics and scaling guidance, see:

- `src/utilities/README.md`
