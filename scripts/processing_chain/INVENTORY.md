# Notebook → pipeline inventory

## LV1 (01-Generate_Tobac_Tracking_from_3D_output.ipynb)

### Compute (migrate to `tracking_pipeline` + `model_helpers`)
- **Run discovery**: `cs_runs` list, paths from `model_data_root`/`RUN_ERISWILL_{dom}x100/ensemble_output/{cs_run}/`, JSON meta, `filelist_3d_nc`, flare/ref experiment selection by `lflare` and indices.
- **Domain**: `reduced_domain` from `attrs` (flare_lat/lon, resolution_deg, max_altitude).
- **Load 3D**: `make_3d_preprocessor`, `xr.open_mfdataset`, `update_dataset_metadata`, `convert_units_3d`, select `['t','nf']` or full spec for paths.
- **prep_tobac_input**: flare − ref field, mask below threshold; units 1/dm3. (Move to `model_helpers` or `tracking_pipeline`.)
- **Tobac**: build `parameter_features`, `tobac.feature_detection_multithreshold`, `tobac.linking_trackpy`, `tobac.segmentation.segmentation`; write features/tracks/mask CSVs and segmentation NetCDF.
- **LV1b path extraction**: `extract_segmented_tracks(ds, tracks, segm_mask, pre_track_minutes, delta_t, type='integrated'|'extreme'|'vertical')` — notebook version uses path-based coords and integrated/extreme/vertical modes; write per-cell NetCDFs.

### Plotting (exclude from chain)
- All `fig1`, `fig2`, `plt.subplots`, `plot_3d_col_wrap`, `pmmask.plot`, `fig.savefig` cells (plume voxel count plot, tracking top view).

---

## LV3 (05-process-budget.ipynb)

### Compute (migrate to `process_rates`)
- **Load Zarr**: open Meteogram Zarr, get `ds`, `ds_exp` per `PLOT_EXP_IDS`, assign `height_level`/`height_level2` from HMLd/HHLd.
- **Process groups**: `PHYSICS_GROUPS` dict (group → list of (BASE_NAME, SPECTRUM, KIND)); `classify_tendency`; build `proc_vars` from `ds_exp` SUM_ variables.
- **Rates**: `bulk_rate(varname, bin_slice, kind)`, `spectral_rate(varname, kind)` (use global `ds_exp`, `rho` in notebook → pass explicitly).
- **build_rates(kind, bin_slice, spectrum)**: aggregate per process group from `proc_vars`.
- **build_spectral_rates(kind, spectrum)**: same but keep bins dimension.
- **build_rates_dataset(R, eid, ds_exp, config)**: stack rate dicts into xr.Dataset, add process labels/descriptions from metadata_config, add spectral concentrations, set attrs (title, created_utc, processing_commit, exp_id, sign_convention, etc.).
- **IMMERSION_FREEZING** cross-assign: rates_N_ice[IF] = -rates_N_liq[IF], same for Q.
- **Per-experiment loop**: build `rates_by_exp`, `rates_ds_by_exp`; write or return datasets.

### Plotting (exclude from chain)
- All View A/B/C/D/E cells (stacked area, stacked bars, dominance maps, spectral waterfall, Hovmöller), heat-release contour, TKE overlay, any `plt`, `fig.savefig`, `apply_publication_style`.

---

## Shared / already in src
- `make_3d_preprocessor`, `convert_units_3d`, `define_bin_boundaries`, `update_dataset_metadata`: `model_helpers` / `namelist_metadata`.
- `tobac_5dspecs`, `extract_segmented_tracks` (signature differs from notebook’s `extract_segmented_tracks` with `type=` and `segm_mask`): unify in `tracking_pipeline`.
- `build_meteogram_zarr`, `add_coords_and_metadata`: `meteogram_io`.
- Process display names: notebook calls `get_process_display_name(proc, spectrum)` (fallback to proc); implement in `process_rates` using group name + spectrum suffix or metadata_config.
