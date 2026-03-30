# Processing Refresh Log

Date: 2026-03-27

Environment:

- Processing and rebuild commands were run in `pcpaper_env`.
- Canonical manuscript-facing outputs now live under `output/tables/`.

## Processed Root

- `data/processed/` was refreshed from these source roots:
- `/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output`
- `/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_200x160x100/ensemble_output`
- `/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output`
- `output/tables/registry/processed_sync_report.md` now reports 18 scanned runs and 67 stage rows.
- Current processed-root status summary: 28 `already_present`, 39 `missing`.
- Missing remote LV3 rates for `cs-eriswil__20260304_110254` were regenerated locally into `data/processed/cs-eriswil__20260304_110254/lv3_rates/`.
- The regenerated `20260304` LV3 stage wrote 5 local NetCDF files and occupies about `19G`.

## Registry Outputs

- `availability_check.csv`: 9 rows
- `analysis_registry.csv`: 33 rows
- `paper_core_subset.csv`: 29 rows
- `first_ice_onset_metrics.csv`: 23 rows
- `initiation_process_fractions.csv`: 260 rows
- `pilot_process_attribution_matrix.csv`: 17 rows
- `psd_stats.csv`: 60 rows
- `ridge_metrics.csv`: 5 rows
- `growth_summary.csv`: 5 rows
- `claim_register.csv`: 8 rows
- `figure_inventory.csv`: 11 rows

## Paper Tables

- `tab_experiment_matrix.csv`: 29 rows
- `tab_initiation_metrics.csv`: 9 rows
- `tab_process_attribution.csv`: 17 rows
- `tab_growth_summary.csv`: 5 rows
- `tab_psd_stats_selected.csv`: 5 rows
- `tab_phase_budget_summary.csv`: 3 rows
- `tab_phase_budget_long.csv`: 42 rows

## Validation

- All 7 compiled paper tables exist in both `output/tables/paper/` and `data/registry/paper_tables/`.
- Matching canonical and legacy CSV file sizes were verified for all 7 compiled paper tables.
- The current paper subset contains 29 included rows, 17 matched flare/reference rows, 11 rows with local LV1, 15 rows with local LV2, and 15 rows with local LV3.
- The promoted cloud phase-budget tables for `20260304110446` and range `ALLBB` were rebuilt successfully after fixing local LV3 and ExtPar path resolution.

## Remaining Gaps

- `output/tables/registry/processed_sync_report.md` still lists 39 missing stage rows across runs that do not yet have complete promoted processed outputs.
- The remaining missing stages are concentrated in older runs that still lack LV1 tracking, LV2 meteograms, LV2 3D products, or LV3 rates.
- The paper-facing growth summary still covers 5 runs because only those runs currently have compatible local LV1 plume-path products.

## Storage

- `data/processed/cs-eriswil__20260304_110254/lv3_rates`: `19G`
- `output/tables`: `381K`
- `cwq` on `/work`: `17.98T / 25T`
