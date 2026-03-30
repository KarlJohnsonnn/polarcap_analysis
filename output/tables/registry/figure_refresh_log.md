# Figure Refresh Log

Date: 2026-03-27

Environment:

- Figure regeneration commands were run in `pcpaper_env`.
- Outputs were written to the legacy figure tree under `output/gfx/` and are referenced from `output/tables/report/analysis_refresh_report.{md,html}`.

## Regenerated Figures

- `output/gfx/png/01/cloud_field_overview_mass_profiles_steps_symlog_20260304110446_ALLBB.png`
  - regenerated from `scripts/analysis/forcing/run_cloud_field_overview.py`
  - size: `336955` bytes
- `output/gfx/png/03/figure12_ensemble_mean_plume_path_foo.png`
  - regenerated from `scripts/analysis/growth/run_plume_lagrangian_evolution.py`
  - size: `1179813` bytes
- `output/gfx/png/04/figure13_psd_alt_time_mass_20260121131632.png`
  - regenerated from `scripts/analysis/growth/run_psd_waterfall.py`
  - size: `741808` bytes
- `output/gfx/png/04/figure13_psd_alt_time_number_20260121131632.png`
  - regenerated from `scripts/analysis/growth/run_psd_waterfall.py`
  - size: `756875` bytes

## PSD Suite

- `scripts/analysis/growth/run_psd_waterfall.py` regenerated `12` figure13 PNG files in `output/gfx/png/04/`.
- Total size of the current `figure13_psd_alt_time_*.png` set: `8613799` bytes.
- The selected report previews use run `20260121131632` because it is the featured run in `output/tables/paper/tab_psd_stats_selected.csv`.

## Interactive Artifact

- `output/gfx/html/05/cs-eriswil__20260304_110254/ridge_growth_Q_stn0_interactive.html`
  - existing interactive artifact referenced from the refresh report
  - size: `190138` bytes

## Notes

- The cloud-overview figure required the canonical local meteogram and ExtPar path fixes before regeneration succeeded.
- The plume-lagrangian figure required the plume loader to support the canonical nested `data/processed/<cs_run>/lv1_paths/` layout.
- The PSD waterfall suite regenerated successfully after the plume-loader fix.
- No new MP4 spectral-waterfall animation was generated in this pass; the report links the existing interactive HTML artifact instead.
