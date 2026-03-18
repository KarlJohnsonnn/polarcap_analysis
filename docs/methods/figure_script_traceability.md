# Figure–script traceability

Every manuscript-critical figure or table should map to:

1. **Script** — in `scripts/processing_chain/` or `scripts/analysis/`
2. **Config** — YAML or CLI args used
3. **Output path** — stable file name under a known directory

Example mapping:

| Figure / table | Script | Config / args | Output |
|----------------|--------|---------------|--------|
| Cloud field overview | `scripts/analysis/forcing/run_cloud_field_overview.py` | `notebooks/config/process_budget.yaml` + optional CLI overrides | `scripts/processing_chain/output/01/cloud_field_overview_mass_profiles_steps_symlog_<exp>_<range>.png` |
| Cloud phase budget summary | `scripts/analysis/forcing/run_cloud_phase_budget_table.py` | same cloud-overview CLI/config inputs; phase windows from `cloud_field_overview` settings | `scripts/processing_chain/output/01/tables/cloud_phase_budget_summary_<exp>_<range>.csv` + `.tex` |
| Plume-lagrangian evolution | `scripts/analysis/growth/run_plume_lagrangian_evolution.py` | CLI flags for processed root, HOLIMO file, and plume kind | `scripts/processing_chain/output/03/figure12_ensemble_mean_plume_path_foo.png` |
| PSD waterfall suite | `scripts/analysis/growth/run_psd_waterfall.py` | CLI flags for processed root, plot kinds, run labels, and optional HOLIMO overlay | `scripts/processing_chain/output/04/figure13_psd_alt_time_<kind>_<run_id>.png` + `scripts/processing_chain/output/04/stats/figure13_psd_stats_<kind>_<run_id>.csv` + `scripts/processing_chain/output/04/tables/figure13_psd_stats_<kind>_<run_id>.tex` |
| Plume path time–diameter | `run_lv1_tracking` + plume_path_plot | domain, cs_run | `notebooks/plume_path/output/` or processing_chain output |
| Spectral waterfall | `scripts/analysis/growth/run_spectral_waterfall.py` | `process_budget_NQall.yaml` | `scripts/processing_chain/output/05/<cs_run>/...` |
| Experiment registry | `build_experiment_registry.py` | — | `data/registry/experiment_registry.csv` |
| Flare–reference pairs | `build_reference_pairs.py` | — | `data/registry/flare_reference_pairs.csv` |
| Analysis-ready merged registry | `build_analysis_registry.py` | availability + experiment + pair CSV inputs | `data/registry/analysis_registry.csv` |
| Paper core subset | `build_analysis_registry.py` | merged registry filter flags | `data/registry/paper_core_subset.csv` |
| PSD statistics inventory | `run_psd_stats.py` | promoted `scripts/processing_chain/output/04/stats/` CSV directory | `data/registry/psd_stats.csv` |
| Ridge growth summary | `run_ridge_metrics.py` | promoted PSD stats CSV | `data/registry/ridge_metrics.csv` |
| Joined growth summary | `run_growth_summary.py` | ridge metrics CSV + promoted PSD stats CSV | `data/registry/growth_summary.csv` |
| First excess-ice / INP onset | `run_first_ice_metrics.py` | local LV2 Zarr, merged registry | `data/registry/first_ice_onset_metrics.csv` |
| Early initiation process fractions | `run_initiation_process_summary.py` | local LV3 rates, merged registry | `data/registry/initiation_process_fractions.csv` |
 
Update this table as new figures are added. The repository is published with the paper; reviewers should be able to reproduce each result from the named script and config.
