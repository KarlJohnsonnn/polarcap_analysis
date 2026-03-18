# Publication analysis scripts

Thin drivers for manuscript-critical diagnostics. Each script calls `src/utilities` and writes tables or figures to a stable path.

- **registry/** — Experiment metadata, availability, flare–reference pairing.
- **forcing/** — Forcing and setup summaries.
- **initiation/** — First-ice onset and freezing-pathway metrics.
- **growth/** — Plume ridge, PSD stats, spectral diagnostics, growth-summary joins.
- **impacts/** — Liquid depletion, radar/precipitation-facing metrics.
- **synthesis/** — Claim register, figure inventory.

Outputs go to `data/registry/` or paths documented in each script. Every paper figure should map to a script and output file here.

Current promoted products:

- `registry/build_analysis_registry.py` -> `data/registry/analysis_registry.csv`, `data/registry/paper_core_subset.csv`
- `forcing/run_cloud_field_overview.py` -> `scripts/processing_chain/output/01/cloud_field_overview_mass_profiles_steps_symlog_<exp>_<range>.png`
- `forcing/run_cloud_phase_budget_table.py` -> `scripts/processing_chain/output/01/tables/cloud_phase_budget_summary_<exp>_<range>.csv`, `.tex`
- `growth/run_plume_lagrangian_evolution.py` -> `scripts/processing_chain/output/03/figure12_ensemble_mean_plume_path_foo.png`
- `growth/run_psd_waterfall.py` -> `scripts/processing_chain/output/04/figure13_psd_alt_time_<kind>_<run_id>.png`, `scripts/processing_chain/output/04/stats/figure13_psd_stats_<kind>_<run_id>.csv`, `scripts/processing_chain/output/04/tables/figure13_psd_stats_<kind>_<run_id>.tex`
- `growth/run_psd_stats.py` -> `data/registry/psd_stats.csv`
- `growth/run_growth_summary.py` -> `data/registry/growth_summary.csv`
- `growth/run_ridge_metrics.py` -> `data/registry/ridge_metrics.csv`
- `growth/run_spectral_waterfall.py` -> `scripts/processing_chain/output/05/<cs_run>/...`
- `initiation/run_first_ice_metrics.py` -> `data/registry/first_ice_onset_metrics.csv`
- `initiation/run_initiation_process_summary.py` -> `data/registry/initiation_process_fractions.csv`

`growth/run_process_dominance.py` and `impacts/` remain deferred on purpose until the registry, ridge, PSD, and initiation products are stable.
