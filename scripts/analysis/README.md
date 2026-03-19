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
- `forcing/run_cloud_field_overview.py` -> `output/gfx/png/01/cloud_field_overview_mass_profiles_steps_symlog_<exp>_<range>.png`
- `forcing/run_cloud_phase_budget_table.py` -> `output/gfx/csv/01/cloud_phase_budget_summary_<exp>_<range>.csv`, `output/gfx/tex/01/cloud_phase_budget_summary_<exp>_<range>.tex`
- `growth/run_plume_lagrangian_evolution.py` -> `output/gfx/png/03/figure12_ensemble_mean_plume_path_foo.png`
- `growth/run_psd_waterfall.py` -> `output/gfx/png/04/figure13_psd_alt_time_<kind>_<run_id>.png`, `output/gfx/csv/04/figure13_psd_stats_<kind>_<run_id>.csv`, `output/gfx/tex/04/figure13_psd_stats_<kind>_<run_id>.tex`
- `growth/run_psd_stats.py` -> `data/registry/psd_stats.csv`
- `growth/run_growth_summary.py` -> `data/registry/growth_summary.csv`
- `growth/run_ridge_metrics.py` -> `data/registry/ridge_metrics.csv`
- `growth/run_spectral_waterfall.py` -> `output/gfx/...` plus promoted gallery assets through the publication wrapper
- `initiation/run_first_ice_metrics.py` -> `data/registry/first_ice_onset_metrics.csv`
- `initiation/run_initiation_process_summary.py` -> `data/registry/initiation_process_fractions.csv`
- `initiation/run_pilot_process_attribution.py` -> `data/registry/pilot_process_attribution_matrix.csv`
- `synthesis/run_paper_tables.py` -> compiled manuscript tables in `data/registry/paper_tables/*.csv` and `article_draft/PolarCAP/tables/*.tex`
- `synthesis/run_claim_summary.py` -> `data/registry/claim_register.csv`
- `synthesis/run_figure_inventory.py` -> `data/registry/figure_inventory.csv`

`growth/run_process_dominance.py` and `impacts/` remain deferred on purpose until the registry, ridge, PSD, and initiation products are stable.

## Refresh the paper tables

Recommended order when new promoted outputs arrive:

1. `python scripts/analysis/registry/build_analysis_registry.py`
2. `python scripts/analysis/growth/run_psd_stats.py`
3. `python scripts/analysis/growth/run_ridge_metrics.py`
4. `python scripts/analysis/growth/run_growth_summary.py`
5. `python scripts/analysis/initiation/run_first_ice_metrics.py`
6. `python scripts/analysis/initiation/run_initiation_process_summary.py`
7. `python scripts/analysis/initiation/run_pilot_process_attribution.py`
8. `python scripts/analysis/synthesis/run_paper_tables.py`
9. `python scripts/analysis/synthesis/run_claim_summary.py`
10. `python scripts/analysis/synthesis/run_figure_inventory.py`

The paper draft then loads the synchronized `.tex` files from
`article_draft/PolarCAP/tables/` via `\input{tables/...}`.
