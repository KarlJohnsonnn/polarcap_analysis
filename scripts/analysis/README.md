# Publication analysis scripts

Thin drivers for manuscript-critical diagnostics. Each script calls `src/utilities` and writes canonical tables and statistics to `output/tables/`, while keeping legacy compatibility copies where the draft pipeline still expects them.

- **registry/** — Experiment metadata, availability, flare–reference pairing.
- **forcing/** — Forcing and setup summaries.
- **initiation/** — First-ice onset and freezing-pathway metrics.
- **growth/** — Plume ridge, PSD stats, spectral diagnostics, growth-summary joins.
- **impacts/** — Liquid depletion, radar/precipitation-facing metrics.
- **synthesis/** — Claim register, figure inventory.

Registry-style CSV outputs go to `output/tables/registry/` (mirrored to `data/registry/` for compatibility), and compiled manuscript tables go to `output/tables/paper/` (mirrored to `data/registry/paper_tables/`). Every paper figure should map to a script and output file here.

Current promoted products:

- `registry/build_analysis_registry.py` -> `output/tables/registry/analysis_registry.csv`
- `forcing/run_cloud_field_overview.py` -> `output/gfx/png/01/cloud_field_overview_mass_profiles_steps_symlog_<exp>_<range>.png`
- `forcing/run_cloud_phase_budget_table.py` -> `output/tables/phase_budget/cloud_phase_budget_summary_<exp>_<range>.csv`, `output/tables/phase_budget/cloud_phase_budget_summary_<exp>_<range>.tex`
- `growth/run_plume_lagrangian_evolution_slim.py` -> `output/gfx/png/03/figure12_ensemble_mean_plume_path_slim.png`
- `growth/run_psd_waterfall.py` -> `output/gfx/png/04/figure13_psd_alt_time_<kind>_<run_id>.png`, `output/gfx/csv/04/figure13_psd_stats_<kind>_<run_id>.csv`, `output/gfx/tex/04/figure13_psd_stats_<kind>_<run_id>.tex`
- `growth/run_psd_stats.py` -> `output/tables/registry/psd_stats.csv`
- `growth/run_growth_summary.py` -> `output/tables/registry/growth_summary.csv`
- `growth/run_ridge_metrics.py` -> `output/tables/registry/ridge_metrics.csv`
- `growth/run_spectral_waterfall.py` -> `output/gfx/...` plus promoted gallery assets through the publication wrapper
- `initiation/run_first_ice_metrics.py` -> `output/tables/registry/first_ice_onset_metrics.csv`
- `initiation/run_initiation_process_summary.py` -> `output/tables/registry/initiation_process_fractions.csv`
- `initiation/run_pilot_process_attribution.py` -> `output/tables/registry/pilot_process_attribution_matrix.csv`
- `synthesis/run_paper_tables.py` -> compiled manuscript tables in `output/tables/paper/*.csv` and `article_draft/PolarCAP/tables/*.tex`
- `synthesis/run_claim_summary.py` -> `output/tables/registry/claim_register.csv`
- `synthesis/run_figure_inventory.py` -> `output/tables/registry/figure_inventory.csv`

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
