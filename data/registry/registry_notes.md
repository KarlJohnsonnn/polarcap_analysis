# Registry notes

- Canonical registry outputs now live in `output/tables/registry/` and are mirrored into `data/registry/` for backward compatibility with older scripts and notebooks.
- **availability_check.csv** — Produced by `scripts/analysis/registry/check_availability.sh`. Local plan_pc metadata and remote run file counts; `lv1_ready` / `lv2_ready` indicate pipeline readiness.
- **experiment_registry.csv** — Produced by `scripts/analysis/registry/build_experiment_registry.py` from `data/plan_pc/*.json`. One row per (cs_run, exp_id).
- **flare_reference_pairs.csv** — Produced by `scripts/analysis/registry/build_reference_pairs.py`. Stores the matched reference experiment plus a `pair_method` field.
- **analysis_registry.csv** — Produced by `scripts/analysis/registry/build_analysis_registry.py`. Merges experiment metadata, availability, pairing, and analysis-usable flags.
- **paper_core_subset.csv** — Produced by `scripts/analysis/registry/build_analysis_registry.py`. First filtered paper subset from the merged registry.
- **psd_stats.csv** — Produced by `scripts/analysis/growth/run_psd_stats.py`. Stable CSV inventory of figure13-style PSD table outputs.
- **ridge_metrics.csv** — Produced by `scripts/analysis/growth/run_ridge_metrics.py`. Ridge-growth summary from promoted PSD diagnostics.
- **growth_summary.csv** — Produced by `scripts/analysis/growth/run_growth_summary.py`. Joined ridge and PSD summary table for manuscript-facing growth diagnostics.
- **first_ice_onset_metrics.csv** — Produced by `scripts/analysis/initiation/run_first_ice_metrics.py`. LV2-based onset diagnostics for excess ice and excess INP.
- **initiation_process_fractions.csv** — Produced by `scripts/analysis/initiation/run_initiation_process_summary.py`. Early-window process-dominance table from LV3 rates.
- **pilot_process_attribution_matrix.csv** — Produced by `scripts/analysis/initiation/run_pilot_process_attribution.py`. Wide pilot matrix of dominant and runner-up early processes.
- **paper_tables/** — Legacy mirror of the canonical compiled manuscript tables in `output/tables/paper/`, with synchronized copies under `article_draft/PolarCAP/tables/`.
- **claim_register.csv** — Produced by `scripts/analysis/synthesis/run_claim_summary.py`. Compact claim/evidence register summarizing what the current promoted tables support.
- **figure_inventory.csv** — Produced by `scripts/analysis/synthesis/run_figure_inventory.py`. Lightweight manuscript artifact inventory linking figures and compiled tables to scripts and output paths.
