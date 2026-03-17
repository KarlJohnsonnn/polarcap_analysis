# Registry notes

- **availability_check.csv** — Produced by `scripts/analysis/registry/check_availability.sh`. Local plan_pc metadata and remote run file counts; `lv1_ready` / `lv2_ready` indicate pipeline readiness.
- **experiment_registry.csv** — Produced by `scripts/analysis/registry/build_experiment_registry.py` from `data/plan_pc/*.json`. One row per (cs_run, exp_id).
- **flare_reference_pairs.csv** — Produced by `scripts/analysis/registry/build_reference_pairs.py`. Stores the matched reference experiment plus a `pair_method` field.
- **analysis_registry.csv** — Produced by `scripts/analysis/registry/build_analysis_registry.py`. Merges experiment metadata, availability, pairing, and analysis-usable flags.
- **paper_core_subset.csv** — Produced by `scripts/analysis/registry/build_analysis_registry.py`. First filtered paper subset from the merged registry.
- **psd_stats.csv** — Produced by `scripts/analysis/growth/run_psd_stats.py`. Stable CSV inventory of figure13-style PSD table outputs.
- **ridge_metrics.csv** — Produced by `scripts/analysis/growth/run_ridge_metrics.py`. Ridge-growth summary from promoted PSD diagnostics.
- **first_ice_onset_metrics.csv** — Produced by `scripts/analysis/initiation/run_first_ice_metrics.py`. LV2-based onset diagnostics for excess ice and excess INP.
- **initiation_process_fractions.csv** — Produced by `scripts/analysis/initiation/run_initiation_process_summary.py`. Early-window process-dominance table from LV3 rates.
