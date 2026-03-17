# Publication analysis scripts

Thin drivers for manuscript-critical diagnostics. Each script calls `src/utilities` and writes tables or figures to a stable path.

- **registry/** — Experiment metadata, availability, flare–reference pairing.
- **forcing/** — Forcing and setup summaries.
- **initiation/** — First-ice onset and freezing-pathway metrics.
- **growth/** — Plume ridge, PSD stats, process-dominance.
- **impacts/** — Liquid depletion, radar/precipitation-facing metrics.
- **synthesis/** — Claim register, figure inventory.

Outputs go to `data/registry/` or paths documented in each script. Every paper figure should map to a script and output file here.

Current promoted products:

- `registry/build_analysis_registry.py` -> `data/registry/analysis_registry.csv`, `data/registry/paper_core_subset.csv`
- `growth/run_psd_stats.py` -> `data/registry/psd_stats.csv`
- `growth/run_ridge_metrics.py` -> `data/registry/ridge_metrics.csv`
- `initiation/run_first_ice_metrics.py` -> `data/registry/first_ice_onset_metrics.csv`
- `initiation/run_initiation_process_summary.py` -> `data/registry/initiation_process_fractions.csv`

`growth/run_process_dominance.py` and `impacts/` remain deferred on purpose until the registry, ridge, PSD, and initiation products are stable.
