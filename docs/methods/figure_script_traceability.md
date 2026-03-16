# Figure–script traceability

Every manuscript-critical figure or table should map to:

1. **Script** — in `scripts/processing_chain/` or `scripts/analysis/`
2. **Config** — YAML or CLI args used
3. **Output path** — stable file name under a known directory

Example mapping:

| Figure / table | Script | Config / args | Output |
|----------------|--------|---------------|--------|
| Plume path time–diameter | `run_lv1_tracking` + plume_path_plot | domain, cs_run | `notebooks/plume_path/output/` or processing_chain output |
| Spectral waterfall | `run_spectral_waterfall.py` | `process_budget_NQall.yaml` | `scripts/processing_chain/output/05/<cs_run>/...` |
| Experiment registry | `build_experiment_registry.py` | — | `data/registry/experiment_registry.csv` |
| Flare–reference pairs | `build_reference_pairs.py` | — | `data/registry/flare_reference_pairs.csv` |

Update this table as new figures are added. The repository is published with the paper; reviewers should be able to reproduce each result from the named script and config.
