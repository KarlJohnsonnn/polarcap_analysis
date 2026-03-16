# Registry notes

- **availability_check.csv** — Produced by `scripts/analysis/registry/check_availability.sh`. Local plan_pc metadata and remote run file counts; `lv1_ready` / `lv2_ready` indicate pipeline readiness.
- **experiment_registry.csv** — Produced by `scripts/analysis/registry/build_experiment_registry.py` from `data/plan_pc/*.json`. One row per (cs_run, exp_id).
- **flare_reference_pairs.csv** — Produced by `scripts/analysis/registry/build_reference_pairs.py`. Flare–reference pairing using same logic as LV1 tracking.
- **paper_core_subset.csv** — Optional derived subset of runs used for paper figures; can be hand-maintained or generated from the registry.
