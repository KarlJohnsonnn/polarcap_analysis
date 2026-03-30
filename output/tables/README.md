# Output Tables

Canonical manuscript-facing tabular outputs live under `output/tables/`.

Layout:

- `output/tables/registry/` — promoted registry and statistics CSV/TXT products (`analysis_registry.csv`, `growth_summary.csv`, `claim_register.csv`, `figure_inventory.csv`, ...).
- `output/tables/paper/` — compiled manuscript CSV and LaTeX tables used by the article draft.
- `output/tables/phase_budget/` — compact and long-form cloud phase-budget tables promoted from the cloud overview workflow.
- `output/tables/psd/` — figure13 PSD statistics CSV and LaTeX tables.
- `output/tables/spectral_growth/` — ridge-growth CSV exports from the spectral waterfall workflow.
- `output/tables/growth_bundle/` — per-run and ensemble bundle tables, CSV snapshots, and supporting figures.

Compatibility mirrors:

- `data/registry/` remains a synchronized compatibility copy for legacy scripts.
- `data/registry/paper_tables/` remains a synchronized compatibility copy for older manuscript tooling.
- `output/gfx/csv/01`, `output/gfx/csv/04`, `output/gfx/csv/05`, `output/gfx/tex/01`, and `output/gfx/tex/04` remain synchronized where existing figure scripts still expect them.
- `output/growth_bundle/` remains a synchronized compatibility copy of `output/tables/growth_bundle/`.

Processed-data provenance and gap-filling logs are kept separately from this tree.
