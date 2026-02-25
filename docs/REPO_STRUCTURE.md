# Repository structure and conventions

## Main folders

- `notebooks/`
  - Primary analysis notebooks copied from active development.
  - Keep notebook names stable to preserve cross-references.

- `data/processed/`
  - Processed plume-track outputs consumed by notebooks.
  - Treat as source data for reproducibility.

- `data/observations/holimo_data/`
  - HOLIMO observation files used for model/observation comparisons.

- `src/utilities/`
  - Shared utilities used by notebooks.
  - New reusable helpers should be added here first, then used in notebooks.

- `web/cs_data_viewer/`
  - Browser-based viewer files (`cosmo-viewer.html`, manifest tools).

- `docs/`
  - Project notes, workflow docs, and reproducibility notes.

- `article_draft/`
  - Manuscript assets, figure scripts, and draft-specific helper code.

## Current additions for organization

- `src/utilities/style_profiles.py`
  - Central plotting-style profiles (`timeseries`, `2d`, `hist`) with context-manager usage.

## Suggested workflow

1. Add generalized logic to `src/utilities/`.
2. Keep notebooks focused on analysis flow and figure assembly.
3. Document run choices and thresholds in `docs/`.
