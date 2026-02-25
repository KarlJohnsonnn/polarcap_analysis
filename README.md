# polarcap_analysis

Structured analysis repository for PolarCAP model output, plume-tracking workflows, HOLIMO observations, and article drafting assets.

## Scope

This repo bundles:
- analysis notebooks for plume tracking and visualization
- processed model products used by the notebooks
- HOLIMO observation files used for comparison
- shared Python utilities
- web-based `cs_data_viewer`
- draft article code assets

## Layout

- `notebooks/` - analysis notebooks (tracking, top/side view, merged diagnostics, strong-precip cases, clean plume-path workflow)
- `data/processed/` - processed plume-path products (NetCDF)
- `data/observations/holimo_data/` - HOLIMO in-situ data
- `src/utilities/` - shared helper library used by notebooks
- `web/cs_data_viewer/` - local data viewer frontend
- `docs/` - analysis and workflow documentation
- `article_draft/` - manuscript-related code assets (figure scripts, notes)

## Quick start

1. Create/activate your analysis environment.
2. Add this repo root to `PYTHONPATH` (or install as editable package later).
3. Run notebooks from `notebooks/` using kernels that include dependencies from `src/utilities`.

## Notes

- Data folders are intentionally included to keep analyses reproducible.
- Keep run metadata and threshold choices documented in notebooks/docs when adding new diagnostics.
