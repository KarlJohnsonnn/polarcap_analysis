# polarcap_analysis

Structured analysis repository for PolarCAP model output, plume-tracking workflows, HOLIMO observations, and article drafting assets.

**Latest figures (PNGs/videos):** [output/gallery/](output/gallery/) — paper-draft figures, videos, and captions (viewer: [notebooks/gallery.ipynb](notebooks/gallery.ipynb)); [live gallery](https://KarlJohnsonnn.github.io/polarcap_analysis/docs/gallery.html) (GitHub Pages).

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

1. Create/activate your analysis environment. On DKRZ Levante, use NumPy 1.x (e.g. `pip install -r requirements.txt`) to avoid ABI errors with xarray/pandas; see `scripts/processing_chain/README.md` for details.
2. Run one-time notebook bootstrap setup: `bash scripts/ipython_startup/install.sh`.
3. Run notebooks from `notebooks/` using kernels that include dependencies from `src/utilities`.

## Figure gallery (GitHub Pages)

Paper-draft figures and videos are in **output/gallery/** and can be viewed as a single live page on GitHub Pages:

- **Enable:** Settings → Pages → Source: branch **main**, folder **/ (root)**. The repo root includes `.nojekyll` so static files are served as-is.
- **URL after deploy:** `https://<owner>.github.io/polarcap_analysis/docs/gallery.html`

See [docs/README.md](docs/README.md#github-pages-figure-gallery) for details.

## QuicklookBrowser

`QuicklookBrowser` is a lightweight browser for quicklook plots and videos in
`scripts/processing_chain/output` and `notebooks/output`.

Use it like this:
1. Run `python web/cs_data_viewer/generate_manifest.py` from the repo root.
2. Start and open it with `./qlb`.

It will try to load `web/cs_data_viewer/manifest.json` automatically, sort the
indexed media by modification time with newest files first, and can refresh the
manifest from the browser when the helper server is running.

## Notes

- Data folders are intentionally included to keep analyses reproducible.
- Keep run metadata and threshold choices documented in notebooks/docs when adding new diagnostics.
