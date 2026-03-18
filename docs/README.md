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
  - Browser-based QuicklookBrowser files (`cosmo-viewer.html`, manifest tools, helper server).

- `docs/`
  - Project notes, workflow docs, and reproducibility notes.
  - **Figure gallery:** [gallery.html](gallery.html) — live page of paper-draft figures and videos (for GitHub Pages, see below).

- `article_draft/`
  - Manuscript assets, figure scripts, and draft-specific helper code.

## Current additions for organization

- `src/utilities/style_profiles.py`
  - Central plotting-style profiles (`timeseries`, `2d`, `hist`) with context-manager usage.

## GitHub Pages (figure gallery)

To serve the **paper figure gallery** at `https://<owner>.github.io/polarcap_analysis/docs/gallery.html`:

1. **Settings → Pages** → Build and deployment: Source **Deploy from a branch**.
2. Branch: **main**, Folder: **/ (root)**.
3. Root has a `.nojekyll` file so static files (including `output/gallery/`) are served as-is.
4. After the build, open the Pages URL above or the link from the repo homepage.

**Test locally:** From repo root run `python3 -m http.server 8000`, then open `http://localhost:8000/docs/gallery.html`. All asset paths are relative (`../output/gallery/...`) and resolve when the server root is the repo root.

## Suggested workflow

1. Add generalized logic to `src/utilities/`.
2. Keep notebooks focused on analysis flow and figure assembly.
3. Document run choices and thresholds in `docs/`.
