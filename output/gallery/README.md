# Paper draft figure gallery

Latest paper-ready figures and videos. **Quicklook on GitHub:** images below render when you browse this folder; click video links to play in GitHub’s blob view.

---

## Figures

### Fig 1 — Cloud field overview

![Cloud field overview](fig01_cloud_field_overview.png)

*Time–height mass concentration profiles (symlog) for all meteogram stations (ALLBB). Droplet and ice mass from COSMO-SPECS ensemble.*

---

### Fig 12 — Ensemble-mean plume path

![Plume path](fig12_plume_path.png)

*Ensemble-mean Lagrangian plume path. Trajectories from selected plume realisations with altitude and time; used for PAMTRA forward sampling.*

---

### Fig 13 — PSD altitude–time (mass)

![PSD mass](fig13_psd_mass.png)

*PSD mass mixing ratio along the plume path as a function of altitude and time.*

---

### Fig 13 — PSD altitude–time (number)

![PSD number](fig13_psd_number.png)

*PSD number concentration along the plume path as a function of altitude and time.*

---

## Videos (click to play on GitHub)

- **[Spectral waterfall — number](spectral_waterfall_N_cs-eriswil__20260304_110254_exp0_stn0-1-2_ALLBB_evolution_nframes242.mp4)** — Time evolution of droplet number size distribution (cs-eriswil, 20260304, ALLBB).
- **[Spectral waterfall — mass](spectral_waterfall_Q_cs-eriswil__20260304_110254_exp0_stn0-1-2_ALLBB_evolution_nframes242.mp4)** — Time evolution of droplet mass size distribution (cs-eriswil, 20260304, ALLBB).

---

## Files in this folder

| File | Description |
|------|-------------|
| `figure_captions.yaml` | Caption text for each figure; edit to match the manuscript. |
| `gallery.ipynb` | Jupyter notebook that loads all images/videos with captions. Run from repo root. |

Figures are merged from `notebooks/output` and `output/gfx`; this directory is the canonical set for the draft.

**Live gallery page:** Enable [GitHub Pages](https://docs.github.com/en/pages) with **Source: branch `main`, folder `/` (root)** so that `output/gallery` is served. Then open **[docs/gallery.html](../docs/gallery.html)** or `https://<owner>.github.io/polarcap_analysis/docs/gallery.html` for a single page with all figures and embedded videos.
