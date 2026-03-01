# Logbook

## 2026-02-28: Resolved comparison notebook issues and implemented Korolev WBF regime

### Scope

- Investigated and resolved broadcasting and masking issues in notebooks `01-growth-rate-sensitivity.ipynb` and `02-lagrangian-icnc-lwc-comparison.ipynb`.
- Analyzed `CLOUDLAB/Omanovic_etal_2024/korolev.py`.
- Implemented notebook `04-korolev-wbf-comparison.ipynb` to apply the Korolev and Mazin (2003) formulation to COSMO-SPECS data, comparing theoretical critical updraft velocities ($u_{zi}$, $u_{zw}$) against actual model updraft ($w$) to classify and plot WBF regime frequencies.

### Key decisions and fixes

- **Notebook 01 & 02 Fixes:** The `calculate_mean_diameter` helper function was returning a `MaskedArray` containing zeros where data was absent. Assigning this directly into standard `np.zeros` arrays caused the mask to be lost and `0.0` values to propagate, leading to `np.nanmin` returning `0.0` or causing subsequent `ValueError` during shape broadcasting across the (time, cell) matrix. Rewrote the array operation into a robust local block calculating the number-weighted mean (`num / den`) using `np.where` with `np.nan` fill values, entirely side-stepping the masked array limitations. Notebook 03 had already been fixed and didn't exhibit any similar output errors.
- **Korolev Implementation (Notebook 04):** Extracted `temp`, `rho`, `p0`, `pp`, `qv`, `icnc`, `cdnc`, `qi`, `qc`, and `wt` from the integrated dataset. Carefully aligned units: `icnc` and `cdnc` converted from per-liter/per-cm³ to per-$m^3$, and `qi`/`qc` converted from $g/L$ or $g/m^3$ to $kg/kg$ (divided by air density). Passed the aligned inputs into the exact `korolev()` function from Omanovic et al. Computed $u_{zi}$ and $u_{zw}$ and created masks for WBF (`wc < w < wi`), Mixed-Phase Growth (`w > wi`), and Evaporation (`w < wc`). Plotted ensemble average frequencies over elapsed time to enable direct visual comparison with Omanovic 2024/2025.

---

## 2026-02-28: Extended literature review + CLOUDLAB code deep-dive

### Scope

- Deep-dive into new CLOUDLAB publications: Omanovic et al. 2025 (JAMES, Lagrangian trajectories), Miller et al. 2025 (ACP, INF quantification), Fuchs et al. 2025 (EGUsphere, growth rates).
- Reviewed external literature: Chen et al. 2024 (GRL, critical AgI size), Huetner et al. 2025 (Science Advances, AgI surface reconstruction), ChenML et al. 2025 (ACP, turbulence + seeding).
- Analyzed all figure scripts from `CLOUDLAB/omanovic_etal_2025/figures/` (f01–f12) and `CLOUDLAB/miller_etal_2025/scripts/` (fig2–fig6, def functions).

### Key findings

- **Omanovic 2025:** Lagrangian trajectories in ICON 65 m; default bulk microphysics underestimates growth rates by up to 3×; ventilation coefficient tuning (up to 3×) partially helps; WBF LWC depletion systematically too slow. Microphysical budget analysis computes T*, qv*, buoyancy perturbations and vertical acceleration along plume. Growth rate sensitivity explored via 3×3 ventilation × capacity matrix.
- **Miller 2025:** First field INF (0.07–1.63%) from 16 experiments; immersion freezing dominant; constant INF per experiment; aggregation-adjusted ICNC using IceDetectNet. Scripts provide reusable data loading, cross-correlation time alignment, background subtraction, and residual analysis framework.
- **Huetner 2025:** Ag-terminated AgI(0001) undergoes (2×2) vacancy reconstruction preserving hexagonal symmetry for epitaxial ice growth; I-terminated surface incompatible. This is the first atomic-resolution explanation of why AgI nucleates ice.
- **ChenML 2025:** WRF-LES with fast spectral-bin microphysics; turbulence accelerates glaciation but causes positive-to-negative seeding effect transition downwind ("robbing Peter to pay Paul").

### Files updated

- `article_draft/PolarCAP/01_introduction.tex`: Added 4 new paragraphs summarising Marcolli 2016 + Huetner 2025 (atomic-level nucleation), Chen 2024 + Miller 2025 (INP characterisation and field INF), Omanovic 2025 (Lagrangian modeling and growth-rate bias), ChenML 2025 (turbulence effects). Extended CLOUDLAB publications table with Miller 2025, Fuchs 2025, Omanovic 2025.
- `article_draft/PolarCAP/05_results.tex`: Rewrote "Context from CLOUDLAB colleagues" paragraph with quantitative details from Miller 2025 (INF), Omanovic 2025 (growth rate underestimation, ventilation tuning), Fuchs 2025b (LWP/reflectivity benchmarks), Ramelli 2024 (growth rates). Positioned PolarCAP's bin-resolved budgets as the resolution of the ventilation tuning problem.
- `article_draft/PolarCAP/biblio.bib`: Added Omanovic2025, Huetner2025, ChenML2025, Kanji2017IceNucleatingParticles.
- `docs/CLOUDLAB_comparison_analysis.md`: Major expansion: added per-figure tables for Omanovic 2024 (f02–f11), Omanovic 2025 (f01–f12), Miller 2025 (fig2–fig6 + defs). Extended overlap table to 12 aspects × 6 studies. Added Section 7 (external papers) with 6 summaries. Revised value/advantages tables with evidence from new papers. Updated suggested notebooks to align with Omanovic 2025 methodology.
- `docs/logbook.md`: This entry.

### Connection to article red line

The new literature strengthens the narrative: (1) AgI nucleation is now understood from atomic surface structure through laboratory critical sizes to field-measured INFs; (2) bulk two-moment models systematically underestimate growth and WBF efficiency; (3) COSMO-SPECS spectral-bin microphysics resolves these limitations naturally via size-dependent process rates; (4) the Lagrangian framework pioneered by Omanovic 2025 for ICON is directly applicable to our plume-tracking approach, and our bin-resolved budgets add the missing size-resolved process attribution.

---

## 2026-02-28: Implemented comparison notebooks and corrected models

### Scope

- Implemented notebook `01-growth-rate-sensitivity.ipynb` to compare bin-resolved growth rates from COSMO-SPECS with Omanovic 2025 bulk results and Ramelli 2024.
- Implemented notebook `02-lagrangian-icnc-lwc-comparison.ipynb` to replicate Omanovic 2025 f05/f12 style analysis (ICNC, CDNC, mean diameter, relative LWC mass depletion). 
- Implemented notebook `03-inf-model-vs-field.ipynb` to compare theoretical INF parameterizations used in COSMO-SPECS against Miller 2025 field INFs.
- Critically reviewed prior work, identified, and corrected scientific errors in the generated notebooks.

### Key corrections

- **Notebook 01 (Growth rates):** Fixed a NumPy broadcasting error by explicitly enforcing `.transpose('time', 'cell', 'diameter')` before extracting the `nf` arrays and ensuring operations on `calculate_mean_diameter` matched the last axis correctly.
- **Notebook 02 (WBF depletion):** The initial implementation used `nw` (cloud droplet number concentration) to calculate the relative LWC change. WBF depletion is a mass-transfer process. I fixed this by using the `qw` (liquid water mass mixing ratio) variable to correctly compute the relative LWC mass depletion, matching the exact style of Omanovic 2025. Also resolved `axis=1` percentile computation errors by enforcing a `('time', 'cell')` transposition on bulk variables.
- **Notebook 03 (INF comparison):** The initial implementation incorrectly computed the model INF by taking `ICNC / (ICNC + CDNC)`. CDNC represents background cloud droplets, NOT the unactivated seeding aerosol particles. Since the unactivated AgI tracer (`ninp`) is not available in the processed data, I rewrote the notebook to plot the theoretical activation parameterization (used by the model) against the field data for a proper scientific comparison. Ensured bulk arrays transpose correctly to `('time', 'cell')` before value extraction.

---

## 2026-02-28: CLOUDLAB comparison analysis

### Scope

- Studied open-source plot scripts from Fuchs, Omanovic, and Ramelli (CLOUDLAB colleagues).
- Documented analysis types, processing steps, and similarities to PolarCAP.
- Created `docs/CLOUDLAB_comparison_analysis.md` with:
  - Per-figure breakdown (Fuchs fig02–08, A1, C1; Omanovic plume/korolev; Ramelli fig2–5).
  - Overlap table (PSD, time–diameter, vertical structure, plume ID, HOLIMO, radar, LWP).
  - Quantitative benchmarks (LWP slope/R², reflectivity regression, growth rates).
  - Ways to incorporate findings and advantages of spectral-bin microphysics.
  - Suggested next notebooks: LWP, reflectivity, growth-rate, vertical-profile comparisons.
- Updated `article_draft/PolarCAP/05_results.tex` with paragraph "Context from CLOUDLAB colleagues" citing Fuchs, Ramelli, Omanovic and pointing to the doc.
- Added `Fuchs2025b` to biblio for SmHOLIMO paper (in prep).

### Key takeaways

- Fuchs: SmHOLIMO vs HOLIMO CDSD, vertical profiles, LWP–MWR, reflectivity from PSD.
- Ramelli: HoloBalloon CDNC/ICNC, ice PSD waterfall, linear growth 0.2–0.8 µm s⁻¹.
- Omanovic: Plume mask (ICNC diff), Korolev–Mazin WBF.
- PolarCAP adds: Lagrangian tracking, bin-resolved tendency budgets, process attribution.

## 2026-02-26: 200x160 comparison diagnostics (iteration 1)

### Notebook

- `notebooks/legacy_figures/200x160_comparison_diagnostics.ipynb`

### Cell output snapshot

```text
[stage] threshold occurrence compute     170.5 s
[stage] figure C saved                     1.4 s
Saved: fig-comparison-a-joint-freq-200x160.png
Saved: fig-comparison-b-diameter-resolved-hist-200x160.png
Saved: fig-comparison-c-threshold-occurrence-200x160.png
[done] total elapsed  5309.4 s
```

### Short summary

- End-to-end workflow completed successfully and produced all three target comparison figures.
- Runtime was dominated by the diameter-resolved histogram stage (multi-run, multi-bin loop).
- Threshold occurrence computation was comparatively fast (~170 s).
- The run confirmed stability of the memory-reduction strategy (sampling/striding + reduced compute targets), at the cost of longer wall-clock time in the spectral histogram section.

### Next-iteration notes

- Keep dashboard monitoring active during diameter-loop execution.
- For faster turnaround, use the notebook preset switch:
  - `preset = 'fast'` for iterative checks.
  - `preset = 'balanced'` for higher-detail figure generation.
- Article-consistent plot styling is now applied through `utilities.style_profiles` (`use_style('hist')`, `use_style('2d')`).

## 2026-02-27: Local continuation after remote diagnostics

### Scope shift

- Continued locally with a condensed-data-first workflow and targeted plotting refinements, instead of rerunning full raw 5D diagnostics.
- Consolidated outcomes from recent cursor-chat implementation notes into a practical local direction for faster iteration.

### Key outcomes from implementation notes

- Comparison figures were retuned with robust central-quantile bounds (99.6%, 0.2-99.8 percentiles) for axes, bins, and color ranges so structure is visible instead of outlier-dominated.
- Diagnostics were refactored away from eager high-dimensional loads (`persist()`/`.values` on large fields) toward memory-safe sampling, reduced computes, and stride presets.
- Workflow switched to processed plume-cell products in `data/processed` (tracked/condensed artifacts) instead of raw 5D model fields; this keeps intermediate analysis local and bounded.
- FoO and elapsed-time overlay logic was aligned to mission-frame timing and seeding-start anchors so HOLIMO/model comparisons use consistent windows.
- FoO plotting was further refined to mission-wise comparisons and matched time-window slicing, preventing hidden/empty observation curves caused by frame mismatches.

### Runtime and execution learnings

- The diameter-resolved histogram stage remains the dominant cost; threshold-occurrence diagnostics are comparatively cheap.
- Monitoring runtime telemetry during long histogram loops remains necessary to distinguish healthy slow runs from memory-risk states.
- Preset-based run controls (`fast`, `balanced`, `full`) are the preferred way to trade detail vs turnaround without code rewrites.

### Current local direction

- Use condensed intermediate products as the default input path for notebook diagnostics.
- Keep robust quantile-based bounds as default plotting behavior for comparison panels.
- Apply mission-aligned elapsed/FoO handling as baseline for HOLIMO overlays before further figure polishing.

## 2026-02-27: FoO histogram binning and mission aggregation update

### Scope

- Revisited FoO histogram construction in `notebooks/plume_path/01-plot-plume-path-sum.ipynb` with explicit model/observation diameter handling for log-spaced bins.
- Simplified right-panel comparison to one model curve and one combined HOLIMO observational curve across all seeding missions.

### Decisions made

- Treated provided diameter arrays as bin centers and converted them to log-space bin edges before histogramming, instead of passing centers directly as `np.histogram` bins.
- Kept separate bin-edge generation for model and observations to respect instrument/model grid differences while preserving physically consistent log spacing.
- Aggregated observational evidence by summing mission-wise histogram mass first, then normalizing once to percent FoO; this avoids bias from normalizing each mission independently before combining.
- Standardized plot semantics to two curves for final comparison readability: `model=orange`, `obs (all missions)=royalblue`.

### Result

- FoO panel now uses consistent log-diameter binning and a single combined observational reference curve, improving interpretability and reducing mission-fragmented visual noise.

## 2026-02-27: Figure 11 layout and FoO panel refinements

### Scope

- Final layout and styling for `figure11_plume_path_plot_clean_symlog_with_zoom_panel_hist.png`: despined axes, single HOLIMO missions legend, FoO/concentration panel with max-amplitude markers and simplified legend.

### Decisions made

- **Despined axes:** Removed top and right spines from both pcolormesh panels (`ax_main`); removed top and left spines from the histogram panel (`ax_hist`) so the right-side y-axis reads cleanly. Keeps focus on data and reduces visual clutter.
- **HOLIMO missions legend on one panel only:** Added `add_holimo_legend` to `plot_plume_path_sum` and pass `add_holimo_legend=(i == 0)` in the panel loop so the “HOLIMO missions” legend appears only on the first (symlog) panel; the zoomed elapsed-time panel has no duplicate legend.
- **Max-amplitude markers on FoO panel:** Short horizontal dashed lines at the maximum of each curve (model and obs), drawn to the right of the peak over ~12% of the log x-span, with the numeric value annotated at the end of the segment (e.g. 6.19, 9.48). Makes peak values readable without cluttering the legend.
- **Legend without stats:** FoO/concentration legend shows only “COSMO-SPECS” and “HOLIMO”; mean/std/max were removed from the legend text and are conveyed by the max-amplitude annotations instead.
- **Absolute vs relative mode:** The existing `absolute_mode` switch is unchanged: time-mean concentration (absolute) or FoO % (relative), with axis titles and units set accordingly.

### Result

- Figure 11 has a consistent despined look, a single HOLIMO legend on the symlog panel, and a clear FoO/concentration comparison with peak values on the plot and a minimal legend.

## 2026-02-27: Figure 11 interpretation text and time–diameter ridge

### Scope

- Draft results text for the plume path figure (Fig.\,11): explain the plot, list representative elapsed-time vs.\ diameter tuples along the ridge, and interpret the linear trend in log–log space in light of CLOUDLAB prior work. Conclusion to be finalised later.

### Decisions made

- **Representative (t, D) tuples:** Listed indicative values (0.2, 1, 3, 7, 15)\,min vs.\ (6, 20, 50, 95, 180)\,µm in a small table in `article_draft/PolarCAP/05_results.tex` to characterise the ridge; exact numbers depend on tracked cells and time window.
- **Interpretation (draft):** Linear trend in log(elapsed time) vs.\ log(diameter) implies power-law growth $D \propto t^\alpha$. Linked to diffusion-limited ($\alpha \approx 1/2$) and other regimes; cited \citet{Ramelli2024} for CLOUDLAB growth rates. Noted that $\alpha$ and process attribution (deposition, riming, aggregation) will be finalised with tendency budgets and HOLIMO; model vs.\ HOLIMO peak diameter difference (model ~120--150\,µm, HOLIMO ~80--100\,µm) flagged for discussion.
- **Caption:** Extended figure caption to describe all three panels (symlog overview, linear zoom, time-mean concentration histogram) and HOLIMO marker semantics.
