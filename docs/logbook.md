# Logbook

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
