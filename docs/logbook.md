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
