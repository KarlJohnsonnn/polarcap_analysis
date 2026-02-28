# CLOUDLAB Comparison Analysis

Documentation of analysis types, processing steps, and similarities between PolarCAP and CLOUDLAB colleagues (Fuchs, Omanovic, Ramelli). Prepared for incorporating their findings into the PolarCAP article and highlighting spectral-bin microphysics advantages.

---

## 1. CLOUDLAB Code Overview

### Fuchs et al. 2025b (SmHOLIMO – small cloud droplets)

**Data sources:** SmHOLIMO/HOLIMO NetCDF, radiosondes, RPG FMCW radar, MWR, GPS.

| Figure | Script | Analysis types | Processing steps |
|--------|--------|----------------|------------------|
| fig02 | fig02.py | Instrument resolution characterisation | USAF resolution target: experimental resolution vs. reconstruction depth; theoretical `D_res,rec` (diffraction limit), `D_res,pix` (pixel limit) |
| fig04 | fig04.py | Time–height reflectivity + vertical profiles | Radar pcolormesh (Zh); TBS height from HOLIMO GPS; radiosonde T, RH, wind; 30-point rolling mean on wind |
| fig05 | fig05.py | CDSD time series + vertical PSD profiles | `find_peaks` for ascent/descent; 5 cloud segments (cloud top → base); LWC threshold 0.01 g m⁻³; SmHOLIMO vs HOLIMO `Water_PSDlogNorm` |
| fig06 | fig06.py | Vertical profiles + SmHOLIMO/HOLIMO ratios | CDNC, mean D, LWC, optical depth τc(z); effective radius from volume/surface; trapezoid integration for τ*c; 20-point rolling mean; ratio panels |
| fig07 | fig07.py | LWP time series + model–MWR scatter | LWP from trapezoid(Water_content, altitude); 5-min MWR rolling mean ± 1σ; linear regression with 95% CI |
| fig08 | fig08.py | Reflectivity comparison (radar vs in situ) | Ze from PSD: Z = Σ(N·D⁶); dBZ = 10·log₁₀(Z)·0.93; nearest-neighbour match radar–HOLIMO; LWC > 0.01, dBZ > -60; linregress |
| figA1 | figA1.py | Spatial particle distribution by D_mean | Particle-level data; spatial bins (x,y,z); mean diameter per bin; hist2d for counts; pcolormesh for D_mean |
| figC1 | figC1.py | Single hologram vs 1 s average CDSD | Error bars: dpix/√N, CDSD/√N; comparison of statistical robustness |

### Omanovic et al. 2024 (ICON bulk plume analysis)

**Data sources:** ICON model output (qni, qnc, qc, qi, etc.), plume mask from ICNC difference.

| Component | Script | Analysis types | Processing steps |
|-----------|--------|----------------|------------------|
| Plume mask | plume_extract.py | Plume identification | diff_icnc = sim - ref; mask where diff_icnc ≥ 0.001; qc ≥ 0.1 g kg⁻¹ |
| Plume quantities | plume_quantities.py | Variable extraction in plume | Apply plume mask to ddt_qv_vap, etc. |
| WBF conditions | korolev.py | Korolev & Mazin 2003, Korolev 2007 | Ice/water equilibrium fall speeds; WBF regime classification |
| Domain | f01.py | Domain map | Cartopy, nested domains (1 km, 260 m, 130 m) |
| AgI freezing | f02.py | Sigmoid INP activation | Sigmoid fit to Marcolli 2016 frozen fraction vs temperature |
| Radiosonde | f03.py | Model–obs T/RH profiles | COSMO vs radiosonde vertical profiles |
| Radar dBZ | f06.py | Reflectivity cross-section | Model dBZ vs observed radar elevation scan |
| ICNC histograms | f07.py, f08.py | Model–obs ICNC | Log-scale frequency histograms of ICNC (COSMO vs HOLIMO) |
| CDNC reduction | f09.py | WBF signature | CDNC time series and reduction histograms (model vs obs) |
| WBF evolution | f10a.py | Multi-timestep WBF | 7-panel WBF regime fractions, w cross-sections, ice/droplet radii |
| INP sensitivity | f11.py | Emission rate sweep | ICNC/CDNC histograms for 10⁶, 3×10⁶, 10⁷ m⁻³ s⁻¹ |

### Omanovic et al. 2025 (ICON Lagrangian trajectory analysis)

**Data sources:** ICON 65 m output along Lagrangian trajectories (CSV), HOLIMO NetCDF, seeding specs.

| Figure | Script | Analysis types | Processing steps |
|--------|--------|----------------|------------------|
| f01 | f01.py | Domain + emission + INP activation | Nested domain map; emission field at max height; sigmoid frozen fraction |
| f03 | f03.py | Ventilation coefficient theory | F_v for oblate spheroids and columns; dual-axis Re/diameter |
| f05 | f05.py | ICNC + LWC evolution along trajectories | Filter qni > 0.001; relative ΔLWC = (qc_seed − qc_ref)/qc_ref; median ± std across trajectories |
| f06 | f06.py | PSD + growth rates | Log-log PSD; mean diameter; growth rate = 2r/grt; ICNC-percentile analysis (P5, P95) |
| f07 | f07.py | Growth rate sensitivity (3×3 matrix) | Ventilation coeff × capacity constant heatmap; observed median overlay |
| f08 | f08.py | LWC response sensitivity | Relative ΔLWC for "strongest" vs "best match" configurations |
| f09 | f09.py | Ensemble statistics | Concatenated 5-mission box plots: ice concentration + ΔLWC |
| f10 | f10.py | Radar reflectivity comparison | Model dBZ vs radar observations |
| f11 | f11_fD1_fD2_fD3_fD4.py | **Microphysical budget analysis** | T*, qv*, qh* perturbations; buoyancy B = T*/T_ref + 0.61qv* − qh*; vertical acceleration dw/dt = gB; Brunt-Väisälä oscillation period |
| f12 | f12.py | Microphysical time series | ICNC, IWC, CDNC, LWC evolution; ice radius from qi/qni |

### Miller et al. 2025 (INF quantification)

**Data sources:** HOLIMO NetCDF, POPS aerosol CSV, mission parameters CSV, radar NetCDF.

| Figure | Script | Analysis types | Processing steps |
|--------|--------|----------------|------------------|
| fig2 | fig2_sm058timeseries.py | 4-panel time series | Radar dBZ pcolormesh; POPS aerosol with background subtraction; ICNC (measured + aggregate-adjusted); CDNC |
| fig3 | fig3_sm058scatter.py | ICNC–seed scatter + INF | Linear regression ICNC vs seed conc; INF = ICNC_adj/(ICNC_adj + seed_adj); median INF |
| fig4 | fig4_INFsubplotsresidualsanalysis.py | INF residual analysis | INF vs T, residence time, CDNC; residuals from each regression vs other variables |
| fig6 | fig6_INFlabcomparison.py | Field–lab INF comparison | Field INF vs Marcolli 2016, Chen 2024, DeMott 1995; log-linear fit |
| def | def_get_adjustedICNC.py | Aggregation-adjusted ICNC | ICNC_adj = ICNC + ICNC × agg_fraction × AggFactor (+ riming) |
| def | def_get_pops_and_holo_FULL.py | Data loading + sync | Cross-correlation time alignment; 5s rolling mean; 1s resampling |

### Ramelli et al. 2024 (ice crystal growth)

**Data sources:** HoloBalloon NetCDF, particle-level data, radar (Mira, RPG FMCW).

| Figure | Script | Analysis types | Processing steps |
|--------|--------|----------------|------------------|
| fig2/S1 | fig2_figS1.py | Radar + CDNC/ICNC + PSD waterfall | Time–height radar; CDNC/ICNC timeseries; `Ice_Pristine_PSDlogNormMajsiz` pcolor; background vs seeding windows; violin LWC/IWC |
| fig3/S2 | fig3_figS2.py | 2D PSD (maj vs min axis) + growth | `calculate_PSD` from particle data; hist2d; aspect-ratio isolines; growth-time PSD |
| fig5 | fig5.py | Growth rate vs aspect ratio | AR < 9.5 filter; growth rate = majsiz/growthtime; boxplot by AR bin; linear regression |

**Helper functions:**
- `calculate_PSD`: histogram by size/growthtime; volume from rules; noNorm/linNorm/logNorm
- `helper_functions_radar`: read_radar_netcdfs, despeckle, LDR filter, linval2db

---

## 2. PolarCAP Analysis (Current)

| Notebook | Analysis types | Processing steps |
|----------|----------------|------------------|
| 01-plot-plume-path-sum | Lagrangian plume path; time–diameter evolution | TOBAC tracking; integrated concentration per bin; symlog elapsed time; HOLIMO overlay; FoO histogram |
| 03-plot-psd-waterfall | Altitude-resolved PSD; phase partitioning | Stacked altitude bands; CDNC/ICNC overlay; skewed waterfall coordinates |

**Key variables:** ICNC, CDNC, equivalent diameter, elapsed time since ignition, altitude.

---

## 3. Similarities and Overlap

| Aspect | PolarCAP | Fuchs | Omanovic 2024 | Omanovic 2025 | Ramelli | Miller 2025 |
|--------|----------|-------|---------------|---------------|---------|-------------|
| PSD / CDSD | ✓ 66-bin resolved | ✓ Water_PSDlogNorm | — | ✓ HOLIMO PSD | ✓ Ice_Pristine_PSD | — |
| Time–diameter evolution | ✓ plume path | ✓ CDSD time series | — | ✓ Lagrangian trajectory | ✓ PSD waterfall | — |
| Vertical structure | ✓ altitude bands | ✓ cloud segments | — | — | ✓ height in radar | — |
| Plume identification | ✓ TOBAC | — | ✓ ICNC diff mask | ✓ qni > 0.001 filter | ✓ seeding windows | ✓ POPS plume definition |
| HOLIMO comparison | ✓ missions overlay | ✓ SmHOLIMO vs HOLIMO | ✓ ICNC histograms | ✓ ice conc + LWC | ✓ HoloBalloon | ✓ ICNC, CDNC, INF |
| Radar | (PAMTRA planned) | ✓ Ze from PSD, scatter | ✓ dBZ cross-section | ✓ dBZ comparison | ✓ time–height, scans | ✓ dBZ time series |
| LWP/LWC depletion | (planned) | ✓ MWR, trapezoid | ✓ CDNC reduction | ✓ relative ΔLWC | — | — |
| Growth regime | ✓ D ∝ t^α | — | — | ✓ growth rate = 2r/grt | ✓ linear growth, AR | — |
| INP/nucleation | ✓ parameterisation | — | ✓ sigmoid fit | ✓ emission sensitivity | — | ✓ INF 0.07–1.63% |
| WBF classification | (bin-resolved) | — | ✓ Korolev-Mazin bulk | — | — | — |
| Microphysical budgets | ✓ bin-resolved tendencies | — | — | ✓ T*, qv*, B, dw/dt | — | — |
| Sensitivity analysis | ✓ INP rate, CCN | — | ✓ INP emission rate | ✓ ventilation × capacity | — | ✓ INF vs T, time, CDNC |

---

## 4. Ways to Incorporate CLOUDLAB Findings

### 4.1 Quantitative benchmarks

- **Fuchs fig07:** LWP slope (1.12 SmHOLIMO, 1.05 HOLIMO), R² (0.99, 0.96). Use as target for model–MWR LWP comparison.
- **Fuchs fig08:** Reflectivity slope 0.82 (SmHOLIMO), 0.66 (HOLIMO) vs radar. Compare PAMTRA-derived Ze with similar scatter and regression.
- **Ramelli fig5:** Growth rates 0.2–0.8 µm s⁻¹ (pristine). Compare with model-derived α and dD/dt from plume-path ridge.

### 4.2 Methodological alignment

- **Vertical segmentation:** Fuchs uses 5 cloud segments (top → base). PolarCAP uses altitude bands in the waterfall; align bin definitions for direct comparison.
- **Time windows:** Ramelli uses explicit start/end for seeding vs background. PolarCAP uses elapsed time since ignition; ensure mission timing is consistent.
- **PSD normalisation:** All use dN/d(log D) or equivalent. Keep units and bin edges consistent (log spacing).

### 4.3 Narrative integration

- **Resolution (Fuchs fig02):** At z_rec > 25 mm, D_res,exp degrades. If PolarCAP resolves smaller ice at depth, this is a spectral-bin advantage.
- **SmHOLIMO vs HOLIMO (Fuchs fig05–06):** SmHOLIMO shows higher CDNC, smaller mean D, higher τ*c. Spectral-bin model can be positioned similarly: more detailed PSD → different bulk moments.
- **Ramelli linear growth:** Our α ≈ 0.8–1 is consistent. Emphasise that spectral-bin model captures the full PSD (including aggregates), not only pristine subset.

---

## 5. Value of PolarCAP Contribution

1. **Lagrangian, process-resolved view:** TOBAC tracks plumes; bin-resolved tendency budgets allow attribution (deposition, riming, aggregation). CLOUDLAB in situ and Fuchs focus on instantaneous PSDs; Omanovic 2025 uses Lagrangian trajectories but with bulk microphysics; PolarCAP adds spectral resolution along trajectories.
2. **Spectral-bin microphysics:** 66 bins, no imposed PSD shape. Omanovic 2025 showed that bulk two-moment schemes require up to 3× ventilation coefficient tuning to match observed growth rates; COSMO-SPECS resolves size-dependent deposition naturally.
3. **Size-resolved process attribution:** Where Omanovic 2024 classifies WBF conditions using bulk mean radii (Korolev-Mazin), COSMO-SPECS can identify the dominant process for each individual size bin—deposition dominates small bins while riming/aggregation affects larger ones.
4. **Multi-platform closure:** Model + PAMTRA + MWR + HOLIMO. Omanovic 2025 demonstrates the Lagrangian framework; we add spectral-bin resolution and forward-modeled radar from the full PSD.
5. **Bridging laboratory and field INP constraints:** Miller 2025 field INFs and Chen 2024 critical-size thresholds directly constrain our nucleation parameterisation, closing the loop from particle properties to plume-scale ice production.

---

## 6. Advantages of Spectral-Bin Microphysics (for article)

| Advantage | Evidence / argument |
|-----------|---------------------|
| No assumed PSD shape | Model resolves full distribution; bulk schemes assume gamma/log-normal. Omanovic 2025 noted bulk PSD assumptions limit WBF representation. |
| Process attribution | Tendency budgets separate deposition, riming, aggregation per size bin. Omanovic 2025 budgets are integrated quantities; ours are size-resolved. |
| Size-dependent growth | Different bins grow at different rates. Omanovic 2025 needs ventilation tuning (×1.5–3) because bulk schemes apply a single growth rate to the mean crystal; SBM resolves this naturally. |
| Ventilation resolution | ChenML 2025 showed turbulence accelerates AgI dispersion + growth; SBM resolves size-dependent ventilation that couples directly to turbulent diffusivity. |
| Radar closure | Forward Ze from full PSD via PAMTRA; bulk schemes approximate Ze from moments. |
| Robustness across regimes | Same scheme for nucleation, growth, sedimentation; no regime-dependent parameter switches. |
| INP size resolution | Chen 2024 shows nucleation depends on particle size (≥90 nm threshold); SBM can represent the size-dependent activation spectrum while bulk schemes use a single frozen fraction. |
| Surface-science grounding | Huetner 2025 atomic-resolution work shows that Ag-terminated surface governs nucleation; this surface-area-dependent mechanism maps naturally onto a bin-resolved INP representation. |

---

## 7. External Papers (non-CLOUDLAB) Relevant to PolarCAP

### Huetner et al. 2025 — Surface reconstructions govern ice nucleation on AgI
- **Citation:** Huetner, J. I. et al. (2025). Science Advances, eaea2378. doi:10.1126/sciadv.aea2378
- **Key findings:** Ag-terminated basal plane of AgI undergoes (2×2) vacancy reconstruction preserving hexagonal symmetry → epitaxial ice growth. I-terminated surface has rectangular reconstruction → only 3D ice clusters. Surface atomic structure, not just bulk lattice match, determines nucleation efficiency.
- **Relevance:** Provides atomic-level grounding for why AgI is effective. The surface-area-dependent mechanism maps naturally onto bin-resolved INP representations in COSMO-SPECS.

### Chen et al. 2024 — Critical size of AgI seeding particles
- **Citation:** Chen, J. et al. (2024). Geophys. Res. Lett., 51, e2023GL106680.
- **Key findings:** Flare-generated AgI has nucleation ability matching pure AgI above ~200 nm. Non-AgI impurities suppress efficiency below ~90 nm (vs 40 nm for pure AgI). Derived critical mass ice-active site density.
- **Relevance:** Constrains the size-dependent nucleation spectrum used in our INP parameterisation. Spectral-bin microphysics can resolve this activation threshold per size bin.

### Miller et al. 2025 — Quantified INF in natural clouds
- **Citation:** Miller, A. J. et al. (2025). Atmos. Chem. Phys., 25, 5387–5407.
- **Key findings:** First field INF quantification (0.07–1.63%); immersion freezing dominant; constant INF per experiment; aerosol-limited regime; field INFs fall between Marcolli 2016 and DeMott 1995 lab values.
- **Relevance:** Directly constrains our nucleation rate. The linear ICNC–seed relationship validates the Lagrangian plume tracking approach.

### Omanovic et al. 2025 — Lagrangian trajectories in ICON
- **Citation:** Omanovic, N. et al. (2025). J. Adv. Model. Earth Syst., 17, e2025MS005016.
- **Key findings:** Default ICON two-moment underestimates growth rates by up to 3×; ventilation coefficient tuning partially helps; WBF LWC depletion systematically too slow; plume dynamics driven by large-scale forcing + topography; microphysical budget analysis (T*, qv*, buoyancy, vertical acceleration).
- **Relevance:** Most directly comparable to PolarCAP. Demonstrates limitations of bulk microphysics that spectral-bin overcomes. Their Lagrangian trajectory + ensemble statistics approach is methodologically aligned with ours.

### Chen, M. et al. 2025 — Turbulence and glaciogenic seeding
- **Citation:** Chen, M. et al. (2025). Atmos. Chem. Phys., 25, 7581–7596.
- **Key findings:** WRF-LES with spectral-bin microphysics (fast SBM); stronger turbulence enhances AgI dispersion, nucleation, and growth; faster glaciation → positive-to-negative seeding effect transition; "robbing Peter to pay Paul" more pronounced at higher AgI rates.
- **Relevance:** Independent confirmation that spectral-bin microphysics is needed for seeding studies. Turbulence–microphysics coupling is relevant for interpreting plume dynamics in our study.

### Marcolli et al. 2016 — AgI ice nucleation review
- **Citation:** Marcolli, C. et al. (2016). Atmos. Chem. Phys., 16, 8915–8937.
- **Key findings:** Comprehensive review of 60 years of AgI nucleation studies. Nucleation depends on particle position in droplet, surface charges, lattice match, dissolution history.
- **Relevance:** Foundational reference for AgI nucleation parameterisation; sigmoid fit used in Omanovic 2024/2025.

---

## 8. Suggested Next Notebooks (in `notebooks/`)

Aligned with article red line: *Lagrangian, process-resolved ice growth in seeded stratus*.

1. **`notebooks/cloudlab_comparison/01-growth-rate-sensitivity.ipynb`**
   - Adopt Omanovic 2025 ventilation × capacity sensitivity matrix approach.
   - Compare bin-resolved growth rates from COSMO-SPECS with their bulk-tuned values.
   - Show that SBM naturally captures the spread without ad-hoc tuning.

2. **`notebooks/cloudlab_comparison/02-lagrangian-icnc-lwc-comparison.ipynb`**
   - Replicate Omanovic 2025 f05/f12 style: ICNC + relative ΔLWC time series along trajectories.
   - Overlay COSMO-SPECS, ICON (Omanovic 2025 data if available), and HOLIMO.
   - Highlight differences in WBF efficiency between bulk and spectral-bin.

3. **`notebooks/cloudlab_comparison/03-inf-model-vs-field.ipynb`**
   - Compare model-derived nucleated fractions with Miller 2025 field INFs.
   - Scatter plot: model INF vs observed INF per mission, colored by temperature.
   - Adopt Miller fig4 residual analysis style.

4. **`notebooks/cloudlab_comparison/04-reflectivity-model-vs-radar.ipynb`**
   - PAMTRA Ze along plume vs radar.
   - Scatter + 1:1 + regression (Fuchs fig08 style).

5. **`notebooks/process_attribution/01-bin-resolved-tendency-map.ipynb`**
   - Time–diameter map with dominant process color overlay.
   - Central figure for the article highlighting SBM advantage.

---

## 9. References (for biblio)

All new citations added to `article_draft/PolarCAP/biblio.bib`:
- Omanovic2025 (JAMES, Lagrangian trajectories)
- Huetner2025 (Science Advances, AgI surface reconstructions)
- ChenML2025 (ACP, turbulence + seeding)
- Miller2025, Chen2024, Marcolli2016, Fuchs2025, Fuchs2025b (already present)
- Ramelli2024, Omanovic2024a, Omanovic2024b (already present)
