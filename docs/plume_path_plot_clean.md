# Plume Path Plot Clean - Working Notes

## Why this file exists

This is the compact "redo-fast" documentation for `plume_path_plot_clean.ipynb`: what broke, what was fixed, what must stay consistent, and where to optimize next.

## Core outcomes from today

- Built a clean plotting notebook around a robust loader + plotting API (`load_plume_path_runs`, `plot_plume_path_sum`).
- Fixed runaway x ticks (`Locator.MAXTICKS`) by adaptive elapsed-time tick logic.
- Resolved empty panels by applying the required model-time correction (`+3h` for early timestamps).
- Resolved alignment/merge failures by figure1-style preprocessing (swap to `time`, dedup, sort, reindex to common 10 s grid).
- Implemented full-cell aggregation workflow (per-cell load -> align -> concat on `cell`) so all tracked cells contribute.
- Added practical diagnostics (`diagnostics_table`) to quickly verify data availability in chosen `xlim`.
- Added HOLIMO overlay pipeline with elapsed anchors, optional inset colorbar, and label separation handling.
- Added symlog behavior for ignition-at-zero and early-growth visibility, including optional panel shifts.

## Latest updates (new)

- Hardened edge cases where `run[kind]` is not an `xarray.Dataset` (e.g., `[]` from missing files):
  - `build_common_xlim` skips invalid entries,
  - `diagnostics_table` reports `invalid kind type: <type>`,
  - `plot_plume_path_sum` only plots valid dataset entries.
- Fixed Matplotlib inset bug by removing unsupported `ha=` from `inset_axes(...)`.
- Refactored HOLIMO overlay from dense 2D field to compact mission-wise overlays:
  - first as mean diameter ± std trajectories,
  - then to scatter clouds for lower visual clutter.
- Corrected HOLIMO time anchoring to use `seeding_start_times` (elapsed since seeding start), with growth-based fallback.
- Updated HOLIMO scatter color semantics:
  - dot color now reflects HOLIMO number concentration at the plotted mean-diameter point,
  - mapping uses the same normalization range as the model panel colorbar.
- Updated legend semantics:
  - legend markers are now fixed black (`alpha=0.8`) for mission identity only,
  - scatter dots remain value-colored by concentration.

## Non-negotiable loader behavior (keep this)

1. Read cell files per run/kind (figure1-style file discovery).
2. Preprocess each dataset:
   - swap `path -> time` where needed,
   - apply timestamp correction when needed,
   - remove duplicate times,
   - sort by `time`.
3. Build one common run time grid (`10s`) across cells.
4. Reindex each cell dataset to that grid (`nearest`, tolerance `5s`).
5. Concatenate over `cell`; keep zeros/nans handling explicit (`xr.where(ds_kind > 0, ds_kind, np.nan)`).

### Minimal snippet (loader pattern)

```python
ds_time = pd.date_range(start, end, freq="10s")
aligned = [ds.reindex(time=ds_time, method="nearest", tolerance="5s", fill_value=0) for ds in raw]
ds_kind = xr.concat(aligned, dim="cell")
ds_kind = xr.where(ds_kind > 0, ds_kind, np.nan)
```

## Axis behavior that matters

- Use `x_axis_fmt` modes intentionally:
  - `elapsed`: direct post-ignition minutes.
  - `log`: suppresses detail near zero unless shifted.
  - `symlog`: keeps ignition marker at `0` but still resolves sub-minute behavior (`linthresh=0.1`).
- Current notebook includes two symlog `linscale` values (`0.001` and `0.01`) in different branches. This should be unified to one value to avoid inconsistent rendering between pre/post overlay axis re-application.
- `build_common_xlim(...)` currently anchors to `12:29:00` by default when no explicit anchor is passed. Keep this domain assumption documented if reused in other days/cases.

## HOLIMO overlay notes

- Overlay config is optional but works best when these are always set:
  - `obs_ids`, `time_frames_plume`, `growth_times_min`,
  - `var`, `norm`, `threshold`, `unit_factor`.
- Use computed elapsed anchors (`compute_holimo_elapsed_anchors`) unless manually validated anchor times exist.
- Inset colorbar in low-data panel is a good compromise; keep host selection by minimal panel fill.
- Seeding-label overlap was reduced via `label_min_sep` and dynamic `y` stepping; maintain this pattern.

## Fast debug checklist (first 2 minutes)

1. `diagnostics_table(..., xlim=...)` -> verify `n_time_in_xlim > 0`.
2. Check dataset time range really overlaps requested window.
3. Check `kind` exists (`integrated` vs `vertical`) before plotting.
4. Confirm expected variable exists (`nf`, `qi`, `qs`) after loading.
5. For empty plot: test with `xlim=None` once to confirm data presence.

## Suggestions for code optimization

- Refactor notebook helpers into one reusable module (e.g., `python/plotting_scripts/plume_plot_helpers.py`) and keep notebook as orchestration only.
- Add a small cache layer for processed run loads to avoid repeated `open_dataset` + reindex work.
- Separate plotting concerns:
  - axis formatting,
  - data preparation,
  - overlays,
  - figure layout.
  This will reduce accidental regressions when tweaking one feature (like symlog).
- Normalize all unit conversions at load time (especially HOLIMO scaling) so plotting code never re-applies unit factors ad hoc.
- Add tiny regression tests for:
  - duplicate-time handling,
  - xlim overlap detection,
  - symlog zero/near-zero ticks,
  - overlay colorbar placement fallback.

## Suggestions for data analysis and interpretation

- Quantify early growth phase robustly:
  - compare slopes in first minute after ignition (or anchor) across runs A-H,
  - report uncertainty from time alignment sensitivity (e.g., +/- 10 s).
- For D-H shifted panels:
  - store explicit per-run shift metadata and include in figure caption to avoid interpretability drift.
- Compare integrated and vertical views side-by-side for key runs:
  - integrated gives bulk growth behavior,
  - vertical highlights altitude-dependent microphysics structure.
- Add one compact diagnostic panel per run:
  - total finite counts in xlim,
  - first valid timestamp,
  - median/peak `nf` over time.
  This helps distinguish physical absence from pipeline masking/reindex gaps.

## Repro command cell pattern to keep

```python
fig, _ = plot_plume_path_sum(
    datasets,
    kind="integrated",
    variable="nf",
    xlim=xlim,
    common_xlim_minutes=35,
    ylim=(1, 2e3),
    zlim=(1e-1, 2e3),
    cmap=new_jet3_soft,
    log_norm=True,
    x_axis_fmt="symlog",
    add_missing_data=True,
    holimo_overlay=holimo_overlay_cfg,
)
fig.savefig("plume_path_plot_clean_symlog.png")
```

## Investigation roadmap from related notebooks (plan only)

This section is based on a full pass through:
- `python/plotting_scripts/03-Top+Side_View_Animation.ipynb`
- `python/plotting_scripts/04-Merged-Visualization.ipynb`
- `python/plotting_scripts/04-time-height-spectra-holimo.ipynb`
- `python/plotting_scripts/05-Visualise-strong-precip.ipynb`

The goal is deeper process-level analysis, not only figure production.

### 1) Build a consistent multi-run analysis matrix first

Use the same variable set and units for all runs before plotting:
- Dynamics: `w`, `dw`, `u`, `v`, `t`, `rho`
- Thermodynamics: `qv`, `S_water`, `S_ice`
- Microphysics bulk: `qc`, `qi`, `qs`, `qi+qs`
- Spectral: `nw`, `nf`, `qw`, `qfw`
- Number metrics: `cdnc`, `icnc`

Plan:
- Standardize all outputs to one unit convention (`g m-3`, `L-1`, `%`, `m s-1`).
- Keep one shared preprocessing function for plume-integrated, plume-max, and point-meteogram extractions.
- Save one compact per-run diagnostics table (min/max/mean/std, finite counts, first/last valid time).

### 2) Histograms and distribution analysis (high priority)

Existing hooks already exist in notebooks (`hist2d`, inset histogram axes, commented histogram blocks). Expand to:

- **1D histograms (per run, per phase window)**
  - `w`, `S_ice`, `S_water`, `icnc`, `cdnc`, `qi+qs`, `dw`
  - use both full domain and plume-masked distributions
  - report quantiles (5/25/50/75/95%) for each

- **2D histograms / density maps**
  - `qc` vs `qi+qs` (already hinted in `hist2d` logic)
  - `w` vs `S_ice`
  - `S_ice` vs `icnc`
  - `dw` vs `qfw` tendency
  - add marginal histograms + log-scaled color counts

- **Time-evolving histograms**
  - sliding 5 min windows around flare ignition and precipitation onset
  - compare pre-seeding / growth / mature / decay phases

### 3) Strong precipitation special-case campaign across cs_runs

Use the run groups already present in notebooks:
- 50x40 main: `cs-eriswil__20251125_114053`, `cs-eriswil__20251129_230943`
- 50x40 later campaign: `cs-eriswil__20260121_131528`, `cs-eriswil__20260127_211338`
- 200x160 references: `cs-eriswil__20251209_001346`, `cs-eriswil__20260123_180947`
- event-focused run: `cs-eriswil__20260210_113944`

Plan:
- Define objective "strong precip" masks (e.g., top 5% `qi+qs` plume maxima, sustained >N minutes).
- For each flagged event, produce a fixed diagnostic card:
  - map (lat/lon integrated `qi+qs`)
  - time-height slices (`w`, `S_ice`, `icnc`, `qc`, `qi`, `qs`)
  - spectral tendency panels (`d/dt qw`, `d/dt qfw`)
  - event histogram panel (1D + 2D)
- Rank events by severity metrics (peak intensity, duration, vertical depth).

### 4) Vertical velocity and supersaturation process analysis

Notebook 05 already computes `S_ice`, `S_water` and has meteogram panels. Extend with:

- **Joint process regimes**
  - classify each grid point/time into:
    - updraft supersaturated (`w>0`, `S_ice>0`)
    - updraft subsaturated
    - downdraft supersaturated
    - downdraft subsaturated
  - track regime occupancy over time and altitude

- **Lag analysis**
  - cross-correlation of `w` and `S_ice` with `icnc`/`qfw` tendencies
  - estimate lead-lag windows for initiation of ice growth

- **Vertical coherence**
  - altitude-by-altitude correlation matrices for `w`, `S_ice`, `icnc`
  - identify coherent growth layers vs detached layers

### 5) Droplet growth/shrinkage and ice growth/shrinkage

Notebook 04 already differentiates plume variables over path/time (`differentiate(coord='path')`).

Plan:
- Track `d/dt mean_qc` and `d/dt mean_qi` with confidence bands across cells.
- Add sign-persistence maps:
  - sustained growth zones (`d/dt > 0` for >= M steps)
  - sustained decay zones (`d/dt < 0`)
- Separate by diameter bands:
  - small droplet, transition, large ice bins (using diameter indices already used for line markers).

### 6) WBF-oriented analysis (Bergeron-Wegener-Findeisen)

Use co-located fields to construct a WBF proxy:
- WBF-favorable if `S_ice > 0` and `S_water <= 0` with simultaneous increase in ice (`nf`/`qfw`) and decrease in liquid (`nw`/`qw`).

Plan:
- Compute WBF mask in time-height and plume-path coordinates.
- Quantify:
  - WBF fraction of plume volume over time
  - WBF layer thickness over altitude
  - conversion efficiency proxy: `(+d qfw/dt) / (-d qw/dt)` in WBF mask
- Compare WBF metrics between strong-precip and weak-precip runs.

### 7) HOLIMO-model comparison deepening

Notebook 04-time-height already has `plot_ts_spectra(...)` and summary statistics.

Plan:
- Harmonize time windows and altitude windows across all comparisons.
- Compare not only integrated spectra but:
  - median spectra per phase window
  - percentile envelopes (e.g., 25-75)
  - bias by diameter range (small/medium/large bins)
- Add event-anchored comparison:
  - align by first observed ice signal instead of wall-clock only.

### 8) Plot products to implement later (ordered deliverables)

1. Multi-run diagnostics table (CSV + markdown summary)
2. Strong-event ranking table
3. Hist package:
   - per-run 1D histograms
   - per-run 2D hist panels
4. Process panels:
   - `w/S_ice/icnc` time-height
   - WBF mask + conversion efficiency
5. Comparative summary figure:
   - strong vs weak precip composites
6. HOLIMO vs model phase-aligned spectra report

### 9) Practical cautions found during notebook review

- Some notebooks trigger expensive `persist()` calls and can stall/interrupt; stage computations and cache intermediates.
- Tick formatting in elapsed-time axes must remain bounded (avoid fixed dense locators over long ranges).
- Keep time-reference consistency (`datetime`, `elapsed`, `log/symlog`) when comparing runs.
- Explicitly store thresholds used for masks (`qi+qs`, `S_ice`, `w`) for reproducibility.

## References (today, portable)

- Tick fix and clean notebook
  - UUID: `982bb72c-4dcf-4584-9c86-e929929de92f`
  - File: `/Users/schimmel/.cursor/projects/Users-schimmel-code-polarcap/agent-transcripts/982bb72c-4dcf-4584-9c86-e929929de92f/982bb72c-4dcf-4584-9c86-e929929de92f.jsonl`
- Legend and HOLIMO adjustments
  - UUID: `ce4d0ffe-1618-4463-bd76-67441826c325`
  - File: `/Users/schimmel/.cursor/projects/Users-schimmel-code-polarcap/agent-transcripts/ce4d0ffe-1618-4463-bd76-67441826c325/ce4d0ffe-1618-4463-bd76-67441826c325.jsonl`
- Waterfall compatibility fixes
  - UUID: `601bb949-4c70-4bc2-ac88-0124d72c3e5f`
  - File: `/Users/schimmel/.cursor/projects/Users-schimmel-code-polarcap/agent-transcripts/601bb949-4c70-4bc2-ac88-0124d72c3e5f/601bb949-4c70-4bc2-ac88-0124d72c3e5f.jsonl`

