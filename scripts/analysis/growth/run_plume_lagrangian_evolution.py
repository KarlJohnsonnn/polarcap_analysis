#!/usr/bin/env python3
"""Render the notebook 03 plume-lagrangian summary figure as a reproducible script.

Figure construction lives in ``utilities.plume_lagrangian`` (ensemble assembly, HOLIMO overlay, histogram)
and ``utilities.plume_path_plot`` (``plot_plume_path_sum`` for the Hovmöller-style panels).

Units and spectral normalization (read before interpreting the PNG)
---------------------------------------------------------------------
**Axes.** Elapsed time is in minutes; equivalent diameter is micrometres (see ``render_plume_lagrangian_figure``
for linear vs logarithmic time axes).

**Model field on the heatmaps (ensemble mean ``nf``).** ``build_ensemble_mean_datasets`` averages ``nf`` across
runs, then divides each diameter bin by ``Δln(D) = ln(D_edge[i+1]) - ln(D_edge[i])`` (bin edges from
``_diam_log_edges``). The plotted quantity is therefore **⟨nf⟩ / Δln(D)** — a density in logarithmic diameter
space (same family as **dN/d ln D** when bin values are content integrated over the bin). It is **not** a plain
**per µm** spectrum (dN/dD) and not “integrated in the bin only” without that width factor.

**Model units.** Plume-path ``nf`` is already in **per litre** (or g L⁻¹, per NetCDF); the figure does **not**
multiply the model by 1000. The colorbar shows ensemble-mean ``n_f/Δln D`` with the file ``units`` attribute.

**HOLIMO units.** Ice PSD variables differ by normalization (see ``HOLIMO_ICE_PSD_META`` in
``utilities.plume_lagrangian``): e.g. ``Ice_PSDnoNorm`` (cm⁻³ per bin), ``Ice_PSDlinNorm`` (cm⁻³ µm⁻¹),
``Ice_PSDlogNorm`` (cm⁻³ per log bin). Default ``Ice_PSDlogNorm`` matches log-diameter panels. HOLIMO is scaled by
``holimo_scale_cm3_to_litres`` (typically ``HOLIMO_CM3_TO_L1`` = 1000) to convert **cm⁻³ → L⁻¹** for overlay;
``*Majsiz`` products list unknown units and use scale 1 until you set a factor explicitly.

**Mass vs number.** Ice ``nf`` is number-related unless your product uses mass; still **per Δln(D)** after ensemble
processing.

**Histogram.** Model curve uses native model units; HOLIMO uses the same cm⁻³→L⁻¹ factor as the scatter overlay.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

# Headless backend: avoids needing a display when saving PNG from CLI or batch jobs.
matplotlib.use("Agg")

# This file lives at scripts/analysis/growth/ — three parents up is the repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
# Allow `python scripts/.../run_*.py` without installing the package as editable.
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utilities.plume_lagrangian import (  # noqa: E402  # imports after sys.path mutation
    DEFAULT_KIND,
    DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
    load_plume_lagrangian_context,
    render_plume_lagrangian_figure,
    save_plume_lagrangian_figure,
)
from utilities.style_profiles import apply_publication_style  # noqa: E402

# Manuscript-facing caption; keep this wording aligned with gallery and registry copies.
FIGURE_CAPTION = """Ensemble-mean Lagrangian plume evolution in equivalent-diameter space compared
with HOLIMO observations. Colors show the ensemble-mean ice number density per logarithmic diameter interval
along the plume; symbols identify the three HOLIMO missions, the lower-left panel zooms the first 15 min
after seeding, and the lower-right panel compares time-averaged size spectra. The comparison highlights
growth from 1-10 um into 10-50 um and 100-300 um size ranges, while the observational spread remains
largest in the coarse-particle tail."""

def main() -> None:
    # Defaults match notebook 03 promotion; override paths when data live elsewhere (e.g. HPC scratch).
    parser = argparse.ArgumentParser(
        description="Render the plume-lagrangian evolution figure promoted from notebook 03."
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        default=REPO_ROOT / "data" / "processed",
        help="Processed plume-path root directory.",
    )
    parser.add_argument(
        "--holimo-file",
        type=Path,
        default=REPO_ROOT
        / "data"
        / "observations"
        / "holimo_data"
        / "CL_20230125_1000_1140_SM058_SM060_ts1.nc",
        help="HOLIMO NetCDF file used for overlay and histogram panels.",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default=DEFAULT_KIND,
        help="Plume-path dataset kind to plot, typically 'extreme'.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PNG path. Defaults to output/gfx/png/03/figure12_ensemble_mean_plume_path_foo.png.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=500,
        help="PNG output DPI.",
    )
    parser.add_argument(
        "--smooth-model-diameter-bins",
        type=int,
        default=DEFAULT_MODEL_DIAMETER_SMOOTHING_BINS,
        help="Centered rectangular smoothing window along model diameter bins. Use 3 for the requested smoothing.",
    )
    args = parser.parse_args()

    apply_publication_style()
    context = load_plume_lagrangian_context(
        REPO_ROOT,
        processed_root=args.processed_root,
        holimo_file=args.holimo_file,
        kind=args.kind,
        smooth_model_diameter_bins=args.smooth_model_diameter_bins,
    )
    print(context["diag"].to_string(index=False))
    print(f"model diameter smoothing bins -> {context['smooth_model_diameter_bins']}")
    for key, val in context.items():
        print(f"{key}: ")
    fig, output_path = render_plume_lagrangian_figure(context, output_path=args.output)
    save_plume_lagrangian_figure(fig, output_path, dpi=args.dpi)


if __name__ == "__main__":
    main()
