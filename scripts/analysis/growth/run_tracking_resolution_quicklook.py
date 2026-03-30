#!/usr/bin/env python3
"""
Publication-style Tobac quicklook: column-summed Δn_f (flare − ref) maps with track overlays.

Mirrors the legacy notebook panels (``sum('altitude')``, log colour scale, track markers)
for two resolutions — typically 400 m (domain 50x40) and 100 m (200x160) — with
experiments selected via ``discover_3d_runs`` (flare list = lflare=true).

Writes:
  - One 4×4 figure per resolution (16 time frames, subsampled).
  - One 4×8 facet figure: left block 400 m, right block 100 m (same frame indices).

Example:
  PYTHONPATH=src python scripts/analysis/growth/run_tracking_resolution_quicklook.py \\
    --cs-run cs-eriswil__20260123_180947 --flare-idx 0 --ref-idx 0

Requires CS_RUNS_DIR (or --root) with RUN_ERISWILL_50x40x100 and RUN_ERISWILL_200x160x100
layout. Optional LV1a track CSVs under --tracking-root/<cs_run>/lv1_tracking/ for overlays.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent.parent / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.colors as mcolors  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import xarray as xr  # noqa: E402

from utilities.model_helpers import convert_units_3d, make_3d_preprocessor  # noqa: E402
from utilities.namelist_metadata import update_dataset_metadata  # noqa: E402
from utilities.plotting import new_fjet2, set_name_tick_params  # noqa: E402
from utilities.processing_paths import get_output_root, get_runs_root  # noqa: E402
from utilities.style_profiles import FULL_COL_IN, apply_publication_style  # noqa: E402
from utilities.tracking_pipeline import (  # noqa: E402
    RunContext,
    discover_3d_runs,
    prep_tobac_input,
)


def _load_nf_flare_ref(ctx: RunContext) -> tuple[xr.DataArray, xr.DataArray]:
    """Load ``nf`` (and ``t``) for flare and reference in the reduced tracking domain."""
    reduced = ctx.reduced_domain()
    nml_f = ctx.meta[ctx.flare_exp_name]["INPUT_ORG"]
    nml_r = ctx.meta[ctx.ref_exp_name]["INPUT_ORG"]

    def _one(nc: str, nml: dict) -> xr.Dataset:
        pre = make_3d_preprocessor(nc, ctx.extpar_file, nml)
        ds = xr.open_mfdataset(nc, preprocess=pre, chunks={"time": 4}, parallel=True)
        rid = nc.split("/")[-1].split("_")[1].split(".")[0]
        ds.attrs["ncfile"] = nc
        ds.attrs["run_id"] = rid
        ds = update_dataset_metadata(ds)
        ds = ds.sel(reduced)
        ds = convert_units_3d(ds, ds["rho"])
        return ds[["nf", "t"]]

    ds_f = _one(ctx.flare_nc_file, nml_f)
    ds_r = _one(ctx.ref_nc_file, nml_r)
    if ds_r.sizes.get("time") != ds_f.sizes.get("time") or not np.array_equal(
        ds_r["time"].values, ds_f["time"].values
    ):
        ds_r = ds_r.interp(time=ds_f["time"])
    return ds_f["nf"], ds_r["nf"]


def build_column_tobac_input(
    ctx: RunContext,
    diam_slice: slice,
) -> xr.DataArray:
    """Δn_f field (3D+time); same construction as LV1a."""
    nf_f, nf_r = _load_nf_flare_ref(ctx)
    q_f = nf_f.isel(diameter=diam_slice).sum("diameter")
    q_r = nf_r.isel(diameter=diam_slice).sum("diameter")
    return prep_tobac_input(q_f, q_r, mask_threshold=1e-12)


def subsample_time(da: xr.DataArray, max_frames: int = 16) -> xr.DataArray:
    """Evenly stride time like the legacy notebook (≈16 panels)."""
    nt = int(da.sizes["time"])
    if nt <= max_frames:
        return da
    step = max(1, nt // max_frames)
    idx = np.arange(0, nt, step)[:max_frames]
    return da.isel(time=idx)


def subsample_pair(
    da_a: xr.DataArray, da_b: xr.DataArray, max_frames: int = 16
) -> tuple[xr.DataArray, xr.DataArray]:
    """Same time indices on *da_a* and *da_b* (must share length after alignment)."""
    nt = int(da_a.sizes["time"])
    if int(da_b.sizes["time"]) != nt:
        raise ValueError("subsample_pair: time dimensions must match (interp first).")
    if nt <= max_frames:
        return da_a, da_b
    step = max(1, nt // max_frames)
    idx = np.arange(0, nt, step)[:max_frames]
    return da_a.isel(time=idx), da_b.isel(time=idx)


def _mean_dt_seconds(da: xr.DataArray) -> float:
    t = da["time"].values.astype("datetime64[s]").astype(float)
    d = np.diff(t)
    if d.size == 0:
        return 30.0
    return float(np.mean(d))


def load_tracks_csv(path: Path | None) -> pd.DataFrame | None:
    if path is None or not path.is_file():
        return None
    df = pd.read_csv(path)
    if "time" not in df.columns:
        return None
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    return df


def _overlay_markers(ax: plt.Axes) -> None:
    """Observation and seeding sites (legacy notebook)."""
    ax.scatter(
        [7.8730], [47.0695], s=25, marker="x", color="red", zorder=5, clip_on=False
    )
    ax.scatter(
        [7.90522], [47.07425], s=25, marker="x", color="green", zorder=5, clip_on=False
    )


def overlay_tracks(ax: plt.Axes, tracks: pd.DataFrame | None, t_sel: np.datetime64, dt: float) -> None:
    if tracks is None or len(tracks) == 0:
        return
    t0 = np.datetime64(t_sel)
    tol = max(0.6 * dt, 5.0)
    tt = tracks["time"].values.astype("datetime64[ns]")
    mask = np.abs((tt - t0).astype("timedelta64[s]").astype(float)) <= tol
    if not np.any(mask):
        return
    sub = tracks.loc[mask]
    for cell_id, grp in sub.groupby("cell"):
        ax.scatter(
            grp["longitude"].values,
            grp["latitude"].values,
            s=22,
            marker="x",
            linewidths=0.6,
            alpha=0.9,
            zorder=6,
            clip_on=False,
        )


def draw_grid_4x4(
    surface: xr.DataArray,
    *,
    resolution_label: str,
    tracks: pd.DataFrame | None,
    threshold: float,
    cs_run: str,
    exp_name: str,
    out: Path,
    legend_handles: bool = False,
) -> None:
    """4×4 maps: column-summed field, log scale."""
    cmap = new_fjet2
    norm = mcolors.LogNorm(vmin=threshold, vmax=threshold * 1e3)
    dt = _mean_dt_seconds(surface)
    nt = int(surface.sizes["time"])
    nrows, ncols = 4, 4
    fig_w = FULL_COL_IN * 2.1
    fig_h = FULL_COL_IN * 1.55
    fig, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(fig_w, fig_h),
        sharex=True,
        sharey=True,
        layout="tight",
    )
    mappable = None
    for i in range(nrows * ncols):
        ax = axes.flat[i]
        if i >= nt:
            ax.set_visible(False)
            continue
        sl = surface.isel(time=i)
        pm = ax.pcolormesh(
            sl["longitude"],
            sl["latitude"],
            sl.values,
            shading="nearest",
            cmap=cmap,
            norm=norm,
        )
        mappable = pm
        set_name_tick_params(ax)
        tval = surface["time"].values[i]
        ax.set_title(np.datetime_as_string(np.datetime64(tval), unit="s")[-8:], fontsize=6)
        _overlay_markers(ax)
        overlay_tracks(ax, tracks, np.datetime64(tval), dt)
        ax.set_xlabel("")
        ax.set_ylabel("")

    if mappable is not None:
        cbar = fig.colorbar(
            mappable,
            ax=axes.ravel().tolist(),
            shrink=0.35,
            orientation="horizontal",
            pad=0.06,
        )
        cbar.set_label(r"$\Delta n_f$ (${\rm L^{-1}}$)")  # noqa: W605

    tag = f"{resolution_label}  |  {cs_run}  |  flare {exp_name}  |  column sum"
    fig.suptitle(tag, fontsize=8, y=1.02)
    if legend_handles:
        ax0 = axes.flat[0]
        h_obs = Line2D(
            [0], [0], marker="x", color="r", linestyle="None", markersize=5, label="Holimo site"
        )
        h_seed = Line2D(
            [0], [0], marker="x", color="g", linestyle="None", markersize=5, label="Seeding"
        )
        h_trk = Line2D(
            [0], [0], marker="x", color="0.15", linestyle="None", markersize=5, label="Track"
        )
        ax0.legend(
            handles=[h_obs, h_seed, h_trk],
            loc="lower right",
            fontsize=5,
            framealpha=0.9,
        )
    fig.savefig(out, dpi=300, bbox_inches="tight")
    plt.close(fig)


def draw_facet_4x8(
    surf_400: xr.DataArray,
    surf_100: xr.DataArray,
    *,
    tracks_400: pd.DataFrame | None,
    tracks_100: pd.DataFrame | None,
    threshold: float,
    cs_run: str,
    exp400: str,
    exp100: str,
    out: Path,
) -> None:
    """One figure: rows = time slice (0..3), cols 0–3 = 400 m, 4–7 = 100 m (4 columns each)."""
    cmap = new_fjet2
    norm = mcolors.LogNorm(vmin=threshold, vmax=threshold * 1e3)
    dt4 = _mean_dt_seconds(surf_400)
    dt1 = _mean_dt_seconds(surf_100)
    nframes = min(16, int(surf_400.sizes["time"]), int(surf_100.sizes["time"]))
    fig_w = FULL_COL_IN * 3.35
    fig_h = FULL_COL_IN * 1.55
    fig, axes = plt.subplots(4, 8, figsize=(fig_w, fig_h), sharex=False, sharey=False, layout="tight")
    mappable = None
    for row in range(4):
        for loc in range(4):
            idx = row * 4 + loc
            if idx >= nframes:
                axes[row, loc].set_visible(False)
                axes[row, loc + 4].set_visible(False)
                continue
            for col_off, (surf, tracks, dt, lab) in enumerate(
                (
                    (surf_400, tracks_400, dt4, "400 m"),
                    (surf_100, tracks_100, dt1, "100 m"),
                )
            ):
                ax = axes[row, loc + 4 * col_off]
                sl = surf.isel(time=idx)
                pm = ax.pcolormesh(
                    sl["longitude"],
                    sl["latitude"],
                    sl.values,
                    shading="nearest",
                    cmap=cmap,
                    norm=norm,
                )
                mappable = pm
                set_name_tick_params(ax)
                tval = surf["time"].values[idx]
                ax.set_title(
                    f"{lab[:5]}  {np.datetime_as_string(np.datetime64(tval), unit='s')[-8:]}",
                    fontsize=5,
                )
                _overlay_markers(ax)
                overlay_tracks(ax, tracks, np.datetime64(tval), dt)
                ax.set_xlabel("")
                ax.set_ylabel("")
            if row == 0 and loc == 0:
                axes[0, 0].annotate(
                    "400 m",
                    xy=(0.5, 1.18),
                    xycoords="axes fraction",
                    ha="center",
                    fontsize=7,
                    fontweight="semibold",
                )
                axes[0, 4].annotate(
                    "100 m",
                    xy=(0.5, 1.18),
                    xycoords="axes fraction",
                    ha="center",
                    fontsize=7,
                    fontweight="semibold",
                )

    if mappable is not None:
        cbar = fig.colorbar(
            mappable,
            ax=axes.ravel().tolist(),
            shrink=0.28,
            orientation="horizontal",
            pad=0.05,
        )
        cbar.set_label(r"$\Delta n_f$ (${\rm L^{-1}}$)")  # noqa: W605

    fig.suptitle(
        f"Resolution facet  |  {cs_run}  |  {exp400} vs {exp100}",
        fontsize=8,
        y=1.01,
    )
    fig.savefig(out, dpi=300, bbox_inches="tight")
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="400 m / 100 m Tobac quicklook facet figures")
    p.add_argument("--root", default=None, help="COSMO runs root (default: $CS_RUNS_DIR)")
    p.add_argument("--cs-run", required=True, help="Ensemble id, e.g. cs-eriswil__20260123_180947")
    p.add_argument(
        "--cs-run-100",
        default=None,
        help="Optional different ensemble id for 200x160 (default: same as --cs-run)",
    )
    p.add_argument("--domain-400", default="50x40", help="Domain tag for 400 m")
    p.add_argument("--domain-100", default="200x160", help="Domain tag for 100 m")
    p.add_argument("--flare-idx", type=int, default=0)
    p.add_argument(
        "--ref-idx",
        type=int,
        default=0,
        help="Reference experiment index in the ref-only list (default 0). Use -1 for auto-match.",
    )
    p.add_argument("--threshold", type=float, default=1.0, help="Tobac / colour floor (L⁻¹)")
    p.add_argument(
        "--diameter-slice",
        default="0:",
        help="Python slice on diameter axis, e.g. '0:' (full nf), '30:50' (legacy qi band)",
    )
    p.add_argument(
        "--tracer-tag",
        default="nf",
        help="Suffix for track CSV names: {flare_exp}_{tag}_tobac_track.csv",
    )
    p.add_argument(
        "--tracking-root",
        default=None,
        help="Parent of <cs_run>/lv1_tracking (default: get_output_root for --cs-run)",
    )
    p.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: repo output/gallery)",
    )
    return p.parse_args()


def _parse_slice(s: str) -> slice:
    parts = s.split(":")
    if len(parts) == 2:
        a, b = parts
        start = int(a) if a.strip() else None
        stop = int(b) if b.strip() else None
        return slice(start, stop)
    if len(parts) == 3:
        a, b, c = parts
        start = int(a) if a.strip() else None
        stop = int(b) if b.strip() else None
        step = int(c) if c.strip() else None
        return slice(start, stop, step)
    raise ValueError(f"Invalid slice: {s!r}")


def main() -> None:
    args = parse_args()
    runs_root = get_runs_root(args.root)
    if not runs_root:
        print("Set CS_RUNS_DIR or pass --root", file=sys.stderr)
        sys.exit(1)

    ctx400, err400 = discover_3d_runs(
        runs_root,
        args.domain_400,
        args.cs_run,
        flare_idx=args.flare_idx,
        ref_idx=args.ref_idx,
        threshold=args.threshold,
    )
    cs100 = args.cs_run_100 or args.cs_run
    ctx100, err100 = discover_3d_runs(
        runs_root,
        args.domain_100,
        cs100,
        flare_idx=args.flare_idx,
        ref_idx=args.ref_idx,
        threshold=args.threshold,
    )
    if ctx400 is None:
        print(f"400 m context: {err400}", file=sys.stderr)
    if ctx100 is None:
        print(f"100 m context: {err100}", file=sys.stderr)
    if ctx400 is None or ctx100 is None:
        sys.exit(1)

    dslice = _parse_slice(args.diameter_slice)
    apply_publication_style()

    tob400 = build_column_tobac_input(ctx400, dslice)
    tob100 = build_column_tobac_input(ctx100, dslice)
    col400 = tob400.sum("altitude")
    col100 = tob100.sum("altitude")
    if col100.sizes["time"] != col400.sizes["time"] or not np.array_equal(
        col100["time"].values, col400["time"].values
    ):
        col100 = col100.interp(time=col400["time"])
    sur400, sur100 = subsample_pair(col400, col100)

    if args.tracking_root:
        tr_parents = Path(args.tracking_root)
        tcsv_400 = tr_parents / args.cs_run / "lv1_tracking" / f"{ctx400.flare_exp_name}_{args.tracer_tag}_tobac_track.csv"
        tcsv_100 = tr_parents / cs100 / "lv1_tracking" / f"{ctx100.flare_exp_name}_{args.tracer_tag}_tobac_track.csv"
    else:
        out400 = Path(get_output_root(None, runs_root=runs_root, cs_run=args.cs_run))
        out100 = Path(get_output_root(None, runs_root=runs_root, cs_run=cs100))
        tcsv_400 = out400 / args.cs_run / "lv1_tracking" / f"{ctx400.flare_exp_name}_{args.tracer_tag}_tobac_track.csv"
        tcsv_100 = out100 / cs100 / "lv1_tracking" / f"{ctx100.flare_exp_name}_{args.tracer_tag}_tobac_track.csv"
    tr400 = load_tracks_csv(tcsv_400)
    tr100 = load_tracks_csv(tcsv_100)

    repo = Path(__file__).resolve().parents[3]
    out_dir = Path(args.out_dir) if args.out_dir else repo / "output" / "gallery"
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"tobac_quicklook_{args.tracer_tag}_{args.cs_run.replace('__', '_')}"

    draw_grid_4x4(
        sur400,
        resolution_label="400 m",
        tracks=tr400,
        threshold=args.threshold,
        cs_run=args.cs_run,
        exp_name=ctx400.flare_exp_name,
        out=out_dir / f"{stem}_400m.png",
        legend_handles=True,
    )
    draw_grid_4x4(
        sur100,
        resolution_label="100 m",
        tracks=tr100,
        threshold=args.threshold,
        cs_run=cs100,
        exp_name=ctx100.flare_exp_name,
        out=out_dir / f"{stem}_100m.png",
        legend_handles=False,
    )
    draw_facet_4x8(
        sur400,
        sur100,
        tracks_400=tr400,
        tracks_100=tr100,
        threshold=args.threshold,
        cs_run=f"{args.cs_run} | {cs100}",
        exp400=ctx400.flare_exp_name,
        exp100=ctx100.flare_exp_name,
        out=out_dir / f"{stem}_facet_4x8.png",
    )
    print("Wrote:")
    print(f"  {out_dir / f'{stem}_400m.png'}")
    print(f"  {out_dir / f'{stem}_100m.png'}")
    print(f"  {out_dir / f'{stem}_facet_4x8.png'}")
    if tr400 is None:
        print(f"  (no track overlay 400 m: missing {tcsv_400})")
    if tr100 is None:
        print(f"  (no track overlay 100 m: missing {tcsv_100})")


if __name__ == "__main__":
    main()
