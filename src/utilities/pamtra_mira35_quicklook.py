"""Paper-style quicklooks comparing PAMTRA output with raw MIRA35 observations."""
# pyright: reportMissingImports=false

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from matplotlib.ticker import FuncFormatter

from style_profiles import FULL_COL_IN, MAX_H_IN, MM, format_elapsed_minutes_tick

OBS_REFLECTIVITY_CANDIDATES = ("Zg", "Z", "Ze", "SNRg", "SNR")
OBS_VELOCITY_CANDIDATES = ("VELg", "VEL")
MIRA_FILL_THRESHOLD = 1.0e20
PAMTRA_FILL_THRESHOLD = -9990.0
DEFAULT_HEIGHT_ASL_M = 921.0
DEFAULT_MODEL_T0 = np.datetime64("2023-01-25T12:30:00")
DEFAULT_OBSERVATION_IDS = ("SM059", "SM058", "SM060")
DEFAULT_OBSERVATION_T0_CANDIDATES = (
    np.datetime64("2023-01-25T10:50:00"),
    np.datetime64("2023-01-25T10:28:00"),
    np.datetime64("2023-01-25T11:15:00"),
)
DEFAULT_OBSERVATION_TIME_FRAMES = (
    (np.datetime64("2023-01-25T10:56:00"), np.datetime64("2023-01-25T11:04:00")),
    (np.datetime64("2023-01-25T10:35:00"), np.datetime64("2023-01-25T10:42:00")),
    (np.datetime64("2023-01-25T11:24:00"), np.datetime64("2023-01-25T11:29:00")),
)


@dataclass(frozen=True)
class PamtraMira35QuicklookContext:
    """Prepared and windowed datasets for the comparison quicklook."""

    observation: xr.Dataset
    pamtra: xr.Dataset
    reflectivity_source: str
    velocity_source: str
    flip_pamtra_mdv: bool
    observation_label: str
    observation_alignment: str
    model_t0: np.datetime64
    observation_t0: np.datetime64 | tuple[np.datetime64, ...]
    elapsed_start_min: float
    elapsed_end_min: float
    height_min_m: float
    height_max_m: float


def _mask_obs_fill(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float).copy()
    arr[arr > MIRA_FILL_THRESHOLD] = np.nan
    return arr


def _mask_pamtra_fill(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float).copy()
    arr[arr <= PAMTRA_FILL_THRESHOLD] = np.nan
    return arr


def _needs_db_conversion(values: np.ndarray, units: str | None) -> bool:
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return False
    units_norm = (units or "").lower()
    if "db" in units_norm:
        return False
    return float(np.nanmin(finite)) >= 0.0 and float(np.nanmax(finite)) <= 1.0e6


def _to_decibels(values: np.ndarray) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=float)
    valid = np.isfinite(values) & (values > 0.0)
    out[valid] = 10.0 * np.log10(values[valid])
    return out


def _dedupe_time(ds: xr.Dataset) -> xr.Dataset:
    time_vals = np.asarray(ds["time"].values)
    _, unique_idx = np.unique(time_vals, return_index=True)
    return ds.isel(time=np.sort(unique_idx)).sortby("time")


def _elapsed_minutes(times: np.ndarray, t0: np.datetime64) -> np.ndarray:
    return ((np.asarray(times) - np.datetime64(t0)) / np.timedelta64(1, "m")).astype(float)


def _add_elapsed_coord(ds: xr.Dataset, t0: np.datetime64) -> xr.Dataset:
    return ds.assign_coords(elapsed_min=("time", _elapsed_minutes(ds["time"].values, t0)))


def _finite_elapsed_bounds(ds: xr.Dataset) -> tuple[float, float]:
    finite_mask = np.isfinite(np.asarray(ds["reflectivity_dbz"].values, dtype=float)).any(axis=1)
    finite_mask |= np.isfinite(np.asarray(ds["mean_doppler_velocity"].values, dtype=float)).any(axis=1)
    if not finite_mask.any():
        elapsed = np.asarray(ds["elapsed_min"].values, dtype=float)
        return float(np.nanmin(elapsed)), float(np.nanmax(elapsed))
    elapsed = np.asarray(ds["elapsed_min"].values, dtype=float)[finite_mask]
    return float(np.nanmin(elapsed)), float(np.nanmax(elapsed))


def _edges_from_centers(centers: np.ndarray) -> np.ndarray:
    centers = np.asarray(centers, dtype=float)
    if centers.ndim != 1 or centers.size == 0:
        raise ValueError("Expected a non-empty 1D coordinate.")
    if centers.size == 1:
        return np.array([centers[0] - 0.5, centers[0] + 0.5], dtype=float)
    inner = 0.5 * (centers[:-1] + centers[1:])
    lower = centers[0] - 0.5 * (centers[1] - centers[0])
    upper = centers[-1] + 0.5 * (centers[-1] - centers[-2])
    return np.concatenate(([lower], inner, [upper]))


def _nanmean_no_warning(values: np.ndarray, axis: int = 0) -> np.ndarray:
    counts = np.sum(np.isfinite(values), axis=axis)
    sums = np.nansum(values, axis=axis)
    out = np.full_like(sums, np.nan, dtype=float)
    np.divide(sums, counts, out=out, where=counts > 0)
    return out


def _rebin_obs_to_target_elapsed(
    data: xr.DataArray,
    target_elapsed: np.ndarray,
    *,
    average_in_linear_dbz: bool,
) -> np.ndarray:
    values = np.asarray(data.values, dtype=float)
    elapsed = np.asarray(data["elapsed_min"].values, dtype=float)
    target_elapsed = np.asarray(target_elapsed, dtype=float)
    edges = _edges_from_centers(target_elapsed)
    out = np.full((target_elapsed.size, values.shape[1]), np.nan, dtype=float)

    work = values.copy()
    if average_in_linear_dbz:
        valid = np.isfinite(work)
        work[valid] = 10.0 ** (work[valid] / 10.0)

    for idx in range(target_elapsed.size):
        mask = (elapsed >= edges[idx]) & (elapsed < edges[idx + 1])
        if not mask.any():
            continue
        rebinned = _nanmean_no_warning(work[mask], axis=0)
        if average_in_linear_dbz:
            pos = np.isfinite(rebinned) & (rebinned > 0.0)
            tmp = np.full_like(rebinned, np.nan, dtype=float)
            tmp[pos] = 10.0 * np.log10(rebinned[pos])
            rebinned = tmp
        out[idx] = rebinned
    return out


def build_aligned_observation_composite(
    observation: xr.Dataset,
    *,
    model_elapsed: np.ndarray,
    observation_ids: tuple[str, ...] = DEFAULT_OBSERVATION_IDS,
    observation_t0s: tuple[np.datetime64, ...] = DEFAULT_OBSERVATION_T0_CANDIDATES,
    observation_time_frames: tuple[tuple[np.datetime64, np.datetime64], ...] = DEFAULT_OBSERVATION_TIME_FRAMES,
) -> xr.Dataset:
    """Compose curated radar plume windows onto the model elapsed-time grid."""

    if not (len(observation_ids) == len(observation_t0s) == len(observation_time_frames)):
        raise ValueError("Observation IDs, t0 values, and time frames must have equal length.")

    reflectivity_parts: list[np.ndarray] = []
    velocity_parts: list[np.ndarray] = []
    mission_names: list[str] = []

    for obs_id, seed_t0, (time_lo, time_hi) in zip(observation_ids, observation_t0s, observation_time_frames):
        obs_sel = observation.sel(time=slice(time_lo, time_hi))
        if obs_sel.sizes.get("time", 0) == 0:
            continue
        obs_sel = _add_elapsed_coord(obs_sel, seed_t0)
        reflectivity_parts.append(
            _rebin_obs_to_target_elapsed(
                obs_sel["reflectivity_dbz"],
                model_elapsed,
                average_in_linear_dbz=True,
            )
        )
        velocity_parts.append(
            _rebin_obs_to_target_elapsed(
                obs_sel["mean_doppler_velocity"],
                model_elapsed,
                average_in_linear_dbz=False,
            )
        )
        mission_names.append(obs_id)

    if not reflectivity_parts:
        raise ValueError("No observation plume windows contained radar samples.")

    reflectivity_stack = np.stack(reflectivity_parts, axis=0)
    velocity_stack = np.stack(velocity_parts, axis=0)
    reflectivity = _nanmean_no_warning(reflectivity_stack, axis=0)
    velocity = _nanmean_no_warning(velocity_stack, axis=0)

    return xr.Dataset(
        data_vars={
            "reflectivity_dbz": (("time", "height"), reflectivity),
            "mean_doppler_velocity": (("time", "height"), velocity),
        },
        coords={
            "time": ("time", model_elapsed),
            "elapsed_min": ("time", model_elapsed),
            "height": observation["height"].values,
            "mission": ("mission", np.asarray(mission_names, dtype=object)),
        },
        attrs={"alignment": "curated plume-window composite"},
    )


def _select_observation_windows(
    mission: str | None,
) -> tuple[tuple[str, ...], tuple[np.datetime64, ...], tuple[tuple[np.datetime64, np.datetime64], ...], str]:
    if mission is None or mission.lower() == "composite":
        return (
            DEFAULT_OBSERVATION_IDS,
            DEFAULT_OBSERVATION_T0_CANDIDATES,
            DEFAULT_OBSERVATION_TIME_FRAMES,
            "composite",
        )

    mission_norm = mission.upper()
    try:
        idx = DEFAULT_OBSERVATION_IDS.index(mission_norm)
    except ValueError as exc:
        valid = ", ".join(("composite", *DEFAULT_OBSERVATION_IDS))
        raise ValueError(f"Unknown observation mission {mission!r}. Use one of: {valid}") from exc

    return (
        (DEFAULT_OBSERVATION_IDS[idx],),
        (DEFAULT_OBSERVATION_T0_CANDIDATES[idx],),
        (DEFAULT_OBSERVATION_TIME_FRAMES[idx],),
        mission_norm,
    )


def _load_mira35_parts(directory: Path, pattern: str) -> xr.Dataset:
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No MIRA35 files matching {pattern!r} found in {directory}")

    keep = {"time", "microsec", "range", *OBS_REFLECTIVITY_CANDIDATES, *OBS_VELOCITY_CANDIDATES}
    parts: list[xr.Dataset] = []
    for path in files:
        with xr.open_dataset(path, engine="scipy") as raw:
            names = [name for name in keep if name in raw]
            parts.append(raw[names].load())

    ds = xr.concat(parts, dim="time", data_vars="minimal", coords="minimal", compat="override")
    time_seconds = np.asarray(ds["time"].values, dtype="int64")
    microseconds = np.asarray(ds["microsec"].values, dtype="int64") if "microsec" in ds else np.zeros_like(time_seconds)
    time_coord = (
        np.datetime64("1970-01-01T00:00:00")
        + time_seconds.astype("timedelta64[s]")
        + microseconds.astype("timedelta64[us]")
    )
    return ds.assign_coords(time=("time", time_coord))


def _select_best_obs_field(ds: xr.Dataset, candidates: tuple[str, ...], *, log_to_db: bool) -> tuple[str, xr.DataArray]:
    best_name: str | None = None
    best_values: np.ndarray | None = None
    best_fraction = -1.0

    for name in candidates:
        if name not in ds:
            continue
        values = _mask_obs_fill(ds[name].values)
        if log_to_db and _needs_db_conversion(values, ds[name].attrs.get("units")):
            values = _to_decibels(values)

        fraction = float(np.isfinite(values).mean())
        if fraction > best_fraction:
            best_name = name
            best_values = values
            best_fraction = fraction

    if best_name is None or best_values is None or best_fraction <= 0.0:
        raise ValueError(f"Could not find a finite observation field among {candidates}.")

    da = xr.DataArray(
        best_values,
        dims=("time", "height"),
        coords={"time": ds["time"].values, "height": ds["height"].values},
    )
    return best_name, da


def load_mira35_observations(
    directory: str | Path,
    *,
    pattern: str = "*.mmclx",
    height_asl_m: float = DEFAULT_HEIGHT_ASL_M,
) -> tuple[xr.Dataset, str, str]:
    """Load raw MIRA35 NetCDFs and expose reflectivity/MDV on time-height coordinates."""

    directory = Path(directory)
    raw = _load_mira35_parts(directory, pattern)
    height = np.asarray(raw["range"].values, dtype=float) + float(height_asl_m)
    raw = raw.assign_coords(height=("range", height)).swap_dims({"range": "height"}).drop_vars("range")
    raw = _dedupe_time(raw)

    reflectivity_name, reflectivity = _select_best_obs_field(raw, OBS_REFLECTIVITY_CANDIDATES, log_to_db=True)
    velocity_name, velocity = _select_best_obs_field(raw, OBS_VELOCITY_CANDIDATES, log_to_db=False)

    obs = xr.Dataset(
        data_vars={
            "reflectivity_dbz": reflectivity,
            "mean_doppler_velocity": velocity,
        }
    )
    obs["reflectivity_dbz"].attrs.update(units="dBZ", long_name="MIRA35 reflectivity")
    obs["mean_doppler_velocity"].attrs.update(units="m s-1", long_name="MIRA35 mean Doppler velocity")
    return obs, reflectivity_name, velocity_name


def load_pamtra_output(path: str | Path, *, flip_mdv: bool = True) -> xr.Dataset:
    """Load the maintained PAMTRA NetCDF output and expose 2-D time-height fields."""

    path = Path(path)
    with xr.open_dataset(path) as raw:
        ds = raw.load()

    reflectivity = _mask_pamtra_fill(ds["Ze"].isel(frequency=0).values)
    velocity = _mask_pamtra_fill(ds["mean_doppler_velocity"].isel(frequency=0).values)
    if flip_mdv:
        velocity = -velocity

    out = xr.Dataset(
        data_vars={
            "reflectivity_dbz": (("time", "height"), reflectivity),
            "mean_doppler_velocity": (("time", "height"), velocity),
        },
        coords={"time": ds["time"].values, "height": ds["height"].values},
        attrs=dict(ds.attrs),
    )
    out["reflectivity_dbz"].attrs.update(units="dBZ", long_name="PAMTRA reflectivity")
    out["mean_doppler_velocity"].attrs.update(units="m s-1", long_name="PAMTRA mean Doppler velocity")
    return out


def _candidate_alignment_score(
    observation: xr.Dataset,
    pamtra: xr.Dataset,
    observation_t0: np.datetime64,
    model_t0: np.datetime64,
) -> tuple[float, float]:
    obs_elapsed = _elapsed_minutes(observation["time"].values, observation_t0)
    pam_elapsed = _elapsed_minutes(pamtra["time"].values, model_t0)
    overlap_lo = max(float(np.nanmin(obs_elapsed)), float(np.nanmin(pam_elapsed)))
    overlap_hi = min(float(np.nanmax(obs_elapsed)), float(np.nanmax(pam_elapsed)))
    if overlap_hi <= overlap_lo:
        return (-np.inf, -np.inf)

    obs_mask = (obs_elapsed >= overlap_lo) & (obs_elapsed <= overlap_hi)
    obs_vals = np.asarray(observation["reflectivity_dbz"].isel(time=obs_mask).values, dtype=float)
    finite_fraction = float(np.isfinite(obs_vals).mean()) if obs_vals.size else -np.inf
    overlap_span = overlap_hi - overlap_lo
    return overlap_span, finite_fraction


def choose_observation_t0(
    observation: xr.Dataset,
    pamtra: xr.Dataset,
    *,
    model_t0: np.datetime64,
    candidates: tuple[np.datetime64, ...] = DEFAULT_OBSERVATION_T0_CANDIDATES,
) -> np.datetime64:
    """Pick the observation seeding start that best overlaps the PAMTRA window in t-t0 space."""

    ranked = sorted(
        (
            (*_candidate_alignment_score(observation, pamtra, candidate, model_t0), candidate)
            for candidate in candidates
        ),
        reverse=True,
    )
    best_overlap, best_fraction, best_candidate = ranked[0]
    if not np.isfinite(best_overlap):
        raise ValueError("Could not find an observation seeding time that overlaps the PAMTRA window.")
    if best_fraction < 0.0:
        raise ValueError("Observation seeding candidates produced no finite reflectivity data.")
    return best_candidate


def build_quicklook_context(
    pamtra_path: str | Path,
    mira_directory: str | Path,
    *,
    flip_pamtra_mdv: bool = True,
    height_asl_m: float = DEFAULT_HEIGHT_ASL_M,
    model_t0: str | np.datetime64 = DEFAULT_MODEL_T0,
    observation_mission: str | None = None,
    observation_t0: str | np.datetime64 | None = None,
    time_start: str | np.datetime64 | None = None,
    time_end: str | np.datetime64 | None = None,
    height_min_m: float | None = None,
    height_max_m: float | None = None,
) -> PamtraMira35QuicklookContext:
    """Load both sources, align them by seeding-relative time, and trim to the shared window."""

    pamtra = load_pamtra_output(pamtra_path, flip_mdv=flip_pamtra_mdv)
    observation, reflectivity_source, velocity_source = load_mira35_observations(
        mira_directory,
        height_asl_m=height_asl_m,
    )
    model_t0_np = np.datetime64(model_t0)
    pamtra = _add_elapsed_coord(pamtra, model_t0_np)
    observation_ids, observation_t0s, observation_time_frames, observation_label = _select_observation_windows(
        observation_mission
    )

    if observation_t0 is None:
        observation = build_aligned_observation_composite(
            observation,
            model_elapsed=np.asarray(pamtra["elapsed_min"].values, dtype=float),
            observation_ids=observation_ids,
            observation_t0s=observation_t0s,
            observation_time_frames=observation_time_frames,
        )
        observation_alignment = (
            "curated plume-window composite" if len(observation_ids) > 1 else f"curated plume-window mission {observation_ids[0]}"
        )
        observation_t0_value: np.datetime64 | tuple[np.datetime64, ...] = observation_t0s if len(observation_t0s) > 1 else observation_t0s[0]
    else:
        observation_t0_np = np.datetime64(observation_t0)
        observation = _add_elapsed_coord(observation, observation_t0_np)
        observation_alignment = "single continuous radar window"
        observation_t0_value = observation_t0_np

    obs_elapsed_lo, obs_elapsed_hi = _finite_elapsed_bounds(observation)
    pam_elapsed_lo, pam_elapsed_hi = _finite_elapsed_bounds(pamtra)
    auto_start = max(obs_elapsed_lo, pam_elapsed_lo)
    auto_end = min(obs_elapsed_hi, pam_elapsed_hi)
    if auto_start > auto_end:
        raise ValueError("Observation and PAMTRA windows do not overlap after seeding-time alignment.")

    if time_start is None:
        time_start_min = auto_start
    else:
        time_start_min = float((np.datetime64(time_start) - model_t0_np) / np.timedelta64(1, "m"))
    if time_end is None:
        time_end_min = auto_end
    else:
        time_end_min = float((np.datetime64(time_end) - model_t0_np) / np.timedelta64(1, "m"))

    obs_height = np.asarray(observation["height"].values, dtype=float)
    pam_height = np.asarray(pamtra["height"].values, dtype=float)
    y_min = float(max(np.nanmin(obs_height), np.nanmin(pam_height))) if height_min_m is None else float(height_min_m)
    y_max = float(min(np.nanmax(obs_height), np.nanmax(pam_height))) if height_max_m is None else float(height_max_m)

    observation = observation.where(
        (observation["elapsed_min"] >= time_start_min) & (observation["elapsed_min"] <= time_end_min),
        drop=True,
    ).sel(height=slice(y_min, y_max))
    pamtra = pamtra.where(
        (pamtra["elapsed_min"] >= time_start_min) & (pamtra["elapsed_min"] <= time_end_min),
        drop=True,
    ).sel(height=slice(y_min, y_max))

    return PamtraMira35QuicklookContext(
        observation=observation,
        pamtra=pamtra,
        reflectivity_source=reflectivity_source,
        velocity_source=velocity_source,
        flip_pamtra_mdv=flip_pamtra_mdv,
        observation_label=observation_label,
        observation_alignment=observation_alignment,
        model_t0=model_t0_np,
        observation_t0=observation_t0_value,
        elapsed_start_min=time_start_min,
        elapsed_end_min=time_end_min,
        height_min_m=y_min,
        height_max_m=y_max,
    )


def default_output_path(repo_root: str | Path, pamtra_path: str | Path, *, observation_label: str = "composite") -> Path:
    """Return the standard output location under output/gfx/png/06."""

    repo_root = Path(repo_root)
    stem = Path(pamtra_path).stem.removesuffix("_pamtra")
    suffix = "" if observation_label == "composite" else f"_{observation_label.lower()}"
    return repo_root / "output" / "gfx" / "png" / "06" / f"pamtra_mira35_quicklook_{stem}{suffix}.png"


def _plot_panel(
    ax: plt.Axes,
    data: xr.DataArray,
    *,
    cmap: str,
    vmin: float,
    vmax: float,
    title: str,
    panel_id: str,
) -> plt.Axes:
    mesh = ax.pcolormesh(
        data["elapsed_min"].values,
        data["height"].values,
        np.asarray(data.values, dtype=float).T,
        shading="auto",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        rasterized=True,
    )
    ax.set_title(title, loc="left")
    ax.text(
        0.01,
        0.98,
        panel_id,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontweight="bold",
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 1.5},
    )
    return mesh


def render_quicklook(
    context: PamtraMira35QuicklookContext,
    output_path: str | Path,
    *,
    dpi: int = 400,
    show_suptitle: bool = True,
) -> Path:
    """Render the 2x2 paper-style quicklook figure and save it to disk."""

    fig_height = min(120 * MM, MAX_H_IN)
    fig, axes = plt.subplots(
        2,
        2,
        figsize=(FULL_COL_IN, fig_height),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )

    mesh_obs_z = _plot_panel(
        axes[0, 0],
        context.observation["reflectivity_dbz"],
        cmap="viridis",
        vmin=-40.0,
        vmax=0.0,
        title=f"MIRA35 {context.observation_label} reflectivity ({context.reflectivity_source})",
        panel_id="a",
    )
    _plot_panel(
        axes[0, 1],
        context.pamtra["reflectivity_dbz"],
        cmap="viridis",
        vmin=-40.0,
        vmax=0.0,
        title="PAMTRA reflectivity",
        panel_id="b",
    )
    mesh_obs_v = _plot_panel(
        axes[1, 0],
        context.observation["mean_doppler_velocity"],
        cmap="RdBu_r",
        vmin=-2.5,
        vmax=2.5,
        title=f"MIRA35 {context.observation_label} mean Doppler velocity ({context.velocity_source})",
        panel_id="c",
    )
    _plot_panel(
        axes[1, 1],
        context.pamtra["mean_doppler_velocity"],
        cmap="RdBu_r",
        vmin=-2.5,
        vmax=2.5,
        title="PAMTRA mean Doppler velocity" + (" (sign flipped)" if context.flip_pamtra_mdv else ""),
        panel_id="d",
    )

    for ax in axes[:, 0]:
        ax.set_ylabel("Altitude [m a.s.l.]")
    for ax in axes[1, :]:
        ax.set_xlabel("Time since seeding start, t - t0 [min]")
        span = context.elapsed_end_min - context.elapsed_start_min
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _pos: format_elapsed_minutes_tick(x, span, zero_if_close=True)))
    for ax in axes.ravel():
        ax.set_xlim(context.elapsed_start_min, context.elapsed_end_min)
        ax.set_ylim(context.height_min_m, context.height_max_m)

    cbar_z = fig.colorbar(mesh_obs_z, ax=axes[0, :], shrink=0.9, pad=0.02)
    cbar_z.set_label("Reflectivity [dBZ]")
    cbar_v = fig.colorbar(mesh_obs_v, ax=axes[1, :], shrink=0.9, pad=0.02)
    cbar_v.set_label("Mean Doppler velocity [m s$^{-1}$]")

    if show_suptitle:
        if isinstance(context.observation_t0, tuple):
            obs_t0_text = ", ".join(str(item)[11:16] for item in context.observation_t0)
        else:
            obs_t0_text = str(context.observation_t0)[:16]
        fig.suptitle(
            "PAMTRA vs MIRA35 quicklook: reflectivity and mean Doppler velocity along plume path — "
            f"{context.observation_alignment} (obs t0={obs_t0_text}, model t0={str(context.model_t0)[:16]})",
            y=1.01,
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return output_path
