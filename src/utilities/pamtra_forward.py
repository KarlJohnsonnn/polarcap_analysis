"""Helpers for running PAMTRA on plume-path NetCDF files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import xarray as xr

MOMENT_NAMES = ("mean_doppler_velocity", "spectrum_width", "skewness", "kurtosis")
DEFAULT_FREQS_GHZ = (35.5,)


@dataclass(frozen=True)
class PamtraConfig:
    """Configuration for the maintained PAMTRA wrapper."""

    frequencies_ghz: tuple[float, ...] = DEFAULT_FREQS_GHZ
    random_seed: int = 0
    liquid_density: float = 1000.0
    ice_density: float = 200.0
    ice_aspect_ratio: float = 3.0
    wind_uv: float = 10.0
    turb_edr: float = 1.0e-4
    radar_mode: str = "spectrum"
    radar_aliasing_nyquist_interv: int = 3
    radar_noise_distance_factor: float = -2.0


@dataclass(frozen=True)
class VerticalPlumeInput:
    """Vertical plume-path fields mapped into PAMTRA-ready arrays."""

    time: np.ndarray
    altitude_m: np.ndarray
    height_edges_m: np.ndarray
    diameter_edges_m: np.ndarray
    temperature_k: np.ndarray
    liquid_number_m3: np.ndarray
    ice_number_m3: np.ndarray
    source_kind: str


def _require_pamtra():
    try:
        import pyPamtra
    except ImportError as exc:  # pragma: no cover - depends on runtime environment
        raise RuntimeError(
            "pyPamtra is not available. Activate the dedicated PAMTRA environment "
            "before running the forward wrapper."
        ) from exc
    return pyPamtra


def _dedupe_time(ds: xr.Dataset) -> xr.Dataset:
    if "time" in ds.indexes:
        time_index = ds.indexes["time"]
        if time_index.has_duplicates:
            ds = ds.isel(time=~time_index.duplicated())
    return ds.sortby("time")


def _sort_altitude(ds: xr.Dataset) -> xr.Dataset:
    altitude = np.asarray(ds["altitude"].values, dtype=float)
    order = np.argsort(altitude)
    if not np.array_equal(order, np.arange(order.size)):
        ds = ds.isel(altitude=order)
    return ds


def _diameter_to_m(values: np.ndarray, units: str | None) -> np.ndarray:
    units_norm = (units or "").lower()
    if "mm" in units_norm:
        return values * 1.0e-3
    if "um" in units_norm or "mum" in units_norm or "mic" in units_norm or values.max(initial=0.0) > 1.0e-2:
        return values * 1.0e-6
    return values.astype(float)


def _infer_edges_from_centers(centers: np.ndarray) -> np.ndarray:
    centers = np.asarray(centers, dtype=float)
    if centers.ndim != 1 or centers.size == 0:
        raise ValueError("Expected a non-empty 1D coordinate.")
    if centers.size == 1:
        delta = max(50.0, abs(centers[0]) * 0.05)
        return np.array([max(0.0, centers[0] - delta), centers[0] + delta], dtype=float)

    inner = 0.5 * (centers[:-1] + centers[1:])
    lower = max(0.0, centers[0] - 0.5 * (centers[1] - centers[0]))
    upper = centers[-1] + 0.5 * (centers[-1] - centers[-2])
    return np.concatenate(([lower], inner, [upper]))


def _select_number_field(ds: xr.Dataset, primary: str, fallbacks: Sequence[str]) -> xr.DataArray | None:
    for name in (primary, *fallbacks):
        if name in ds:
            return ds[name]
    return None


def _number_to_m3(field: xr.DataArray | None, fallback: str, shape: tuple[int, ...]) -> np.ndarray:
    if field is None:
        return np.zeros(shape, dtype=float)

    values = np.nan_to_num(np.asarray(field.values, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)
    values = np.clip(values, a_min=0.0, a_max=None)
    units = str(field.attrs.get("units", "")).lower()
    if "cm-3" in units or "cm^-3" in units:
        factor = 1.0e6
    elif "l-1" in units or "l^-1" in units or "dm-3" in units or "dm^-3" in units or "1/dm3" in units:
        factor = 1.0e3
    elif "m-3" in units or "m^-3" in units:
        factor = 1.0
    else:
        factor = 1.0e6 if fallback == "liquid" else 1.0e3
    return values * factor


def _interp_missing_1d(values: np.ndarray, coord: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    coord = np.asarray(coord, dtype=float)
    valid = np.isfinite(values)
    if valid.all():
        return values
    if not valid.any():
        return np.full_like(values, np.nan, dtype=float)
    return np.interp(coord, coord[valid], values[valid], left=values[valid][0], right=values[valid][-1])


def _sanitize_temperature_k(values: np.ndarray, altitude_m: np.ndarray) -> np.ndarray:
    """Fill sparse temperature gaps so PAMTRA receives a finite profile."""

    temp = np.asarray(values, dtype=float).copy()
    altitude_m = np.asarray(altitude_m, dtype=float)
    if temp.ndim != 2:
        raise ValueError("Expected temperature with dimensions (time, altitude).")
    if not np.isfinite(temp).any():
        lapse = 273.15 - 6.5e-3 * (altitude_m - altitude_m.min(initial=0.0))
        return np.repeat(lapse[np.newaxis, :], temp.shape[0], axis=0)

    for idx in range(temp.shape[0]):
        temp[idx] = _interp_missing_1d(temp[idx], altitude_m)

    valid_rows = np.isfinite(temp).any(axis=1)
    if valid_rows.any() and not valid_rows.all():
        valid_idx = np.flatnonzero(valid_rows)
        for idx in np.flatnonzero(~valid_rows):
            nearest = valid_idx[np.argmin(np.abs(valid_idx - idx))]
            temp[idx] = temp[nearest]

    time_coord = np.arange(temp.shape[0], dtype=float)
    for idx in range(temp.shape[1]):
        temp[:, idx] = _interp_missing_1d(temp[:, idx], time_coord)

    surface_ref = np.nanmedian(temp[:, 0]) if np.isfinite(temp[:, 0]).any() else np.nanmedian(temp)
    if not np.isfinite(surface_ref):
        surface_ref = 273.15
    lapse_profile = surface_ref - 6.5e-3 * (altitude_m - altitude_m.min(initial=0.0))
    temp = np.where(np.isfinite(temp), temp, lapse_profile[np.newaxis, :])
    return np.clip(temp, 180.0, 330.0)


def _surface_temperature_k(profile_k: np.ndarray) -> float:
    finite = np.asarray(profile_k, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size:
        return float(finite[0])
    return 273.15


def open_vertical_plume_path(path: str | Path) -> VerticalPlumeInput:
    """Open a raw LV1 plume-path file and standardize it for PAMTRA."""

    ds = xr.open_dataset(path)
    if "path" in ds.dims and "time" in ds.coords and "time" not in ds.dims:
        ds = ds.swap_dims({"path": "time"})
    if "time" not in ds.dims:
        raise ValueError(f"{path} does not expose a time dimension after path->time conversion.")
    if "altitude" not in ds.dims:
        raise ValueError(f"{path} does not contain an altitude dimension. Use a vertical plume-path file.")
    if "diameter" not in ds.dims and "diameter" not in ds.coords:
        raise ValueError(f"{path} does not define a diameter coordinate.")

    ds = _sort_altitude(_dedupe_time(ds))
    source_kind = str(ds.attrs.get("kind", "vertical"))
    altitude_m = np.asarray(ds["altitude"].values, dtype=float)
    height_edges_m = _infer_edges_from_centers(altitude_m)

    if "diameter_edges" in ds.coords:
        diameter_edges_m = _diameter_to_m(
            np.asarray(ds["diameter_edges"].values, dtype=float),
            ds["diameter_edges"].attrs.get("units"),
        )
    else:
        diameter_edges_m = _infer_edges_from_centers(
            _diameter_to_m(np.asarray(ds["diameter"].values, dtype=float), ds["diameter"].attrs.get("units"))
        )

    ice_field = _select_number_field(ds, "nf", ("ni", "icnc"))
    liquid_field = _select_number_field(ds, "nw", ("cdnc",))
    temperature = ds["temperature"] if "temperature" in ds else ds["t"]
    expected_shape = (ds.sizes["time"], ds.sizes["altitude"], ds.sizes["diameter"])

    return VerticalPlumeInput(
        time=np.asarray(ds["time"].values),
        altitude_m=altitude_m,
        height_edges_m=height_edges_m,
        diameter_edges_m=diameter_edges_m,
        temperature_k=_sanitize_temperature_k(
            np.asarray(temperature.transpose("time", "altitude").values, dtype=float),
            altitude_m,
        ),
        liquid_number_m3=_number_to_m3(liquid_field, "liquid", expected_shape),
        ice_number_m3=_number_to_m3(ice_field, "ice", expected_shape),
        source_kind=source_kind,
    )


def _temperature_to_levels(temp_layers: np.ndarray) -> np.ndarray:
    temp_layers = np.asarray(temp_layers, dtype=float)
    if temp_layers.size == 1:
        return np.array([temp_layers[0], temp_layers[0]], dtype=float)
    inner = 0.5 * (temp_layers[:-1] + temp_layers[1:])
    return np.concatenate(([temp_layers[0]], inner, [temp_layers[-1]]))


def _build_pamtra(pyPamtra, plume: VerticalPlumeInput, cfg: PamtraConfig):
    d_bound = plume.diameter_edges_m
    d_mean = 0.5 * (d_bound[:-1] + d_bound[1:])
    n_bins = d_mean.size

    pam = pyPamtra.pyPamtra()
    pam.df.addHydrometeor(
        (
            "liquid",
            -99.0,
            1,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            0,
            n_bins,
            "fullBin",
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            "mie-sphere",
            "khvorostyanov01_drops",
            0.0,
        )
    )
    pam.df.addHydrometeor(
        (
            "ice",
            -99.0,
            -1,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            0,
            n_bins,
            "fullBin",
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            -99.0,
            "ssrg-rt3",
            "heymsfield10_particles",
            0.0,
        )
    )
    pam = pyPamtra.importer.createUsStandardProfile(pam, hgt_lev=plume.height_edges_m)
    pam.p["wind_uv"][:] = cfg.wind_uv
    pam.p["turb_edr"][:] = cfg.turb_edr
    if "groundtemp" in pam.p:
        pam.p["groundtemp"][:] = _surface_temperature_k(plume.temperature_k[0])
    pam.nmlSet["passive"] = False
    pam.nmlSet["randomseed"] = cfg.random_seed
    pam.nmlSet["radar_mode"] = cfg.radar_mode
    pam.nmlSet["radar_aliasing_nyquist_interv"] = cfg.radar_aliasing_nyquist_interv
    pam.nmlSet["hydro_adaptive_grid"] = False
    pam.nmlSet["conserve_mass_rescale_dsd"] = False
    pam.nmlSet["radar_use_hildebrand"] = True
    pam.nmlSet["radar_noise_distance_factor"] = cfg.radar_noise_distance_factor
    pam.nmlSet["hydro_fullspec"] = True
    pam.set["verbose"] = 0
    pam.set["pyVerbose"] = 0
    pam.df.addFullSpectra()

    pam.df.dataFullSpec["d_bound_ds"][:] = d_bound
    pam.df.dataFullSpec["d_ds"][:] = d_mean
    pam.df.dataFullSpec["rho_ds"][..., 0, :] = cfg.liquid_density
    pam.df.dataFullSpec["rho_ds"][..., 1, :] = cfg.ice_density
    pam.df.dataFullSpec["area_ds"][..., 0, :] = np.pi / 4.0 * d_mean**2
    pam.df.dataFullSpec["area_ds"][..., 1, :] = np.pi / 4.0 * d_mean**2
    pam.df.dataFullSpec["mass_ds"][..., 0, :] = np.pi / 6.0 * cfg.liquid_density * d_mean**3
    pam.df.dataFullSpec["mass_ds"][..., 1, :] = np.pi / 6.0 * cfg.ice_density * d_mean**3
    pam.df.dataFullSpec["as_ratio"][..., 0, :] = 1.0
    pam.df.dataFullSpec["as_ratio"][..., 1, :] = cfg.ice_aspect_ratio
    return pam


def _normalize_ze(raw: np.ndarray, n_height: int, n_freq: int) -> np.ndarray:
    arr = np.asarray(raw).squeeze()
    if arr.ndim == 1:
        return arr[:, np.newaxis]
    if arr.shape == (n_freq, n_height):
        return arr.T
    if arr.shape[:2] == (n_height, n_freq):
        return arr.reshape(n_height, n_freq)
    return arr.reshape(n_height, n_freq)


def _normalize_moments(raw: np.ndarray, n_height: int, n_freq: int) -> np.ndarray:
    arr = np.asarray(raw).squeeze()
    if arr.ndim == 2 and arr.shape == (n_height, len(MOMENT_NAMES)):
        return arr[:, np.newaxis, :]
    if arr.ndim == 3 and arr.shape == (n_freq, n_height, len(MOMENT_NAMES)):
        return arr.transpose(1, 0, 2)
    if arr.ndim == 3 and arr.shape[:2] == (n_height, n_freq):
        return arr
    return arr.reshape(n_height, n_freq, len(MOMENT_NAMES))


def _normalize_spectra(raw: np.ndarray, n_height: int, n_freq: int) -> np.ndarray:
    arr = np.asarray(raw).squeeze()
    if arr.ndim == 2:
        return arr[:, np.newaxis, :]
    if arr.ndim == 3 and arr.shape[0] == n_freq:
        return arr.transpose(1, 0, 2)
    if arr.ndim == 3 and arr.shape[0] == n_height:
        return arr
    n_vel = arr.size // max(1, n_height * n_freq)
    return arr.reshape(n_height, n_freq, n_vel)


def _normalize_velocity(raw: np.ndarray, n_freq: int) -> np.ndarray:
    arr = np.asarray(raw).squeeze()
    if arr.ndim == 1:
        return np.repeat(arr[np.newaxis, :], n_freq, axis=0)
    if arr.ndim == 2 and arr.shape[0] == n_freq:
        return arr
    if arr.ndim == 2 and arr.shape[1] == n_freq:
        return arr.T
    return arr.reshape(n_freq, -1)


def _build_output_dataset(
    plume: VerticalPlumeInput,
    cfg: PamtraConfig,
    ze_steps: list[np.ndarray],
    moment_steps: list[np.ndarray],
    spectra_steps: list[np.ndarray],
    radar_velocity: np.ndarray,
    source_path: str | Path,
) -> xr.Dataset:
    ze = np.stack(ze_steps, axis=0)
    moments = np.stack(moment_steps, axis=0)
    spectra = np.stack(spectra_steps, axis=0)
    velocity_bin = np.arange(radar_velocity.shape[1], dtype=int)

    ds = xr.Dataset(
        data_vars={
            "Ze": (("time", "height", "frequency"), ze),
            "radar_moments": (("time", "height", "frequency", "moment"), moments),
            "radar_spectra": (("time", "height", "frequency", "velocity_bin"), spectra),
            "radar_velocity": (("frequency", "velocity_bin"), radar_velocity),
        },
        coords={
            "time": plume.time,
            "height": plume.altitude_m,
            "frequency": np.asarray(cfg.frequencies_ghz, dtype=float),
            "moment": list(MOMENT_NAMES),
            "velocity_bin": velocity_bin,
        },
        attrs={
            "source_file": str(source_path),
            "plume_kind": plume.source_kind,
            "radar_mode": cfg.radar_mode,
            "frequencies_ghz": ",".join(f"{freq:g}" for freq in cfg.frequencies_ghz),
        },
    )
    ds["Ze"].attrs.update(units="dBZ", long_name="PAMTRA equivalent reflectivity")
    ds["radar_spectra"].attrs.update(units="dB", long_name="PAMTRA Doppler spectrum")
    ds["radar_velocity"].attrs.update(units="m s-1", long_name="Doppler velocity grid")
    for idx, name in enumerate(MOMENT_NAMES):
        ds[name] = ds["radar_moments"].isel(moment=idx)
    ds["mean_doppler_velocity"].attrs["units"] = "m s-1"
    ds["spectrum_width"].attrs["units"] = "m s-1"
    ds["skewness"].attrs["units"] = "1"
    ds["kurtosis"].attrs["units"] = "1"
    return ds


def run_pamtra_on_plume_path(
    source_path: str | Path,
    output_path: str | Path,
    *,
    config: PamtraConfig | None = None,
    limit_times: int | None = None,
) -> Path:
    """Run PAMTRA on a single vertical plume-path file and write NetCDF output."""

    cfg = config or PamtraConfig()
    plume = open_vertical_plume_path(source_path)
    if plume.source_kind != "vertical":
        raise ValueError(
            f"{source_path} is marked as '{plume.source_kind}'. PAMTRA profiles require vertical plume-path files."
        )

    pyPamtra = _require_pamtra()
    pam = _build_pamtra(pyPamtra, plume, cfg)
    n_height = plume.altitude_m.size
    n_freq = len(cfg.frequencies_ghz)
    n_times = plume.time.size if limit_times is None else min(limit_times, plume.time.size)

    ze_steps: list[np.ndarray] = []
    moment_steps: list[np.ndarray] = []
    spectra_steps: list[np.ndarray] = []
    radar_velocity = None

    for idx in range(n_times):
        if "temp" in pam.p:
            pam.p["temp"][0, 0, :] = plume.temperature_k[idx]
        if "temp_lev" in pam.p:
            pam.p["temp_lev"][0, 0, :] = _temperature_to_levels(plume.temperature_k[idx])
        if "groundtemp" in pam.p:
            pam.p["groundtemp"][:] = _surface_temperature_k(plume.temperature_k[idx])
        pam.df.dataFullSpec["n_ds"][:, :, :, 0, :] = plume.liquid_number_m3[idx][np.newaxis, np.newaxis, :, :]
        pam.df.dataFullSpec["n_ds"][:, :, :, 1, :] = plume.ice_number_m3[idx][np.newaxis, np.newaxis, :, :]
        pam.runPamtra(np.asarray(cfg.frequencies_ghz, dtype=float))

        ze_steps.append(_normalize_ze(pam.r["Ze"], n_height, n_freq))
        moment_steps.append(_normalize_moments(pam.r["radar_moments"], n_height, n_freq))
        spectra_steps.append(_normalize_spectra(pam.r["radar_spectra"], n_height, n_freq))
        if radar_velocity is None:
            radar_velocity = _normalize_velocity(pam.r["radar_vel"], n_freq)

    out_ds = _build_output_dataset(
        plume=VerticalPlumeInput(
            time=plume.time[:n_times],
            altitude_m=plume.altitude_m,
            height_edges_m=plume.height_edges_m,
            diameter_edges_m=plume.diameter_edges_m,
            temperature_k=plume.temperature_k[:n_times],
            liquid_number_m3=plume.liquid_number_m3[:n_times],
            ice_number_m3=plume.ice_number_m3[:n_times],
            source_kind=plume.source_kind,
        ),
        cfg=cfg,
        ze_steps=ze_steps,
        moment_steps=moment_steps,
        spectra_steps=spectra_steps,
        radar_velocity=radar_velocity if radar_velocity is not None else np.empty((n_freq, 0)),
        source_path=source_path,
    )
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_ds.to_netcdf(output_path)
    return output_path


def discover_plume_path_inputs(paths: Iterable[str | Path], kind: str = "vertical") -> list[Path]:
    """Expand input files or directories into concrete plume-path NetCDF paths."""

    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_file():
            files.append(path)
            continue
        if not path.exists():
            raise FileNotFoundError(path)
        files.extend(sorted(path.glob(f"data_*_{kind}_plume_path_*_cell*.nc")))
    return sorted(dict.fromkeys(files))
