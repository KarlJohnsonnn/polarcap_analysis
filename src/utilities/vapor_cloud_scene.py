"""
NCAR VAPOR helpers for COSMO-SPECS 3D cloud-seeding visuals.

Requires the **conda** package from ``ncar-vapor`` (see VAPOR docs). The unrelated
PyPI package named ``vapor`` (AWS CloudFormation) shadows NCAR VAPOR if installed;
:class:`session.Session` from that package lacks ``CreatePythonDataset`` — we detect that.

Refs: https://ncar.github.io/VaporDocumentationWebsite/pythonAPIReference/classReference.html
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr


def is_ncar_vapor_available() -> bool:
    """True if NCAR VAPOR Python API is importable (not the PyPI ``vapor`` package)."""
    try:
        from vapor import session as vapor_session  # type: ignore
    except Exception:
        return False
    cls = getattr(vapor_session, "Session", None)
    return cls is not None and callable(getattr(cls, "CreatePythonDataset", None))


def volume_to_vapor_c_order(da: xr.DataArray) -> np.ndarray:
    """
    VAPOR ``AddNumpyData`` expects C-contiguous data with X (longitude) fastest varying.

    Input ``da`` must have dims ``altitude``, ``latitude``, ``longitude`` (optionally ``time`` — use
    ``isel`` first). Order in memory: (nz, ny, nx).
    """
    order = ("altitude", "latitude", "longitude")
    for d in order:
        if d not in da.dims:
            raise ValueError(f"volume_to_vapor_c_order: missing dim {d!r}, got {da.dims}")
    extra = [x for x in da.dims if x not in order]
    if extra:
        raise ValueError(f"volume_to_vapor_c_order: extra dims {extra}; slice to one timestep first")
    arr = da.transpose(*order).values
    return np.ascontiguousarray(np.nan_to_num(arr, nan=0.0).astype(np.float32, copy=False))


def surface_to_vapor_c_order(da: xr.DataArray) -> np.ndarray:
    """2D field with dims ``latitude``, ``longitude``; X (lon) fastest."""
    if da.dims != ("latitude", "longitude"):
        da = da.transpose("latitude", "longitude")
    arr = da.values
    return np.ascontiguousarray(np.nan_to_num(arr, nan=0.0).astype(np.float32, copy=False))


def load_3d_member(
    nc_file: str,
    extpar_file: str,
    nml: Dict[str, Any],
    reduced_domain: Dict[str, slice],
    variables: List[str],
    chunks: Optional[Dict[str, int]] = None,
) -> xr.Dataset:
    """Open one COSMO-SPECS 3D NetCDF with the same preprocessor as LV1 tracking."""
    from utilities.model_helpers import convert_units_3d, make_3d_preprocessor
    from utilities.namelist_metadata import update_dataset_metadata

    preprocess = make_3d_preprocessor(nc_file, extpar_file, nml)
    ds = xr.open_mfdataset(
        nc_file,
        preprocess=preprocess,
        chunks=chunks or {"time": 1},
        parallel=False,
    )
    ds.attrs["ncfile"] = nc_file
    ds.attrs["run_id"] = nc_file.split("/")[-1].split("_")[1].split(".")[0]
    ds = update_dataset_metadata(ds)
    ds = ds.sel(reduced_domain)
    ds = convert_units_3d(ds, ds["rho"])
    present = [v for v in variables if v in ds.data_vars]
    missing = [v for v in variables if v not in ds.data_vars]
    if missing:
        # Caller may pass optional diagnostics
        pass
    if not present:
        raise KeyError(f"None of {variables} found in dataset; vars={list(ds.data_vars)[:20]}...")
    return ds[present]


def integrated_nf_delta(ds_flare: xr.Dataset, ds_ref: xr.Dataset) -> xr.DataArray:
    """Size-integrated ice number concentration difference (flare − ref), same as tobac input."""
    nf_f = ds_flare["nf"].sum(dim="diameter")
    nf_r = ds_ref["nf"].sum(dim="diameter")
    delta = nf_f - nf_r
    delta.name = "delta_nf"
    delta.attrs.setdefault("long_name", "Δ ice number concentration (flare − ref)")
    delta.attrs.setdefault("units", nf_f.attrs.get("units", "L-1"))
    return delta


def extpar_hsurf_dataarray(extpar_file: str) -> xr.DataArray:
    """HSURF on model grid (cropped like the rest of the pipeline)."""
    from utilities.plotting import get_extpar_data

    lat2d, lon2d, hsurf = get_extpar_data(extpar_file)
    return xr.DataArray(
        hsurf,
        dims=("latitude", "longitude"),
        coords={
            "latitude": ("latitude", np.linspace(lat2d.min(), lat2d.max(), lat2d.shape[0])),
            "longitude": ("longitude", np.linspace(lon2d.min(), lon2d.max(), lon2d.shape[1])),
        },
        name="HSURF",
        attrs={"units": "m", "long_name": "terrain height"},
    )


def export_seed_plume_netcdf(
    delta_nf: xr.DataArray,
    qi_flare: Optional[xr.DataArray],
    hsurf: Optional[xr.DataArray],
    path: Union[str, Path],
) -> Path:
    """
    Write a CF-friendly NetCDF for ``Session.OpenDataset`` when time-varying fields are needed.

    Variables are (time, altitude, latitude, longitude) except ``HSURF`` (latitude, longitude).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data_vars: Dict[str, Any] = {"delta_nf": delta_nf}
    if qi_flare is not None:
        data_vars["qi_flare"] = qi_flare
    ds_out = xr.Dataset(data_vars)
    if hsurf is not None:
        ds_out["HSURF"] = hsurf
    ds_out.attrs["title"] = "COSMO-SPECS seed-plume VAPOR cache"
    ds_out.to_netcdf(path, mode="w")
    return path


def apply_volume_transfer_function(vol_renderer: Any, arr: np.ndarray) -> None:
    """Set colormap range from finite percentiles (ignores zeros for max if possible)."""
    flat = arr[np.isfinite(arr)]
    if flat.size == 0:
        return
    lo = float(np.percentile(flat, 2))
    hi = float(np.percentile(flat, 99.5))
    if hi <= lo:
        hi = lo + 1e-6
    tf = vol_renderer.GetPrimaryTransferFunction()
    tf.SetMinMapValue(lo)
    tf.SetMaxMapValue(hi)


def build_seed_plume_python_scene(
    delta_nf_t: xr.DataArray,
    qi_t: Optional[xr.DataArray] = None,
    hsurf: Optional[xr.DataArray] = None,
    image_path: Optional[Union[str, Path]] = None,
    resolution: Tuple[int, int] = (1280, 720),
    show_interactive: bool = False,
) -> Any:
    """
    Create a VAPOR :class:`Session` with ``PythonDataset``: volume ``delta_nf`` and optional
    ``TwoDData`` terrain. ``delta_nf_t`` / ``qi_t`` must be single-timestep (no ``time`` dim).

    Returns the session object (caller may keep reference while GUI is open).
    """
    if not is_ncar_vapor_available():
        raise RuntimeError(
            "NCAR VAPOR not available. Use: conda install -c conda-forge -c ncar-vapor vapor "
            "in an environment **without** the PyPI ``vapor`` package."
        )
    from vapor import renderer, session  # type: ignore

    ses = session.Session()
    try:
        ses.SetResolution((resolution[0], resolution[1]))
    except Exception:
        try:
            ses.SetResolution(resolution[0], resolution[1])
        except Exception:
            pass
    data = ses.CreatePythonDataset()
    vol_arr = volume_to_vapor_c_order(delta_nf_t)
    data.AddNumpyData("delta_nf", vol_arr)
    vol = data.NewRenderer(renderer.VolumeRenderer)
    vol.SetVariableName("delta_nf")
    vol.SetLightingEnabled(True)
    apply_volume_transfer_function(vol, vol_arr)

    if qi_t is not None:
        try:
            qi_arr = volume_to_vapor_c_order(qi_t)
            data.AddNumpyData("qi_flare", qi_arr)
            iso = data.NewRenderer(renderer.VolumeIsoRenderer)
            iso.SetVariableName("qi_flare")
            qflat = qi_arr.ravel()
            qflat = qflat[np.isfinite(qflat) & (qflat > 0)]
            if qflat.size > 0:
                iso.SetIsoValues([float(np.percentile(qflat, 90))])
        except Exception:
            pass

    if hsurf is not None:
        h2 = surface_to_vapor_c_order(hsurf)
        data.AddNumpyData("HSURF", h2)
        try:
            twod = data.NewRenderer(renderer.TwoDDataRenderer)
            twod.SetVariableName("HSURF")
        except Exception:
            pass

    cam = ses.GetCamera()
    cam.ViewAll()
    if image_path is not None:
        ses.RenderToImage(str(image_path))
    if show_interactive:
        ses.Show()
    return ses


def build_volume_scene_from_netcdf(
    nc_path: Union[str, Path],
    volume_var: str = "delta_nf",
    image_path: Optional[Union[str, Path]] = None,
    resolution: Tuple[int, int] = (1280, 720),
    show_interactive: bool = False,
) -> Any:
    """Open a CF-NetCDF with ``Session.OpenDataset`` and add a ``VolumeRenderer`` for *volume_var*."""
    if not is_ncar_vapor_available():
        raise RuntimeError("NCAR VAPOR not available (see build_seed_plume_python_scene docstring).")
    from vapor import renderer, session  # type: ignore

    ses = session.Session()
    try:
        ses.SetResolution((resolution[0], resolution[1]))
    except Exception:
        try:
            ses.SetResolution(resolution[0], resolution[1])
        except Exception:
            pass
    data = ses.OpenDataset(str(nc_path))
    vol = data.NewRenderer(renderer.VolumeRenderer)
    vol.SetVariableName(volume_var)
    vol.SetLightingEnabled(True)
    try:
        with xr.open_dataset(str(nc_path)) as _xrds:
            if volume_var in _xrds:
                apply_volume_transfer_function(vol, np.asarray(_xrds[volume_var].values, dtype=np.float64))
    except Exception:
        pass
    cam = ses.GetCamera()
    cam.ViewAll()
    if image_path is not None:
        ses.RenderToImage(str(image_path))
    if show_interactive:
        ses.Show()
    return ses
