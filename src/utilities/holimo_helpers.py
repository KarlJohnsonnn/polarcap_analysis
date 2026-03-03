from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import xarray as xr
import pandas as pd
import datetime

from .tools import load_holimo_data as _load_holimo_raw
from .tools import calculate_mean_diameter


HOLIMO_DEFAULT_VARS: List[str] = [
    "Water_concentration", "Ice_concentration",
    "Water_meanD", "Ice_meanD",
    "Water_content", "Ice_content",
    "Water_PSDnoNorm", "Ice_PSDnoNorm",
    "instData_Height", "diameter",
]
# #########################################################################################
# #########################################################################################
# # Define time frames

time_frame_tbs    = [   np.datetime64('2023-01-25T10:20:00'), # start time
                        np.datetime64('2023-01-25T11:50:00')] # end time

time_frames_plume = [   [   np.datetime64('2023-01-25T10:35:00'), np.datetime64('2023-01-25T10:42:00')   ],
                        [   np.datetime64('2023-01-25T10:55:00'), np.datetime64('2023-01-25T11:05:00')   ],
                        [   np.datetime64('2023-01-25T11:25:00'), np.datetime64('2023-01-25T11:35:00')   ],
                    ]


def load_holimo_dataset(holimo_path: str) -> xr.Dataset:
    """Load raw HOLIMO dataset and fix time coordinate.

    Wrapper around utils.tools.load_holimo_data.
    """
    data_holimo = xr.open_dataset(holimo_path)
    time_holimo = [
        datetime.datetime.strptime(''.join([x.decode('utf-8') for x in data_holimo['timestr'][:, i].values]), '%Y-%m-%d_%H:%M:%S.%f') 
        for i in range(data_holimo.time.size)
        ]
    data_holimo = data_holimo.assign_coords({"time": ("time", time_holimo)})

    return data_holimo


def prepare_holimo_quicklook(
    holimo_ds: xr.Dataset,
    *,
    variables: Optional[Sequence[str]] = None,
    timeframe: Optional[Tuple[np.datetime64, np.datetime64]] = None,
) -> Tuple[xr.Dataset, np.ndarray, np.ndarray]:
    """Select variables, slice in time, convert units, and derive bulk fields.

    Returns processed dataset and the diameter boundaries for liquid/ice in µm.
    """
    ds = holimo_ds
    if variables is None:
        #variables = HOLIMO_DEFAULT_VARS
        #variables = holimo_ds.data_vars
        pass

    # Select variables and timeframe
    for v in ("Water_PSDnoNorm", "Ice_PSDnoNorm"):
        if v in ds:
            if tuple(ds[v].dims) != ("time", "diameter"):
                ds[v] = ds[v].transpose("time", "diameter")

    # Derive mean diameters from PSDs if present
    if ("Water_PSDnoNorm" in ds) and ("diameter" in ds.coords):
        liq_md = calculate_mean_diameter(ds["Water_PSDnoNorm"].values, ds["diameter"].values).T
        ds["mdw_bulk"] = xr.DataArray(liq_md, dims="time", coords={"time": ds.time.values})
        ds["mdw_bulk"].attrs["unit"] = "µm"
    if ("Ice_PSDnoNorm" in ds) and ("diameter" in ds.coords):
        ice_md = calculate_mean_diameter(ds["Ice_PSDnoNorm"].values, ds["diameter"].values).T
        ds["mdf_bulk"] = xr.DataArray(ice_md, dims="time", coords={"time": ds.time.values})
        ds["mdf_bulk"].attrs["unit"] = "µm"

    # Mean altitude
    if "instData_Height" in ds:
        ds["mean_altitude"] = ds["instData_Height"].mean().astype(float)
        ds["mean_altitude"].attrs["unit"] = "m"
        ds["instData_Height"].attrs["unit"] = "m"

    # Boundaries from attributes (convert m->µm where appropriate)
    ice_lowest_size = float(ds.attrs.get("iceLowestSize", np.nan)) * 1.0e6
    water_max_size = float(ds.attrs.get("waterMaxSize", np.nan)) * 1.0e6
    hist_min_size = float(ds.attrs.get("histMinSize", np.nan)) * 1.0e6
    hist_max_size = float(ds.attrs.get("histMaxSize", np.nan)) * 1.0e6

    lbb = np.array([hist_min_size, water_max_size], dtype=float)  # cloud droplet bounds [µm]
    cbb = np.array([ice_lowest_size, hist_max_size], dtype=float) # ice crystal bounds [µm]

    return ds, lbb, cbb


def summarize_holimo(ds: xr.Dataset, lbb: np.ndarray, cbb: np.ndarray) -> str:
    """Create a quick textual summary (min/max/mean/std) and boundaries."""
    lines: List[str] = []
    for var in ds.data_vars:
        try:
            v = ds[var]
            vmin = np.nanmin(v.values)
            vmax = np.nanmax(v.values)
            vmean = np.nanmean(v.values)
            vstd = np.nanstd(v.values)
            unit = v.attrs.get("unit", v.attrs.get("units", ""))
            lines.append(f"{var:20s} {vmin:12.4e} {vmax:12.4e}, {vmean:12.4e} {vstd:12.4e}, {tuple(v.values.shape)}  --  {unit}")
        except Exception:
            # skip non-numeric or ragged arrays
            continue

    lines.append("--------------------------------")
    lines.append(
        f"boundaries (liquid spectrum):   hist_min_size = {lbb[0]:.0f} µm    -to-   water_max_size = {lbb[1]:.0f} µm"
    )
    lines.append(
        f"boundaries (frozen spectrum): ice_lowest_size = {cbb[0]:.0f} µm   -to-    hist_max_size = {cbb[1]:.0f} µm"
    )
    lines.append("")
    lines.append(f"cloud droplet boundaries: lbb = [{lbb[0]:.0f}, {lbb[1]:.0f}] µm")
    lines.append(f"ice crystal boundaries: cbb = [{cbb[0]:.0f}, {cbb[1]:.0f}] µm")
    return "\n".join(lines)


def load_and_prepare_holimo(
    holimo_path: str,
    *,
    timeframe: Optional[Tuple[np.datetime64, np.datetime64]] = None,
    variables: Optional[Sequence[str]] = None,
) -> Tuple[xr.Dataset, np.ndarray, np.ndarray]:
    """Convenience function: load, prepare, and return dataset with boundaries."""
    raw = load_holimo_dataset(holimo_path)
    return prepare_holimo_quicklook(raw, variables=variables, timeframe=timeframe)


def rebin_timeseries(
    ds: xr.Dataset,
    *,
    step: str = "30S",
    time_coord_name: str = "time_rebinned",
    variables: Optional[Sequence[str]] = None,
    agg: str = "mean",
    suffix: str = "_rebinned_t",
) -> xr.Dataset:
    """Rebin HOLIMO time series to a fixed step using xarray.resample, independent of model time.

    Args:
        ds: HOLIMO dataset with a time coordinate
        step: resampling step (e.g., "30S")
        time_coord_name: name for the shared resampled time coordinate
        variables: variables to rebin; defaults to all data_vars containing 'time' in dims
        agg: aggregation method for resample: 'mean'|'median'|'sum'|'min'|'max'
        suffix: suffix for rebinned variables

    Returns:
        xr.Dataset with additional rebinned variables and a shared time coordinate in ds[time_coord_name]
    """
    if "time" not in ds.coords:
        raise ValueError("Dataset must have a 'time' coordinate")

    if variables is None:
        variables = [v for v in ds.data_vars if "time" in ds[v].dims]

    # Ensure numpy datetime64 for resampling
    if not np.issubdtype(ds.time.dtype, np.datetime64):
        ds = ds.assign_coords(time=pd.to_datetime(ds.time.values))

    aggregator = agg.lower()
    valid_aggs = {"mean", "median", "sum", "min", "max"}
    if aggregator not in valid_aggs:
        raise ValueError(f"agg must be one of {sorted(valid_aggs)}")

    shared_time = None
    out_vars = {}
    for v in variables:
        if v not in ds or "time" not in ds[v].dims:
            continue
        resampler = ds[v].resample(time=step, closed="left", label="left")
        rebinned = getattr(resampler, aggregator)()
        # ensure the rebinned data uses its own time dimension to avoid conflicts
        rebinned = rebinned.rename({"time": time_coord_name})
        out_vars[v + suffix] = rebinned
        if shared_time is None:
            shared_time = rebinned[time_coord_name]

    # Attach shared resampled time coordinate (as its own dimension), then variables
    if shared_time is not None:
        ds = ds.assign_coords({time_coord_name: shared_time.values})
    ds = ds.assign(**out_vars)

    # Record reduction history
    try:
        history = list(ds.attrs.get("reduction_history", []))
        info = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "type": "time_resample",
            "step": step,
            "agg": aggregator,
            "affected_vars": [v for v in variables if v in out_vars or (v + suffix) in out_vars],
            "input_len": int(ds.time.size) if "time" in ds.sizes else None,
            "output_len": int(shared_time.size) if shared_time is not None else None,
            "time_range": [
                str(ds.time.values[0]) if "time" in ds.coords else None,
                str(ds.time.values[-1]) if "time" in ds.coords else None,
            ],
            "outputs": {
                v + suffix: {
                    "dim": time_coord_name,
                    "len": int(out_vars[v + suffix].sizes.get(time_coord_name, 0)),
                }
                for v in variables
                if (v + suffix) in out_vars
            },
        }
        history.append(info)
        ds.attrs["reduction_history"] = history
    except Exception:
        # non-fatal
        pass
    return ds


# Backward-compatible alias used by notebooks
def interpolate_timeseries(ds: xr.Dataset, *_args, variables: Optional[Sequence[str]] = None, step: str = "30S", agg: str = "mean") -> xr.Dataset:
    """Alias for rebin_timeseries with fixed step default of 30 seconds.

    Note: positional args are accepted for backward compatibility but ignored.
    """
    return rebin_timeseries(ds, step=step, variables=variables, agg=agg)



def print_reduction_history(ds: xr.Dataset, latest_only: bool = True) -> str:
    """Return and print a compact reduction history summary from ds.attrs."""
    history = ds.attrs.get("reduction_history", [])
    if not history:
        msg = "No reduction operations recorded."
        print(msg)
        return msg

    items = [history[-1]] if latest_only else history
    lines: List[str] = ["Reduction history:"]
    for i, h in enumerate(items, 1):
        hdr = f"[{h.get('timestamp','')}] {h.get('type','')}"
        if h.get("type") == "time_resample":
            hdr += f" step={h.get('step')} agg={h.get('agg')} input_len={h.get('input_len')} output_len={h.get('output_len')}"
        lines.append(hdr)
        aff = h.get("affected_vars", [])
        if aff:
            lines.append("  vars: " + ", ".join(aff))
    summary = "\n".join(lines)
    print(summary)
    return summary

def _rebin_along_axis(data: np.ndarray, bin_centers: np.ndarray, bin_edges: np.ndarray, 
                     bin_axis: int, statistic: str) -> np.ndarray:
    """Apply binning along a specific axis, preserving other dimensions."""
    from scipy.stats import binned_statistic
    
    # Move bin axis to last position for easier processing
    data_moved = np.moveaxis(data, bin_axis, -1)
    original_shape = data_moved.shape
    
    # Flatten all but the last dimension
    data_flat = data_moved.reshape(-1, original_shape[-1])
    
    # Apply binning to each row (each represents one set of other coordinates)
    n_output_bins = len(bin_edges) - 1
    result_flat = np.full((data_flat.shape[0], n_output_bins), np.nan)
    
    for i in range(data_flat.shape[0]):
        row = data_flat[i]
        if not np.all(np.isnan(row)):
            binned_vals, _, _ = binned_statistic(bin_centers, row, statistic=statistic, bins=bin_edges)
            result_flat[i] = binned_vals
    
    # Reshape back and move axis back to original position
    new_shape = original_shape[:-1] + (n_output_bins,)
    result_reshaped = result_flat.reshape(new_shape)
    result = np.moveaxis(result_reshaped, -1, bin_axis)
    
    return result


def rebin_logspace_bins(
    ds: xr.Dataset,
    *,
    variables: Optional[Sequence[str]] = None,
    n_bins: int = 20,
    bin_coord: str = "bins",
    bins_coord_name: str = "bins_rebinned",
    bin_range: Optional[Tuple[float, float]] = None,
    statistic: str = "mean",
    suffix: str = "_rebinned_b",
) -> xr.Dataset:
    """Rebin spectral data along log-spaced bins using scipy.stats.binned_statistic."""
    from scipy.stats import binned_statistic
    
    if bin_coord not in ds.coords:
        raise ValueError(f"Coordinate '{bin_coord}' not found in dataset")
    
    variables = variables or [v for v in ds.data_vars if bin_coord in ds[v].dims]
    
    # Get bin centers and create log-spaced edges
    bin_centers = ds[bin_coord].values
    if bin_range is None:
        bin_min, bin_max = np.nanmin(bin_centers), np.nanmax(bin_centers)
        if bin_min <= 0:
            bin_min = np.nanmin(bin_centers[bin_centers > 0])
    else:
        bin_min, bin_max = bin_range
    
    bin_edges = np.logspace(np.log10(bin_min), np.log10(bin_max), n_bins + 1)
    new_bin_centers = np.sqrt(bin_edges[:-1] * bin_edges[1:])  # Geometric mean
    
    # Create new coordinate
    new_coord = xr.DataArray(
        new_bin_centers, 
        dims=[bins_coord_name],
        attrs=ds[bin_coord].attrs.copy()
    )
    
    # Process each variable
    out_vars = {}
    for var in variables:
        if var not in ds or bin_coord not in ds[var].dims:
            continue
            
        da = ds[var]
        bin_axis = list(da.dims).index(bin_coord)
        
        # Apply rebinning preserving all dimensions
        if da.ndim == 1:
            binned_vals, _, _ = binned_statistic(bin_centers, da.values, statistic=statistic, bins=bin_edges)
            new_data = binned_vals
        else:
            new_data = _rebin_along_axis(da.values, bin_centers, bin_edges, bin_axis, statistic)
        
        # Create new dimensions and coordinates
        new_dims = [bins_coord_name if d == bin_coord else d for d in da.dims]
        new_coords = {bins_coord_name if d == bin_coord else d: 
                     new_coord if d == bin_coord else da.coords[d] 
                     for d in da.dims}
        
        out_vars[f"{var}{suffix}"] = xr.DataArray(
            new_data, dims=new_dims, coords=new_coords, attrs=da.attrs.copy()
        )
    
    # Update dataset and record history
    ds_out = ds.assign_coords({bins_coord_name: new_coord}).assign(**out_vars)
    
    try:
        history = list(ds_out.attrs.get("reduction_history", []))
        history.append({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "type": "logspace_rebin", "bin_coord": bin_coord, "n_bins": n_bins,
            "bin_range": [float(bin_min), float(bin_max)], "statistic": statistic,
            "affected_vars": list(out_vars.keys()),
            "input_bins": int(len(bin_centers)), "output_bins": int(len(new_bin_centers))
        })
        ds_out.attrs["reduction_history"] = history
    except Exception:
        pass  # Non-fatal
    
    return ds_out


def prepare_holimo_for_overlay(
    holimo_path: str,
    time_window: Tuple[np.datetime64, np.datetime64],
    *,
    resample_s: int = 10,
    smoothing_time_bins: int = 8,
    smoothing_diameter_bins: int = 0,
    min_coverage_frac: float = 0.01,
    expected_dt_s: float = 1,
) -> xr.Dataset:
    """Load HOLIMO, slice time, convert diameter to µm, resample, mask by coverage, smooth. For plume-path overlay."""
    ds, _lbb, _cbb = load_and_prepare_holimo(holimo_path)
    ds = ds.sel(time=slice(*time_window)).assign_coords(diameter=ds.diameter * 1e6)
    min_samples = max(1, int(round(resample_s / expected_dt_s * min_coverage_frac)))
    ds_mean = ds.resample(time=f"{int(resample_s)}s").mean()
    count_vars = [v for v, da in ds.data_vars.items() if "time" in da.dims and np.issubdtype(da.dtype, np.number)]
    coverage = ds[count_vars].resample(time=f"{resample_s}s").count().to_array("var")
    reduce_dims = [d for d in coverage.dims if d not in ("time", "var")]
    if reduce_dims:
        coverage = coverage.min(dim=reduce_dims)
    valid = (coverage >= min_samples).all("var")
    ds_out = ds_mean.where(valid).rolling(time=smoothing_time_bins, center=True, min_periods=2).mean()
    if smoothing_diameter_bins > 1:
        ds_out = ds_out.rolling(diameter=smoothing_diameter_bins, center=True, min_periods=2).mean()
    return ds_out


def check_rebinned_data_structure(ds: xr.Dataset, suffix: str = "_rebinned") -> None:
    """Debug helper to check structure of rebinned data."""
    print("=== Rebinned Data Structure Check ===")
    
    rebinned_vars = [v for v in ds.data_vars if suffix in v]
    if not rebinned_vars:
        print(f"No variables found with suffix '{suffix}'")
        return
    
    for var in rebinned_vars:
        da = ds[var]
        print(f"\nVariable: {var}")
        print(f"  Dimensions: {da.dims}")
        print(f"  Shape: {da.shape}")
        print(f"  Coordinates:")
        for coord in da.coords:
            coord_info = da.coords[coord]
            coord_type = type(coord_info.values.flat[0]).__name__ if coord_info.size > 0 else "empty"
            print(f"    {coord}: {coord_info.shape} ({coord_type})")
            if hasattr(coord_info, 'attrs') and 'unit' in coord_info.attrs:
                print(f"      unit: {coord_info.attrs['unit']}")


