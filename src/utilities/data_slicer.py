from __future__ import annotations

from pathlib import Path
from typing import Any

import xarray as xr

AxisValue = int | float | str | slice | tuple[Any, Any] | list[Any]

_AXIS_ALIASES: dict[str, tuple[str, ...]] = {
    "time": ("time",),
    "lat": ("lat", "latitude", "y"),
    "lon": ("lon", "longitude", "x"),
    "alt": ("alt", "altitude", "z"),
    "diameter": ("diameter", "bin"),
}


def _all_dataset_axes(ds: xr.Dataset) -> set[str]:
    return set(ds.dims) | set(ds.coords)


def _canonical_axis_name(key: str) -> str | None:
    for canonical, aliases in _AXIS_ALIASES.items():
        if key == canonical or key in aliases:
            return canonical
    return None


def _resolve_axis_target(canonical: str, ds: xr.Dataset) -> str | None:
    for candidate in _AXIS_ALIASES[canonical]:
        if candidate in ds.dims or candidate in ds.coords:
            return candidate
    return None


def _normalize_axis_value(value: AxisValue) -> AxisValue:
    if isinstance(value, tuple):
        if len(value) != 2:
            raise ValueError("Tuple slice bounds must contain exactly two values.")
        return slice(value[0], value[1])
    return value


def normalize_slice_dict(slice_dict: dict[str, AxisValue] | None, ds: xr.Dataset) -> dict[str, AxisValue]:
    """
    Normalize a user-provided slice dictionary to dataset axis names.

    Supported values per axis: scalar, tuple(low, high), slice(...), or list.
    Aliases are resolved for logical axes:
    - lat -> latitude/y
    - lon -> longitude/x
    - alt -> altitude/z
    - diameter -> diameter/bin
    """
    if not slice_dict:
        return {}

    known_axes = _all_dataset_axes(ds)
    normalized: dict[str, AxisValue] = {}

    for raw_key, raw_value in slice_dict.items():
        canonical = _canonical_axis_name(raw_key)

        if canonical is None:
            if raw_key in known_axes:
                target = raw_key
            else:
                continue
        else:
            target = _resolve_axis_target(canonical, ds)
            if target is None:
                continue

        normalized[target] = _normalize_axis_value(raw_value)

    return normalized


def _resolved_keys(slice_dict: dict[str, AxisValue] | None, ds: xr.Dataset) -> set[str]:
    if not slice_dict:
        return set()
    resolved: set[str] = set()
    known_axes = _all_dataset_axes(ds)
    for raw_key in slice_dict:
        canonical = _canonical_axis_name(raw_key)
        if canonical is None:
            if raw_key in known_axes:
                resolved.add(raw_key)
            continue
        target = _resolve_axis_target(canonical, ds)
        if target is not None:
            resolved.add(raw_key)
    return resolved


def _is_sel_axis(ds: xr.Dataset, axis: str) -> bool:
    return axis in ds.coords or axis in ds.indexes


def _shape_dict(ds: xr.Dataset) -> dict[str, int]:
    return {dim: int(size) for dim, size in ds.sizes.items()}


def _effective_bounds(ds: xr.Dataset, used_dims: list[str]) -> dict[str, dict[str, Any]]:
    bounds: dict[str, dict[str, Any]] = {}
    for dim in used_dims:
        size = int(ds.sizes.get(dim, 0))
        entry: dict[str, Any] = {"size": size}

        if size == 0:
            bounds[dim] = entry
            continue

        if dim in ds.coords:
            coord = ds.coords[dim]
            entry["start"] = coord.isel({dim: 0}).item()
            entry["end"] = coord.isel({dim: -1}).item()
        else:
            entry["start"] = 0
            entry["end"] = size - 1

        bounds[dim] = entry
    return bounds


def slice_dataset(
    ds: xr.Dataset,
    slice_dict: dict[str, AxisValue] | None,
    *,
    strict: bool = False,
) -> tuple[xr.Dataset, dict[str, Any]]:
    """
    Slice an xarray Dataset from a user-provided dictionary.

    Uses .sel() for coordinate-like axes and falls back to .isel() where needed.
    Returns (sliced_dataset, metadata).
    """
    shape_before = _shape_dict(ds)
    normalized = normalize_slice_dict(slice_dict, ds)

    if strict and slice_dict:
        unresolved = set(slice_dict) - _resolved_keys(slice_dict, ds)
        if unresolved:
            raise KeyError(f"Unknown or unavailable slice keys: {sorted(unresolved)}")

    ds_out = ds
    used_dims: list[str] = []

    for axis, value in normalized.items():
        used_dims.append(axis)
        if _is_sel_axis(ds_out, axis):
            ds_out = ds_out.sel({axis: value})
        else:
            ds_out = ds_out.isel({axis: value})

    metadata = {
        "used_dims": used_dims,
        "effective_bounds": _effective_bounds(ds_out, used_dims),
        "shape_before": shape_before,
        "shape_after": _shape_dict(ds_out),
    }
    return ds_out, metadata


def slice_dataset_to_zarr(
    ds: xr.Dataset,
    slice_dict: dict[str, AxisValue] | None,
    out_path: str | Path | None = None,
    *,
    overwrite: bool = False,
) -> tuple[xr.Dataset, dict[str, Any]]:
    """
    Slice dataset and optionally persist the result to a Zarr store.

    If out_path is None, no write is performed.
    """
    ds_sliced, metadata = slice_dataset(ds, slice_dict)

    if out_path is not None:
        target = Path(out_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        ds_sliced.to_zarr(target, mode="w" if overwrite else "w-")
        metadata["zarr_path"] = str(target)
    else:
        metadata["zarr_path"] = None

    return ds_sliced, metadata
