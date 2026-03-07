"""
LV3 process-rate dataset generation from meteogram Zarr.

Builds bulk and spectral microphysical tendency rates per process group,
stacks them into xarray datasets with process labels and metadata_config
descriptions. Used by scripts/processing_chain/run_lv3.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import xarray as xr

from utilities.processing_metadata import add_provenance_to_dataset, git_head

# Conversion factors: raw [kg⁻¹ s⁻¹] × ρ → [cm⁻³ s⁻¹]; raw [kg kg⁻¹ s⁻¹] × ρ → [g cm⁻³ s⁻¹]
_CONV = {"N": 1e-6, "Q": 1e-3}

# Physics-aware process groups: group -> [(BASE_NAME, SPECTRUM, KIND), ...]
# SPECTRUM: W = warm/liquid, F = frozen/ice. KIND: N = number, Q = mass.
PHYSICS_GROUPS: Dict[str, List[Tuple[str, str, str]]] = {
    "CONDENSATION": [
        ("CONDN", "W", "N"), ("CONDQ", "W", "Q"),
        ("CONDNFROD", "F", "N"), ("CONDQFROD", "F", "Q"),
        ("CONDQWFROD", "F", "Q"),
    ],
    "DROP_COLLISION": [
        ("KOLLN", "W", "N"), ("KOLLQ", "W", "Q"),
        ("KOLLN_INS", "W", "N"), ("KOLLQ_INS", "W", "Q"),
    ],
    "RIMING": [("KOLLNI", "F", "N"), ("KOLLQI", "F", "Q"), ("KOLLQWF", "F", "Q")],
    "CONTACT_FREEZING": [
        ("KOLLNFRODI", "F", "N"), ("KOLLQFRODI", "F", "Q"),
        ("KOLLNFROD", "F", "N"), ("KOLLQFROD", "F", "Q"),
        ("KOLLNFROD_INS", "F", "N"), ("KOLLQFROD_INS", "F", "Q"),
    ],
    "AGGREGATION": [("KNF", "F", "N"), ("KQF", "F", "Q"), ("KQWF", "F", "Q")],
    "IMMERSION_FREEZING": [("IMMERN", "W", "N"), ("IMMERQ", "W", "Q")],
    "HOMOGENEOUS_FREEZING": [("HOMN", "W", "N"), ("HOMQ", "W", "Q")],
    "BREAKUP": [("BREAN", "W", "N"), ("BREAQ", "W", "Q")],
    "MELTING": [
        ("DNFMELT", "F", "N"), ("DQFMELT", "F", "Q"), ("DQFWMELT", "F", "Q"),
        ("DNWMELT", "W", "N"), ("DQWMELT", "W", "Q"),
    ],
    "DEPOSITION": [("DEPONF", "F", "N"), ("DEPOQF", "F", "Q")],
    "REFREEZING": [("DQFFRIER", "F", "Q")],
}

_BASE_TO_GROUP: Dict[str, Tuple[str, str, str]] = {}
for grp, members in PHYSICS_GROUPS.items():
    for base, spec, knd in members:
        _BASE_TO_GROUP[base.upper()] = (grp, spec, knd)


def classify_tendency(varname: str) -> Optional[Tuple[str, str, str, str]]:
    """Return (base_name, kind, process_group, spectrum) or None."""
    base = varname.removeprefix("SUM_").removeprefix("P_").removeprefix("N_")
    match = _BASE_TO_GROUP.get(base.upper())
    if match is None:
        return None
    grp, spec, kind = match
    return base, kind, grp, spec


def build_proc_vars(ds: xr.Dataset) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """Build proc_vars[group][kind][spectrum] = [varnames] from SUM_* variables."""
    sum_vars = sorted(v for v in ds.data_vars if v.startswith("SUM_"))
    proc_vars: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for sv in sum_vars:
        stripped = sv.replace("SUM_", "")
        if stripped.startswith("P_") or stripped.startswith("N_"):
            continue
        info = classify_tendency(sv)
        if info is None:
            continue
        _base, kind, grp, spec = info
        proc_vars.setdefault(grp, {"N": {"W": [], "F": []}, "Q": {"W": [], "F": []}})
        proc_vars[grp][kind][spec].append(sv)
    return proc_vars


def bulk_rate(
    ds_exp: xr.Dataset,
    rho: Optional[xr.DataArray],
    varname: str,
    bin_slice: Union[slice, Tuple[int, Optional[int]]],
    kind: str = "N",
) -> xr.DataArray:
    """Sum a SUM_ variable over bin_slice, convert to display units."""
    data = ds_exp[varname]
    if "bins" in data.dims:
        if isinstance(bin_slice, tuple):
            data = data.isel(bins=slice(bin_slice[0], bin_slice[1])).sum(dim="bins")
        else:
            data = data.isel(bins=bin_slice).sum(dim="bins")
    if rho is not None:
        data = data * rho * _CONV[kind]
    return data


def spectral_rate(
    ds_exp: xr.Dataset,
    rho: Optional[xr.DataArray],
    varname: str,
    kind: str = "N",
) -> xr.DataArray:
    """Return full bin-resolved rate in display units."""
    data = ds_exp[varname]
    if rho is not None:
        data = data * rho * _CONV[kind]
    return data


def build_rates(
    ds_exp: xr.Dataset,
    rho: Optional[xr.DataArray],
    proc_vars: Dict[str, Dict[str, Dict[str, List[str]]]],
    kind: str,
    bin_slice: Union[slice, Tuple[int, Optional[int]]],
    spectrum: Optional[str] = None,
) -> Dict[str, xr.DataArray]:
    """Aggregate net variables per process group → dict of DataArrays."""
    rates = {}
    for grp, d in proc_vars.items():
        vlist = d[kind]["W"] + d[kind]["F"] if spectrum is None else d[kind][spectrum]
        if vlist:
            rates[grp] = sum(bulk_rate(ds_exp, rho, v, bin_slice, kind) for v in vlist)
    return rates


def build_spectral_rates(
    ds_exp: xr.Dataset,
    rho: Optional[xr.DataArray],
    proc_vars: Dict[str, Dict[str, Dict[str, List[str]]]],
    kind: str,
    spectrum: Optional[str] = None,
) -> Dict[str, xr.DataArray]:
    """Like build_rates but keeps the bins dimension."""
    rates = {}
    for grp, d in proc_vars.items():
        vlist = d[kind]["W"] + d[kind]["F"] if spectrum is None else d[kind][spectrum]
        if vlist:
            rates[grp] = sum(spectral_rate(ds_exp, rho, v, kind) for v in vlist)
    return rates


def get_process_display_name(proc: str, spectrum: Optional[str]) -> str:
    """Human-readable label for process and spectrum (e.g. 'Condensation (liq)')."""
    labels = {
        "W": "liq",
        "F": "ice",
    }
    suf = f" ({labels[spectrum]})" if spectrum in labels else ""
    return proc.replace("_", " ").title() + suf


def _tendency_meta_for_process(config: Optional[Dict], process_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Get description and unit for a process group from metadata_config tendencies."""
    if not config or "tendencies" not in config or "process_groups" not in config:
        return None, None
    vars_in_group = config.get("process_groups", {}).get(process_name, [])
    if not vars_in_group:
        return None, None
    base_var = vars_in_group[0]
    for cat, entries in config.get("tendencies", {}).items():
        if base_var in entries:
            e = entries[base_var]
            return e.get("description"), e.get("unit")
    return None, None


def _stack_proc_dict(
    proc2da: Dict[str, xr.DataArray],
    dim: str,
) -> Optional[xr.DataArray]:
    """Stack dict of {process_name: DataArray} along new dimension *dim*."""
    if not proc2da:
        return None
    procs = sorted(proc2da)
    das = [
        xr.DataArray(proc2da[p]).expand_dims({dim: [p]}) if not isinstance(proc2da[p], xr.DataArray)
        else proc2da[p].expand_dims({dim: [p]})
        for p in procs
    ]
    out = xr.concat(das, dim=dim)
    out = out.assign_coords({dim: procs})
    return out


def _add_spectral_concentrations(
    ds_out: xr.Dataset,
    ds_exp: xr.Dataset,
    config: Optional[Dict],
) -> None:
    """Copy spectral number/mass concentrations from ds_exp with metadata_config attrs."""
    spectral = (config or {}).get("variables", {}).get("SPECTRAL", {})
    order = ["NW", "NF", "QW", "QF", "QFW", "QWS", "QWA", "QFS", "QFA", "QIA"]
    names = [v for v in order if v in spectral] + [v for v in spectral if v not in order]
    for vname in names:
        cand = [d for d in ds_exp.data_vars if d.upper() == vname]
        if not cand:
            continue
        key = cand[0]
        da = ds_exp[key]
        if not isinstance(da, xr.DataArray):
            da = xr.DataArray(da)
        ds_out[vname] = da
        meta = spectral.get(vname, {})
        ds_out[vname].attrs.update({
            "long_name": meta.get("long_name", vname),
            "standard_name": meta.get("standard_name", ""),
            "description": meta.get("description", ""),
            "units": meta.get("units", ""),
        })


def build_rates_dataset(
    R: Dict[str, Any],
    eid: int,
    ds_exp: Optional[xr.Dataset] = None,
    config: Optional[Dict] = None,
    *,
    repo_root: Optional[Path] = None,
) -> xr.Dataset:
    """Build a single xr.Dataset containing all rate variables and process metadata."""
    commit = git_head(repo_root)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    ds_out = xr.Dataset(attrs={
        "title": "PolarCAP microphysical process budget rates",
        "created_utc": now,
        "processing_commit": commit,
        "processing_commit_short": commit[:12] if commit else None,
        "exp_id": int(eid),
        "exp_label": R.get("exp_label"),
        "sign_convention": "+ source, - sink",
        "notes": "Rates derived from SUM_* microphysical tendencies; aggregated over bin ranges.",
        "metadata_config": "src/utilities/metadata_config.json",
        "specs_document": "docs/specs_params_and_variables.md",
        "stage": "lv3",
        "processing_level": "LV3",
    })

    if ds_exp is not None:
        _add_spectral_concentrations(ds_out, ds_exp, config)

    for kind, unit in (("N", R.get("unit_N")), ("Q", R.get("unit_Q"))):
        for spec, key, bins in (
            ("W", f"rates_{kind}_liq", "LBB"),
            ("F", f"rates_{kind}_ice", "CBB"),
        ):
            proc2da = R.get(key, {})
            da = _stack_proc_dict(proc2da, dim=f"process_{kind}_{spec}")
            if da is None:
                continue
            dim = f"process_{kind}_{spec}"
            procs = da[dim].values.tolist()
            ds_out[dim] = xr.DataArray(procs, dims=(dim,))
            ds_out[f"process_label_{kind}_{spec}"] = xr.DataArray(
                [get_process_display_name(p, spec if spec in ("W", "F") else None) for p in procs],
                dims=(dim,), attrs={"description": "Human-readable process label"},
            )
            descs = [_tendency_meta_for_process(config, p) for p in procs]
            ds_out[f"process_description_{kind}_{spec}"] = xr.DataArray(
                [d[0] or "" for d in descs],
                dims=(dim,), attrs={"description": "Process description from metadata_config"},
            )
            vname = f"rate_{kind}_{spec}"
            ds_out[vname] = da
            u = unit
            if config and not u and procs:
                _, u = _tendency_meta_for_process(config, procs[0])
            ds_out[vname].attrs.update({
                "units": u or "",
                "kind": kind,
                "spectrum": spec,
                "bin_range": bins,
                "description": "Signed microphysical tendency rate by process.",
            })

    for kind, unit in (("N", R.get("unit_N")), ("Q", R.get("unit_Q"))):
        for spec in ("W", "F"):
            proc2da = R.get(f"spec_rates_{kind}_{spec}", {})
            da = _stack_proc_dict(proc2da, dim=f"process_spec_{kind}_{spec}")
            if da is None:
                continue
            dim = f"process_spec_{kind}_{spec}"
            procs = da[dim].values.tolist()
            ds_out[dim] = xr.DataArray(procs, dims=(dim,))
            ds_out[f"process_spec_label_{kind}_{spec}"] = xr.DataArray(
                [get_process_display_name(p, spec) for p in procs],
                dims=(dim,), attrs={"description": "Human-readable process label"},
            )
            descs = [_tendency_meta_for_process(config, p) for p in procs]
            ds_out[f"process_spec_description_{kind}_{spec}"] = xr.DataArray(
                [d[0] or "" for d in descs],
                dims=(dim,), attrs={"description": "Process description from metadata_config"},
            )
            vname = f"spec_rate_{kind}_{spec}"
            ds_out[vname] = da
            u = unit
            if config and not u and procs:
                _, u = _tendency_meta_for_process(config, procs[0])
            ds_out[vname].attrs.update({
                "units": u or "",
                "kind": kind,
                "spectrum": spec,
                "description": "Diameter-resolved signed tendency rate by process.",
            })

    return ds_out


def build_rates_for_experiments(
    ds: xr.Dataset,
    exp_ids: List[int],
    config: Optional[Dict] = None,
    LBB: Union[slice, Tuple[int, int]] = slice(30, 50),
    CBB: Union[slice, Tuple[int, Optional[int]]] = slice(30, 50),
    *,
    repo_root: Optional[Path] = None,
) -> Tuple[Dict[int, Dict], Dict[int, xr.Dataset]]:
    """
    Build rates dict and rates Dataset for each experiment index in exp_ids.
    Returns (rates_by_exp, rates_ds_by_exp). IMMERSION_FREEZING cross-assign applied.
    """
    rates_by_exp: Dict[int, Dict] = {}
    rates_ds_by_exp: Dict[int, xr.Dataset] = {}
    unit_N = r"cm$^{-3}$ s$^{-1}$"
    unit_Q = r"g cm$^{-3}$ s$^{-1}$"
    for eid in exp_ids:
        ds_exp = ds.isel(expname=eid)
        if "HMLd" in ds_exp.coords:
            ds_exp = ds_exp.assign_coords(height_level=ds_exp.HMLd)
        if "HHLd" in ds_exp.coords:
            ds_exp = ds_exp.assign_coords(height_level2=ds_exp.HHLd)
        rho = ds_exp["RHO"] if "RHO" in ds_exp.data_vars else None
        proc_vars = build_proc_vars(ds_exp)
        exp_label = str(ds_exp.expname.values) if "expname" in ds_exp.coords else str(eid)
        R = {
            "exp_label": exp_label,
            "unit_N": unit_N,
            "unit_Q": unit_Q,
            "rates_N_liq": build_rates(ds_exp, rho, proc_vars, "N", LBB, spectrum="W"),
            "rates_Q_liq": build_rates(ds_exp, rho, proc_vars, "Q", LBB, spectrum="W"),
            "rates_N_ice": build_rates(ds_exp, rho, proc_vars, "N", CBB, spectrum="F"),
            "rates_Q_ice": build_rates(ds_exp, rho, proc_vars, "Q", CBB, spectrum="F"),
            "spec_rates_N_W": build_spectral_rates(ds_exp, rho, proc_vars, "N", spectrum="W"),
            "spec_rates_N_F": build_spectral_rates(ds_exp, rho, proc_vars, "N", spectrum="F"),
            "spec_rates_Q_W": build_spectral_rates(ds_exp, rho, proc_vars, "Q", spectrum="W"),
            "spec_rates_Q_F": build_spectral_rates(ds_exp, rho, proc_vars, "Q", spectrum="F"),
        }
        if "IMMERSION_FREEZING" in R["rates_N_liq"]:
            R["rates_N_ice"]["IMMERSION_FREEZING"] = -R["rates_N_liq"]["IMMERSION_FREEZING"]
        if "IMMERSION_FREEZING" in R["rates_Q_liq"]:
            R["rates_Q_ice"]["IMMERSION_FREEZING"] = -R["rates_Q_liq"]["IMMERSION_FREEZING"]
        rates_by_exp[eid] = R
        rates_ds_by_exp[eid] = build_rates_dataset(R, eid, ds_exp=ds_exp, config=config, repo_root=repo_root)
    return rates_by_exp, rates_ds_by_exp
