"""
LV3 process-rate dataset generation from meteogram Zarr.

Builds bulk and spectral microphysical tendency rates per process group,
stacks them into xarray datasets with process labels and metadata_config
descriptions. Used by scripts/processing_chain/run_lv3.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr

from utilities.processing_metadata import add_provenance_to_dataset, git_head

# Conversion factors: raw [kg⁻¹ s⁻¹] × ρ → [m⁻³ s⁻¹]; raw [kg kg⁻¹ s⁻¹] × ρ → [g m⁻³ s⁻¹]
_CONV = {"N": 1.0, "Q": 1e3}

# Physics-aware process groups: group -> [(BASE_NAME, SPECTRUM, KIND), ...]
# SPECTRUM: W = warm/liquid, F = frozen/ice. KIND: N = number, Q = mass.
# Grouping aligns with docs/specs_params_and_variables.md (Output + Microphysical process tendencies).
# Categories for plot ordering: (1) Liquid-only (2) Freezing (3) Riming (4) Ice-only (5) Melting.
PHYSICS_GROUPS: Dict[str, List[Tuple[str, str, str]]] = {
    # ── Liquid-only (warm-phase, no phase change) ──
    # CONDN/CONDQ: condensation/evaporation on liquid drops          (cond_mixxd.f90)
    "CONDENSATION": [
        ("CONDN", "W", "N"), ("CONDQ", "W", "Q"),
    ],
    # BREAN/BREAQ: spontaneous drop breakup                          (breakxd.f90)
    "BREAKUP": [("BREAN", "W", "N"), ("BREAQ", "W", "Q")],
    # KOLLN/KOLLQ: drop-drop coalescence                             (koll_contactxd.f90)
    "DROP_COLLISION": [("KOLLN", "W", "N"), ("KOLLQ", "W", "Q")],
    # KOLLN_INS/KOLLQ_INS: drop + insol. aerosol scavenging (liquid fraction stays liquid)
    "DROP_INS_COLLISION": [("KOLLN_INS", "W", "N"), ("KOLLQ_INS", "W", "Q")],
    # ── Freezing (liquid → ice) ──
    # IMMERN/IMMERQ: stochastic immersion freezing of supercooled drops  (immersion_koopxd.f90)
    "IMMERSION_FREEZING": [("IMMERN", "W", "N"), ("IMMERQ", "W", "Q")],
    # HOMN/HOMQ: homogeneous freezing of solution droplets           (homogeneous_freezing.f90)
    "HOMOGENEOUS_FREEZING": [("HOMN", "W", "N"), ("HOMQ", "W", "Q")],
    # KOLLNFROD_INS/KOLLQFROD_INS: drop + insol. aerosol → frozen drop   (koll_insolxd.f90)
    "CONTACT_FREEZING": [
        ("KOLLNFROD_INS", "F", "N"), ("KOLLQFROD_INS", "F", "Q"),
    ],
    # ── Riming (drop captured by ice, both liquid-side loss and ice-side gain) ──
    # KOLLNI/KOLLQI: liquid-side loss (drops consumed)               (koll_ice_dropsxd.f90, in DNW/DQW budget)
    # KOLLNFRODI/KOLLQFRODI: ice-side number/mass redistribution     (koll_ice_dropsxd.f90, in DNFROD/DQFROD budget)
    # KOLLNFROD/KOLLQFROD: alt riming pathway (DM15 kernel)          (koll_contactxd_DM15.f90)
    # KOLLQWF: liquid water shell gain on ice from captured drops    (koll_ice_dropsxd.f90)
    "RIMING": [
        ("KOLLNI", "W", "N"), ("KOLLQI", "W", "Q"),
        ("KOLLNFRODI", "F", "N"), ("KOLLQFRODI", "F", "Q"),
        ("KOLLNFROD", "F", "N"), ("KOLLQFROD", "F", "Q"),
        ("KOLLQWF", "F", "Q"),
    ],
    # ── Ice-only (deposition, aggregation, refreezing) ──
    # DEPONF/DEPOQF: heterogeneous nucleation from vapour            (depoxd.f90)
    # CONDNFROD/CONDQFROD/CONDQWFROD: vapour deposition on existing ice (cond_mixxd.f90)
    "DEPOSITION": [
        ("DEPONF", "F", "N"), ("DEPOQF", "F", "Q"),
        ("CONDNFROD", "F", "N"), ("CONDQFROD", "F", "Q"), ("CONDQWFROD", "F", "Q"),
    ],
    # KNF/KQF/KQWF: ice-ice collision/aggregation (wet ice)         (koll_eis_eisxd.f90)
    "AGGREGATION": [("KNF", "F", "N"), ("KQF", "F", "Q"), ("KQWF", "F", "Q")],
    # DQFFRIER: refreezing of liquid water shell on ice core         (frierenxd.f90)
    "REFREEZING": [("DQFFRIER", "F", "Q")],
    # ── Melting (ice → liquid) ──
    # ice-side: DNFMELT/DQFMELT/DQFWMELT  liquid-side: DNWMELT/DQWMELT  (schmelzenxd.f90)
    "MELTING": [
        ("DNFMELT", "F", "N"), ("DQFMELT", "F", "Q"), ("DQFWMELT", "F", "Q"),
        ("DNWMELT", "W", "N"), ("DQWMELT", "W", "Q"),
    ],
}

# Suggested bar/legend order for spectral waterfall: liquid → freezing → riming → ice → melting.
PROCESS_PLOT_ORDER: List[str] = [
    "CONDENSATION", "BREAKUP", "DROP_COLLISION", "DROP_INS_COLLISION",
    "IMMERSION_FREEZING", "HOMOGENEOUS_FREEZING", "CONTACT_FREEZING",
    "RIMING", "DEPOSITION", "AGGREGATION", "REFREEZING", "MELTING",
]

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
    """Build proc_vars[group][kind][spectrum] = [varnames] from SUM_* variables.

    SUM_* in the meteogram Zarr are accumulated tendencies; rates (fluxes) are
    computed on the fly via tendency_to_rate in bulk_rate/spectral_rate.
    """
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


def tendency_to_rate(
    da: xr.DataArray,
    time_dim: str = "time",
) -> xr.DataArray:
    """Compute rate (flux) from accumulated tendency: rate = d(tendency)/dt.

    Meteogram Zarr stores SUM_* as accumulated tendencies. The instantaneous rate is
    CONDN = (SUM_COND(t_{n+1}) - SUM_COND(t_n)) / (t_{n+1} - t_n). Uses forward
    difference for all steps; first and last time steps are preserved so that rate exists
    at seeding start and at the end (first step uses interval [t0,t1], last step uses [t_{N-2},t_{N-1}]).
    Output has the same time dimension as the input.
    """
    if time_dim not in da.dims:
        return da
    t = da[time_dim]
    n = da.sizes[time_dim]
    if n < 2:
        return da
    dt_fwd = t.diff(dim=time_dim).astype("timedelta64[s]").astype(float)
    diff_fwd = da.diff(dim=time_dim)
    rate_mid = diff_fwd / dt_fwd  # length n-1, valid at t[1], t[2], ..., t[n-1]
    # First step: rate at t[0] = (SUM[1]-SUM[0])/dt so seeding start has a rate
    rate_first = (da.isel({time_dim: 1}) - da.isel({time_dim: 0})) / dt_fwd.isel({time_dim: 0})
    rate_first = rate_first.expand_dims({time_dim: 1}).assign_coords({time_dim: t.isel({time_dim: [0]})})
    # Last step: rate at t[n-1] = (SUM[n-1]-SUM[n-2])/dt (backward diff)
    rate_last = (da.isel({time_dim: -1}) - da.isel({time_dim: -2})) / dt_fwd.isel({time_dim: -1})
    rate_last = rate_last.expand_dims({time_dim: 1}).assign_coords({time_dim: t.isel({time_dim: [n - 1]})})
    if n == 2:
        rate = xr.concat([rate_first, rate_last], dim=time_dim)
    else:
        # Middle: t[1]..t[n-2] (drop last from rate_mid to avoid duplicate with rate_last)
        rate_mid_mid = rate_mid.isel({time_dim: slice(0, -1)})
        rate = xr.concat([rate_first, rate_mid_mid, rate_last], dim=time_dim)
    return rate


def bulk_rate(
    ds_exp: xr.Dataset,
    rho: Optional[xr.DataArray],
    varname: str,
    bin_slice: Union[slice, Tuple[int, Optional[int]]],
    kind: str = "N",
) -> xr.DataArray:
    """Sum a SUM_ variable over bin_slice, convert to display units. Uses tendency_to_rate for SUM_*."""
    data = ds_exp[varname]
    if varname.startswith("SUM_"):
        data = tendency_to_rate(data, time_dim="time")
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
    """Return full bin-resolved rate in display units. Uses tendency_to_rate for SUM_*."""
    data = ds_exp[varname]
    if varname.startswith("SUM_"):
        data = tendency_to_rate(data, time_dim="time")
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
        "processing_commit": commit or "",
        "processing_commit_short": commit[:12] if commit else "",
        "exp_id": int(eid),
        "exp_label": R.get("exp_label") or "",
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


def mirror_immersion_freezing(rate_map: Dict[str, Dict[str, xr.DataArray]]) -> None:
    """Mirror immersion freezing as liquid loss and ice gain."""
    for kind in ("N", "Q"):
        liq_key = f"rates_{kind}_liq"
        ice_key = f"rates_{kind}_ice"
        liq = rate_map.get(liq_key, {})
        ice = rate_map.get(ice_key, {})
        if "IMMERSION_FREEZING" in liq:
            ice["IMMERSION_FREEZING"] = liq["IMMERSION_FREEZING"]
            del liq["IMMERSION_FREEZING"]


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
    unit_N = r"m$^{-3}$ s$^{-1}$"
    unit_Q = r"g m$^{-3}$ s$^{-1}$"
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
        mirror_immersion_freezing(R)
        rates_by_exp[eid] = R
        rates_ds_by_exp[eid] = build_rates_dataset(R, eid, ds_exp=ds_exp, config=config, repo_root=repo_root)
    return rates_by_exp, rates_ds_by_exp


# ── Spectral waterfall data helpers ──────────────────────────────────────────

def _pad_1d(arr: np.ndarray, n: int, fill_value: float = 0.0) -> np.ndarray:
    a = np.asarray(arr)
    return a[:n] if a.size >= n else np.pad(a, (0, n - a.size), constant_values=fill_value)


def _clean_array(arr: np.ndarray) -> np.ndarray:
    return np.nan_to_num(np.asarray(arr, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)


def panel_process_values(
    proc_map: Dict[str, xr.DataArray],
    procs: List[str],
    station_idx: int,
    h0: float,
    h1: float,
    twindow: slice,
    bin_slice: slice,
) -> Dict[str, np.ndarray]:
    """Time-mean spectral values per process over a height/station/time window."""
    if not procs:
        return {}
    stacked = xr.concat([proc_map[p] for p in procs], dim="process")
    stacked = (
        stacked.isel(station=station_idx)
        .sel(height_level=slice(h0, h1))
        .mean(dim="height_level")
        .sel(time=twindow)
    )
    if stacked.sizes.get("time", 0) == 0:
        return {}
    vals = np.asarray(stacked.mean(dim="time").isel(bins=bin_slice).values)
    return {p: vals[i] for i, p in enumerate(procs)}


def panel_concentration_profile(
    spec_conc_da: Optional[xr.DataArray],
    station_idx: int,
    h0: float,
    h1: float,
    twindow: slice,
    bin_slice: slice,
) -> np.ndarray:
    """Time-mean spectral concentration profile over a height/station/time window."""
    if spec_conc_da is None:
        return np.zeros(0)
    
    sliced = (
        spec_conc_da.isel(station=station_idx)
        .sel(height_level=slice(h0, h1))
        .mean(dim="height_level")
        .sel(time=twindow)
    )
    if sliced.sizes.get("time", 0) == 0:
        return np.zeros(0)
    
    vals = np.asarray(sliced.mean(dim="time").isel(bins=bin_slice).values)
    return _clean_array(vals)



def merge_liq_ice_net(
    net_w: Dict[str, np.ndarray],
    net_f: Dict[str, np.ndarray],
    n: int,
    color_fn=None,
) -> Dict[str, Tuple[str, np.ndarray]]:
    """Sum liquid and ice net arrays per process → {proc: (color, net_array)}.

    *color_fn* defaults to ``proc_color`` from style_profiles if not given.
    """
    if color_fn is None:
        from utilities.style_profiles import proc_color
        color_fn = proc_color
    out: Dict[str, Tuple[str, np.ndarray]] = {}
    for p in set(net_w) | set(net_f):
        w = _clean_array(_pad_1d(net_w[p], n)) if p in net_w else np.zeros(n)
        f = _clean_array(_pad_1d(net_f[p], n)) if p in net_f else np.zeros(n)
        out[p] = (color_fn(p), w + f)
    return out


def normalize_net_stacks(
    net_map: Dict[str, Tuple[str, np.ndarray]],
    mode: str,
) -> Dict[str, Tuple[str, np.ndarray]]:
    """Normalize net process arrays: 'none' | 'bin' (per-bin fractions) | 'panel' (max-abs=1)."""
    if mode == "none":
        return net_map
    if mode == "bin":
        pos_sum = np.zeros_like(next(iter(net_map.values()))[1])
        neg_sum = np.zeros_like(pos_sum)
        for _, (_, arr) in net_map.items():
            pos_sum += np.maximum(0.0, arr)
            neg_sum += np.maximum(0.0, -arr)
        out: Dict[str, Tuple[str, np.ndarray]] = {}
        for p, (c, arr) in net_map.items():
            pos_part = np.divide(np.maximum(0.0, arr), pos_sum, out=np.zeros_like(arr), where=pos_sum > 0)
            neg_part = np.divide(np.maximum(0.0, -arr), neg_sum, out=np.zeros_like(arr), where=neg_sum > 0)
            out[p] = (c, pos_part - neg_part)
        return out
    if mode == "panel":
        vmax = max(float(np.nanmax(np.abs(arr))) for _, (_, arr) in net_map.items()) if net_map else 0.0
        if vmax <= 0:
            return net_map
        return {p: (c, arr / vmax) for p, (c, arr) in net_map.items()}
    raise ValueError(f"Unknown normalize mode: {mode}")
