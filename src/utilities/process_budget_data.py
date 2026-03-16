"""
Process budget data loading: discover dataset, load YAML config, build rates.
All loaded state is returned in a single cfg dict with lowercase keys.
"""
import json
import yaml
from pathlib import Path
from typing import Optional, Union
import numpy as np
import xarray as xr

from utilities.process_rates import (
    build_rates_for_experiments,
    build_rates,
    build_spectral_rates,
    build_proc_vars,
    spectral_rate,
)
from polarcap_runtime import is_server


def discover_candidate_datasets(repo_root: Path) -> list[Path]:
    patterns = [
        '**/*meteogram*.zarr',
        '**/*meteogram*.nc',
        '**/*lv3*rates*.zarr',
        '**/*lv3*rates*.nc',
    ]
    cands = []
    for patt in patterns:
        cands.extend(repo_root.glob(patt))
    cands = sorted({p for p in cands if 'archive' not in p.parts})
    return cands


def open_dataset_auto(path: Path) -> xr.Dataset:
    if path.suffix == '.zarr':
        return xr.open_zarr(path)
    return xr.open_dataset(path)


def make_synthetic_rates(n_time=180, n_height=52, n_station=2):
    rng = np.random.default_rng(42)
    procs = [
        'CONDENSATION', 'DROP_COLLISION', 'RIMING', 'MELTING',
        'DEPOSITION', 'IMMERSION_FREEZING',
    ]
    t0 = np.datetime64('2023-01-25T12:00:00')
    time = t0 + np.arange(n_time) * np.timedelta64(10, 's')
    z = np.linspace(400.0, 2600.0, n_height)
    rates = {}
    tt = np.linspace(0.0, 1.0, n_time)[:, None, None]
    zz = np.linspace(0.0, 1.0, n_height)[None, :, None]
    ss = np.linspace(0.9, 1.1, n_station)[None, None, :]
    for k, p in enumerate(procs):
        phase_t = np.sin(2.0 * np.pi * (tt * (1.0 + 0.2 * k) + 0.1 * k))
        phase_z = np.cos(np.pi * (zz * (1.0 + 0.1 * k)))
        core = np.abs(phase_t * phase_z) * (1.0 + 0.25 * k)
        burst = np.exp(-((tt - (0.35 + 0.07 * k)) ** 2) / (0.002 + 0.0008 * k))
        noise = 0.08 * rng.random((n_time, n_height, n_station))
        arr = (core + 0.9 * burst) * ss + noise
        arr = 3e-13 * np.maximum(arr, 0.0)
        rates[p] = xr.DataArray(
            arr,
            dims=('time', 'height_level', 'station'),
            coords={'time': time, 'height_level': z, 'station': np.arange(n_station)},
            name=p,
        )
    return {
        0: {
            'exp_label': 'synthetic_demo',
            'unit_N': r'm$^{-3}$ s$^{-1}$',
            'unit_Q': r'g m$^{-3}$ s$^{-1}$',
            'rates_N_liq': rates,
        }
    }


def _cfg_get(cfg: dict, *keys, default=None):
    cur = cfg
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _slice_from_cfg(entry, default_slice):
    if not isinstance(entry, dict):
        return default_slice
    return slice(entry.get("start", default_slice.start), entry.get("stop", default_slice.stop))


def _chunk_summary(ds: xr.Dataset) -> dict[str, tuple[int, ...]]:
    """Return a compact, logger-safe summary of chunk metadata."""
    if not ds.chunks:
        return {}
    summary = {}
    for dim, spec in ds.chunks.items():
        if isinstance(spec, int):
            summary[dim] = (int(spec),)
            continue
        vals = []
        for chunk in spec[:4]:
            vals.append(int(chunk[0]) if isinstance(chunk, tuple) else int(chunk))
        summary[dim] = tuple(vals)
    return summary


def _rechunk_meteogram_for_env(ds: xr.Dataset, cfg: dict) -> xr.Dataset:
    """Rechunk meteogram dataset for current machine (server vs laptop). Controlled by config zarr.rechunk_on_open; optional zarr.memory_fraction, zarr.max_chunk_mb, zarr.min_chunk_mb, zarr.target_chunk_mb."""
    if not getattr(ds, "chunks", None) or not any(ds.chunks.values()):
        return ds
    from utilities.compute_fabric import auto_chunk_dataset

    target_mb = _cfg_get(cfg, "zarr", "target_chunk_mb", default=None)
    memory_fraction = _cfg_get(cfg, "zarr", "memory_fraction", default=0.12)
    max_chunk_mb = _cfg_get(cfg, "zarr", "max_chunk_mb", default=512)
    min_chunk_mb = _cfg_get(cfg, "zarr", "min_chunk_mb", default=64)
    prefer_dims = ("time", "height_level", "station", "bins", "expname")
    _, chunk_dict = auto_chunk_dataset(
        ds,
        target_chunk_mb=target_mb,
        memory_fraction=memory_fraction,
        max_chunk_mb=max_chunk_mb,
        min_chunk_mb=min_chunk_mb,
        prefer_dims=prefer_dims,
    )
    chunk_dict["expname"] = 1
    return ds.chunk({k: v for k, v in chunk_dict.items() if k in ds.sizes})


def load_process_budget_data(
    repo_root: Path,
    config_path: Optional[Union[str, Path]] = None,
) -> dict:
    """
    Load dataset and build process rates. All state is returned in a single cfg dict with lowercase keys.
    Keys include: ds, rates_by_exp, size_ranges, plot_range_keys, active_range_key, plot_exp_ids,
    plot_stn_ids, exp_idx, stn_idx, seed_start, time_coarsen, rate_floor, station_labels,
    experiment_meta, diameter_um, time_window, height_sel_m.
    """
    cfg = {}
    if config_path:
        cp = Path(config_path)
        if cp.is_file():
            with open(cp, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
        else:
            print(f"Config file not found: {cp}")

    cs_run = _cfg_get(cfg, "ensemble", "cs_run", default="cs-eriswil__20260304_110254")

    if is_server():
        root = Path(_cfg_get(cfg, "paths", "server_root", default=None))
        data_dir = root / "ensemble_output" / cs_run
    else:
        local_meteogram_root = _cfg_get(cfg, "paths", "local_meteogram_root", default=None)
        if local_meteogram_root:
            data_dir = Path(local_meteogram_root).expanduser() / cs_run
        else:
            data_dir = Path.home() / "data" / "cosmo-specs" / "meteograms" / cs_run

    zarr_candidates = sorted(data_dir.glob("Meteogram_*.zarr"))
    zarr_path = zarr_candidates[-1] if zarr_candidates else None
    if not zarr_path:
        cands = discover_candidate_datasets(repo_root)
        if cands:
            zarr_path = cands[-1]

    if zarr_path:
        print(f"Zarr store: {zarr_path}")
        ds = open_dataset_auto(zarr_path)
        if _cfg_get(cfg, "zarr", "rechunk_on_open", default=False):
            ds = _rechunk_meteogram_for_env(ds, cfg)
            if ds.chunks:
                print(f"  rechunk (env): {_chunk_summary(ds)}")
    else:
        raise ValueError(f"No Zarr dataset found for config: {zarr_path}")

    # size ranges (keys stay AERLBB etc. for compatibility with rates_N_liq_AERLBB)
    yaml_sr = _cfg_get(cfg, "size_ranges", default={}) or {}
    aerlbb = _slice_from_cfg(yaml_sr.get("AERLBB", {}), slice(None, 30))
    crybb = _slice_from_cfg(yaml_sr.get("CRYBB", {}), slice(30, 50))
    precbb = _slice_from_cfg(yaml_sr.get("PRECBB", {}), slice(50, None))
    allbb = _slice_from_cfg(yaml_sr.get("ALLBB", {}), slice(None, None))

    size_ranges = {
        "AERLBB": {"slice": aerlbb, "label": yaml_sr.get("AERLBB", {}).get("label", "Range AERLBB"), "tag": yaml_sr.get("AERLBB", {}).get("tag", "aerlbb")},
        "CRYBB": {"slice": crybb, "label": yaml_sr.get("CRYBB", {}).get("label", "Range CRYBB"), "tag": yaml_sr.get("CRYBB", {}).get("tag", "crybb")},
        "PRECBB": {"slice": precbb, "label": yaml_sr.get("PRECBB", {}).get("label", "Range PRECBB"), "tag": yaml_sr.get("PRECBB", {}).get("tag", "precbb")},
        "ALLBB": {"slice": allbb, "label": yaml_sr.get("ALLBB", {}).get("label", "Range ALLBB"), "tag": yaml_sr.get("ALLBB", {}).get("tag", "allbb")},
    }
    cfg["size_ranges"] = size_ranges

    plot_exp_ids = _cfg_get(cfg, "selection", "plot_experiment_ids", default=[1])

    rates_by_exp, _ = build_rates_for_experiments(
        ds=ds,
        exp_ids=plot_exp_ids,
        config=None,
        LBB=aerlbb,
        CBB=crybb,
        repo_root=repo_root,
    )

    for eid in plot_exp_ids:
        ds_exp = ds.isel(expname=eid)
        if "HMLd" in ds_exp.coords:
            ds_exp = ds_exp.assign_coords(height_level=ds_exp.HMLd)
        if "HHLd" in ds_exp.coords:
            ds_exp = ds_exp.assign_coords(height_level2=ds_exp.HHLd)
        rho = ds_exp["RHO"] if "RHO" in ds_exp.data_vars else None
        proc_vars = build_proc_vars(ds_exp)
        for range_key, range_cfg in size_ranges.items():
            b = range_cfg["slice"]
            rates_by_exp[eid][f"rates_N_liq_{range_key}"] = build_rates(ds_exp, rho, proc_vars, "N", b, spectrum="W")
            rates_by_exp[eid][f"rates_Q_liq_{range_key}"] = build_rates(ds_exp, rho, proc_vars, "Q", b, spectrum="W")
            rates_by_exp[eid][f"rates_N_ice_{range_key}"] = build_rates(ds_exp, rho, proc_vars, "N", b, spectrum="F")
            rates_by_exp[eid][f"rates_Q_ice_{range_key}"] = build_rates(ds_exp, rho, proc_vars, "Q", b, spectrum="F")

        sum_vars = sorted(v for v in ds_exp.data_vars if v.startswith("SUM_"))
        proc_vars_pos = {}
        proc_vars_neg = {}
        from utilities.process_rates import classify_tendency
        for sv in sum_vars:
            stripped = sv.replace("SUM_", "")
            if stripped.startswith("P_"):
                info = classify_tendency(sv)
                if info is not None:
                    base, kind, grp, spec = info
                    proc_vars_pos.setdefault(grp, {"N": {"W": [], "F": []}, "Q": {"W": [], "F": []}})
                    proc_vars_pos[grp][kind][spec].append(sv)
            elif stripped.startswith("N_"):
                info = classify_tendency(sv)
                if info is not None:
                    base, kind, grp, spec = info
                    proc_vars_neg.setdefault(grp, {"N": {"W": [], "F": []}, "Q": {"W": [], "F": []}})
                    proc_vars_neg[grp][kind][spec].append(sv)

        def _build_pos_neg(kind, spectrum):
            net = build_spectral_rates(ds_exp, rho, proc_vars, kind, spectrum)
            r_pos, r_neg = {}, {}
            for grp in net:
                n = net[grp]
                pos_v = proc_vars_pos.get(grp, {}).get(kind, {}).get(spectrum, []) if spectrum else []
                neg_v = proc_vars_neg.get(grp, {}).get(kind, {}).get(spectrum, []) if spectrum else []
                if not pos_v:
                    pos_v = proc_vars_pos.get(grp, {}).get(kind, {}).get("W", []) + proc_vars_pos.get(grp, {}).get(kind, {}).get("F", [])
                if not neg_v:
                    neg_v = proc_vars_neg.get(grp, {}).get(kind, {}).get("W", []) + proc_vars_neg.get(grp, {}).get(kind, {}).get("F", [])
                r_pos[grp] = sum(spectral_rate(ds_exp, rho, v, kind) for v in pos_v) if pos_v else xr.where(n > 0, n, 0.0)
                r_neg[grp] = sum(spectral_rate(ds_exp, rho, v, kind) for v in neg_v) if neg_v else xr.where(n < 0, n, 0.0)
            return r_pos, r_neg

        rates_by_exp[eid]["spec_rates_N_W_pos"], rates_by_exp[eid]["spec_rates_N_W_neg"] = _build_pos_neg("N", "W")
        rates_by_exp[eid]["spec_rates_N_F_pos"], rates_by_exp[eid]["spec_rates_N_F_neg"] = _build_pos_neg("N", "F")
        rates_by_exp[eid]["spec_rates_Q_W_pos"], rates_by_exp[eid]["spec_rates_Q_W_neg"] = _build_pos_neg("Q", "W")
        rates_by_exp[eid]["spec_rates_Q_F_pos"], rates_by_exp[eid]["spec_rates_Q_F_neg"] = _build_pos_neg("Q", "F")

        # Spectral concentrations
        if rho is not None:
            if "NW" in ds_exp.data_vars:
                rates_by_exp[eid]["spec_conc_N_W"] = ds_exp["NW"] * rho
            if "NF" in ds_exp.data_vars:
                rates_by_exp[eid]["spec_conc_N_F"] = ds_exp["NF"] * rho
            if "QW" in ds_exp.data_vars:
                rates_by_exp[eid]["spec_conc_Q_W"] = ds_exp["QW"] * rho * 1000.0
            if "QF" in ds_exp.data_vars:
                rates_by_exp[eid]["spec_conc_Q_F"] = ds_exp["QF"] * rho * 1000.0

    _raw_expnames = [v.decode() if isinstance(v, bytes) else str(v) for v in ds.expname.values]
    if is_server():
        _config_dir = root / "ensemble_output"
    else:
        _cfg_root = _cfg_get(cfg, "paths", "local_ensemble_config_root", default=None)
        _config_dir = Path(_cfg_root).expanduser() if _cfg_root else Path.home() / "data" / "cosmo-specs" / "polarcap_analysis" / "data" / "ensemble_output"
    config_json = _config_dir / f"{cs_run}.json"

    experiment_meta = []
    if config_json.is_file():
        with open(config_json) as _f:
            _cfg_json = json.load(_f)
        for ename in _raw_expnames:
            entry = _cfg_json.get(ename, {})
            sbm = entry.get("INPUT_ORG", {}).get("sbm_par", {})
            flare = entry.get("INPUT_ORG", {}).get("flare_sbm", {})
            lflare = sbm.get("lflare", False)
            emission = flare.get("flare_emission", 0.0)
            ishape = sbm.get("ishape", -1)
            ikeis = sbm.get("ikeis", -1)
            is_ref = (not lflare) or emission == 0
            label = f"REF ishape={ishape}" if is_ref else f"EMIS {emission:.0e} ishape={ishape}"
            experiment_meta.append({
                "expname": ename, "lflare": lflare, "emission": emission,
                "ishape": ishape, "ikeis": ikeis, "label": label, "is_reference": is_ref,
            })
    else:
        experiment_meta = [{"expname": e, "lflare": False, "emission": 0, "ishape": -1, "ikeis": -1, "label": e, "is_reference": False} for e in _raw_expnames]
    cfg["experiment_meta"] = experiment_meta

    cfg["ds"] = ds
    cfg["rates_by_exp"] = rates_by_exp
    _apply_cfg_defaults(cfg)

    from utilities.meteogram_io import _compute_bin_coords
    if ds is not None and "bins" in ds.sizes:
        m_edges, m_cen, r_edges, r_cen = _compute_bin_coords(n_bins=ds.sizes["bins"])
        cfg["diameter_um"] = r_cen * 2e6
    else:
        cfg["diameter_um"] = None

    return cfg


def _apply_cfg_defaults(cfg: dict) -> None:
    """Flatten selection/plotting/time into lowercase top-level keys. station_labels: selection.station_labels, else stations.labels."""
    cfg.setdefault("plot_range_keys", _cfg_get(cfg, "plotting", "plot_range_keys", default=["ALLBB"]))
    cfg.setdefault("active_range_key", _cfg_get(cfg, "plotting", "active_range_key", default="ALLBB"))
    cfg.setdefault("plot_exp_ids", _cfg_get(cfg, "selection", "plot_experiment_ids", default=[1]))
    cfg.setdefault("plot_stn_ids", _cfg_get(cfg, "selection", "plot_station_ids", default=[0, 1, 2]))
    cfg.setdefault("exp_idx", _cfg_get(cfg, "selection", "experiment_index_default", default=1))
    cfg.setdefault("stn_idx", _cfg_get(cfg, "selection", "station_index_default", default=0))
    seed_utc = _cfg_get(cfg, "time", "seed_start", default=_cfg_get(cfg, "time", "seed_start_utc", default="2023-01-25T12:30:00"))
    cfg.setdefault("seed_start", np.datetime64(seed_utc))
    cfg.setdefault("time_coarsen", _cfg_get(cfg, "time", "coarsen", default="30s"))
    cfg.setdefault("rate_floor", float(_cfg_get(cfg, "time", "rate_floor", default=1e-18)))
    cfg.setdefault(
        "station_labels",
        _cfg_get(cfg, "selection", "station_labels") or _cfg_get(cfg, "stations", "labels", default={0: "S1", 1: "S2", 2: "S3"}),
    )
    cfg.setdefault("height_sel_m", _cfg_get(cfg, "plotting", "height_sel_m", default=[1400, 1200, 1000, 800]))
    offsets_min = _cfg_get(cfg, "time", "window_offsets_min", default=[0.0, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5])
    cfg.setdefault("time_window", [cfg["seed_start"] + np.timedelta64(int(t * 60), "s") for t in offsets_min])


def stn_label(si: int, station_labels: dict) -> str:
    """Return display label for station index."""
    return station_labels.get(si, f'Station {si + 1}')


def select_rates_for_range(r: dict, range_key: str) -> dict:
    """Return a copy of r with base keys mapped to range_key versions."""
    rsel = dict(r)
    rsel["rates_N_liq"] = r[f"rates_N_liq_{range_key}"]
    rsel["rates_Q_liq"] = r[f"rates_Q_liq_{range_key}"]
    rsel["rates_N_ice"] = r[f"rates_N_ice_{range_key}"]
    rsel["rates_Q_ice"] = r[f"rates_Q_ice_{range_key}"]
    return rsel
