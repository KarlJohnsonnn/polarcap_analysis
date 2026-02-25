# /work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/eriswil/python_util/fast_quicklooks/init_common.py
from __future__ import annotations

import os, re, json
from dataclasses import dataclass
from typing import Dict, Tuple, List

import numpy as np
import glob


@dataclass(frozen=True)
class InitContext:
    cs_run: str
    root_dir: str
    data_dir: str
    obs_dir: str
    png_dir: str
    meta_file: str
    cfg: dict
    expnames: List[str]
    domain: str
    station_coords: Dict[str, Tuple[float, float]]
    extpar_file: str

def get_station_coords_from_cfg(meta_file: str) -> Dict[str, Tuple[float, float]]:
    if not os.path.exists(meta_file):
        raise FileNotFoundError(f"Meta file {meta_file} not found!")
    with open(meta_file, "r") as f:
        cfg_dict = json.load(f)
    expname = next(iter(cfg_dict))
    stationlist = cfg_dict[expname]["INPUT_DIA"]["diactl"]["stationlist_tot"]
    if not stationlist:
        raise ValueError("Station list is empty!")
    arr = np.array(stationlist).reshape(-1, 5)
    out: Dict[str, Tuple[float, float]] = {}
    for row in arr:
        station_id = str(row[-1].split("_")[0])
        if station_id in ("SE", "OB"):
            continue
        out[station_id] = (float(row[2]), float(row[3]))
    return out


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def print_flare_table(cfg: dict) -> None:
    print("\nTable of flare emissions:")
    print("expname:           fe_per_gridcell:           fe_per_m3:           fe_per_l:")
    for key, val in cfg.items():
        
        flare_height = val.get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_height", None)
        fe = val.get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_emission", None)
        height_res = -np.diff(np.array(val.get('model_height', None)))
        height_res = height_res[-flare_height]
        
        # grid_dx, grid_dy = get_grid_cell_sizes(lat_2D_extpar, lon_2D_extpar)
        # Vcell = grid_dx * grid_dy * height_res # flire height might be wrong cause of slicing
                    
        # return (fe,                  # 1/gridcell/s
        #         fe / Vcell,          # Convert to 1/m3/s
        #         fe / (Vcell * 1e3),  # Convert to 1/L/s
        # )
        fe = val.get("INPUT_ORG", {}).get("flare_sbm", {}).get("flare_emission", None)
        if fe is None:
            print(f"   {key:10s}   missing")
        else:
            print(f"   {key:10s}   {fe:16.8e} (1/gridcell/s)")


def init_analysis(
    cs_run: str = "",
    root_dir: str = "/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/eriswil",
    obs_dir: str = "/work/bb1262/user/schimmel/cloudlab_data/",
    plots_subdir: str = "plots/",
    plot_dir_name_template: str = "mean-max_stations_height_data_{cs_run}",
    make_plots_dir: bool = True,
    verbose: bool = True,
) -> InitContext:
    
    if cs_run == "":
        print("no cs_run provided, using default: cs-eriswil__20250629_001720")
        cs_run = "cs-eriswil__20250629_001720"
    print(f'init_analysis(cs_run={cs_run})')
    
    if not re.match(r"^cs-eriswil__\d{8}_\d{6}$", cs_run):
        raise ValueError(f"Invalid cs_run: {cs_run}")
    
    data_dir = f"{root_dir}/ensemble_output/{cs_run}"
    obs_dir = f"{obs_dir}/holimo"
    meta_file = f"{data_dir}/{cs_run}.json"
    
    if not os.path.isfile(meta_file):
        raise FileNotFoundError(f"Meta file not found: {meta_file}")

    with open(meta_file, "r") as f:
        cfg = json.load(f)

    expnames = list(cfg.keys())
    try:
        domain_full = str(cfg[expnames[0]]["domain"])
        domain = "x".join(domain_full.split("x")[:2])  # e.g., "400x400"
    except Exception:
        domain = ""
        
    extpar_file = glob.glob(f"{root_dir}/COS_in/extPar_Eriswil_{domain}.nc")[0]
    if not os.path.isfile(extpar_file):
        raise FileNotFoundError(f"Extpar file not found: {extpar_file}")

    png_dirname = plot_dir_name_template.format(cs_run=cs_run, domain=domain)
    png_dir = f"{root_dir}/{plots_subdir}/{png_dirname}"
    if make_plots_dir:
        _ensure_dir(png_dir)

    station_coords = get_station_coords_from_cfg(meta_file)


    return InitContext(
        cs_run=cs_run,
        root_dir=root_dir,
        data_dir=data_dir,
        obs_dir=obs_dir,
        png_dir=png_dir,
        meta_file=meta_file,
        cfg=cfg,
        expnames=expnames,
        domain=domain,
        station_coords=station_coords,
        extpar_file=extpar_file,
    )