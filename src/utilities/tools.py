import os
import sys
import os.path as osp
sys.path.append('/work/bb1262/user/schimmel/cosmo-specs-torch/PaperCode/polarcap1/utils')
import datetime
import time
import glob
#import argparse
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patheffects as PathEffects
# The code is attempting to import a module named `colormaps` and aliasing it as `cmaps`. This module
# likely contains predefined color maps that can be used for visualizations in Python. However, please
# note that the `colormaps` module is not a standard Python library, so you may need to install it
# separately before running this code.
import colormaps as cmaps 
from PIL import Image
from typing import Literal
import json
import pandas as pd
import numpy as np
import xarray as xr
xr.set_options(keep_attrs=True)
import gc
import tobac
import dask
from pathlib import Path
import re
import logging


from scipy import ndimage, stats

sys.path.append(os.path.abspath(os.getcwd()))

import utilities.namelist_metadata as nml

# Activate Dask progress bar (optional), this becomes very useful to visualize when xarray is loading data into memory

HHL_100 = np.array(
    [21750.     , 21250.     , 20757.139  , 20271.424  , 19792.77   ,
    19321.164  , 18856.52   , 18398.84   , 17948.035  , 17504.105  ,
    17066.959  , 16636.604  , 16212.949  , 15795.984  , 15385.625  ,
    14981.875  , 14584.639  , 14193.924  , 13809.645  , 13431.789  ,
    13060.27   , 12695.09   , 12336.16   , 11983.479  , 11636.957  ,
    11305.816  , 10993.863  , 10691.877  , 10395.394  , 10104.415  ,
    9818.855  ,  9538.722  ,  9263.934  ,  8994.48   ,  8730.282  ,
    8471.342  ,  8217.576  ,  7968.985  ,  7725.489  ,  7487.091  ,
    7253.7065 ,  7025.327  ,  6801.875  ,  6583.3545 ,  6369.6763 ,
    6160.847  ,  5956.7905 ,  5757.4966 ,  5562.88   ,  5372.9463 ,
    5187.612  ,  5006.8774 ,  4830.6626 ,  4658.971  ,  4491.7188 ,
    4328.8955 ,  4170.424  ,  4016.3093 ,  3866.4639 ,  3720.892  ,
    3579.5159 ,  3442.3264 ,  3309.2402 ,  3180.2622 ,  3055.3098 ,
    2934.3826 ,  2817.398  ,  2704.3606 ,  2595.1875 ,  2489.8696 ,
    2388.3289 ,  2290.5696 ,  2196.5044 ,  2106.138  ,  2019.3873 ,
    1936.2528 ,  1856.6514 ,  1780.5786 ,  1707.956  ,  1638.7838 ,
    1572.9791 ,  1510.5466 ,  1451.3988 ,  1395.5356 ,  1342.879  ,
    1293.4243 ,  1247.0886 ,  1203.8766 ,  1163.7102 ,  1126.5802 ,
    1092.4036 ,  1061.1849 ,  1032.8417 ,  1007.3736 ,   984.69806,
    964.8196 ,   947.65533,   933.19617,   921.36383,   912.16296 ]
    )*1.0e-3

RGRENZ = np.array(
    [1.0109210e-09, 1.25992106e-09, 1.58740110e-09, 1.99999994e-09, 2.51984211e-09,
    3.17480220e-09, 3.99999989e-09, 5.03968423e-09, 6.34960440e-09,
    7.99999977e-09, 1.00793685e-08, 1.26992088e-08, 1.59999995e-08,
    2.01587369e-08, 2.53984176e-08, 3.19999991e-08, 4.03174738e-08,
    5.07968352e-08, 6.39999982e-08, 8.06349476e-08, 1.01593670e-07,
    1.27999996e-07, 1.61269895e-07, 2.03187341e-07, 2.55999993e-07,
    3.22539790e-07, 4.06374681e-07, 5.11999986e-07, 6.45079581e-07,
    8.12749363e-07, 1.02399997e-06, 1.29015916e-06, 1.62549873e-06,
    2.04799994e-06, 2.58031832e-06, 3.25099745e-06, 4.09599988e-06,
    5.16063665e-06, 6.50199490e-06, 8.19199977e-06, 1.03212733e-05,
    1.30039898e-05, 1.63839995e-05, 2.06425466e-05, 2.60079796e-05,
    3.27679991e-05, 4.12850932e-05, 5.20159592e-05, 6.55359981e-05,
    8.25701864e-05, 1.04031918e-04, 1.31071996e-04, 1.65140373e-04,
    2.08063837e-04, 2.62143993e-04, 3.30280745e-04, 4.16127674e-04,
    5.24287985e-04, 6.60561491e-04, 8.32255348e-04, 1.04857597e-03,
    1.32112298e-03, 1.66451070e-03, 2.09715194e-03, 2.64224596e-03,
    3.32902139e-03, 4.19430388e-03]
)


specs_variable_metadata = {
    "variables": [
        {"name": "t", "units": "K", "long_name": "Temperature"},
        {"name": "rho", "units": "kg/m^3", "long_name": "air density"},
        {"name": "qv", "units": "kg/kg", "long_name": "humidity mixing ratio (MR)"},
        {"name": "ut", "units": "unknown", "long_name": "u wind component"},
        {"name": "vt", "units": "unknown", "long_name": "v wind component"},
        {"name": "wt", "units": "unknown", "long_name": "wwind component"},
        {"name": "qc", "units": "kg/kg", "long_name": "cloud liquid water MR"},
        {"name": "qr", "units": "kg/kg", "long_name": "rain water MR"},
        {"name": "qi", "units": "kg/kg", "long_name": "ice water MR"},
        {"name": "qs", "units": "kg/kg", "long_name": "snow water MR"},
        {"name": "nw", "units": "#/kg", "long_name": "liquid droplet number concentration"},
        {"name": "qw", "units": "kg/kg", "long_name": "water mass in liquid droplets MR"},
        {"name": "qws", "units": "kg/kg", "long_name": "soluble aerosol mass in liquid droplets MR"},
        {"name": "qwa", "units": "kg/kg", "long_name": "total aerosol mass in liquid droplets MR"},
        {"name": "nf", "units": "#/kg", "long_name": "mixed-phase droplet number concentration"},
        {"name": "qf", "units": "kg/kg", "long_name": "frozen water mass in mixed-phase droplets MR"},
        {"name": "qfs", "units": "kg/kg", "long_name": "soluble aerosol mass in mixed-phase droplets MR"},
        {"name": "qfa", "units": "kg/kg", "long_name": "total aerosol mass in mixed-phase droplets MR"},
        {"name": "qfw", "units": "kg/kg", "long_name": "liquid water mass in mixed-phase droplets MR"},
        {"name": "ni", "units": "#/kg", "long_name": "insoluble aerosol particles number concentration"},
        {"name": "qia", "units": "kg/kg", "long_name": "insoluble aerosol mass in insoluble aerosol particles MR"}
    ]
}

# for time-height plots
def create_fade_cmap(pyplot_cmap, n_fade=32):
    """Create colormap with fade effect"""
    # if n_fade < 2:
    #     n_fade = 2
    #     print(f'n_fade must be at least 2, setting to 2')
        
    fcolor = pyplot_cmap(0.0)
    fade_colors = np.ones((n_fade, 4))
    fade_colors[:, 3] = np.linspace(0, 1, n_fade)[::-1]
    for i in range(3):
        fade_colors[:, i] = np.linspace(fcolor[i], 1.0, n_fade)
    return mcolors.ListedColormap(np.vstack((fade_colors[::-1], pyplot_cmap(np.linspace(0, 1, 128)))))

def create_new_jet(n_colors=128):
    # for time-height plots
    cmap_new_timeheight_np = np.vstack([
        cmaps.matter(np.linspace(0,1,n_colors)[::-1]), 
        cmaps.haline(np.linspace(0,1,n_colors)[::-1])])
    return mcolors.ListedColormap(cmap_new_timeheight_np[::-1])

# for time-height plots
cmap_new_timeheight_np = np.vstack([
    cmaps.matter(np.linspace(0,1,128)[::-1]), 
    cmaps.haline(np.linspace(0,1,128)[::-1])])
# legacy name
cmap_new_timeheight_nofade = mcolors.ListedColormap(cmap_new_timeheight_np[::-1])
cmap_new_timeheight = create_fade_cmap(cmap_new_timeheight_nofade, 32)

# new name
jet2 = mcolors.ListedColormap(cmap_new_timeheight_np[::-1])
jet2_fade = create_fade_cmap(jet2, 16)


    
cmap_jet2 = mcolors.LinearSegmentedColormap.from_list(
    'mycmap', [
        (0.0, 'lightgrey'), 
        (0.2, 'blue'), 
        (0.4, 'cyan'), 
        (0.5, 'lime'), 
        (0.6, 'yellow'), 
        (0.9, 'red'), 
        (1.0, 'purple')
        ]
    ) 
# colormap for ensemble lines in bulk and spectra plots
cmap_ensemble_lines_np = np.vstack([
    cmaps.matter(np.linspace(0,1,128)[-40::-1]), 
    cmaps.haline(np.linspace(0,1,128)[:60:-1])])

cmap_ensemble_lines = mcolors.ListedColormap(cmap_ensemble_lines_np)
cmap_ensemble_lines_r = mcolors.ListedColormap(cmap_ensemble_lines_np[::-1]) 



ldr_colors = (
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),

    (0.0, 0.0, 0.3),
    (0.0, 0.0, 0.3),
    (0.0, 0.0, 0.3),
    (0.0, 0.0, 0.3),

    (0.0, 0.7, 1.0),
    (0.0, 0.7, 1.0),
    (0.0, 0.7, 1.0),

    (0.0, 0.9, 0.0),
    (0.0, 0.9, 0.0),
    (0.0, 0.9, 0.0),
    (0.0, 0.9, 0.0),

    (1.0, 0.8, 0.0),
    (1.0, 0.8, 0.0),
    (1.0, 0.8, 0.0),
    (1.0, 0.8, 0.0),

    (1.0, 0.0, 0.0),
    (1.0, 0.0, 0.0),
    (1.0, 0.0, 0.0),
    (1.0, 0.0, 0.0),

    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6),
    (0.6, 0.6, 0.6)
)

ldr_map = mcolors.ListedColormap(ldr_colors, "LDR")


# linear depolarisation ratio plot
colors1 = plt.cm.binary(np.linspace(0.5, 0.5, 1))
colors2 = plt.cm.jet(np.linspace(0, 0, 178))
colors3 = plt.cm.jet(np.linspace(0, 1, 77))
colors = np.vstack((colors1, colors2, colors3))
ldr_map_2 = mcolors.LinearSegmentedColormap.from_list('ldr_map_2', colors)



# Define the original colormap and create modified versions
n = 512  # Number of color samples
midpoint = n // 2
cmap = plt.cm.coolwarm
colors = cmap(np.linspace(0, 1, n))

# Create white transition in the middle
cmap_bwr = mcolors.LinearSegmentedColormap.from_list("bw", [colors[midpoint-1], 'white', colors[midpoint+1]])
colors_bwr = cmap_bwr(np.linspace(0, 1, 16))
colors = np.vstack([colors[:midpoint], colors_bwr, colors[midpoint:]])
midpoint = len(colors) // 2

# Create the final colormaps
cmap_coolwarm_new = mcolors.LinearSegmentedColormap.from_list('coolwarm_new', colors)
cmap_cw = mcolors.LinearSegmentedColormap.from_list('cmap_coolwarm', colors)
cmap_lcw = mcolors.LinearSegmentedColormap.from_list('cmap_lcw', colors[:midpoint])
cmap_ucw = mcolors.LinearSegmentedColormap.from_list('cmap_ucw', colors[midpoint:])

cmap_lower_coolwarm = cmap_lcw
cmap_upper_coolwarm = cmap_ucw
cmap_lower_coolwarm_fade = create_fade_cmap(cmap_lcw, 16)
cmap_upper_coolwarm_fade = create_fade_cmap(cmap_ucw, 16)





def save_fig(fig, png_filename, dpi=300):
    os.makedirs(os.path.dirname(png_filename), exist_ok=True)
    fig.savefig(png_filename, dpi=dpi, bbox_inches='tight')
    print(f'        \nSaved {os.path.abspath(png_filename)}\n')


def define_bin_boundaries():

    """Define bin boundaries from bin edges. Radius of computes in units of m """
    n_bins = 67
    nmax = 2 # controls mass ratio between adjacent bins
    r_min = 1.0e-9  # 1 nm
    rhow = 1.0e3  # kg/m^3
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min**3
    j0w = (nmax - 1.0) / np.log(2.0)
    MGRENZ = m0w * np.exp(np.arange(n_bins) / j0w)
    RGRENZ = np.cbrt(MGRENZ / fact)
    return RGRENZ

def rename_variables(data_3D):
    """Rename variables according to Meteogram output convention."""
    for iens in data_3D.keys():
        for var in list(data_3D[iens].variables):
            if var.startswith('d') and var.endswith(('_sum', '_sum_p', '_sum_n')):
                prefix = var[var.rfind('_sum'):]
                new_var = prefix.upper()[1:] + '_' + var[1:var.rfind('_sum')].upper()
                data_3D[iens] = data_3D[iens].rename({var: new_var})
    return data_3D


def format_dataset(data, dtime, lon000, lat000, height, diameter_µm):
    """Format individual dataset with coordinates."""
    return data.assign_coords({
        "time": ("time", dtime),
        "x": ("x", lon000),
        "y": ("y", lat000),
        "z": ("z", height),
        "diameter": ("bin", (diameter_µm[1:]+diameter_µm[:-1])/2.0),
        "diameter_bins": ("bins", diameter_µm),
    })


def save_component(data, output_path, ensemble, filename):
    """Save a data component with proper metadata."""
    # Setup efficient encoding for each variable
    encoding = {
        var: {
            'zlib': True,
            'complevel': 5,
            'shuffle': True,
            'chunksizes': data[var].chunks,  # Use same chunks as input
            'dtype': data[var].dtype,  # Preserve dtype
            '_FillValue': 0.0,  # Match SPECS output convention
        }
        for var in data.data_vars
    }
    
    t0 = time.time()
    # rename coordinates
    data = data.rename({'x': 'lon', 'y': 'lat', 'z': 'height'})
    
    # Add coordinate attributes
    data.lon.attrs.update({
        'long_name': 'Longitude',
        'units': 'degrees_east',
        '_FillValue': 0.0
    })
    data.lat.attrs.update({
        'long_name': 'Latitude',
        'units': 'degrees_north',
        '_FillValue': 0.0
    })
    data.height.attrs.update({
        'long_name': 'Height above ground',
        'units': 'km',
        '_FillValue': 0.0
    })
    data.diameter.attrs.update({
        'long_name': 'Particle diameter',
        'units': 'µm',
        '_FillValue': 0.0
    })

    # Add variable attributes based on meteogram_namelist
    for var in data.data_vars:
        # Check if variable exists in any of the attribute dictionaries
        if var in nml.nml_attrs_meteogram_net:
            attrs = nml.nml_attrs_meteogram_net[var]
        elif var in nml.nml_attrs_meteogram_pos:
            attrs = nml.nml_attrs_meteogram_pos[var]
        elif var in nml.nml_attrs_meteogram_neg:
            attrs = nml.nml_attrs_meteogram_neg[var]
        else:
            # Default attributes for variables not in meteogram_namelist
            attrs = {
                'units': get_default_units(var),
                '_FillValue': 0.0
            }
        
        data[var].attrs.update(attrs)
    
    output_filepath = os.path.join(output_path, ensemble, filename)
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

    print(f"Saving {list(data.data_vars)} to: {output_filepath}")
    data.to_netcdf(
        output_filepath,
        encoding=encoding,
        mode="w",
        compute=False,
        engine='netcdf4'
    )
    print(f"Done saving {output_filepath}    took: {time.time() - t0:.2f} seconds")


def get_default_units(var):
    """Return default units for common SPECS variables."""
    units_map = {
        't': 'K',                    # Temperature
        'qv': 'kg/kg',              # Humidity mixing ratio
        'ut': 'm/s',                # Wind components
        'vt': 'm/s',
        'wt': 'm/s',
        'qc': 'kg/kg',              # Cloud liquid water mixing ratio
        'qr': 'kg/kg',              # Rain water mixing ratio
        'qi': 'kg/kg',              # Ice water mixing ratio
        'qs': 'kg/kg',              # Snow water mixing ratio
        'nw': '#/kg',               # Liquid droplet number concentration
        'qw': 'kg/kg',              # Water mass in liquid droplets
        'qws': 'kg/kg',             # Soluble aerosol mass in liquid droplets
        'qwa': 'kg/kg',             # Insoluble aerosol mass in liquid droplets
        'nf': '#/kg',               # Mixed-phase droplet number concentration
        'qf': 'kg/kg',              # Frozen water mass in mixed-phase droplets
        'qfs': 'kg/kg',             # Soluble aerosol mass in mixed-phase droplets
        'qfa': 'kg/kg',             # Insoluble aerosol mass in mixed-phase droplets
        'qfw': 'kg/kg',             # Liquid water mass in mixed-phase droplets
        'ni': '#/kg',               # Insoluble aerosol particles number concentration
        'qia': 'kg/kg',             # Insoluble aerosol mass in aerosol particles
    }
    return units_map.get(var, '')


def get_variable_subset(add_vars, cell_ds, all_variable_names=None) -> list[str]:
    """Get the appropriate variable subset based on the add_vars parameter."""
    if add_vars not in [
        'fluxes_total_mixing', 'fluxes_positive_mixing', 'fluxes_negative_mixing',
        'fluxes_total_numbers', 'fluxes_positive_numbers', 'fluxes_negative_numbers',
        'numbers', 'mixing', 'all'
    ]:
        raise ValueError(f"Invalid add_vars value: {add_vars}")
    

    if all_variable_names is None:
        all_variable_names = cell_ds.data_vars
    
    if 'fluxes' in add_vars:
        if 'numbers' in add_vars:
            all_variable_names = nml.N_LIST_ALL
        elif 'mixing' in add_vars:
            all_variable_names = nml.Q_LIST_ALL

        if 'total' in add_vars:
            var_name_subset = [v for v in all_variable_names if 'SUM_' in v]
        elif 'positive' in add_vars:
            var_name_subset = [v.replace('SUM_', 'SUM_P_') for v in all_variable_names if 'SUM_' in v]
        elif 'negative' in add_vars:
            var_name_subset = [v.replace('SUM_', 'SUM_N_') for v in all_variable_names if 'SUM_' in v]
    
    elif add_vars == 'numbers':
        var_name_subset = ['nw', 'nf', 'ni']
    
    elif add_vars == 'mixing':
        var_name_subset = ['qw', 'qf', 'qi', 'qfw', 'qwa', 'qfs', 'qfa', 'qia']
    
    elif add_vars == 'all':
        # Combine all possible variables
        var_name_subset = []
        # Add number variables
        var_name_subset.extend(['nw', 'nf', 'ni'])
        # Add mixing variables
        var_name_subset.extend(['qw', 'qf', 'qi', 'qfw', 'qwa', 'qfs', 'qfa', 'qia'])
        # Add flux variables
        for flux_type in ['SUM_', 'SUM_P_', 'SUM_N_']:
            var_name_subset.extend([
                flux_type + v[4:] if v.startswith('SUM_') else flux_type + v 
                for v in nml.Q_LIST_ALL + nml.N_LIST_ALL
            ])
    
    # Filter to only include variables that exist in the dataset
    return [v for v in var_name_subset if v in cell_ds.data_vars]


def time2delta_t(time_array):
    return np.mean(np.diff(time_array.astype('datetime64[ns]')).astype(float)) * 1e-9


def monitor_computation(delayed_obj, output_path=None, icell=None):
    """Monitor and visualize Dask computation.
    
    Args:
        delayed_obj: Dask delayed object to compute
        output_path: Optional path to save visualization HTML
        icell: Optional cell number to save visualization HTML
    """
    from dask.diagnostics import Profiler, ResourceProfiler, ProgressBar
    try:
        with Profiler() as prof, ResourceProfiler() as rprof, ProgressBar().register():
            result = delayed_obj.compute()

        
        if output_path:
            # Save raw profiling data instead of visualization
            import json
            profile_data = {
                'tasks': [
                    {
                        'key': task.key,
                        'start': task.start,
                        'end': task.end,
                        'worker': task.worker
                    }
                    for task in prof.results
                ],
                'resources': [
                    {
                        'time': point.time,
                        'mem': point.mem,
                        'cpu': point.cpu
                    }
                    for point in rprof.results
                ]
            }
            
            json_path = os.path.join(output_path, f'dask-profile-icell{icell}.json')
            with open(json_path, 'w') as f:
                json.dump(profile_data, f)
            print(f"Profile data saved to: {json_path}")

            png_path = json_path.replace('.json', f'_visualization_icell{icell}.png')
            visualize_profile(json_path)
            print(f'Profile visualization saved: {png_path}')

            return result
        else:
            print('Warning: no output path provided')
            return None
        
    except Exception as e:
        print(f"Warning: monitor_computation failed with error: {e}")
        return None
        

def visualize_profile(json_path):
    """
    Visualize Dask profiling data from JSON file.
    
    Args:
        json_path (str): Path to the JSON file containing profiling data
    """
    # Load the profile data
    with open(json_path, 'r') as f:
        profile_data = json.load(f)
    
    resources = profile_data['resources']
    
    # Extract time series data
    times = np.array([r['time'] for r in resources])
    memory = np.array([r['mem'] for r in resources])  # Memory in MB
    cpu = np.array([r['cpu'] for r in resources])     # CPU percentage
    
    # Convert absolute timestamps to relative times in seconds
    times = times - times[0]
    
    # Create the visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    
    # Plot memory usage
    ax1.plot(times, memory, 'b-', label='Memory Usage')
    ax1.set_ylabel('Memory (MB)')
    ax1.set_title('Resource Usage Over Time')
    ax1.grid(True)
    ax1.legend()
    
    # Plot CPU usage
    ax2.plot(times, cpu, 'r-', label='CPU Usage')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('CPU (%)')
    ax2.grid(True)
    ax2.legend()
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = json_path.replace('.json', '_visualization.png')
    plt.savefig(plot_path)
    print(f"Visualization saved to: {plot_path}")
    
    # Print some statistics
    print("\nResource Usage Statistics:")
    print(f"Duration: {times[-1]:.2f} seconds")
    print(f"Peak Memory: {np.max(memory):.2f} MB")
    print(f"Average Memory: {np.mean(memory):.2f} MB")
    print(f"Peak CPU: {np.max(cpu):.2f}%")
    print(f"Average CPU: {np.mean(cpu):.2f}%")


def process_cell(cell_ds, root_path, ensemble, ensemble_tag, icell, add_vars='fluxes_total', debug=True, chunk_size=100):
    """Process a single cell and calculate fluxes efficiently using lazy loading.
    
    Args:
        args (tuple): Contains cell_ds, all_variable_names, output_path, ensemble, ensemble_tag, icell
        add_vars (str): Which flux variables to process
        debug (bool): Whether to print debug information and timings
        
    Returns:
        xarray.Dataset: Computed fluxes
    """

    base_filename = f'{ensemble}_{ensemble_tag}_cell{icell}' if ensemble else f'cell{icell}'

    cell_ds = cell_ds.chunk({'z': chunk_size})
    delta_t = time2delta_t(cell_ds.time.values)

    if debug:
        print(f"\nProcessing cell {icell}")
        print(f"Initial dataset size: {cell_ds.nbytes / 1e6:.2f} MB")


    if 'fluxes' in add_vars:
        delta_height = -cell_ds.z.diff('z') * 1000
        data_spectro = (cell_ds.diff('time', n=1) / delta_t * cell_ds['rho'] * 1.0e-3) * delta_height
        
    elif add_vars in ['numbers', 'mixing']:
        data_spectro = cell_ds * cell_ds['rho'] * 1.0e-3
        
    elif add_vars == 'all':
        # Pre-calculate common factors
        delta_height = -cell_ds.z.diff('z') * 1000
        density_factor = cell_ds['rho'] * 1.0e-3
        flux_proc = lambda x: (x.diff('time', n=1) / delta_t * density_factor) * delta_height
        basic_proc = lambda x: x * density_factor
        
        # Define all variable groups
        groups = {
            'flux_number': (nml.N_LIST_ALL, flux_proc),
            'flux_mixing': (nml.Q_LIST_ALL, flux_proc),
            'flux_number_pos': ([v.replace('SUM_', 'SUM_P_') for v in nml.N_LIST_ALL], flux_proc),
            'flux_mixing_pos': ([v.replace('SUM_', 'SUM_P_') for v in nml.Q_LIST_ALL], flux_proc),
            'flux_number_neg': ([v.replace('SUM_', 'SUM_N_') for v in nml.N_LIST_ALL], flux_proc),
            'flux_mixing_neg': ([v.replace('SUM_', 'SUM_N_') for v in nml.Q_LIST_ALL], flux_proc),
            'numbers': (['nw', 'nf', 'ni'], basic_proc),
            'mixing': (['qw', 'qf', 'qi', 'qfw', 'qwa', 'qfs', 'qfa', 'qia'], basic_proc)
        }

        # Process each group of variables
        processed_datasets = []
        for vars, proc in groups.values():
            existing_vars = [v for v in vars if v in cell_ds]
            subset = cell_ds[existing_vars]
            processed_datasets.append(proc(subset))
            
        data_spectro = xr.merge(processed_datasets)
        
    data_spectro = monitor_computation(data_spectro, os.path.join(root_path, ensemble), icell)
    save_component(data_spectro, root_path, ensemble, f"{base_filename}_{add_vars}.nc")
    gc.collect()
    
    return data_spectro


def track_plumes(tobac_input, delta_x, delta_y, delta_t, threshold=1e-7):
    """Track plumes using tobac."""
    
    parameters_features = {
        "position_threshold": "center",
        "threshold": threshold,
        "n_min_threshold": 0
    }
    
    statistics = {
        "mean_qi": np.mean,
        "total_qi": np.sum,
        "max_qi": np.max,
        "percentiles": (np.percentile, {"q": [95, 99]})
    }
    
    #dxy, dt = tobac.get_spacings(tobac_input, grid_spacing=max(delta_x, delta_y), time_spacing=delta_t)
    dxy, dt = max(delta_x, delta_y), delta_t
    features = tobac.feature_detection_multithreshold(tobac_input, dxy, **parameters_features, statistic=statistics)
    track = tobac.linking_trackpy(features, tobac_input, dt=dt, dxy=dxy, v_max=100)
    
    return track, tobac_input

def get_time_shift(metadata=None, flare_starttime=5286):
    # Convert datetime operations to numpy datetime64
    # flare burn start for 9UTC run usually 5286 s after 9UTC
    t01 = np.datetime64('2023-01-25T09:00:00') + np.timedelta64(5286, 's')
    t11 = np.datetime64('2023-01-25T12:00:00') + np.timedelta64(5286, 's')

    if isinstance(metadata, dict):
        # Convert string date to numpy datetime64
        ydate_ini = metadata['INPUT_ORG']['runctl']['ydate_ini']
        Y, M, D, h = ydate_ini[:4], ydate_ini[4:6], ydate_ini[6:8], ydate_ini[8:10]
        dt_ini = np.datetime64(f'{Y}-{M}-{D}T{h}:00:00')
        t11 = dt_ini + np.timedelta64(int(metadata['INPUT_ORG']['flare_sbm']['flare_starttime']), 's')

    return -(t11 - t01)

def adjust_time_for_later_run_starts(model_data, time_shift):
    for id in model_data.keys():
        model_data[id] = model_data[id].assign_coords({'time': model_data[id].time + time_shift})
    return model_data

def smooth(x_in, window_size=12, mode='same'):
    """Smooth data using normalized convolution to handle NaN values.
    
    Args:
        x: Input array to smooth
        window_size: Size of smoothing window
        mode: Convolution mode ('same', 'valid', or 'full')
        
    Returns:
        Smoothed array with same shape as input, with NaN values at edges for 'valid' mode
    """
    # Handle nan values with normalized convolution
    x = np.array(x_in)
    
    # Create mask for invalid values (NaN, masked, 0, -999)
    invalid_mask = (np.isnan(x) | np.ma.getmask(x) | (x == 0) | (x == -999))
    valid_mask = ~invalid_mask
    
    if not valid_mask.any():
        return np.copy(x)
    
    kernel = np.ones(window_size) / window_size
    
    # Always create output array with same size as input
    result = np.zeros(len(x_in))
    
    # Perform convolution on valid data
    smooth_valid = np.convolve(np.where(valid_mask, x, 0), kernel, mode=mode)
    norm = np.convolve(valid_mask.astype(float), kernel, mode=mode)
    
    # Ensure smooth_valid and norm have the same length as result
    if len(smooth_valid) > len(result):
        # For 'valid' mode, we need to pad the result
        print(f"Smooth valid length: {len(smooth_valid)}, Result length: {len(result)}")
        pad_width = (len(smooth_valid) - len(result)) // 2
        result = np.pad(result, (pad_width, pad_width), mode='constant', constant_values=np.nan)
    
    # Apply smoothing only to valid points
    result = smooth_valid / np.maximum(norm, 1)
    
    # If we padded the result, remove the padding
    if len(result) > len(x):
        pad_width = (len(result) - len(x)) // 2
        result = result[pad_width:-pad_width]
    
    # Fill edges with NaN for 'valid' mode
    if mode == 'valid':
        # Calculate number of NaN values to add at each end
        result_valid = np.zeros(len(x_in))
        n_nan = (window_size - 1) // 2
        result_valid[:n_nan] = np.nan
        result_valid[-n_nan:] = np.nan
        result_valid[n_nan:-n_nan] = result
        result = result_valid
    
    # Create final mask by expanding invalid_mask to include neighboring values
    n_neighbors = window_size // 2
    expanded_mask = np.zeros_like(invalid_mask)
    for i in range(-n_neighbors, n_neighbors + 1):
        if i < 0:
            expanded_mask[:i] |= invalid_mask[-i:]
        elif i > 0:
            expanded_mask[i:] |= invalid_mask[:-i]
        else:
            expanded_mask |= invalid_mask
    
    # Apply the expanded mask to the result
    result = np.ma.masked_where(expanded_mask, result)
    
    return result



def extract_cell_data(track, data_3D, icell):
    """Extract data for each cell track."""
    cells_ds = []
    for _, cell_track in track.groupby("cell"):
        path_time = np.array([np.datetime64(dt) for dt in cell_track.time.values], dtype='datetime64[ns]')
        slicer = {
            'time': xr.DataArray(path_time, dims="path"),
            'x': xr.DataArray(cell_track.longitude.values, dims="path"),
            'y': xr.DataArray(cell_track.latitude.values, dims="path"),
            'method': 'nearest'
        }
        cells_ds.append(data_3D.sel(**slicer))
    return cells_ds[icell]


def get_runs_summary(json_path="cosmo-specs-runs/eriswil/runs_summary.json"):
    """
    Read the runs summary JSON and convert it to a dictionary with folder names as keys
    and all other fields as a nested dictionary.
    
    Args:
        json_path (str): Path to the runs_summary.json file
        
    Returns:
        dict: Dictionary with format {folder_name: {field1: value1, ...}}
    """
    import json
    
    # Read the JSON file
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    # Convert list of dicts to dict of dicts
    runs_dict = {}
    for entry in data:
        folder = entry.pop('folder')  # Remove and get the folder name
        runs_dict[folder] = entry     # Store remaining dict as value
        
    return runs_dict

def setup_dask_client(n_workers=1, threads_per_worker=4):
    """Setup Dask client with appropriate resources."""
    from dask.distributed import Client, LocalCluster
    # Try random port
    cluster = LocalCluster(n_workers=n_workers, 
                          threads_per_worker=threads_per_worker,
                          dashboard_address=':0')  # ':0' means use any available port
    dask.config.set(scheduler='distributed')
    return Client(cluster)


def parse_arguments():
    import argparse
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(description='Process plume data for a single cell')
    parser.add_argument(
        '--root_path', type=str, help='Root data directory',
        default='/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/eriswil/'
    )
    parser.add_argument(
        '--ensemble', type=str, help='Ensemble identifier', 
        default='cs-eriswil__20241206_101403'
    )
    parser.add_argument(
        '--cell_number', type=int, help='Cell number to process', 
        default=0
    )
    parser.add_argument(
        '--add_variables', type=str, 
        choices=['fluxes_total_mixing', 'fluxes_positive_mixing', 'fluxes_negative_mixing',
                'fluxes_total_numbers', 'fluxes_positive_numbers', 'fluxes_negative_numbers',
                'numbers', 'mixing', 'all'],
        help='Which flux variables to process',
        default='fluxes_positive_mixing',
    )
    parser.add_argument(
        '--debug', 
        action='store_true', 
        help='Enable debug output', 
        default=False
    )
    parser.add_argument(
        '--chunk_size', 
        type=int, 
        default=10,
        help='Chunk size for spatial dimensions'
    )
    parser.add_argument(
        '--n_workers', type=int, 
        default=30,
        help='Number of Dask workers'
    )
    return parser.parse_args()


def get_time_height(time_steps, metadata):

    # list of half-height-levels in m, converted to km
    height = HHL_100
    radius = RGRENZ

    # convert from time in decimal hours to datetime
    t0_3Dout = metadata[f'INPUT_ORG']['sbm_par']['nc_output_hcomb'][0]
    # convert t0_3Dout seconds to datetime.timedelta
    t0_3Dout = datetime.timedelta(seconds = t0_3Dout)

    delta_t = metadata[f'INPUT_ORG']['sbm_par']['nc_output_hcomb'][2]
    dt0 = datetime.datetime.strptime(metadata[f'INPUT_ORG']['runctl']['ydate_ini'], '%Y%m%d%H')
    dt0 = dt0 + t0_3Dout
    time = np.array([dt0 + datetime.timedelta(seconds = float(delta_t * its)) for its in time_steps])

    return time, height, None

def time_steps_to_datetime64(time_steps, metadata):
    """Convert model time steps to datetime values based on metadata."""
    t0_3Dout = datetime.timedelta(seconds=metadata['INPUT_ORG']['sbm_par']['nc_output_hcomb'][0])
    delta_t = metadata['INPUT_ORG']['sbm_par']['nc_output_hcomb'][2]
    dt0 = datetime.datetime.strptime(metadata['INPUT_ORG']['runctl']['ydate_ini'], '%Y%m%d%H') + t0_3Dout
    return np.array([dt0 + datetime.timedelta(seconds=float(delta_t * its)) for its in time_steps], dtype='datetime64[ns]')


def get_relative_paths():
    data_path = './data_model/'
    obs_path = './data_obs/'
    plot_path = './plots/'
    return {'data': data_path, 'obs': obs_path, 'plot': plot_path}

def open_metadata(root_path, ensemble):
    
    file_name = os.path.join(root_path, f'{ensemble}/{ensemble}.json')
    print(f'open_metadata {file_name}')
    try:
        # load meta data from json file
        with open(file_name) as f:
            metadata = json.load(f)
            lmgrid = metadata[next(iter(metadata))]['INPUT_ORG']['lmgrid']
            lmgrid.update({
                "origin_lat": 47.070522, # TODO: get origin from metadata
                "origin_lon": 7.872991 # TODO: get origin from metadata
            })
            # Extract the first two numbers from the domain string
            domain = metadata[next(iter(metadata))]['domain']
            xx, yy = map(int, domain.split('x')[:2])
    
    except FileNotFoundError:
        print(f"File {file_name} not found")
        return None, None, None, None, None
    
    return metadata


def open_3D_data(data_path: str, ensemble: str) -> xr.Dataset:
    """Open 3D data for a specific ensemble.
    
    Args:
        data_path: Path to data directory
        ensemble: Ensemble identifier
        
    Returns:
        xr.Dataset: Dataset containing 3D data
    """
    try:
        # Construct file pattern for the ensemble
        file_pattern = os.path.join(data_path, f'3D_{ensemble}.nc')
        
        # Open dataset with dask for parallel processing
        ds = xr.open_dataset(file_pattern)#, chunks={'time': -1})
        
        # Ensure coordinates are properly named
        #if 'x' not in ds.coords and 'lon' in ds.coords:
        #    #ds = ds.rename({'lon': 'x', 'lat': 'y'})   # made DEBUG edits here WS
        #    # reverse the z-axis
        #    ds = ds.isel(z=slice(None, None, -1))
            
        return ds
        
    except Exception as e:
        raise RuntimeError(f"Failed to open 3D data for ensemble {ensemble}: {str(e)}")


def set_name_tick_params(ax: mpl.axes.Axes) -> None: # type: ignore
    """
    Sets the title and tick parameters for an axes.
    Args:
        ax (mpl.axes.Axes): The axes to configure.
    """
    tick_kwargs = {
        'which': 'both', 
        'direction': 'inout', 
        'top': True, 
        'right': True, 
        'bottom': True, 
        'left': True
        }
    #
    # Configure tick parameters
    ax.tick_params(**tick_kwargs)
    ax.minorticks_on()    
    ax.tick_params(which='major', length=5)
    ax.tick_params(which='minor', length=3)
    ax.xaxis.set_ticks_position('both')
    ax.yaxis.set_ticks_position('both')
    ax.grid(True, which='major', linestyle='--', linewidth='0.11', color='black', alpha=0.5, zorder=99.1)
    ax.grid(True, which='minor', linestyle=':', linewidth='0.075', color='black', alpha=0.25, zorder=99.1)
    ax.set_axisbelow(False)

def get_ensembles_colors(n_colors):
    colors_top = mpl.cm.get_cmap('Reds_r')(np.linspace(0, 1, n_colors))[:n_colors//2]
    colors_bottom = mpl.cm.get_cmap('Oranges')(np.linspace(0, 1, n_colors))[n_colors//2:]
    cmap = mcolors.ListedColormap(np.vstack((colors_top, colors_bottom)), name='RedOrange')
    return cmap(np.linspace(0, 1, n_colors))

def create_ensemble_legend(lines, labels):
    """Create legend with header row"""
    header_row = [
        ('', 12), ('FE(/s/cell)', 12), ('FDN(M/m³)', 15), ('FDP(nm)', 15),
        ('FSig', 15), ('DNAP(/cm³)', 12), ('DN(M/m³)', 17), ('DP(nm)', 17),
        ('Sig', 19), ('Shape', 0), ('dnap_init', 12)
    ]
    header = " ".join([f"{col:{w}}" for col, w in header_row])
    
    figlegend, axlegend = plt.subplots(figsize=(10, 0.2 * (len(labels) + 1)))
    axlegend.legend([plt.Line2D([], [], alpha=0.0)] + lines, [header] + labels,
                    loc='center', prop={'family': 'monospace', 'size': 8})
    axlegend.set_frame_on(False)
    axlegend.set(xticks=[], yticks=[])
    figlegend.tight_layout()
    return figlegend, axlegend

def calculate_fall_velocity(track):
    """Calculate fall velocity from track data"""
    if isinstance(track.time, pd.DatetimeIndex):
        times = track.time
    elif isinstance(track.time, np.ndarray):
        times = pd.to_datetime(track.time)
    elif isinstance(track.time, object):
        times = pd.to_datetime(track.time)
    else:
        raise ValueError("Invalid time type")
    
    # Check if we have enough data points
    if len(times) <= 1:
        # Return zero or NaN for single point or empty track
        return np.zeros_like(track.altitude.values) * np.nan
    
    # Get the first time value safely using iloc instead of direct indexing
    first_time = times.iloc[0] if hasattr(times, 'iloc') else times[0]
    
    # Convert the Series of timedeltas to seconds using .dt accessor
    #time_seconds = (times - first_time).dt.total_seconds()
    try:
        time_seconds = (times - first_time).dt.total_seconds()
    except:
        time_seconds = (times - first_time).total_seconds()
        print('warning: time_seconds is not a datetime64')
    
    return np.gradient(track.altitude.values, time_seconds)



def get_domain_resolution(metadata): 
    dlon = metadata['INPUT_ORG']['lmgrid']['dlon']
    err_msg = f"raise ValueError(f'Unknown resolution: {dlon}')"
    return '100m' if dlon == 0.001 else '400m' if dlon == 0.004 else exec(err_msg)


def format_model_label_as_table(metadata_file_name, run_id):
    
    with open(metadata_file_name, 'r') as f:
        metadata = json.load(f)
    metadata_entry = metadata[next(iter(metadata))]
    
    def fmt_arr(arr, factor=1, na_for_zero=True):
        """Format array with optional unit conversion"""
        arr = [arr] if not isinstance(arr, (list, np.ndarray)) else arr
        vals = ["N/A" if (na_for_zero and v == 0) else f"{v * factor:4.1e}" for v in arr]
        return "[" + ",".join(vals) + "]"
    
    # Extract parameters with defaults
    org = metadata_entry.get('INPUT_ORG', {})
    flare = org.get('flare_sbm', {})
    sbm = org.get('sbm_par', {})
    val = {'dnap_init': sbm.get('dnap_init', 0.0), 'flare_emission': flare.get('flare_emission', 0.0), 'dn_in': sbm.get('dn_in', [0.0]), 'flare_dn': flare.get('flare_dn', [0.0]), 'dp_in': sbm.get('dp_in', [0.0]), 'flare_dp': flare.get('flare_dp', [0.0]), 'sig_in': sbm.get('sig_in', [0.0]), 'flare_sig': flare.get('flare_sig', [0.0]), 'ishape': sbm.get('ishape', 0)}
    
    # Format all values without the ASCII frame
    return (
        f"Parameter:              Background                        Flare              ishape\n"
        f"DNAP/FPR:             {val['dnap_init']:^15.1f}                   {fmt_arr(val['flare_emission'], 1e-6):^15}    {val['ishape']:^15d}\n"
        f"DNb/DNf:            {fmt_arr(val['dn_in']):^15}           {fmt_arr(val['flare_dn']):^15}\n"
        f"DPb/DPf:             {fmt_arr(val['dp_in'], 1e9):^15}           {fmt_arr(val['flare_dp'], 1e9):^15}\n"
        f"Sigb/Sigf:            {fmt_arr(val['sig_in']):^15}           {fmt_arr(val['flare_sig']):^15}\n"
    )

    
def format_model_table(metadata_entry, include_header=True):
    """Format model parameters as an aligned table with automatic column width adjustment.
    
    Args:
        metadata_entry: Dictionary containing model metadata
        include_header: Whether to include the header row (default: True)
        
    Returns:
        str: Formatted table as a string with aligned columns
    """
    def fmt_arr(arr, factor=1, na_for_zero=True):
        """Format array with optional unit conversion"""
        arr = [arr] if not isinstance(arr, (list, np.ndarray)) else arr
        vals = ["N/A" if (na_for_zero and v == 0) else f"{v * factor:.1e}" for v in arr]
        return "[" + ",".join(vals) + "]"
    
    # Extract parameters with defaults
    org = metadata_entry.get('INPUT_ORG', {})
    flare = org.get('flare_sbm', {})
    sbm = org.get('sbm_par', {})
    
    # Prepare data for the table
    params = {
        "FPR": {"bg": "N/A", "flare": flare.get('flare_emission', 0.0), "ishape": sbm.get('ishape', 0)},
        "DNAP (1/cm³)": {"bg": sbm.get('dnap_init', 0.0), "flare": fmt_arr(flare.get('flare_emission', 0.0), 1e-6), "ishape": fmt_arr(sbm.get('ishape', 0), 1e-6)},
        "DNb (1/m³)": {"bg": fmt_arr(sbm.get('dn_in', [0.0])), "flare": fmt_arr(flare.get('flare_dn', [0.0]))},
        "DPb (µm)": {"bg": fmt_arr(sbm.get('dp_in', [0.0]), 1e9), "flare": fmt_arr(flare.get('flare_dp', [0.0]), 1e9)},
        "Sigb (-)": {"bg": fmt_arr(sbm.get('sig_in', [0.0])), "flare": fmt_arr(flare.get('flare_sig', [0.0]))},
    }
    
    # Calculate column widths based on content
    param_width = max(len("Parameter"), max(len(p) for p in params.keys())) + 2
    
    # Calculate width for each column dynamically
    col_data = {
        "bg": [params[p]["bg"] for p in params],
        "flare": [params[p]["flare"] for p in params],
    }
    
    # Add ishape only for rows that have it
    ishape_rows = ["FPR", "DNAP (1/cm³)"]
    col_data["ishape"] = [params[p].get("ishape", "") for p in params if p in ishape_rows]
    
    # Calculate column widths
    col_widths = {
        "bg": max(len("Background"), max(len(str(v)) for v in col_data["bg"])) + 4,
        "flare": max(len("Flare"), max(len(str(v)) for v in col_data["flare"])) + 4,
        "ishape": max(len("ishape"), max(len(str(v)) for v in col_data["ishape"] if v is not None)) + 4 if col_data["ishape"] else 0
    }
    
    # Build the table
    lines = []
    
    # Add header if requested
    if include_header:
        header = f"{'Parameter':<{param_width}}{'Background':<{col_widths['bg']}}{'Flare':<{col_widths['flare']}}"
        if col_widths["ishape"] > 0:
            header += f"{'ishape':<{col_widths['ishape']}}"
        lines.append(header)
        lines.append("-" * len(header))
    
    # Add data rows
    for param, values in params.items():
        line = f"{param:<{param_width}}"
        
        # Format background value
        bg_val = values["bg"]
        if isinstance(bg_val, (int, float)) and param != "DNAP (1/cm³)":
            line += f"{bg_val:<{col_widths['bg']}.1e}"
        else:
            line += f"{str(bg_val):<{col_widths['bg']}}"
        
        # Format flare value
        flare_val = values["flare"]
        if isinstance(flare_val, (int, float)) and param != "DNAP (1/cm³)":
            line += f"{flare_val:<{col_widths['flare']}.1e}"
        else:
            line += f"{str(flare_val):<{col_widths['flare']}}"
        
        # Add ishape if applicable
        if param in ishape_rows and col_widths["ishape"] > 0:
            ishape_val = values.get("ishape", "")
            if isinstance(ishape_val, int):
                line += f"{ishape_val:<{col_widths['ishape']}d}"
            else:
                line += f"{str(ishape_val):<{col_widths['ishape']}}"
        
        lines.append(line)
    
    return "\n".join(lines)

def format_model_label(metadata_entry, run_id, model_name):
    with open(metadata_entry, 'r') as f:
        metadata = json.load(f)
        
    print(metadata)
    metadata_entry = metadata[str(run_id)]
    
    def fmt_arr(arr, factor=1, na_for_zero=True):
        """Format array with optional unit conversion"""
        arr = [arr] if not isinstance(arr, (list, np.ndarray)) else arr
        vals = ["N/A" if (na_for_zero and v == 0) else f"{v * factor:4.1e}" for v in arr]
        return "[" + ",".join(vals) + "]"
    
    # Extract parameters with defaults
    org = metadata_entry.get('INPUT_ORG', {})
    flare = org.get('flare_sbm', {})
    sbm = org.get('sbm_par', {})
    
    # Format all values in one return statement
    return (f"{model_name:12s} "
            f"{flare.get('flare_emission', 0.0):4.1e}      "
            f"{fmt_arr(flare.get('flare_dn', [0.0]), 1e-6):<15} "
            f"{fmt_arr(flare.get('flare_dp', [0.0]), 1e9):<15} "
            f"{fmt_arr(flare.get('flare_sig', [0.0])):<15} "
            f"{sbm.get('dnap_init', 0.0):5.1f}        "
            f"{fmt_arr(sbm.get('dn_in', [0.0]), 1e-6):<15} "
            f"{fmt_arr(sbm.get('dp_in', [0.0]), 1e9):<15} "
            f"{fmt_arr(sbm.get('sig_in', [0.0])):<15} "
            f"{sbm.get('ishape', 0):4d}")

# Haversine function to compute the distance between two lat-lon points
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0e3  # Earth radius in meters
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (np.sin(dlat / 2.0)**2 +
         np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance

# Define the KWM class
class KWM(dict):
    """Manage plotting kwargs with priority overrides."""
    def __init__(self, **defaults):
        self.defaults = defaults
        self.current = defaults.copy()
        super().__init__(self.current)
    
    def update(self, **kwargs):
        """Update current kwargs."""
        self.current.update(kwargs)
        super().__init__(self.current)
        return self
    
    def reset(self):
        """Reset to defaults."""
        self.current = self.defaults.copy()
        super().__init__(self.current)
        return self
    
    def __call__(self, **overrides):
        """Get kwargs with temporary overrides."""
        temp = self.__class__(**self.current)
        temp.update(**overrides)
        return temp

def add_ruler(axes, lat_start, lon_start, lat_end, lon_end, 
              minor_tick_interval=500, major_tick_interval=1000, 
              vertical_line_height=0.0, add_top_axis_lines=False, 
              minor_ticklabels=False, major_ticklabels=True, lw=1.0, alpha=0.6):
    """Add distance ruler to lat/lon plot with ticks at 500m and 1000m intervals.
    
    Parameters:
    -----------
    axes : matplotlib.axes.Axes or array of Axes
        The axes to draw the ruler on
    lat_start, lon_start : float
        Starting latitude and longitude
    lat_end, lon_end : float
        Ending latitude and longitude
    minor_tick_interval : float, optional
        Minor tick interval in meters
    major_tick_interval : float, optional
        Major tick interval in meters
    vertical_line_height : float, optional
        Height of vertical lines in latitude units
    """
    # Ensure axes is an array
    axes = np.atleast_1d(axes)
    
    # Calculate total distance and direction vector
    total_distance = haversine_distance(lat_start, lon_start, lat_end, lon_end)
    dx, dy = lon_end - lon_start, lat_end - lat_start
    
    # Define styles for white outline and black line
    styles = [
        {'alpha': alpha-0.3, 'linewidth': lw, 'color': 'white', 'zorder': 98},  # White outline
        {'alpha': alpha, 'linewidth': lw-0.5, 'color': 'black', 'zorder': 99}  # Black line
    ]
    
    # Draw ruler at different intervals
    for interval, label_ticks in [(minor_tick_interval, minor_ticklabels),
                                  (major_tick_interval, major_ticklabels)]:
        num_ticks = max(1, int(total_distance / interval))
        o_factor = 0.15 / num_ticks  # Perpendicular tick size factor
        
        for ax in axes.flatten():
            # Draw ruler line with both styles
            for style in styles:
                ax.plot([lon_start, lon_end], [lat_start, lat_end], **style)
                if add_top_axis_lines:
                    ax.plot([lon_start, lon_end], [lat_start+vertical_line_height, lat_end+vertical_line_height], **style)
            
            # Draw tick marks and labels
            for i in range(num_ticks + 1):
                fraction = i / num_ticks
                lat_tick = lat_start + fraction * dy
                lon_tick = lon_start + fraction * dx
                
                # Calculate perpendicular offsets for tick marks
                o_lon, o_lat = -o_factor * dy, o_factor * dx
                
                # Draw tick mark
                if 0 < i < num_ticks:
                    for style in styles:
                        ax.plot([lon_tick + o_lon, lon_tick - o_lon],
                                [lat_tick + o_lat, lat_tick - o_lat], **style)
                        if add_top_axis_lines:
                            ax.plot([lon_tick + o_lon, lon_tick - o_lon],
                                    [lat_tick + o_lat + vertical_line_height, lat_tick - o_lat + vertical_line_height], **style)
                
                # Add label to the tick (only for 1000m intervals)
                if label_ticks and (0 < i < num_ticks):
                    label = ax.text(
                        lon_tick - o_lon - 0.001,
                        lat_tick + o_lat - 0.009,
                        f'{num_ticks-i:.0f}',
                        fontsize=10,
                        fontweight='semibold',
                        alpha=0.45
                    )
                    label.set_path_effects([
                        PathEffects.withStroke(linewidth=1.2, foreground='w', alpha=0.5)
                    ])
            
            # Add vertical lines at 0km and 8km points if requested
            if add_top_axis_lines:
                # 0km point (start of ruler)
                for style in styles:
                    ax.plot([lon_start, lon_start],
                            [lat_start, lat_start + vertical_line_height],
                            **style)
                    ax.plot([lon_end, lon_end],
                            [lat_end, lat_end + vertical_line_height],
                            **style)

def load_grid_data(metadata_file) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, xr.Dataset]:
    try:
        # Load metadata and extract grid info
        with open(metadata_file) as f:
            metadata = json.load(f)
            first_key = next(iter(metadata))
            nml_input_org = metadata[first_key]['INPUT_ORG']
            lmgrid = nml_input_org['lmgrid']
            lmgrid.update({
                "origin_lat": 47.070522,  # TODO: get from metadata
                "origin_lon": 7.872991    # TODO: get from metadata
            })
            # Get domain dimensions
            xx, yy = map(int, metadata[first_key]['domain'].split('x')[:2])
        
        # Load ExtPar data
        extpar_path = f'{Path(metadata_file).parents[2]}/COS_in/extPar_Eriswil_{xx}x{yy}.nc'
        data_extpar = xr.open_mfdataset(glob.glob(extpar_path), chunks='auto')
        
        # Extract and process coordinates
        lat2D = data_extpar['lat'].values[7:-7, 7:-7]
        lon2D = data_extpar['lon'].values[7:-7, 7:-7]
        
        return (
            np.linspace(lat2D.min(), lat2D.max(), lat2D.shape[0]),  # lat1D
            np.linspace(lon2D.min(), lon2D.max(), lon2D.shape[1]),  # lon1D
            lat2D, lon2D, data_extpar
        )
    except FileNotFoundError:
        print(f"File {metadata_file} not found")
        return (None,) * 5

def load_tracking_csv(file_name):
    tracking_data = pd.read_csv(file_name)
    return tracking_data

def load_processed_data(root_path, ensembles):
    metadata_dict = {}
    procdata_dict = {}

    for ensemble in ensembles:
        metadata_dict[ensemble] = open_metadata(root_path, ensemble)
        procdata_dict[ensemble] = {}



        for ens in metadata_dict[ensemble].keys():
            processed_path = f'{root_path}/3D_{ens}_processed.nc'
            procdata_dict[ensemble].update({ens: xr.open_dataset(processed_path)})
    
    return metadata_dict, procdata_dict

def load_holimo_data(holimo_path):
    # def filter_keys(data, keys_to_include, keys_to_exclude=[]):
    #     return {key: val for key, val in data.items() 
    #             if all(k in key for k in keys_to_include) 
    #             and not any(k in key for k in keys_to_exclude)}


    data_holimo = xr.open_dataset(holimo_path)
    time_holimo = [
        datetime.datetime.strptime(''.join([x.decode('utf-8') for x in data_holimo['timestr'][:, i].values]), '%Y-%m-%d_%H:%M:%S.%f') 
        for i in range(data_holimo.time.size)
        ]
    data_holimo = data_holimo.assign_coords({"time": ("time", time_holimo)})

    return data_holimo


def load_mbr7_data(radar_data_path, hour_start=10, hour_end=12, height_asl=921.0):
    """Load and process MBR7 radar data.
    
    Args:
        radar_data_path: Path to radar data files
        height_asl: Height above sea level in meters (default: 921.0)
        hour_start: Starting hour (default: 10)
        hour_end: Ending hour (default: 13)
        
    Returns:
        xr.Dataset: Dataset with Z, mdv, and ldr variables
    """
    # Load radar data files for specified time range
    radar = xr.open_mfdataset(sorted(glob.glob(f'{radar_data_path}/*.znc'))[hour_start:hour_end])
    
    # Create coordinates
    coords = {'time': np.datetime64('1970-01-01') + np.timedelta64(1, 's') * radar['time'].values,
              'height': radar['range'].values + height_asl}
    radar = radar[['Zh2l', 'VELh2l', 'LDRh2l']].load()
    # Create dataset with processed variables
    return xr.Dataset(
        data_vars={
            'Z': (['time', 'height'], 10 * np.log10(radar['Zh2l'].values)),
            'mdv': (['time', 'height'], radar['VELh2l'].values),
            'ldr': (['time', 'height'], 10 * np.log10(radar['LDRh2l'].values))
        },
        coords=coords
    )

def interpolate_to_model_time(data_array, model_time):
    return data_array.chunk(dict(time=-1)).interpolate_na(dim='time', method='linear')



def configure_norm(var_name, vmin_corr=1.0, vmax_corr=1.0):
    nm = var_name.replace("_P_", "_").replace("_N_", "_")
    vmin = nml.nml_attrs_meteogram_net[nm]['vmin'] * vmin_corr
    vmax = nml.nml_attrs_meteogram_net[nm]['vmax'] * vmax_corr
    linthresh = nml.nml_attrs_meteogram_net[nm]['linthresh']
    linscale = nml.nml_attrs_meteogram_net[nm]['linscale']
    cmap = nml.nml_attrs_meteogram_net[nm]['cmap']
    unit = nml.nml_attrs_meteogram_net[nm]['unit']
    norm = mcolors.SymLogNorm(vmin=vmin, vmax=vmax, linthresh=linthresh, linscale=linscale)
    return norm, cmap, unit


def rebin_data_with_diameter(high_res_data, high_res_diameters, low_res_diameters, method='mean', 
                            min_samples=1, handle_empty='mask', axis=0):
    """
    Rebin high-resolution data to match low-resolution diameter grid with proper handling of 
    missing/invalid data. Handles logarithmic diameter spacing appropriately.

    Parameters:
    -----------
    high_res_data : array-like
        High-resolution data array (1D or N-D)
    high_res_diameters : array-like
        Diameter centers for high-resolution data (in µm)
    low_res_diameters : array-like
        Diameter centers for target low-resolution grid (in µm)
    method : str, optional
        Aggregation method: 'mean', 'sum', 'median', 'min', 'max', or 'std'
        Default is 'mean'
    min_samples : int, optional
        Minimum number of valid samples required in a bin. Default is 1
    handle_empty : str, optional
        How to handle bins with insufficient samples:
        'mask' - mask the bin (returns masked array)
        'nan' - fill with NaN
        'zero' - fill with 0
        Default is 'mask'
    axis : int, optional
        Axis along which to perform aggregation for multi-dimensional data. Default is 0

    Returns:
    --------
    np.ma.MaskedArray or np.ndarray
        Rebinned data matching the shape of low_res_diameters
    """
    import numpy as np
    
    # Convert diameters to numpy arrays
    low_res_diameters = np.asarray(low_res_diameters)
    high_res_diameters = np.asarray(high_res_diameters)
    
    # Convert input data to masked array if not already
    if not isinstance(high_res_data, np.ma.MaskedArray):
        high_res_data = np.ma.masked_invalid(high_res_data)
    
    # Calculate bin edges for low-res diameters
    # For logarithmic spacing, we need to handle the edges carefully
    diameter_edges = np.zeros(len(low_res_diameters) + 1)
    
    # Calculate edges assuming logarithmic spacing
    if len(low_res_diameters) > 1:
        # For interior bins, use geometric mean of adjacent centers
        diameter_edges[1:-1] = np.sqrt(low_res_diameters[:-1] * low_res_diameters[1:])
        
        # For first and last bins, extend by the same factor as the first interior spacing
        first_spacing = diameter_edges[1] / low_res_diameters[0]
        last_spacing = low_res_diameters[-1] / diameter_edges[-2]
        
        diameter_edges[0] = low_res_diameters[0] / first_spacing
        diameter_edges[-1] = low_res_diameters[-1] * last_spacing
    else:
        # Single bin case - create reasonable edges
        diameter_edges[0] = low_res_diameters[0] * 0.5
        diameter_edges[1] = low_res_diameters[0] * 2.0
    
    # Initialize output array with appropriate shape
    output_shape = (len(low_res_diameters),) + high_res_data.shape[1:] if high_res_data.ndim > 1 else (len(low_res_diameters),)
    rebinned_data = np.ma.ones(output_shape)
    rebinned_data.mask = np.ones(output_shape, dtype=bool)
    
    # Define statistical operations
    operations = {
        'mean':   lambda x: np.ma.mean(x, axis=axis)   if x.ndim > 1 else np.ma.mean(x),
        'sum':    lambda x: np.ma.sum(x, axis=axis)    if x.ndim > 1 else np.ma.sum(x),
        'median': lambda x: np.ma.median(x, axis=axis) if x.ndim > 1 else np.ma.median(x),
        'min':    lambda x: np.ma.min(x, axis=axis)    if x.ndim > 1 else np.ma.min(x),
        'max':    lambda x: np.ma.max(x, axis=axis)    if x.ndim > 1 else np.ma.max(x),
        'std':    lambda x: np.ma.std(x, axis=axis)    if x.ndim > 1 else np.ma.std(x)
    }
    
    if method not in operations:
        raise ValueError(f"Method must be one of {list(operations.keys())}")
    
    operation = operations[method]
    
    # Perform binning
    for i in range(len(low_res_diameters)):
        # Find data points within current bin
        mask = (diameter_edges[i] <= high_res_diameters) & (high_res_diameters < diameter_edges[i + 1])
        bin_data = high_res_data[mask]
        
        # Check if we have enough valid samples
        if len(bin_data) >= min_samples:
            # Check if all values are masked (if the array has a mask)
            all_masked = (np.ma.is_masked(bin_data) and 
                        (isinstance(bin_data.mask, bool) or bin_data.mask.all()))
            if not all_masked:
                try:
                    rebinned_data[i] = operation(bin_data)
                except:
                    rebinned_data.mask[i] = True
            else:
                rebinned_data.mask[i] = True
        else:
            rebinned_data.mask[i] = True

    # Handle empty bins according to preference
    if handle_empty == 'nan':
        return rebinned_data.filled(np.nan)
    elif handle_empty == 'zero':
        return rebinned_data.filled(0)
    else:  # 'mask' is default
        return rebinned_data
    
    

def rebin_data_with_time(high_res_data, high_res_times, low_res_times, method='mean', 
                        min_samples=1, handle_empty='mask', axis=0):
    """
    Rebin high-resolution data to match low-resolution time grid with proper handling of 
    missing/invalid data.

    Parameters:
    -----------
    high_res_data : array-like
        High-resolution data array (1D or N-D)
    high_res_times : array-like
        Time centers for high-resolution data (datetime64 or similar)
    low_res_times : array-like
        Time centers for target low-resolution grid (datetime64 or similar)
    method : str, optional
        Aggregation method: 'mean', 'sum', 'median', 'min', 'max', or 'std'
        Default is 'mean'
    min_samples : int, optional
        Minimum number of valid samples required in a bin. Default is 1
    handle_empty : str, optional
        How to handle bins with insufficient samples:
        'mask' - mask the bin (returns masked array)
        'nan' - fill with NaN
        'zero' - fill with 0
        Default is 'mask'

    Returns:
    --------
    np.ma.MaskedArray or np.ndarray
        Rebinned data matching the shape of low_res_times
    """
    # Convert times to integers (seconds since epoch)
    low_res_times = np.asarray(low_res_times).astype('datetime64[s]').astype('int')
    high_res_times = np.asarray(high_res_times).astype('datetime64[s]').astype('int')
    
    # Convert input data to masked array if not already
    if not isinstance(high_res_data, np.ma.MaskedArray):
        high_res_data = np.ma.masked_invalid(high_res_data)
    
    # Calculate bin edges for low-res times
    time_edges = np.zeros(len(low_res_times) + 1)
    time_edges[1:-1] = (low_res_times[:-1] + low_res_times[1:]) / 2
    time_edges[0] = low_res_times[0] - (low_res_times[1] - low_res_times[0]) / 2
    time_edges[-1] = low_res_times[-1] + (low_res_times[-1] - low_res_times[-2]) / 2
    time_edges = time_edges.astype('int')
    
    # Initialize output array with appropriate shape
    output_shape = (len(low_res_times),) + high_res_data.shape[1:] if high_res_data.ndim > 1 else (len(low_res_times),)
    rebinned_data = np.ma.ones(output_shape)
    rebinned_data.mask = np.ones(output_shape, dtype=bool)
    
    # Define statistical operations
    operations = {
        'mean':   lambda x: np.ma.mean(x, axis=axis)   if x.ndim > 1 else np.ma.mean(x),
        'sum':    lambda x: np.ma.sum(x, axis=axis)    if x.ndim > 1 else np.ma.sum(x),
        'median': lambda x: np.ma.median(x, axis=axis) if x.ndim > 1 else np.ma.median(x),
        'min':    lambda x: np.ma.min(x, axis=axis)    if x.ndim > 1 else np.ma.min(x),
        'max':    lambda x: np.ma.max(x, axis=axis)    if x.ndim > 1 else np.ma.max(x),
        'std':    lambda x: np.ma.std(x, axis=axis)    if x.ndim > 1 else np.ma.std(x)
    }
    
    if method not in operations:
        raise ValueError(f"Method must be one of {list(operations.keys())}")
    
    operation = operations[method]
    
    # Perform binning
    for i in range(len(low_res_times)):
        # Find data points within current bin
        mask = (time_edges[i] <= high_res_times) & (high_res_times < time_edges[i + 1])
        bin_data = high_res_data[mask]
        # Check if we have enough valid samples
        if len(bin_data) >= min_samples:
            # Check if all values are masked (if the array has a mask)
            all_masked = (np.ma.is_masked(bin_data) and 
                        (isinstance(bin_data.mask, bool) or bin_data.mask.all()))
            if not all_masked:
                try:
                    rebinned_data[i] = operation(bin_data)
                except:
                    rebinned_data.mask[i] = True
            else:
                rebinned_data.mask[i] = True
        else:
            rebinned_data.mask[i] = True

    # Handle empty bins according to preference
    if handle_empty == 'nan':
        return rebinned_data.filled(np.nan)
    elif handle_empty == 'zero':
        return rebinned_data.filled(0)
    else:  # 'mask' is default
        return rebinned_data


def calculate_mean_diameter(array, diameters, method='arithmetic', handle_zeros='mask'):
    """Calculate mean diameters from N-D array of particle occurrences.
    
    Args:
        array: N-dimensional array where last dimension contains diameter distribution
        diameters: diameter values corresponding to last dimension
        method: 'arithmetic'|'geometric'|'median'|'effective'|'volume'
        handle_zeros: 'mask'|'nan'|'zero'
    Returns:
        MaskedArray of mean diameters over all dimensions except last
    """
    if not isinstance(array, np.ma.MaskedArray):
        array = np.ma.masked_invalid(array)
        array = np.ma.masked_equal(array, 0)
    
    # Define calculation methods
    mean_calcs = {
        'arithmetic': lambda d, w: np.ma.sum(d * w, axis=-1) / np.ma.sum(d, axis=-1),
        'geometric':  lambda d, w: np.exp(np.ma.sum(d * np.log(w), axis=-1) / np.ma.sum(d, axis=-1)),
        'median':     lambda d, w: np.apply_along_axis(
            lambda x: w[np.searchsorted(np.ma.cumsum(x)/np.ma.sum(x), 0.5)] if np.ma.sum(x) > 0 else np.ma.masked,
            axis=-1, arr=d
        ),
        'effective':  lambda d, w: np.ma.sum(d * w**3, axis=-1) / np.ma.sum(d * w**2, axis=-1),
        'volume':     lambda d, w: np.ma.sum(d * w**4, axis=-1) / np.ma.sum(d * w**3, axis=-1)
    }
    
    if method not in mean_calcs:
        raise ValueError(f"Method must be one of {list(mean_calcs.keys())}")
    
    # Create output array with same shape as input minus last dimension
    means = np.ma.zeros(array.shape[:-1])
    
    # Calculate sums along last axis for zero checking
    sums = np.ma.sum(array, axis=-1)
    
    try:
        # Calculate means where sum is non-zero
        means = np.ma.where(    sums > 0,
                                mean_calcs[method](array, diameters),
                                np.ma.masked )
    except:
        # Mask any calculation errors
        means.mask = True
    
    return means



# Function to set common properties for axes
def set_common_properties(ax, y_label, y_lim, y_scale='linear'):
    ax.set(ylabel=y_label, ylim=y_lim, yscale=y_scale)
    ax.tick_params(axis='x', pad=10)
    ax.tick_params(which='both', direction='in')
    ax.grid(True, which='major', linestyle='-', linewidth='0.5', color='black', alpha=0.5 if y_scale == 'log' else 0.25)
    if y_scale == 'log':
        ax.minorticks_on()
        ax.grid(True, which='minor', linestyle=':', linewidth='0.5', color='black', alpha=0.25)

def calculate_haversine_distance(lat1, lat2, lon1, lon2, radius=6371000):
    """Calculate the great-circle distance between two points using haversine formula.
    
    Args:
        lat1, lat2: Latitude points in degrees
        lon1, lon2: Longitude points in degrees
        radius: Earth's radius in meters (default: 6371000m)
        
    Returns:
        float: Distance between points in meters
    """
    # Convert coordinate differences to radians
    phi1, phi2 = np.radians([lat1, lat2])
    lambda1, lambda2 = np.radians([lon1, lon2])
    
    # Haversine formula
    return 2 * radius * np.arcsin(np.sqrt(
        np.sin((phi2-phi1)/2)**2 + 
        np.cos(phi1)*np.cos(phi2)*np.sin((lambda2-lambda1)/2)**2
    ))

def get_grid_cell_sizes(lat, lon):
    """Calculate average grid cell sizes using haversine formula.
    
    Args:
        lat: 2D array of latitude values
        lon: 2D array of longitude values
        
    Returns:
        tuple: Average cell dimensions (dx, dy) in meters
    """
    lat_size, lon_size = lat.shape
    cells = np.zeros((lat_size - 1, lon_size - 1, 2))
    
    # Calculate distances between adjacent grid points
    for i in range(lat_size - 1):
        for j in range(lon_size - 1):
            # Calculate N-S distance
            cells[i,j,0] = calculate_haversine_distance(
                lat[i,j], lat[i+1,j],
                lon[i,j], lon[i,j]
            )
            # For E-W distance, use same calculation
            cells[i,j,1] = calculate_haversine_distance(
                lat[i,j], lat[i,j],
                lon[i,j], lon[i,j+1]
            )
    
    # Return average cell dimensions
    return tuple(cells.mean(axis=(0,1)))


def get_run_parameters(metadata_file, lat_2D_extpar, lon_2D_extpar, model_height):
    """Extract run parameters and calculate flare emission rates.
    
    Args:
        metadata: Dictionary containing ensemble metadata
        lat_2D_extpar: 2D array of latitude values
        lon_2D_extpar: 2D array of longitude values
        model_height: Array of model height levels
        
    Returns:
        dict: Dictionary of ensemble parameters containing:
            - fe: flare emission rate [# l-1 s-1]
            - dn1: first number concentration [Mio m-3]
            - dn2: second number concentration [Mio m-3]
    """
    # Load metadata and extract grid info
    with open(metadata_file) as f:
        metadata = json.load(f)
    

    
    # Calculate parameters for each ensemble
    flare_params = {}
    for ens in metadata:
        
        nml_input_org = metadata[ens]['INPUT_ORG']
        flare_height = nml_input_org['flare_sbm']['flare_hight']
        flare_emission = nml_input_org['flare_sbm']['flare_emission']
        dn_in = nml_input_org['sbm_par']['dn_in']
        
        # Get average grid cell sizes
        height_res = -np.diff(np.array(model_height))
        height_res = height_res[-flare_height]
        
        grid_dx, grid_dy = get_grid_cell_sizes(lat_2D_extpar, lon_2D_extpar)
        cell_volume = grid_dx * grid_dy * height_res # flire height might be wrong cause of slicing
        
        flare_params[ens] = {
            'fe_per_cell': float(flare_emission ), 
            'fe_per_qm': float(flare_emission / cell_volume ),          # Convert to # l-1 s-1
            'fe_per_l': float(flare_emission / (cell_volume * 1e3) * 1e-3),   # Convert to # l-1 s-1
            'dn1': float(dn_in[0]),                                     # Convert to Mio m-3
            'dn2': float(dn_in[1]),                                     # Convert to Mio m-3
            'dz': height_res,
            'grid_dx': grid_dx,
            'grid_dy': grid_dy,
            'cell_volume': cell_volume,
            'flare_height': flare_height
        }


    return flare_params



def rebin_2d(
    x_in: np.ndarray,
    array: np.ma.MaskedArray,
    x_new: np.ndarray,
    statistic: Literal["mean", "std"] = "mean",
    n_min: int = 1,
    *,
    mask_zeros: bool = True,
) -> tuple[np.ma.MaskedArray, list]:
    """Rebins 2-D data in one dimension.

    Args:
        x_in: 1-D array with shape (n,).
        array: 2-D input data with shape (n, m).
        x_new: 1-D target vector (center points) with shape (N,).
        statistic: Statistic to be calculated. Possible statistics are 'mean', 'std'.
            Default is 'mean'.
        n_min: Minimum number of points to have good statistics in a bin. Default is 1.
        mask_zeros: Whether to mask 0 values in the returned array. Default is True.

    Returns:
        tuple: Rebinned data with shape (N, m) and indices of bins without enough data.
    """
    edges = binvec(x_new)
    result = np.zeros((len(x_new), array.shape[1]))
    array_screened = np.ma.masked_invalid(array, copy=True)  # data may contain nan-values
    for ind, values in enumerate(array_screened.T):
        mask = ~values.mask
        if np.ma.any(values[mask]):
            result[:, ind], _, _ = stats.binned_statistic(
                x_in[mask],
                values[mask],
                statistic=statistic,
                bins=edges,
            )
    result[~np.isfinite(result)] = 0
    if mask_zeros is True:
        masked_result = np.ma.masked_equal(result, 0)
    else:
        masked_result = np.ma.array(result)

    # Fill bins with not enough profiles
    x_hist, _ = np.histogram(x_in, bins=edges)
    empty_mask = x_hist < n_min
    masked_result[empty_mask, :] = np.ma.masked
    empty_indices = list(np.nonzero(empty_mask)[0])
    if len(empty_indices) > 0:
        print("No data in %s bins", len(empty_indices))

    return masked_result, empty_indices

def binvec(x: np.ndarray | list, x_scale: Literal["linear", "log"] = "linear") -> np.ndarray:
    """Converts 1-D center points to bins with even spacing in the appropriate scale.

    Args:
        x: 1-D array of N real values.
        x_scale: Whether the data is log-spaced. Default is "linear".
    Returns:
        ndarray: N + 1 edge values.
    Raises:
        ValueError: If the x_scale is not "linear" or "log".

    Examples:
        >>> binvec([1, 2, 3])  # Linear case
            [0.5, 1.5, 2.5, 3.5]
        >>> binvec([1, 2, 4, 8])  # Log case
            [0.707, 1.414, 2.828, 5.657, 11.314]
    """
    x = np.asarray(x)
    
    if x_scale == "log":
        # For logarithmic spacing, calculate edges in log space
        log_x = np.log(x)
        log_dx = np.diff(log_x)
        edge1 = np.exp(log_x[0] - log_dx[0]/2)
        edge2 = np.exp(log_x[-1] + log_dx[-1]/2)
        return np.exp(np.linspace(np.log(edge1), np.log(edge2), len(x) + 1))
    elif x_scale == "linear":
        # Original linear spacing approach
        edge1 = x[0] - (x[1] - x[0]) / 2
        edge2 = x[-1] + (x[-1] - x[-2]) / 2
        return np.linspace(edge1, edge2, len(x) + 1)
    else:
        raise ValueError(f"Invalid x_scale: {x_scale}")

def rebin_1d(
    x_in: np.ndarray,
    array: np.ndarray | np.ma.MaskedArray,
    x_new: np.ndarray,
    x_scale: Literal["linear", "log"] = "linear",
    statistic: str = "mean",
    *,
    mask_zeros: bool = True,
) -> np.ma.MaskedArray:
    """Rebins 1D array.

    Args:
        x_in: 1-D array with shape (n,).
        array: 1-D input data with shape (m,).
        x_new: 1-D target vector (center points) with shape (N,).
        statistic: Statistic to be calculated. Possible statistics are 'mean', 'std'.
            Default is 'mean'.
        mask_zeros: Whether to mask 0 values in the returned array. Default is True.

    Returns:
        Re-binned data with shape (N,).

    """
    edges = binvec(x_new, x_scale=x_scale)
    result = np.zeros(len(x_new))
    array_screened = np.ma.masked_invalid(array, copy=True)  # data may contain nan-values
    mask = ~array_screened.mask
    if np.ma.any(array_screened[mask]):
        result, _, _ = stats.binned_statistic(
            x_in[mask],
            array_screened[mask],
            statistic=statistic,
            bins=edges,
        )
    result[~np.isfinite(result)] = 0
    if mask_zeros:
        return np.ma.masked_equal(result, 0)
    return np.ma.array(result)

def display_pops_seeding_image(filename, figsize=(16,10)):
    """
    Display the POPS seeding particle concentration reference image.
    
    Parameters
    ----------
    figsize : tuple, optional
        Figure size in inches (width, height), default is (12,8)
        
    Returns
    -------
    matplotlib.figure.Figure
        The figure object containing the displayed image
    """

    
    img = Image.open(filename)
    fig = plt.figure(figsize=figsize)
    plt.imshow(img)
    plt.axis('off')
    return fig

def load_mwr_data(mwr_data_path):
    """Load Microwave Radiometer (MWR) data for LWP and IWV.
    
    Args:
        mwr_data_path (str): Path to directory containing MWR data files

    Returns:
        xr.Dataset: Dataset containing LWP and IWV data with time coordinates
    """
    # Find LWP and IWV data files
    lwp_data_files = sorted(glob.glob(f'{mwr_data_path}/*LWP.NC'))
    iwv_data_files = sorted(glob.glob(f'{mwr_data_path}/*IWV.NC'))

    # Load data using xarray
    mwr_lwp = xr.open_mfdataset(lwp_data_files, combine='by_coords', parallel=True)
    mwr_iwv = xr.open_mfdataset(iwv_data_files, combine='by_coords', parallel=True)
    
    # Create combined dataset
    mwr_ds = xr.Dataset(
        data_vars={
            'LWP': ('time', mwr_lwp['LWP'].values),
            'IWV': ('time', mwr_iwv['IWV'].values)
        },
        coords={
            'time': mwr_lwp['time'].values
        }
    )
    return mwr_ds

def convert_to_gif(
    input_mp4: str,
    output_gif: str,
    scale_factor: float = 0.25,
    fps: int = 10
) -> None:
    """Convert an MP4 video to a lower resolution GIF using ffmpeg.
    
    Args:
        input_mp4: Path to input MP4 file
        output_gif: Path to output GIF file
        scale_factor: Factor to scale width and height (default: 0.25 for 1/4 size)
        fps: Frames per second for the output GIF (default: 10)
        
    Example:
        >>> convert_to_gif("video.mp4", "animation.gif")
        'FFMPEG created GIF file: /absolute/path/to/animation.gif'
    """
    # Construct ffmpeg command
    ffmpeg_cmd = "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    
    # Build the filter string to scale the video
    filter_str = (
        f"fps={fps},"  # Set frame rate
        f"scale=iw*{scale_factor}:ih*{scale_factor}:flags=lanczos,"  # Scale dimensions
        "split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse"  # Generate and use palette for better quality
    )
    
    # Construct the full command
    cmd = [
        ffmpeg_cmd,
        "-i", input_mp4,  # Input file
        "-vf", filter_str,  # Video filters
        "-y",  # Overwrite output if exists
        output_gif  # Output file
    ]
    
    # Run ffmpeg command
    import subprocess
    import traceback
    import os
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ffmpeg command: {' '.join(cmd)}")
        print(f"Exit code: {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except FileNotFoundError:
        print(f"ffmpeg not found at path: {ffmpeg_cmd}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    finally:
        print(f'FFMPEG created GIF file: {os.path.abspath(output_gif)}')
        
    

def convert_to_video(
    input_pattern: str,
    output_file: str,
    resolution: str = "1920:1080",
    loop_count: int = 2,
    framerate: int = 20,
    start_frame: int = None,
    frame_step: int = 1  # Add this parameter to control frame stepping
) -> None:
    """Convert a sequence of PNG files into an MP4 video using ffmpeg.
    
    Args:
        input_pattern: Pattern for input PNG files (e.g., "3D_%03d.png")
        output_file: Name of output MP4 file (without extension)
        resolution: Output video resolution (default: "1920:1080")
        loop_count: Number of times to loop the video (default: 2)
        framerate: Frames per second (default: 20)
        start_frame: First frame number to include (optional)
        frame_step: Use every nth frame (default: 1, use every frame)
    """
    # Construct ffmpeg command
    ffmpeg_cmd = "/sw/spack-levante/mambaforge-22.9.0-2-Linux-x86_64-wuuo72/bin/ffmpeg"
    
    # Build the input arguments
    input_args = ["-y"]  # Overwrite output file if it exists
    
    if start_frame is not None:
        input_args.extend(["-start_number", str(start_frame)])
    
    if frame_step > 1:
        # When using frame_step > 1, we need to adjust the framerate
        # to maintain the same playback speed
        effective_framerate = framerate / frame_step
        input_args.extend([
            #"-framerate", str(effective_framerate),
            "-framerate", str(framerate),
            "-i", input_pattern,
            "-vf", f"select='not(mod(n,{frame_step}))',setpts=N/({effective_framerate}*TB)"
        ])
    else:
        input_args.extend([
            "-stream_loop", str(loop_count),
            "-framerate", str(framerate),
            "-i", input_pattern
        ])
    
    # Build the output arguments
    output_args = [
        "-q:v", "0",  # Best quality
        "-pix_fmt", "yuv420p",  # Standard pixel format
        "-codec:v:0", "h264",  # H.264 codec
    ]
    
    # Add scaling as a separate filter if we're not already using a filter
    if frame_step > 1:
        # Scale is already part of the filter chain
        output_args.extend(["-vf", f"scale={resolution}"])
    else:
        output_args.extend(["-vf", f"scale={resolution}"])
    
    # Combine all arguments
    cmd = [ffmpeg_cmd] + input_args + output_args + [f"{output_file}"]
    
    # Run ffmpeg command
    import subprocess
    import traceback
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ffmpeg command: {' '.join(cmd)}")
        print(f"Exit code: {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except FileNotFoundError:
        print(f"ffmpeg not found at path: {ffmpeg_cmd}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        print("\nTraceback:")
        print(traceback.format_exc())
        raise
    finally:
        print(f'FFMPEG created MP4 file: {os.path.abspath(output_file)}')

def open_tracking_data(file_list: list) -> dict:
    """Open tracking data files for a specific ensemble.
    
    Args:
        base_pattern: Path to data directory
        ensemble: Ensemble identifier
        
    Returns:
        dict: Dictionary of cell datasets, keyed by cell number
    """
    try:

        # Load each cell file into a dictionary
        tracking_data = {}
        for file_path in file_list:
            # Extract cell number from filename
            cell_num = int(re.search(r'3D_(\d+)_tracking_v3_cell_\d+\.nc$', file_path).group(1))

            # Store in dictionary with cell number as key
            tracking_data[cell_num] = xr.open_dataset(file_path, chunks={'time': -1})
            tracking_data[cell_num] = tracking_data[cell_num].load()
            
            logging.debug(f"Loaded tracking data for cell {cell_num}")
            
        return tracking_data
        
    except Exception as e:
        raise RuntimeError(f"Failed to open tracking data for ensemble {file_list}: {str(e)}")

def calculate_metrics(model_data, holimo_data, ensemble_nrs):
    """Calculate statistical metrics between model runs and HOLIMO data, handling missing time steps."""
    metrics = {}
    variables = [
        ('Water_concentration', 'nw_bulk', 'Water_concentration_interp'),
        ('Water_meanD', 'mdw_bulk', 'Water_meanD_interp'),
        ('Ice_concentration', 'nf_bulk', 'Ice_concentration_interp'),
        ('Ice_meanD', 'mdf_bulk', 'Ice_meanD_interp')
    ]
    
    for run_id in ensemble_nrs:
        metrics[run_id] = {}
        data = model_data[run_id]
        
        for var_name, model_var, holimo_var in variables:
            try:
                if holimo_var not in holimo_data or model_var not in data:
                    metrics[run_id][var_name] = {'MAE': np.nan, 'MSE': np.nan, 'RMSE': np.nan, 'CORR': np.nan}
                    continue
                    
                model_values = data[model_var].values
                holimo_values = holimo_data[holimo_var]
                
                # Handle length mismatch
                if len(model_values) != len(holimo_values):
                    common_length = min(len(model_values), len(holimo_values))
                    model_values, holimo_values = model_values[:common_length], holimo_values[:common_length]
                    if common_length == 0:
                        metrics[run_id][var_name] = {'MAE': np.nan, 'MSE': np.nan, 'RMSE': np.nan, 'CORR': np.nan}
                        continue
                
                # Filter NaN values
                valid_mask = ~(np.isnan(model_values) | np.isnan(holimo_values))
                if not np.any(valid_mask):
                    metrics[run_id][var_name] = {'MAE': np.nan, 'MSE': np.nan, 'RMSE': np.nan, 'CORR': np.nan}
                    continue
                
                model_valid, holimo_valid = model_values[valid_mask], holimo_values[valid_mask]
                
                # Calculate metrics
                mae = np.mean(np.abs(model_valid - holimo_valid))
                mse = np.mean((model_valid - holimo_valid)**2)
                rmse = np.sqrt(mse)
                corr = np.corrcoef(model_valid, holimo_valid)[0, 1] if len(model_valid) > 1 else np.nan
                
                metrics[run_id][var_name] = {'MAE': mae, 'MSE': mse, 'RMSE': rmse, 'CORR': corr}
            except Exception:
                metrics[run_id][var_name] = {'MAE': np.nan, 'MSE': np.nan, 'RMSE': np.nan, 'CORR': np.nan}
                
    return metrics

def create_metrics_legend(lines, labels, metrics_data):
    """Create legend with statistical metrics comparing model runs with HOLIMO data."""
    # Create headers

    header_0 = [
    ('', 12), ('FE(/s/cell)', 12), ('FDN(M/m³)', 15), ('FDP(nm)', 15),
    ('FSig', 15), ('DNAP(/cm³)', 12), ('DN(M/m³)', 17), ('DP(nm)', 17),
    ('Sig', 19), ('Shape', 0)
    ]
    header_1 = [
    ('Water Concentration', 40), ('Water Mean Diameter', 40),
    ('Ice Concentration', 40), ('Ice Mean Diameter', 40)
    ]

    header4 = " ".join([f"{col:{w}}" for col, w in header_0 + header_1])

    header5 = " ".join([f"{col:{w}}" for col, w in [(' ', sum([b for a, b in header_0+[(' ', 11)]]))] + 
                       [item for _ in range(4) for item in [('MAE', 10), ('MSE', 10), ('RMSE', 10), ('CORR', 10)]]])

    
    
    metrics_labels = [header4, header5]
    
    # Add metrics for each model run
    for i, label in enumerate(labels):
        if i < 2:  # Skip HOLIMO labels
            metrics_labels.append(label)
            continue
            
        try:
            run_id = list(metrics_data.keys())[i-2]
            metrics_str = label
            
            for var in ['Water_concentration', 'Water_meanD', 'Ice_concentration', 'Ice_meanD']:
                if var in metrics_data[run_id]:
                    m = metrics_data[run_id][var]
                    metrics_str += " " + " ".join([
                        f"{m['MAE']:9.2e}" if not np.isnan(m['MAE']) else "    N/A   ",
                        f"{m['MSE']:9.2e}" if not np.isnan(m['MSE']) else "    N/A   ",
                        f"{m['RMSE']:9.2e}" if not np.isnan(m['RMSE']) else "    N/A   ",
                        f"{m['CORR']:9.2f}" if not np.isnan(m['CORR']) else "    N/A   "
                    ])
                else:
                    metrics_str += " " + "    N/A   " * 4
            
            metrics_labels.append(metrics_str)
        except IndexError:
            metrics_labels.append(label)
    
    # Create figure and legend
    figlegend, axlegend = plt.subplots(figsize=(20, 0.2 * (len(metrics_labels) + 1)))
    axlegend.legend([plt.Line2D([], [], alpha=0.0), plt.Line2D([], [], alpha=0.0)] + lines, 
                   metrics_labels, loc='center', prop={'family': 'monospace', 'size': 8})
    axlegend.set_frame_on(False)
    axlegend.set(xticks=[], yticks=[])
    figlegend.tight_layout()
    
    return figlegend, axlegend

