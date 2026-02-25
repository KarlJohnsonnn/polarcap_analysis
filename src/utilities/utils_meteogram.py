#!/usr/bin/env python
# coding: utf-8

import os
import xarray as xr
print(f'xr.__version__: {xr.__version__}')

import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as md
import colormaps as cmaps 

import glob
import numpy as np
import pandas as pd
from io import StringIO

# Add path to utils
import sys
sys.path.append('/work/bb1262/user/schimmel/cosmo-specs-torch/PaperCode/polarcap1')
from utils import cmap_new_timeheight, load_holimo_data, tools


# Dask configuration
import dask.config
from dask_jobqueue import SLURMCluster
from time import sleep as pause
from distributed import Client


# #########################################################################################
# #########################################################################################
# # Coordinates of Eriswil Observatory
COORDINATES_OF_ERISWIL = {
    'ruler_start': (47.05, 7.804),
    'ruler_end': (47.08, 7.90522),
    'seeding': (47.07425, 7.90522),
    'eriswil': (47.070522, 7.872991),
}

def get_closest_station_to_coordinates(station_lat, station_lon, target_lat, target_lon):
    from utils.tools import haversine_distance
    # print(f'dbg:: station_lat: {station_lat}')
    # print(f'dbg:: station_lon: {station_lon}')
    # print(f'dbg:: target_lat: {target_lat}')
    # print(f'dbg:: target_lon: {target_lon}')
    distances = [ haversine_distance(target_lat, target_lon, lat, lon) for lat, lon in zip(station_lat, station_lon) ]
    closest_idx = int(np.argmin(distances))

    print(f"Closest station:         {closest_idx}")
    print(f"Given Coordinates:      ({target_lat:.6f}, {target_lon:.6f})")
    print(f"Coordinates of station: ({station_lat[closest_idx]:.6f}, {station_lon[closest_idx]:.6f})")
    print(f"Distance:                {distances[closest_idx]:.2f} m")
    return closest_idx


def add_station_markers(ax, x_pos, c='orange', a=0.75, s=100, y_offset=0.15):
    y_pos = ax.get_ylim()
    ax.scatter(x_pos, y_pos[1], color=c, marker='v', alpha=a, s=s)
    ax.scatter(x_pos, y_pos[0], color=c, marker='^', alpha=a, s=s)
    ax.vlines(x_pos, y_pos[0]*(1+y_offset), y_pos[1]*(1-y_offset), color=c, alpha=0.3, linewidth=1)


# #########################################################################################
# #########################################################################################
# # Define variable names

Q_vars = [  'QV',       # water vapor
            'QC',       # cloud water (specs)
            'QI',       # cloud ice (specs)
            'Qcrystal', # cloud ice (bin_ind: 30-50)
            'Qdroplet'  # cloud water (bin_ind: 30-50)
            ]
N_vars = [  'Ncrystal', # number conc cloud ice (bin_ind: 30-50)
            'Ndroplet', # number concentration of cloud water (bin_ind: 30-50)
            'NINP'      # number concentration of INP
            ]
r_vars = [  'TKE',      # turbulent kinetic energy
            'Vdroplet', # droplet velocity
            'Vcrystal'  # crystal velocity
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

# #########################################################################################
# #########################################################################################
# # Loading COSMO-SPECS data and other related fucntion


def load_meteogram_data( cs_run, 
                        zarr_file, 
                        extpar_file,
                        meta_file, 
                        holimo_file=None, 
                        slice_station=False, 
                        slice_height=False, 
                        slice_time=None,
                        variable_list=None, 
                        dbg = False):
    

    # check if zarr and extpar exist
    if not os.path.exists(zarr_file):
        raise FileNotFoundError(f'Zarr file {zarr_file} does not exist')
    if not os.path.exists(extpar_file):
        raise FileNotFoundError(f'Extpar file {extpar_file} does not exist')
    if not os.path.exists(meta_file):
        raise FileNotFoundError(f'Meta file {meta_file} does not exist')
    if holimo_file is not None:
        if not os.path.exists(holimo_file):
            raise FileNotFoundError(f'Holimo file {holimo_file} does not exist')

    # load zarr file
    ds_meteogram = xr.open_dataset( zarr_file, engine='zarr', consolidated=False )
    
    # load only required variables or all variables
    req_variables = ['HMLd', 'HHLd', 'RGRENZ_left', 'RGRENZ_right',  'RHO']
    
    if variable_list is  None:
        req_variables.extend( ['NF', 'NW', 'QF', 'QW', 'QFW', 'QV'] )
    elif isinstance(variable_list, str) and variable_list == 'all':
        req_variables = list(ds_meteogram.data_vars)
    elif isinstance(variable_list, list):
        req_variables.extend(variable_list)
    else:
        raise ValueError(f'Invalid variable list: {variable_list}')
        
    req_variables = list(set(req_variables)) # remove duplicates
    
    # select required variables
    ds_meteogram = ds_meteogram[req_variables]

    # slice time to 10:30 - 11:30
    if ds_meteogram.time.values[0] > np.datetime64('2023-01-25T12:00:00'):
        ds_meteogram = ds_meteogram.assign_coords( time = ds_meteogram.time.values - np.timedelta64(2, 'h') )

    if slice_time is not None and isinstance(slice_time, tuple):
        ds_meteogram = ds_meteogram.sel( time = slice(slice_time[0], slice_time[1]) )

    # match altitude and location of meteogram to holimo
    dbg_idx = ds_meteogram.station.size if dbg else None
    station_coords = get_station_coords_from_cfg(meta_file)
    station_lat = np.array(list(station_coords.values()))[:dbg_idx, 0]#, dims='station', attrs={'units': 'deg', 'long_name': 'Latitude'} )
    station_lon = np.array(list(station_coords.values()))[:dbg_idx, 1]#, dims='station', attrs={'units': 'deg', 'long_name': 'Longitude'} )
    ds_meteogram = ds_meteogram.assign_coords( station_lat = station_lat, station_lon = station_lon )

    print(f'dbg:: ds_meteogram.station.size: {ds_meteogram.station.size}')
    print(f'dbg:: station_coords: {ds_meteogram.sizes}')

    # reindex station names (station)
    if ds_meteogram.station.size != len(station_coords):
        print(f'dbg:: ds_meteogram.station.size: {ds_meteogram.station.size}')
        ds_meteogram = ds_meteogram.reindex( station = station_coords )

    ds_meteogram = ds_meteogram.transpose(  'expname', 'station', 'station_lat', 'station_lon', 
                                            'height_level', 'height_level2', 'time', 'bins' )
            
    if slice_station == 'eriswil':
        closest_idx_to_eriswil = get_closest_station_to_coordinates(ds_meteogram.station_lat.values, 
                                                                    ds_meteogram.station_lon.values, 
                                                                    COORDINATES_OF_ERISWIL['eriswil'][0], 
                                                                    COORDINATES_OF_ERISWIL['eriswil'][1] )
        ds_meteogram = ds_meteogram.isel( station = closest_idx_to_eriswil )
        
    elif slice_station and isinstance(slice_station, int) and 0 <= slice_station < ds_meteogram.station.size:
        ds_meteogram = ds_meteogram.isel( station = int(slice_station) )
        
    else:
        print(f'No station selected. Returning all stations.')
        
    
    if 'HHLd' in ds_meteogram.variables and 'HMLd' in ds_meteogram.variables:
        hmld = ds_meteogram.HMLd
        hhld = ds_meteogram.HHLd
    else:
        raise ValueError('HHLd and HMLd not found in dataset')
    
    if 'station' in hmld.sizes.keys():
        hmld = hmld.isel(station=0).values
        hhld = hhld.isel(station=0).values
        
    if hmld[0] > hmld[1]:
        ds_meteogram = ds_meteogram.reindex( HMLd = hmld[..., ::-1] )
        ds_meteogram = ds_meteogram.reindex( HHLd = hhld[..., ::-1] )

    if slice_height == 'holimo':
        holimo_height = tools.load_holimo_data(holimo_file)['instData_Height']
        holimo_height = holimo_height.sel( time = slice(*holimo_timeframe) ).mean().values
        print(f'Holimo height: {holimo_height}')
        ds_meteogram = ds_meteogram.sel( height_level = slice(  holimo_height - 70, holimo_height + 20 ) )
        
    print(f'dbg:1: station_coords: {ds_meteogram.sizes}')
        
    if slice_height is False or slice_height is None:
        print('No height selected. Returning all heights.')
        return ds_meteogram
    
    elif isinstance(slice_height, int):
        ds_meteogram = ds_meteogram.isel( height_level = slice_height )
        
    elif isinstance(slice_height, float):
        ds_meteogram = ds_meteogram.sel( height_level = slice_height )
        
    elif isinstance(slice_height, tuple):
        ds_meteogram = ds_meteogram.sel( height_level = slice(*slice_height) )
        
    else:
        print(f'No height selected. Returning all heights.')
        
    print(f'dbg:2: station_coords: {ds_meteogram.sizes}')
        
    return ds_meteogram


def format_dict_chunks(d, n=5):
    """
    Format a dictionary into chunks of N key-value pairs, separated by newlines.
    
    Args:
        d (dict): The dictionary to format
        n (int): Number of key-value pairs per chunk (default: 5)
        
    Returns:
        str: Formatted string with each chunk on a new line
    """
    
    items = list(d.items())
    chunks = []
    
    for i in range(0, len(items), n):
        chunk = dict(items[i:i+n])
        chunks.append(str(chunk))
    
    return '\n' + '\n'.join(chunks)

def format_list_chunks(lst, n=5):
    """
    Format a list of strings into chunks of N items, separated by newlines.
    
    Args:
        lst (list): The list of strings to format
        n (int): Number of items per chunk (default: 5)
        
    Returns:
        str: Formatted string with each chunk on a new line
    """
    
    chunks = []
    
    for i in range(0, len(lst), n):
        chunk = lst[i:i+n]
        chunks.append(str(chunk))
    
    return '\n' + '\n'.join(chunks)


# def load_cosmo_specs_data(  ncfile_list: list,
#                             height_range: tuple = (1100, 1600),
#                             chunks: dict = {'time': 24},
#                             var_list: list = None,
#                             dbins_water=[30, 50],
#                             dbins_ice=[30, 50]):
#     """
#     Load and process COSMO-SPECS model data files.

#     Parameters:
#     -----------
#     data_dir : str
#         Directory containing the netCDF files
#     pattern : str
#         Glob pattern to match files (default: 'M_00_*.nc')
#     height_range : tuple
#         Height range for vertical selection (default: (800, 2000))
#     chunks : dict
#         Chunking specification for dask (default: {'time': 24})
#     var_list : list
#         List of variables to load (default: None)
#     mode : str
#         Mode to load data (default: 'open_dataset')
#     dbins_water : list
#         List of bin indices for water (default: [30, 50])
#     dbins_ice : list
#         List of bin indices for ice (default: [30, 50])
#     Returns:
#     --------
#     list
#         List of processed xarray datasets
#     """
#     max_time = 422

#     def pad_to_max_time(ds):
#         """Pad datasets to max time dimension with NaN for missing steps (lazy/delayed)."""
#         current_size = ds.time.size
#         if current_size < max_time:
#             missing = max_time - current_size
#             return ds.pad( time=(0, missing), mode='constant', constant_values=np.nan )
#         return ds

#     settings = dict(engine='netcdf4',
#                     parallel=True,
#                     chunks={'time': 50},
#                     preprocess=pad_to_max_time,
#                     combine='nested',
#                     concat_dim='expname')


#     if var_list is None:
#         var_list = ['QV', 'QC', 'QI',  'QF', 'QW',
#                     'HMLd', 'TKE', 'NINP',
#                     'RHO', 'T', 'HHLd',
#                     'NW', 'NF', 'VW', 'VF',
#                     'RGRENZ_left', 'RGRENZ_right']


#     ds = xr.open_mfdataset( ncfile_list, **settings )[var_list]

#     if ds.HHLd.values[0] > ds.HHLd.values[-1]:
#         ds = ds.reindex(HHLd=ds['HHLd'].values[::-1],
#                         HMLd=ds['HMLd'].values[::-1])

#     ds = ds.sel(time=slice(*time_frame_tbs),
#                 HHLd=slice(*height_range),
#                 HMLd=slice(*height_range))


#     # print(f'dbg:: ds.HHLd.values: {ds.HHLd.values}')
#     # print(f'dbg:: ds.HMLd.values: {ds.HMLd.values}')
#     #ds = ds.assign_coords(time=ds['time'].values - np.timedelta64(3, 'h'))
#     ds = ds.assign_coords( bins = (ds['RGRENZ_left'][0, :].values + ds['RGRENZ_right'][0, :].values) * 1.0e6 )
#     ds.bins.attrs['units'] = 'µm'
#     ds.bins.attrs['longname'] = 'diameter boundaries (left + right) * 1e6 converted from m and radius to diameter'

#     ds['Ncrystal'] = xr.DataArray( ds['NF'].isel( bins = slice(*dbins_ice)   ).sum(dim='bins'), attrs=ds['NF'].attrs)
#     ds['Ndroplet'] = xr.DataArray( ds['NW'].isel( bins = slice(*dbins_water) ).sum(dim='bins'), attrs=ds['NW'].attrs)
#     ds['Qcrystal'] = xr.DataArray( ds['QF'].isel( bins = slice(*dbins_ice)   ).sum(dim='bins'), attrs=ds['QF'].attrs)
#     ds['Qdroplet'] = xr.DataArray( ds['QW'].isel( bins = slice(*dbins_water) ).sum(dim='bins'), attrs=ds['QW'].attrs)
#     ds['Vcrystal'] = xr.DataArray( ds['VF'].isel( bins = slice(*dbins_ice)   ).sum(dim='bins'), attrs=ds['VF'].attrs)
#     ds['Vdroplet'] = xr.DataArray( ds['VW'].isel( bins = slice(*dbins_water) ).sum(dim='bins'), attrs=ds['VW'].attrs)

#     expname_list = [ncfile.split('/')[-1].split('.')[0].split('_')[-1] for ncfile in ncfile_list]
#     ds = ds.assign_coords( expname = expname_list )
#     ds.attrs['cs_run'] = ncfile_list[0].split('/')[-2]

#     return ds


# #########################################################################################
# #########################################################################################
# # Create height matrix and add stations height to ds


def create_height_matrix(ds):
    # Suppose meteogram_data is your xarray.Dataset
    stations = ds.station.values
    n_stations = len(stations)
    n_heights = ds.HMLd.shape[1] if ds.HMLd.ndim > 1 else ds.HMLd.shape[0]

    # Build a 2D array of heights: shape (n_heights, n_stations)
    height_matrix = np.zeros((n_heights, n_stations))
    station_matrix = np.zeros((n_heights, n_stations))
    for i, station in enumerate(stations):
        # If heights are different per station:
        height_matrix[:, i] = ds.HMLd.isel(station=i).values
    for j in range(n_heights):
        station_matrix[j, :] = ds.station.values

    return height_matrix, station_matrix


def add_stations_height_to_ds(ds, extpar_file):
    extpar = xr.open_dataset(extpar_file)
    extpar_lat, extpar_lon = extpar.lat.values, extpar.lon.values
    stations_lat, stations_lon = ds.station_lat.values, ds.station_lon.values


    # Extract surface heights at meteogram station locations (one-liner)
    station_heights = []
    for lat, lon in zip(stations_lat.ravel(), stations_lon.ravel()):
        idx = np.argmin( (extpar_lat - lat)**2 + (extpar_lon - lon)**2)
        station_heights.append( extpar.HSURF.values.ravel()[idx] )

    station_heights = np.array(station_heights).reshape(stations_lat.shape)
    ds = ds.assign_coords(station_height = station_heights, dims='station')
    ds.station_height.attrs = {'units': 'm', 'long_name': 'Surface height above mean sea level'}

    print(f"Domain extent: Longitude [{extpar.lon.min().values:.4f}, {extpar.lon.max().values:.4f}]°")
    print(f"Domain extent: Latitude [{extpar.lat.min().values:.4f}, {extpar.lat.max().values:.4f}]°")
    print(f"Elevation range: [{extpar.HSURF.min().values:.1f}, {extpar.HSURF.max().values:.1f}] m")
    print(f"Stations height: {station_heights} m")

    return ds


# #########################################################################################
# #########################################################################################
# # Plot meteogram for COSMO-SPECS model data

def plot_cosmo_specs_meteogram(ds_M_list, norms=None, plt_style='seaborn-v0_8-paper'):
    """
    Create a meteogram plot for COSMO-SPECS model data.

    Parameters:
    -----------
    ds_M_list : list
        List of xarray datasets to plot
    log_norms : list
        List of logarithmic normalizations for each variable
    pmesh_kwargs : dict
        Keyword arguments for pcolormesh plots
    contour_kwargs_white : dict
        Keyword arguments for white contour lines
    contour_kwargs_black : dict
        Keyword arguments for black contour lines
    t_z : xarray.DataArray
        Temperature data for contour overlay

    Returns:
    --------
    tuple
        Figure and axes objects (fig, ax)
    """
    plt.style.use(plt_style)

    plot_var_list = ['QV', 'QC', 'QI', 'Ndroplet', 'Ncrystal', 'TKE', 'NINP', 'Vdroplet', 'Vcrystal',]
    nvars = len(plot_var_list)

    t_z    = (ds_M_list.isel(expname=0)['T'] - 273.15)
    minor_contour_levels = np.arange( -10, -2, 0.5 )  # Temperature levels from 0°C to -10°C

    contour_kwargs_black = {'levels': minor_contour_levels,
                            'colors': 'black',
                            'linestyles': 'dashed',
                            'linewidths': 0.35,
                            'alpha': 0.9,}

    contour_kwargs_white = {'levels': minor_contour_levels,
                            'colors': 'white',
                            'linestyles': 'dashed',
                            'linewidths': 1,
                            'alpha': 0.9,}

    pmesh_kwargs = {    'cmap': cmap_new_timeheight,
                        'add_colorbar': False}

    contour_text_kwargs = dict( boxstyle="round,pad=0.2",
                                facecolor='white',
                                edgecolor='gray',
                                alpha=0.7 )
    if norms is None:
        norms = [   mpl.colors.LogNorm(vmin=1e0, vmax=1e+1), # qv
                    mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # qc
                    mpl.colors.LogNorm(vmin=1e-6, vmax=1e-3), # qi
                    mpl.colors.LogNorm(vmin=1e4, vmax=1e8), # ndroplet
                    mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # Ncrystal
                    mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # tke
                    mpl.colors.LogNorm(vmin=1e-6, vmax=1e-1), # ninp
                    mpl.colors.NoNorm(vmin=-3, vmax=1), # vw
                    mpl.colors.NoNorm(vmin=-5, vmax=1), # vf
                    ]

    fig, ax = plt.subplots( nrows=ds_M_list.expname.values.size,
                            ncols=nvars,
                            figsize=(4*nvars, 3*ds_M_list.expname.values.size),
                            #constrained_layout=True,
                            sharex=True,
                            sharey=True)

    pi_list = []
    for expname, ax_col in zip(ds_M_list.expname.values, ax):
        ds = ds_M_list.sel(expname=expname)
        rho = ds['RHO']
        ds['QV'] = ds['QV']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
        ds['QC'] = ds['QC']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
        ds['QI'] = ds['QI']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
        ds['Ncrystal'] = ds['Ncrystal']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3
        ds['Ndroplet'] = ds['Ndroplet']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3
        ds['TKE'] = ds['TKE']
        ds['NINP'] = ds['NINP']
        ds['Vdroplet'] = ds['Vdroplet']
        ds['Vcrystal'] = ds['Vcrystal']

        for iax, var, norm in zip(ax_col, plot_var_list, norms):
            pi = ds[var].T.plot(ax=iax, norm=norm, **pmesh_kwargs)
            pi_list.append(pi)
            t_z.T.plot.contour( ax = iax, **contour_kwargs_white)
            pcon = t_z.T.plot.contour( ax = iax, **contour_kwargs_black)

            minor_labels = iax.clabel(pcon, inline=True, fontsize=6, fmt='%.1f°C', colors='black')
            for label in minor_labels:
                label.set_bbox(contour_text_kwargs)


    # Place colorbars below the last row, horizontally oriented
    for i in range(nvars):
        fig.colorbar(pi_list[-(i+1)], ax=ax[-1, -(i+1)], orientation='horizontal')

    ax[0, 0].set(title='QV [kg/kg]')
    ax[0, 1].set(title='QC [kg/kg]')
    ax[0, 2].set(title='QI [kg/kg]')
    ax[0, 3].set(title='Ndroplet [1/m^3]')
    ax[0, 4].set(title='Ncrystal [1/m^3]')
    ax[0, 5].set(title='TKE [m^2/s^2]')
    ax[0, 6].set(title='NINP [1/m^3]')
    ax[0, 7].set(title='Vdroplet [m/s]')
    ax[0, 8].set(title='Vcrystal [m/s]')

    # remove labels to save space between subplots
    for i in range(ds_M_list.expname.size):
        for j in range(nvars):
            # hide x-axis labels and ticks for all rows except the last one
            if i < ds_M_list.expname.size - 1:
                ax[i, j].set(xlabel='', xticklabels=[])

            # hide y-axis labels and ticks for all columns except the first one
            if j > 0:
                ax[i, j].set(ylabel='',)

        ax[i, 0].set(ylabel='altitude [km]')



    # Convert y-axis labels from meters to kilometers
    # for i in range(ds_M_list.expname.size):
    for j in range(nvars):


        ax[-1, j].set(ylabel='time [UTC]')
    return fig, ax




# #########################################################################################
# #########################################################################################
# # Plot meteogram for COSMO-SPECS model data

# def plot_cosmo_specs_single_spectra(ds_M_list, norms=None, plt_style='seaborn-v0_8-paper', seeding_nr=1):
#     """
#     Create a meteogram plot for COSMO-SPECS model data.

#     Parameters:
#     -----------
#     ds_M_list : list
#         List of xarray datasets to plot
#     log_norms : list
#         List of logarithmic normalizations for each variable
#     pmesh_kwargs : dict
#         Keyword arguments for pcolormesh plots
#     contour_kwargs_white : dict
#         Keyword arguments for white contour lines
#     contour_kwargs_black : dict
#         Keyword arguments for black contour lines
#     t_z : xarray.DataArray
#         Temperature data for contour overlay

#     Returns:
#     --------
#     tuple
#         Figure and axes objects (fig, ax)
#     """
#     plt.style.use(plt_style)

#     plot_var_list = ['QV', 'QC', 'QI', 'Ndroplet', 'Ncrystal', 'TKE', 'NINP', 'Vdroplet', 'Vcrystal',]
#     nvars = len(plot_var_list)

#     t_z    = (ds_M_list.isel(expname=0)['T'] - 273.15)
#     minor_contour_levels = np.arange( -10, -2, 0.5 )  # Temperature levels from 0°C to -10°C

#     contour_kwargs_black = {'levels': minor_contour_levels,
#                             'colors': 'black',
#                             'linestyles': 'dashed',
#                             'linewidths': 0.35,
#                             'alpha': 0.9,}

#     contour_kwargs_white = {'levels': minor_contour_levels,
#                             'colors': 'white',
#                             'linestyles': 'dashed',
#                             'linewidths': 1,
#                             'alpha': 0.9,}

#     pmesh_kwargs = {    'cmap': cmap_new_timeheight,
#                         'add_colorbar': False}

#     contour_text_kwargs = dict( boxstyle="round,pad=0.2",
#                                 facecolor='white',
#                                 edgecolor='gray',
#                                 alpha=0.7 )
#     if norms is None:
#         norms = [   mpl.colors.LogNorm(vmin=1e0, vmax=1e+1), # qv
#                     mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # qc
#                     mpl.colors.LogNorm(vmin=1e-6, vmax=1e-3), # qi
#                     mpl.colors.LogNorm(vmin=1e4, vmax=1e8), # ndroplet
#                     mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # Ncrystal
#                     mpl.colors.LogNorm(vmin=1e-2, vmax=1e+1), # tke
#                     mpl.colors.LogNorm(vmin=1e-6, vmax=1e-1), # ninp
#                     mpl.colors.NoNorm(vmin=-3, vmax=1), # vw
#                     mpl.colors.NoNorm(vmin=-5, vmax=1), # vf
#                     ]

#     fig, ax = plt.subplots( nrows=2,
#                             ncols=nvars,
#                             figsize=(4*nvars, 3),
#                             #constrained_layout=True,
#                             sharex=True,
#                             sharey=True)

#     pi_list = []
#     for expname, ax_col in zip(ds_M_list.expname.values, ax):
#         ds = ds_M_list.sel(expname=expname)
#         ds = ds.sel(time=slice(*time_frames_plume[seeding_nr])).mean(dim='time')

#         rho = ds['RHO']
#         ds['QV'] = ds['QV']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
#         ds['QC'] = ds['QC']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
#         ds['QI'] = ds['QI']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/cm3
#         ds['Ncrystal'] = ds['Ncrystal']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3
#         ds['Ndroplet'] = ds['Ndroplet']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3
#         ds['TKE'] = ds['TKE']
#         ds['NINP'] = ds['NINP']
#         ds['Vdroplet'] = ds['Vdroplet']
#         ds['Vcrystal'] = ds['Vcrystal']

#         for iax, var, norm in zip(ax_col, plot_var_list, norms):
#             pi = ds[var].T.plot(ax=iax, norm=norm, )#**pmesh_kwargs)
#             pi_list.append(pi)

#     ax[0, 0].set(title='QV [kg/kg]')
#     ax[0, 1].set(title='QC [kg/kg]')
#     ax[0, 2].set(title='QI [kg/kg]')
#     ax[0, 3].set(title='Ndroplet [1/m^3]')
#     ax[0, 4].set(title='Ncrystal [1/m^3]')
#     ax[0, 5].set(title='TKE [m^2/s^2]')
#     ax[0, 6].set(title='NINP [1/m^3]')
#     ax[0, 7].set(title='Vdroplet [m/s]')
#     ax[0, 8].set(title='Vcrystal [m/s]')

#     # remove labels to save space between subplots
#     # for i in range(ds_M_list.expname.size):
#     #     for j in range(nvars):
#     #         # hide x-axis labels and ticks for all rows except the last one
#     #         if i < ds_M_list.expname.size - 1:
#     #             ax[i, j].set(xlabel='', xticklabels=[])

#     #         # hide y-axis labels and ticks for all columns except the first one
#     #         if j > 0:
#     #             ax[i, j].set(ylabel='',)

#     #     ax[i, 0].set(ylabel='altitude [km]')



#     # Convert y-axis labels from meters to kilometers
#     # for i in range(ds_M_list.expname.size):
#     # for j in range(nvars):
#     #     ax[-1, j].set(ylabel='time [UTC]')
#     return fig, ax

# #########################################################################################
# #########################################################################################
# # Helper Functions


def define_bin_boundaries(n_bins = 66, n_max = 2, r_min = 1.0e-9, rhow = 1.0e3, verbose=True):
    """Define bin boundaries from bin edges. Radius of computes in units of m 
    n_bins: number of bins
    n_max: controls mass ratio between adjacent bins
    verbose: print verbose output
    r_min: minimum radius in m
    rhow: density of water in kg/m^3
    """
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min**3
    j0w = (n_max - 1.0) / np.log(2.0)
    mbin_edges = m0w * np.exp(np.arange(n_bins + 1) / j0w)
    rbin_edges = np.cbrt(mbin_edges / fact)
    mbin_centers = (mbin_edges[1:] + mbin_edges[:-1]) / 2.0
    rbin_centers = (rbin_edges[1:] + rbin_edges[:-1]) / 2.0
    
    if verbose:
        print(f'mbin_edges.shape: {mbin_edges.shape}, \nmbin_centers.shape: {mbin_centers.shape}, \nrbin_edges.shape: {rbin_edges.shape}, \nrbin_centers.shape: {rbin_centers.shape}')

        print(f'    mbin_edges[i]:        mbin_centers         rbin_edges          rbin_centers')
        for i in range(len(mbin_edges)):
            try:
                print(f'{mbin_edges[i]:18.4e}  {mbin_centers[i]:18.4e}  {rbin_edges[i]:18.4e}  {rbin_centers[i]:18.4e}  {2e6*rbin_edges[i]:18.4e}  {2e6*rbin_centers[i]:18.4e}')
            except:
                print(f'{mbin_edges[i]:18.4e}  {"-"*18}  {rbin_edges[i]:18.4e}  {"-"*18}  {2e6*rbin_edges[i]:18.4e}  {"-"*18} ')
            
    return mbin_edges, mbin_centers, rbin_edges, rbin_centers


def get_station_coords_from_cfg(meta_file):
    """Extract station coordinates from configuration file.

    Args:
        cfg_dict: Configuration dictionary loaded from JSON

    Returns:
        Dictionary mapping station IDs to (lat, lon) tuples
    """
    import json
    if not os.path.exists(meta_file):
        raise FileNotFoundError(f"Meta file {meta_file} not found!")

    with open(meta_file, 'r') as f:
        cfg_dict = json.load(f)
        expname = list(cfg_dict.keys())[0]
        stationlist = cfg_dict[expname]['INPUT_DIA']['diactl']['stationlist_tot']

    if not stationlist:
        raise ValueError("Station list is empty!")

    # Process the flat list in groups of 5: INT, INT, FLOAT, FLOAT, STRING
    stationlist = np.array(stationlist).reshape(-1, 5)
    n_stations = len(stationlist)

    station_coords = {}
    for i in range(n_stations):
        station_id = str(stationlist[i, -1].split('_')[0])
        if station_id in ['SE', 'OB']:
            continue
        station_coords[station_id] = (float(stationlist[i, 2]), float(stationlist[i, 3]))

    return station_coords

# meta_file = f'{data_dir}/{cs_run}.json'
# STATION_COORDS = get_station_coords_from_cfg(meta_file)
# for station in ['SE', 'OB']:
#     if station in STATION_COORDS:
#         STATION_COORDS.pop(station)


# print(f'STATION_COORDS: ')
# STATION_COORDS




def find_prime_factors(n):
    """Find all prime factors of a number."""
    factors = []
    d = 2
    while d * d <= n:
        while n % d == 0:
            factors.append(d)
            n //= d
        d += 1
    if n > 1:
        factors.append(n)
    return factors

def is_prime(n):
    """Check if a number is prime."""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True


def debug_prime_factors(data):
    # Dictionary values to analyze - extract first element if tuple
    processed_data = {}
    for key, value in data.items():
        if isinstance(value, tuple):
            processed_data[key] = value[0]  # Take first element of tuple
        else:
            processed_data[key] = value

    print("Prime Factor Analysis:")
    print("=" * 50)

    dict_prime_factors = {}
    for key, value in processed_data.items():
        factors = find_prime_factors(value)
        prime_status = is_prime(value)
        dict_prime_factors[key] = {'factors': factors, 'prime_status': prime_status}

    print("\n" + "=" * 50)
    print("Summary:")
    print("Numbers that CANNOT be chunked (primes):")
    for key, value in processed_data.items():
        if is_prime(value):
            print(f"  - {key}: {value}")

    print("\nNumbers that CAN be chunked (composite):")
    for key, value in processed_data.items():
        if not is_prime(value):
            factors = find_prime_factors(value)
            print(f"  - {key}: {value} = {' × '.join(map(str, factors))}")

    return dict_prime_factors


# #########################################################################################
# #########################################################################################
# # Plot Helper Functions

def plot_contour_temperature(ax, x, y, T):
    t_z = (T - 273.15)
    minor_contour_levels = np.arange( -7, -2, 0.5 )  # Temperature levels from 0°C to -10°C
    contour_kwargs_black = {'levels': minor_contour_levels, 'colors': 'black', 'linestyles': '--', 'linewidths': 0.55, 'alpha': 0.9,}
    contour_kwargs_white = {'levels': minor_contour_levels, 'colors': 'white', 'linestyles': '--', 'linewidths': 1.25, 'alpha': 0.9,}
    pconw = ax.contour(x, y, t_z, **contour_kwargs_white)
    pconb = ax.contour(x, y, t_z, **contour_kwargs_black)
    #minor_labels = ax.clabel(pconw, inline=True, fontsize=12, fmt='%.1f°C', colors='white',)
    minor_labels = ax.clabel(pconb, inline=True, fontsize=11, fmt='%.1f°C', colors='black')
    # Add white outline to the text labels
    for label in minor_labels:
        label.set_path_effects([mpl.patheffects.withStroke(linewidth=2, foreground='white')])
    return pconb, pconw, minor_labels

def create_station_xtick_labels(station_lat, station_lon, num_xticks=None):
    """Create xtick labels with lat/lon in two rows."""
    station_lat, station_lon = np.array(station_lat), np.array(station_lon)
    num_xticks = len(station_lat) if num_xticks is None else num_xticks

    if num_xticks >= len(station_lat):
        indices = np.arange(len(station_lat))
    else:
        step = len(station_lat) / (num_xticks - 1) if num_xticks > 1 else 1
        indices = np.round(np.arange(0, len(station_lat), step)).astype(int)
        indices = indices[indices < len(station_lat)]
        if len(indices) < num_xticks:
            indices = np.append(indices, len(station_lat) - 1)

    xtick_labels = [f"{station_lat[idx]:.3f}°N\n{station_lon[idx]:.3f}°E" for idx in indices]
    return xtick_labels, indices


def pcolormesh_stations_height_data(fig, ax, X, Y, Z, cmap='viridis', label='', lat=None, lon=None, temperature=None, ix_obs=None, ix_sed=None, zscale='linear', vlim=None, norm=None ):
        
    if norm is None:
        
        if zscale == 'log':
            # vlim = [1e-10, 1e10]
            norm = mpl.colors.LogNorm()#vmin=vlim[0], vmax=vlim[1])
        elif zscale == 'symlog':
            vlim, linthresh = [1e-10, 1e10], 1e-8
            norm = mpl.colors.SymLogNorm(vmin=vlim[0], vmax=vlim[1], linthresh=linthresh)
        else:
            vlim = []
            norm = mpl.colors.NoNorm()

    if X is None and Y is None:
        X = np.arange(Z.shape[1])
        Y = np.arange(Z.shape[0])
        pc = ax.pcolormesh(X, Y, Z, cmap=cmap, norm=norm)
        
        if label != '':
            cbar = plt.colorbar(pc, ax=ax)
            cbar.set_label(f'{label}')
            return pc, ax, cbar
        else:
            return pc, ax, None

    if ix_obs is None:
        ix_obs = 10 #     ix_obs = 12
    if ix_sed is None:
        ix_sed = 16 #     ix_sed = 18

    pc = ax.pcolormesh(X, Y, Z, cmap=cmap, norm=norm)
    # ax.plot(X[0,:], Y[-1, :],  color='red', alpha=0.75)
    # ax.scatter(X[0,ix_obs], Y[-1, ix_obs],  color='orange', marker='^', alpha=0.75, s=100)
    # ax.scatter(X[0,ix_sed], Y[-1, ix_sed],  color='blue', marker='^', alpha=0.75, s=100)
    ax.set_ylim(700, 1500)
    # ax.set_xlim(-1, 19)
    ax.set_xlabel('station')
    ax.grid(which='both', linestyle='--', alpha=0.25, linewidth=0.15, color='black')
    ax.grid(which='major', linestyle='--', alpha=0.55, linewidth=0.15, color='black')
    if label != '':
        cbar = plt.colorbar(pc, ax=ax)
        cbar.set_label(f'{label}')
    else:
        cbar = None

    if temperature is not None:
        pconb, pconw, minor_labels = plot_contour_temperature(ax, X, Y, temperature.T)

    # Create and set xtick labels
    if lat is not None and lon is not None:
        xtick_labels, tick_positions = create_station_xtick_labels(lat, lon, num_xticks=8)
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(xtick_labels, rotation=45, ha='right')

    ax.set_xlabel('station')



    return pc, ax, cbar


# #########################################################################################
# #########################################################################################
# # Plot Helper Functions for Holimo Data
#

def setup_axes(fig, axes, title=''):
    if axes.ndim == 1:
        axes = axes.reshape(1, -1)

    for ax in axes.flatten():
        ax.set(xlim=time_frame_tbs)
        ax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
        ax.tick_params(axis='both', which='both', direction='out', top=True, right=True)
        ax.grid(which='both', linestyle='--', alpha=0.15, linewidth=0.5, color='black')
        ax.grid(which='minor', linestyle='--', alpha=0.05, linewidth=0.25, color='black')
    fig.suptitle(title)


# # Mean values of the three compartments (+extra variables) in row plot
#




def plot_cosmo_specs_meteogram_mean(ds_M_list, norms=None, style='seaborn-v0_8-talk'):
    """
    Create a meteogram plot for COSMO-SPECS model data.

    Parameters:
    -----------
    ds_M_list : list
        List of xarray datasets to plot
    log_norms : list
        List of logarithmic normalizations for each variable
    pmesh_kwargs : dict
        Keyword arguments for pcolormesh plots
    contour_kwargs_white : dict
        Keyword arguments for white contour lines
    contour_kwargs_black : dict
        Keyword arguments for black contour lines
    t_z : xarray.DataArray
        Temperature data for contour overlay

    Returns:
    --------
    tuple
        Figure and axes objects (fig, ax)
    """
    plt.style.use(style)

    plot_var_list = [   'QV', 'QC', 'QI', 'Qdroplet', 'Qcrystal',
                        'Ndroplet', 'Ncrystal', 'TKE', 'NINP',
                        'Vdroplet', 'Vcrystal']
    titles = [  'QV [g/cm3]', 'QC [g/cm3]', 'QI [g/cm3]', 'Qdroplet [g/cm3]', 'Qcrystal [g/cm3]',
                'Ndroplet [1/cm3]', 'Ncrystal [1/cm3]', 'TKE [m^2/s^2]', 'NINP [1/cm3]',
                'Vdroplet [m/s]', 'Vcrystal [m/s]']
    nvars = len(plot_var_list)

    pmesh_kwargs = {'add_legend': False}

    if norms is None:
        norms = [[2e0, 1e+1], # qv
                [1e-2, 1e+1], # qc
                [1e-6, 1e-3], # qi
                [1e+6, 1e+8], # qdroplet
                [1e+6, 1e+8], # qcrystal
                [1e-2, 1e+1], # Ncrystal
                [1e-1, 1e+1], # tke
                [1e-6, 1e-1], # ninp
                [-3, 1], # vdroplet
                [-5, 1], # vcrystal
                ]
        scales = ['linear', 'linear', 'linear', 'linear', 'linear', 'log', 'log', 'log', 'log', 'linear', 'linear']

    ds_plot = ds_M_list
    rho = ds_plot['RHO']
    ds_plot['QV'] = ds_plot['QV']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/m3
    ds_plot['QC'] = ds_plot['QC']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/m3
    ds_plot['QI'] = ds_plot['QI']              * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/m3
    ds_plot['Qcrystal'] = ds_plot['Qcrystal']  * rho * 1.0e+3 # kg/kg  * kg/m3 * 1e6 -> g/m3
    ds_plot['Ndroplet'] = ds_plot['Ndroplet']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3
    ds_plot['Ncrystal'] = ds_plot['Ncrystal']  * rho * 1.0e-3 # 1/kg  * 1/m3 * 1e-3 -> 1/cm3


    fig, ax = plt.subplots( ncols=nvars, nrows=1, figsize=(nvars*3, 7),)

    for ivar, (var, norm, scale, title) in enumerate(zip(plot_var_list, norms, scales, titles)):
        ds_variable_line = ds_plot[var].mean(dim=['time'])
        ds_variable_mean = ds_plot[var].mean(dim=['time', 'expname']) # time
        ds_variable_std  = ds_plot[var].std(dim=['time', 'expname']) # time
        ax.flat[ivar].set(title=title, #ylim=(norm[0], norm[1]),
                        xscale=scale)

        if 'HHLd' in ds_plot[var].dims:
            pi_line = ds_variable_line.T.plot.line(ax=ax.flat[ivar], y='HHLd', **pmesh_kwargs)
            pi_mean = ds_variable_mean.T.plot.line(ax=ax.flat[ivar], y='HHLd', **pmesh_kwargs)
            pi_std  = ds_variable_std.T.plot.line(ax=ax.flat[ivar], y='HHLd', **pmesh_kwargs)
            for i in [-2., -1., +1., +2.]:
                pi_std = (ds_variable_mean + i * ds_variable_std).T.plot.line(
                    ax=ax.flat[ivar], y='HHLd',  linestyle='--', **pmesh_kwargs)
        elif 'HMLd' in ds_plot[var].dims:
            pi_line = ds_variable_line.T.plot.line(ax=ax.flat[ivar], y='HMLd', **pmesh_kwargs)
            pi_mean = ds_variable_mean.T.plot.line(ax=ax.flat[ivar], y='HMLd', **pmesh_kwargs)
            pi_std = ds_variable_std.T.plot.line(ax=ax.flat[ivar], y='HMLd', **pmesh_kwargs)
            for i in [-2., -1., +1., +2.]:
                pi_std = (ds_variable_mean + i * ds_variable_std).T.plot.line(
                    ax=ax.flat[ivar], y='HMLd',  linestyle='--', **pmesh_kwargs)
        else:
            raise ValueError(f"Variable {var} has unsupported dimensions: {ds_plot[var].dims}")


    # remove labels to save space between subplots
    for j in range(nvars):
        ax.flat[j].set(label='')
    #     # hide y-axis labels and ticks for all columns except the first one
    #     if j > 0:
    #
    fig.tight_layout()
    return fig, ax



# # Compute and Plot Bulk Time Series




def calculate_bulk_timeseries(dsm_in, lbb=[30, 50], cbb=[30, 50], var_list=None):
    from utils.tools import calculate_mean_diameter

    if var_list is None:
        var_list = ['NW', 'NF', 'QW', 'QF', 'QV', 'QFW']

    # convert mixing raitos to number/mass concentrations per 1/cm3 and g/m3
    dsm = dsm_in[var_list] * dsm_in['RHO']
    dsm.attrs = dsm_in.attrs

    dsm['NW'] = dsm['NW'] * 1.0e-6          #  1/kg * kg/m3 to 1/cm3  (*1e-6)
    dsm['NF'] = dsm['NF'] * 1.0e-6          #  1/kg * kg/m3 to 1/cm3  (*1e-6)
    dsm['QV'] = dsm['QV'] * 1.0e+3          # kg/kg * kg/m3 to g/m3   (*1e+3)
    dsm['QW'] = dsm['QW'] * 1.0e+3          # kg/kg * kg/m3 to g/m3   (*1e+3)
    dsm['QF'] = dsm['QF'] * 1.0e+3          # kg/kg * kg/m3 to g/m3   (*1e+3)
    dsm['QFW'] = dsm['QFW'] * 1.0e+3          # kg/kg * kg/m3 to g/m3   (*1e+3)

    dsm['NW'].attrs['units'] = '1/cm3'
    dsm['NF'].attrs['units'] = '1/cm3'
    dsm['QV'].attrs['units'] = 'g/m3'
    dsm['QW'].attrs['units'] = 'g/m3'
    dsm['QF'].attrs['units'] = 'g/m3'
    dsm['QFW'].attrs['units'] = 'g/m3'    

    dims = []
    coords = {}
    # coordinates have to be in this order: expname, station, height_level, time
    if 'expname' in dsm.dims:
        dims = dims + ['expname']
        coords['expname'] = dsm_in.expname

    if 'station' in dsm.dims:
        dims = dims + ['station']
        coords.update( {'station': dsm.station} )

    if 'height_level' in dsm.dims:
        dims = dims + ['height_level']
        coords['height_level'] = dsm_in.height_level

    if 'time' in dsm.dims:
        dims = dims + ['time']
        coords['time'] = dsm.time
        
    dsm = dsm.transpose( *(dims + ['bins']) )
    
    # Calculate diameter bins, removing non-bins dimensions
    r_bins = dsm_in['radius_centers']
    d_bins = r_bins.isel(**{dim: 0 for dim in r_bins.dims if dim != 'bins'}).values * 2e+6
    nw_ma = np.ma.masked_less_equal(dsm['NW'].isel( bins = slice(*lbb) ).values, 0)
    nf_ma = np.ma.masked_less_equal(dsm['NF'].isel( bins = slice(*cbb) ).values, 0)
    
    dsm['mdw_bulk'] = xr.DataArray( calculate_mean_diameter(nw_ma, d_bins[lbb[0]:lbb[1]] ), dims=dims, coords=coords )
    dsm['mdf_bulk'] = xr.DataArray( calculate_mean_diameter(nf_ma, d_bins[cbb[0]:cbb[1]] ), dims=dims, coords=coords )
    dsm['mdw_bulk'].attrs['units'] = 'µm'
    dsm['mdf_bulk'].attrs['units'] = 'µm'

    dsm['nw_bulk'] = xr.DataArray(  dsm['NW'].isel( bins = slice(*lbb) ).sum( dim='bins' ), dims=dims, coords=coords )
    dsm['nf_bulk'] = xr.DataArray(  dsm['NF'].isel( bins = slice(*cbb) ).sum( dim='bins' ), dims=dims, coords=coords )
    dsm['qw_bulk'] = xr.DataArray(  dsm['QW'].isel( bins = slice(*lbb) ).sum( dim='bins' ), dims=dims, coords=coords )
    dsm['qf_bulk'] = xr.DataArray(  dsm['QF'].isel( bins = slice(*cbb) ).sum( dim='bins' ), dims=dims, coords=coords )
    dsm['qfw_bulk'] = xr.DataArray(  dsm['QFW'].isel( bins = slice(*cbb) ).sum( dim='bins' ), dims=dims, coords=coords )

    dsm['nw_bulk'].attrs['units'] = dsm['NW'].attrs['units']
    dsm['nf_bulk'].attrs['units'] = dsm['NF'].attrs['units']
    dsm['qw_bulk'].attrs['units'] = dsm['QW'].attrs['units']
    dsm['qf_bulk'].attrs['units'] = dsm['QF'].attrs['units']
    dsm['qfw_bulk'].attrs['units'] = dsm['QFW'].attrs['units']
    
    dsm = dsm.assign_coords( bins = d_bins)
    dsm = dsm.assign_coords(station_lat = dsm_in.station_lat, station_lon = dsm_in.station_lon )
    
    #update dsm with variables from dsm_in
    for var in dsm.data_vars:
        dsm_in[var] = dsm[var]

    return dsm_in



# def plot_1d_model_bulk_ts(fig, axes, dsm, config_files=None, ylims=None, yscale='log'):
#     #plot_time_frame = [np.datetime64("2023-01-25T10:20:00"), np.datetime64("2023-01-25T11:36:00")]

#     assert len(config_files) > 0, 'number of ensemble config files must be greater than 0'
    
#     # count number of lines for color and marker lists
#     N_lines = sum(dsm.sizes.get(dim, 0) for dim in ['expname', 'station'])
#     colors_lines = tools.cmap_ensemble_lines_r(np.linspace(0, 1, N_lines))
#     markers_list = (['o', 's', 'D', 'v', '^', '<', '>', 'p', '*'])[:N_lines]

#     colors_lines_list = [colors_lines]

#     # ensemble line styles, colored and black
#     style2 = {'alpha': 0.55, 'lw': 0.15, 'linestyle': '--', 'color': 'black'}
#     style1 = {'alpha': 0.85, 'lw': 1.5, 'linestyle': '-', 'markersize': 4.5, 'markevery': 8}

#     variables = ['nw_bulk', 'nf_bulk', 'mdw_bulk', 'mdf_bulk', 'qw_bulk', 'qfw_bulk']
#     positions = [(0,0), (0,1), (1,0), (1,1), (2,0), (2,1)]
#     axis_labels = [r"N$_{liq}$", r"N$_{ice}$", r"D$_{liq}$", r"D$_{ice}$", r"C$_{liq}$", r"C$_{ice}$"]
#     if ylims is None:
#         ylims = [(1, 1100), (5e-8, 3), (1, 15), (40, 300), (0, 0.5), (1e-5, 1)]

#     if yscale == 'log':
#         log_axes = [True, True, False, False, False, True]
#     else:
#         log_axes = [False, False, False, False, False, False]

#     # Plot ensemble lines
#     lines_list = []
#     labels_list = []

#     if 'expname' in dsm.sizes.keys():
#         for iexp in dsm.expname.values:
#             if isinstance(iexp, str):
#                 continue
#             labels_list.append( tools.format_model_label(
#                 config_files[0][str(iexp)], int(iexp), f'Model ({int(iexp):14d}) '))

#     if 'station' in dsm.sizes.keys():
#         for istation in dsm.station.values:
#             if isinstance(istation, str):
#                 continue
#             labels_list.append(f'Station ({int(istation):14d}) - {dsm.station_lat.values[istation]} - {dsm.station_lon.values[istation]}')

#     for colors, marker in zip(colors_lines_list, markers_list): #

#         # Create label using new format_model_label function
#         for ivar, (var, (i, j)) in enumerate(zip(variables, positions)):
#             ax = axes[i, j]

#             # lines = dsm[var].squeeze().plot.line(ax=ax, x='time', hue=dsm[var].dims[1], marker=marker, add_legend=False, **style1)
#             # dsm[var].squeeze().plot.line(ax=ax, x='time', hue=dsm[var].dims[1], add_legend=False, **style2)
            
#             try:
#                 lines = dsm[var].plot.line(ax=ax, x='time', hue='expname', marker=marker, add_legend=False, **style1)
#                 dsm[var].plot.line(ax=ax, x='time', hue='expname', add_legend=False, **style2)
#                 print(f'dbg:: {var} plot.line with expname dimension')
#             except:
#                 try:
#                     lines = dsm[var].squeeze().plot.line(ax=ax, x='time', hue='station', marker=marker, add_legend=False, **style1)
#                     dsm[var].squeeze().plot.line(ax=ax, x='time', hue='station', add_legend=False, **style2)
#                     print(f'dbg:: {var} plot.line with station dimension')
#                 except:
#                     lines = dsm[var].squeeze().plot.line(ax=ax, x='time', marker=marker, add_legend=False, **style1)
#                     dsm[var].squeeze().plot.line(ax=ax, x='time', add_legend=False, **style2)
#                     print(f'dbg:: {var} plot.line at station {dsm.station.values}')
#             print(f'dbg::which wahyn')
#             mtime = dsm[var].mean()
#             title_str = f'(time series) mean({var})   = {mtime.values:.2f} {mtime.attrs["units"]}'
#             ax.set_title(title_str)

#             for line, color in zip(lines, colors):
#                 line.set_color(color)
#                 if ivar == 0:
#                     lines_list.append(line)



#     for ax, label, ylim, is_log in zip(axes.flat, axis_labels, ylims, log_axes):
#         ax.text(1.0, 1.01, label, transform=ax.transAxes, fontsize=24,
#                 fontweight="bold", va="bottom", ha="right", zorder=99)
#         # ax.set_ylim(*ylim)
#         #ax.set_xlim(*plot_time_frame)
#         if is_log:
#             ax.set_yscale('log')
#         ax.set_xlabel('time (UTC)')

#     return fig, axes, lines_list, labels_list


# #########################################################################################
# #########################################################################################
# # Plot Holimo data
#




def plot_holimo_bulk_ts( fig, axes, hd, ylim=None, yscale='linear', formatter='decimal'):

    linewidth = 1.
    alpha = 0.85
    hd.Water_concentration.plot(ax=axes[0,0], yscale=yscale, alpha=alpha, linewidth=linewidth)
    hd.Water_meanD.plot(ax=axes[1,0], alpha=alpha, linewidth=linewidth)
    hd.Water_content.plot(ax=axes[2,0], alpha=alpha, linewidth=linewidth)
    hd.Ice_concentration.plot(ax=axes[0,1], yscale=yscale, alpha=alpha, linewidth=linewidth)
    hd.Ice_meanD.plot(ax=axes[1,1], alpha=alpha, linewidth=linewidth)
    hd.Ice_content.plot(ax=axes[2,1], alpha=alpha, linewidth=linewidth)

    # axes[1,0].plot(hd.time, hd.mdw_bulk, alpha=alpha, linewidth=linewidth)
    # axes[1,1].plot(hd.time, hd.mdf_bulk, alpha=alpha, linewidth=linewidth)

    if ylim is not None:
        axes[0,0].set_ylim(1e1,1e3)
        axes[0,1].set_ylim(2e-3,2e1)
        axes[1,0].set_ylim(8,13)
        axes[1,1].set_ylim(1,300)
        axes[2,0].set_ylim(0.0,0.4)
        axes[2,1].set_ylim(1e-8,2)

    linewidth = 3.5
    alpha = 0.85
    interp_time = hd.time_interp.values
    axes[0,0].plot(interp_time, hd.Water_concentration_interp, alpha=alpha, linewidth=linewidth)
    axes[0,1].plot(interp_time, hd.Ice_concentration_interp, alpha=alpha, linewidth=linewidth)
    axes[1,0].plot(interp_time, hd.Water_meanD_interp, alpha=alpha, linewidth=linewidth)
    axes[1,1].plot(interp_time, hd.Ice_meanD_interp, alpha=alpha, linewidth=linewidth)
    axes[2,0].plot(interp_time, hd.Water_content_interp, alpha=alpha, linewidth=linewidth)
    axes[2,1].plot(interp_time, hd.Ice_content_interp, alpha=alpha, linewidth=linewidth)

    setup_axes(fig, axes, title='')

    if yscale == 'log':
        axes[0, 0].set_yscale('symlog', linthresh=1e0)
        # ax[1, 0].set_yscale('symlog', linthresh=1e0)
        # ax[2, 0].set_yscale('symlog', linthresh=1e0)
        axes[0, 1].set_yscale('symlog', linthresh=1e-3)
        # ax[1, 1].set_yscale('symlog', linthresh=1e-3)
        # ax[2, 1].set_yscale('symlog', linthresh=1e-3)

    # Add custom formatter for y-axis to show decimal notation instead of exponential
    def decimal_formatter(x, pos):
        if x == 0:
            return '0'
        elif abs(x) >= 1:
            return f'{x:.0f}'
        else:
            return f'{x:.1f}'

    # Apply the formatter to ax[0,0] and ax[0,1]
    if formatter == 'decimal':
        axes[0, 0].yaxis.set_major_formatter(plt.FuncFormatter(decimal_formatter))
        axes[0, 1].yaxis.set_major_formatter(plt.FuncFormatter(decimal_formatter))


    # Create lines for legend (only for ax1)
    lines = [   axes[0,0].plot([], [], color='darkblue', alpha=0.4,  linewidth=1.25)[0],
                axes[0,0].plot([], [], color='darkblue', alpha=0.75, linewidth=5.5, zorder=23)[0],
                axes[0,0].plot([], [], color='white',    alpha=1,    linewidth=0.7, linestyle='--', zorder=24)[0] ]

    # Return the lines and label for the legend
    lines_list  = [(lines[0]), (lines[1], lines[2])]
    labels_list = ['HOLIMO 1s (high resolution)', 'HOLIMO 10sec (interpolated to model time, smoothed with 12 point uniform window)']

    return fig, axes, lines_list, labels_list


# #########################################################################################
# #########################################################################################
# # Allocate Resources


def calculate_optimal_scaling(n_time_steps, n_experiments, n_stations, debug_mode=False):
    """Calculate optimal cluster scaling based on workload dimensions.

    Args:
        n_time_steps: Number of time steps in the data
        n_experiments: Number of experiment names
        n_stations: Number of stations
        debug_mode: If True, use minimal resources for testing

    Returns:
        tuple: (n_nodes, n_cpu_per_node, memory_per_node, scale_up_workers, walltime)
    """
    if debug_mode:
        return 1, 64, 32, 2, '00:10:00'

    # Calculate total workload (rough estimate)
    total_workload = n_time_steps * n_experiments * n_stations

    # Base scaling parameters
    base_cpu = 128
    base_memory = 64  # GB
    base_scale_workers = 2
    base_walltime = '02:00:00'

    # Scaling thresholds and multipliers
    if total_workload < 1e5:  # Small workload
        n_nodes = 1
        n_cpu = base_cpu
        memory = base_memory
        scale_workers = base_scale_workers
        walltime = base_walltime

    elif total_workload < 1e6:  # Medium workload
        n_nodes = 2
        n_cpu = base_cpu * 2
        memory = base_memory * 2
        scale_workers = base_scale_workers * 2
        walltime = '06:00:00'

    elif total_workload < 1e7:  # Large workload
        n_nodes = 4
        n_cpu = base_cpu * 2
        memory = base_memory * 3
        scale_workers = base_scale_workers * 4
        walltime = '07:00:00'

    else:  # Very large workload
        n_nodes = 8
        n_cpu = base_cpu * 2
        memory = base_memory * 4
        scale_workers = base_scale_workers * 6
        walltime = '08:00:00'

    # Additional scaling based on number of experiments (parallel loading bottleneck)
    if n_experiments > 50:
        scale_workers = min(scale_workers * 2, 32)  # Cap at 32 workers

    # Additional scaling based on time steps (memory pressure)
    if n_time_steps > 1000:
        memory = min(memory * 1.5, 512)  # Cap at 512GB

    print(f"Workload analysis:")
    print(f"  - Time steps: {n_time_steps}")
    print(f"  - Experiments: {n_experiments}")
    print(f"  - Stations: {n_stations}")
    print(f"  - Total workload estimate: {total_workload}")
    print(f"Optimal scaling:")
    print(f"  - Nodes: {n_nodes}")
    print(f"  - CPU per node: {n_cpu}")
    print(f"  - Memory per node: {memory}GB")
    print(f"  - Scale up workers: {scale_workers}")
    print(f"  - Walltime: {walltime}")

    return n_nodes, n_cpu, memory, scale_workers, walltime


def allocate_resources(n_cpu=16, n_jobs=1, m=0, n_threads_per_process=1, port='7777', part='compute', walltime="02:00:00", account='bb1376'):
    cores_per_node = n_cpu
    processes_per_node = n_cpu // n_threads_per_process
    N_nodes = n_jobs
    memory_per_node_gb = n_cpu if m == 0 else m

    # Dask configuration
    dask.config.set(  { 'distributed.worker.memory.target': False,
                        'distributed.worker.memory.spill': False,
                        'distributed.worker.memory.terminate': 0.95,
                        'array.slicing.split_large_chunks': True, 
                        'distributed.scheduler.worker-saturation': 0.95,
                        'distributed.scheduler.worker-memory-limit': 0.95,
                        } )

    # Fixed cluster configuration
    cluster = SLURMCluster( name='concat_meteos_',
                            cores=cores_per_node,
                            processes=processes_per_node,
                            n_workers=N_nodes,
                            memory=str(memory_per_node_gb) + 'GB',
                            account=account,
                            queue=part,
                            walltime=walltime,
                            scheduler_options = {   "dashboard_address": f":{str(port)}" } ,
                            job_extra_directives=[  '--output=./logs/%j.out',
                                                    '--error=./logs/%j.err',
                                                    '--propagate=STACK',
                                                ],
                            job_script_prologue=[   'source ~/.bashrc',
                                                    'conda activate pcpaper_env',
                                                    'export OMP_NUM_THREADS=' + str(n_threads_per_process),
                                                    'export MKL_NUM_THREADS=' + str(n_threads_per_process),
                                                    'export OPENBLAS_NUM_THREADS=' + str(n_threads_per_process),
                                                    'export VECLIB_MAXIMUM_THREADS=' + str(n_threads_per_process),
                                                    'export NUMEXPR_NUM_THREADS=' + str(n_threads_per_process),
                                                    'ulimit -s unlimited',
                                                    'ulimit -c 0',
                                                ],
                            python='/home/b/b382237/.conda/envs/pcpaper_env/bin/python'
                        )

    # if N_nodes > 1:
    #     cluster.scale(  N_nodes  )

    pause(  5  )

    print(  cluster.job_script()  )
    print(  len(cluster.scheduler.workers)  )

    client = Client(  cluster  )

        # Print dashboard addresses
    dashboard_address = cluster.scheduler_address
    remote_dashboard = f"http://{dashboard_address.split('//')[-1].split(':')[0]}:{port}"
    print(  f"Remote dashboard address: {remote_dashboard}" )
    print(  f"Local dashboard address: http://localhost:{port}" )


    return cluster, client


# #########################################################################################
# #########################################################################################
# # Convert Mixing Ratio to concentration
#
# # Example usage:
# mixing_ratio = 0.001  # kg/kg
# mixing_conversions = convert_mixing_ratio(mixing_ratio, air_density)
# print("Mixing Ratio Conversions:")
# print(mixing_conversions)

# number_concentration = 1000  # #/kg
# number_conversions = convert_number_concentration(number_concentration, air_density)
# print("\nNumber Concentration Conversions:")
# print(number_conversions)


def convert_mixing_ratio(mixing_ratio_kgkg, air_density_kgm3):
    kgm3 = mixing_ratio_kgkg * air_density_kgm3
    gcm3 = kgm3 / 1000
    gL   = kgm3
    return {
        "kg/m³": kgm3,
        "g/cm³": gcm3,
        "g/L": gL
    }

def convert_number_concentration(number_per_kg, air_density_kgm3):
    number_per_m3 = number_per_kg * air_density_kgm3
    number_per_L = number_per_m3 / 1000
    number_per_cm3 = number_per_m3 / 1000 / 1000
    return {
        "#/m³": number_per_m3,
        "#/L": number_per_L,
        "#/cm³": number_per_cm3
    }























#########################################################################################
#########################################################################################
def validate_title_data(ds_plot, cfg):
    """Validate data structure for title string generation."""
    run_ids = list(cfg.keys())
    
    checks = [
        (run_ids, "No run IDs found in configuration"),
        ("timeframe" in ds_plot.attrs, "Dataset missing 'timeframe' attribute"),
        ("INPUT_ORG" in cfg[run_ids[0]], f"'INPUT_ORG' missing from config['{run_ids[0]}']"),
        ("flare_sbm" in cfg[run_ids[0]]["INPUT_ORG"], "'flare_sbm' missing from INPUT_ORG"),
        ("sbm_par"   in cfg[run_ids[0]]["INPUT_ORG"], "'sbm_par' missing from INPUT_ORG")
    ]
    
    for condition, error in checks:
        if not condition:
            raise ValueError(error)
    
    return True

def create_title_string(ds_plot, cfg, cs_run, domain, n_stations, validate=True):
    """Generate formatted title string for plotting station height data."""
    validate and validate_title_data(ds_plot, cfg)
    
    run_ids = list(cfg.keys())
    resolution = "100m" if domain == "200x160" else "400m"
    input_org = cfg[run_ids[0]]["INPUT_ORG"]
    
    return  f'{"*"*100}\n' + \
            f'{resolution} resolution run  -  N_lon x N_lat: {domain} \n' + \
            f'{ds_plot.attrs["timeframe"]}\n' + \
            f'\n{"*"*10}  meteogram output: {"*"*10} \n  N_stations = {n_stations} (meteograms) N_experiments: {len(run_ids)}\n' + \
            f'\n{"*"*10}  expnames:  {"*"*10} {format_list_chunks(run_ids, n=5)}\n' + \
            f'\n{"*"*10}  flare_sbm:  {"*"*10} {format_dict_chunks(input_org["flare_sbm"], n=5)}\n' + \
            f'\n{"*"*10}  sbm_par:  {"*"*10} {format_dict_chunks(input_org["sbm_par"], n=5)}\n' + \
            f'\n{"*"*10}  history:  {"*"*10} \n ' + \
            f'{get_reduction_summary(ds_plot)}\n' + \
            f'\n{"*"*100}'




# #########################################################################################
# #########################################################################################
# # Apply reductions
#
def apply_reductions(ds, **reductions):
    """
    Apply reduction operations to an xarray dataset with flexible dimension handling.
    Records operations in dataset attributes as a computational graph.
    
    Parameters:
    -----------
    ds : xarray.Dataset
        Input dataset
    **reductions : dict
        Keyword arguments specifying reductions:
        - dim='operation' for single reductions (e.g., time='max')
        - dim=['op1', 'op2', 'op3'] for multi-stat reductions (e.g., expname=['min', 'mean', 'max'])
        - dim={'operations': ['op1', 'op2'], 'new_dim': 'stat_name'} for custom new dimension names
    
    Returns:
    --------
    xarray.Dataset
        Reduced dataset with dimensions sorted in original order and operation history in attributes
    """
    import copy
    from datetime import datetime
    
    original_dims = list(ds.sizes.keys())
    
    # Initialize or get existing reduction history
    if 'reduction_history' not in ds.attrs:
        ds.attrs['reduction_history'] = []
    
    # Track this reduction step
    step_info = {
        'timestamp': datetime.now().isoformat(),
        'original_dims': original_dims,
        'operations': {},
        'resulting_dims': None
    }
    
    for dim, operation in reductions.items():
        if isinstance(operation, str):
            # Simple reduction: time='max' -> ds.max(dim='time')
            ds = getattr(ds, operation)(dim=dim)
            step_info['operations'][dim] = {'type': 'simple', 'operation': operation}
            
        elif isinstance(operation, list):
            # Multi-stat reduction: expname=['min', 'mean', 'max']
            stats = [getattr(ds, op)(dim=dim) for op in operation]
            new_dim = f'{dim}_stat' if dim != 'expname' else 'stat_type'
            ds = xr.concat(stats, dim=new_dim)
            ds = ds.assign_coords({new_dim: operation})
            step_info['operations'][dim] = {
                'type': 'multi_stat', 
                'operations': operation, 
                'new_dim': new_dim
            }
            
        elif isinstance(operation, dict):
            # Custom multi-stat: expname={'operations': ['min', 'mean', 'max'], 'new_dim': 'custom_name'}
            ops = operation['operations']
            new_dim = operation['new_dim']
            stats = [getattr(ds, op)(dim=dim) for op in ops]
            ds = xr.concat(stats, dim=new_dim)
            ds = ds.assign_coords({new_dim: ops})
            step_info['operations'][dim] = {
                'type': 'custom_multi_stat',
                'operations': ops,
                'new_dim': new_dim
            }
    
    # Sort dimensions to maintain original order, with new dimensions at the end
    remaining_dims = [d for d in original_dims if d in ds.sizes]
    new_dims = [d for d in ds.sizes if d not in original_dims]
    final_dims = remaining_dims + new_dims
    
    ds = ds.transpose(*final_dims)
    step_info['resulting_dims'] = list(ds.sizes.keys())
    
    # Add this step to the reduction history
    reduction_history = copy.deepcopy(ds.attrs.get('reduction_history', []))
    reduction_history.append(step_info)
    ds.attrs['reduction_history'] = reduction_history
    
    # Update summary attributes for quick reference
    ds.attrs['last_reduction'] = step_info
    ds.attrs['total_reduction_steps'] = len(reduction_history)
    
    return ds

def get_reduction_summary(ds):
    """
    Get a human-readable summary of all reduction operations applied to the dataset.
    
    Parameters:
    -----------
    ds : xarray.Dataset
        Dataset with reduction history
        
    Returns:
    --------
    str
        Formatted summary of reduction operations
    """
    if 'reduction_history' not in ds.attrs:
        return "No reduction operations recorded."
    
    summary = f"Reduction History ({ds.attrs['total_reduction_steps']} steps):\n"
    summary += "=" * 50 + "\n"
    
    for i, step in enumerate(ds.attrs['reduction_history'], 1):
        summary += f"\nStep {i} ({step['timestamp'][:19]}):\n"
        summary += f"  Original dims: {step['original_dims']}\n"
        
        for dim, op_info in step['operations'].items():
            if op_info['type'] == 'simple':
                summary += f"  • {dim} → {op_info['operation']}()\n"
            elif op_info['type'] in ['multi_stat', 'custom_multi_stat']:
                ops_str = ', '.join(op_info['operations'])
                summary += f"  • {dim} → [{ops_str}] as '{op_info['new_dim']}'\n"
        
        summary += f"  Resulting dims: {step['resulting_dims']}\n"
    
    return summary

# Usage examples:
# ds_reduced = apply_reductions(ds, time='max', expname=['min', 'mean', 'max'])
# print(get_reduction_summary(ds_reduced))

# Chain multiple reductions while preserving history:
# ds_step1 = apply_reductions(ds, time='max')
# ds_final = apply_reductions(ds_step1, expname=['min', 'mean', 'max'])


# xarray utils+