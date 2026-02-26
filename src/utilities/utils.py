"""# The above code is a Python comment. Comments in Python start with a hash symbol (#) and are
# used to provide explanations or notes within the code. In this case, the comment appears to
# be incomplete as it only contains the text "lo".


-  single time step per python call (bash parallel)

"""

import os
import sys
import glob

import xarray as xr
xr.set_options(keep_attrs=True)
import numpy as np

import tobac
import utilities.namelist_metadata as nml

# ############################################################
# BEGIN PLOT FCNs
# ############################################################

def move_legend(obj, loc, **kwargs):
    """Recreate a plot's legend at a new location."""
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from matplotlib.legend import Legend
    from matplotlib.text import Text
    
    if isinstance(obj, Axes):
        old_legend  = obj.legend_
    elif isinstance(obj, Figure):
        old_legend  = obj.legends[-1] if obj.legends else None
    else:
        raise TypeError("`obj` must be matplotlib Axes or Figure")

    if not old_legend: 
        raise ValueError(f"{obj} has no legend")
    
    # Extract data and properties
    handles = getattr( old_legend, 'legend_handles' )
    labels  = [ t.get_text() for t in old_legend.get_texts() ]
    props   = { k: v for k, v in old_legend.properties().items() 
                if k in Legend.__init__.__code__.co_varnames and k != 'bbox_to_anchor'   }
    
    title_text  = kwargs.pop( "title", props.pop("title", Text()).get_text() )
    title_props = { k[6:]: kwargs.pop(k) for k in list(kwargs) if k.startswith("title_") }
    
    # Create new legend
    old_legend.remove()
    new_legend = obj.legend(   handles, 
                                labels, 
                                loc=loc, 
                                frameon=old_legend.legendPatch.get_visible(),
                                 **{**props, **kwargs}  )
    if title_text:
        new_legend.set_title(   title_text, **title_props   )
    return new_legend

# ############################################################
# END PLOT FCNs
# ############################################################



def load_and_process_cosmo_specs_3D_output(file_pattern, meta_file, chunks):
    """
    Load and process COSMO-SPECS 3D output with proper chunking.
    
    Parameters:
    -----------
    file_pattern : str
        Pattern to match input files
    meta_file : str
        Path to metadata file
    debug_mode : bool
        Whether to use debug slicing
    variable_subset : list
        List of variables to load
    """

    # First verify file exists and is accessible
    print(f'DEBUG::: Check if file_pattern exists: {file_pattern}')
    if not os.path.exists(file_pattern):
        raise FileNotFoundError(f"Data file not found: {file_pattern}")
    
    print(f'DEBUG::: Check if meta_file exists: {meta_file}')
    if not os.path.exists(meta_file):
        raise FileNotFoundError(f"Data file not found: {meta_file}")
    
    try:
        data_3D = xr.open_dataset(file_pattern, chunks='auto')
        # Then apply chunking after loading
    except Exception as final_error:
        raise RuntimeError( f"\n\nFailed to load data file after multiple attempts."
                            f"\nFinal error: {str(final_error)}")

    # Load grid data with error handling
    print(f'DEBUG::: Load grid data from {meta_file}')
    try:
        lat1D, lon1D, lat2D, lon2D, _ = load_grid_data(meta_file)
    except Exception as e:
        print(f"Warning: Error loading grid data: {str(e)}")
        print("Continuing with data loading...")
    
    
    # Create coordinate arrays
    print(f'DEBUG::: Create coordinate arrays')
    try:
        diameter_µm = define_bin_boundaries() * 1.0e6 * 2.0
        diameter_µm = (diameter_µm[1:] + diameter_µm[:-1]) / 2.0
        time = get_model_datetime_from_meta(meta_file)
        height1D, height3D = get_model_height_from_3D_data(data_3D)
        
        # Create coordinates and then chunk them
        coords = {
            'time': xr.DataArray(time, dims="time"),
            'x': xr.DataArray(lon1D, dims="x"),
            'y': xr.DataArray(lat1D, dims="y"),
            'z': xr.DataArray(height1D, dims="z"),
            'diameter': xr.DataArray(diameter_µm, dims="bin"),
            'lat2D': xr.DataArray(lat2D, dims=('y', 'x')),
            'lon2D': xr.DataArray(lon2D, dims=('y', 'x')),
            #'z_3D': xr.DataArray(height3D, dims=('z', 'y', 'x'))
        }
    except Exception as e:
        print(f"Warning: Error processing metadata and coordinates: {str(e)}")
        coords = None

    return data_3D, coords


def define_bin_boundaries():
    """Define bin boundaries from bin edges. Radius of computes in units of m """
    n_bins = 67
    nmax = 2  # controls mass ratio between adjacent bins
    r_min = 1.0e-9  # 1 nm
    rhow = 1.0e3  # kg/m^3
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min**3
    j0w = (nmax - 1.0) / np.log(2.0)
    mbin_edges = m0w * np.exp(np.arange(n_bins) / j0w)
    rbin_edges = np.cbrt(mbin_edges / fact)
    return rbin_edges

#@dask.delayed
def convert_mixing_ratio_to_concentration(data, rho_var='rho'):
    """Convert mixing ratio to concentration.
    E.g.: 
        - kg/kg to kg/m3 or 
        - #/kg to #/m3
    by multiplying variables with rho (in kg/m3).

    Args:
        data (xr.Dataset): Data to convert

    Returns:
        xr.Dataset: Data with converted units
    """

    # Group variables by type
    vars_dict = {
        'tend': [var for var in nml.tendency_nml_3d_netcdf if var in data.variables],
        'bulk': [var for var in nml.bulk_nml_3d_netcdf if var in data.variables],
        'spec': [var for var in nml.spectral_nml_3d_netcdf if var in data.variables],
        'env':  [var for var in nml.env_nml_3d_netcdf if var in data.variables],
        'flux': [var for var in nml.flux_nml_3d_netcdf if var in data.variables]
    }
        
    # Process tendency variables if they exist
    if len(vars_dict['tend']) > 0:
        # Create DataArrays with proper dimensions for broadcasting
        ntime, nbin = data[vars_dict['tend'][0]].shape
        print(f"Shape of tendency {vars_dict['tend'][0]}: {data[vars_dict['tend'][0]].shape}")
        
        transposed_dims = ['time', 'altitude', 'latitude', 'longitude', 'diameter']
        delta_t = data.time.diff('time').mean().astype(np.float64) * 1.0e-9  # convert to seconds
        #delta_h_expanded = -data.z.diff('z') # in km 
        #delta_h_expanded = delta_h_expanded.expand_dims({'bin': nbin, 'y': ny, 'x': nx, 'time': ntime})
        #delta_h_expanded = delta_h_expanded.transpose(*transposed_dims)

        # Prepare rho with correct dimensions
        rho_expanded = data[rho_var].isel(z=slice(1, None))
        rho_expanded = rho_expanded.expand_dims({'bin': nbin})
        rho_expanded = rho_expanded.transpose(*transposed_dims)
        
        # Calculate flux for all tendency variables
        for tend_var, flux_var in zip(vars_dict['tend'], vars_dict['flux']):
            data[flux_var] = data[tend_var].diff('time', n=1) / delta_t * rho_expanded
            data[tend_var] = data[tend_var] * rho_expanded
            
            data[tend_var].attrs['units'] = 'kg/m3'
            data[flux_var].attrs['units'] = 'kg/m3/s'

    if len(vars_dict['bulk']) > 0:  
        data.update(data[vars_dict['bulk']] * data[rho_var]) 

    if len(vars_dict['spec']) > 0:
        data.update(data[vars_dict['spec']] * data[rho_var])
        
    if len(vars_dict['env']) > 0:
        data.update(data[vars_dict['env']])
    
    return data
    
# def track_plume(data_3D_filename, metadata_filename, output_path, variables_subset=None, threshold=1e-6, debug_mode=False, chunks=None):
def track_plume(data_set, threshold=[1e-6]):
    """
    Track plumes in 3D data using tobac.
    
    Parameters:
    -----------
    data_3D : xarray.Dataset
        Dataset containing 'qi' variable for tracking
    
    Returns:
    --------
    dict
        Dictionary containing 'features', 'track', 'mask', and 'features_mask'
    """
    from time import time as time_time
    
    qi = data_set.qi
    coords = data_set.coords
        
    # Prepare tracking data
    qi_features = (qi - qi.mean()) / qi.std()
    
    # Prepare tobac input
    tobac_input = qi_features.rename({'x': 'longitude', 'y': 'latitude', 'z':'altitude'})
    
    print(f'tobac_input.dims: {tobac_input.dims}')
    print(f'tobac_input.coords: {tobac_input.coords}')
    print(f'tobac_input.min(): {tobac_input.min()}')
    print(f'tobac_input.max(): {tobac_input.max()}')
    
    # Feature detection parameters
    parameters_features = { # detection threshold for ice water mixing ratio `qi` [kg/kg]
    }
    
    # Statistics parameters
    statistics = {
        "mean_qi": np.mean,
        "total_qi": np.sum,
        "max_qi": np.max,
        "percentiles": (np.percentile, {"q": [95, 99]})
    }
    
    tobac_input = tobac_input.compute()
    
    # Calculate spatial and temporal resolution
    delta_x = 1e3 * np.mean(np.diff(tobac_input.longitude.values)) * 111.13295254925466 # 1 degree of longitude in km
    delta_y = 1e3 * np.mean(np.diff(tobac_input.latitude.values)) * 111.13295254925466 # 1 degree of latitude in km
    delta_t = np.mean(np.diff(tobac_input.time.astype('datetime64[s]')).astype(float))
    
    print(f'delta_x: {delta_x:.3f} m')
    print(f'delta_y: {delta_y:.3f} m')
    print(f'delta_t: {delta_t:.3f} s')
    
    # Get spacings from tobac
    tobac_input_iris = tobac_input.to_iris()
    dxy, dt = tobac.get_spacings(tobac_input_iris, grid_spacing=np.max([delta_x, delta_y]), time_spacing=delta_t)
    print(f'    dxy: {dxy:.3f}')
    print(f'     dt: {dt:.3f}')
    
    
    # Feature detection based on precipitation field and thresholds
    features = tobac.feature_detection_multithreshold(
        tobac_input_iris, dxy, 
        position_threshold="center",
        threshold=threshold, 
        statistic=statistics, 
        vertical_axis=1)
    
    # Link features to trajectories
    tracks = tobac.linking_trackpy(
        features, 
        tobac_input_iris, 
        dt=dt, dxy=dxy, v_max=100)
    
    # Segmentation
    mask, features_mask = tobac.segmentation.segmentation(
        features, 
        tobac_input_iris,
        dxy, threshold=threshold[0],
        vertical_coord='altitude'
        )
    
    
    return features, tracks, features_mask, xr.DataArray.from_iris(mask)



def get_cell_counts(data_out_path):
    """Count the number of files per key in the data_out_path"""
    # Get all .nc files in the directory
    all_files = glob.glob(os.path.join(data_out_path, '*_Time_Diameter_*.nc'))
    
    # Extract keys and cell numbers
    file_info = {}
    for filepath in all_files:
        filename = os.path.basename(filepath)
        # Split by '_Time_Diameter_' and get the key and cell number
        parts = filename.split('_Time_Diameter_')
        if len(parts) == 2:
            key = parts[0]
            cell = int(parts[1].replace('.nc', ''))
            
            if key not in file_info:
                file_info[key] = set()
            file_info[key].add(cell)
    
    # Print summary
    print("\nFile counts per key:")
    for key, cells in file_info.items():
        print(f"{key}: {len(cells)} cells")
    
    # Get the unique cell numbers across all keys
    all_cells = set()
    for cells in file_info.values():
        all_cells.update(cells)
    print(f"\nTotal unique cells across all keys: {len(all_cells)}")
    
    return file_info, all_cells




# ############################################################
# BEGIN TOBAC - cell slicing
# ############################################################

# Keep the original function with a wrapper that calls the new implementation
def slice_and_process_data(data_3D, tracking_data, cell_id=1, idxC=2, processing_mode='all', add_cubes=False, add_time=True, extend_time=30):
    """
    Slice data using slicer dictionary and create data arrays
    
    Parameters:
    -----------
    data_3D : xarray.Dataset
        Dataset containing data variables
    tracking_data : pandas.DataFrame
        Tracking data with cell information
    cell_id : int, default=1
        Cell ID to process
    idxC : int, default=2
        Index of the central cell
    processing_mode : str, default='all'
        Mode of processing: 'all', 'center_only', or 'segmentation_only'
    add_cubes : bool, default=False
        Whether to add neighbor cubes
    add_time : bool, default=True
        Whether to add time
    extend_time : int, default=30
        Time in seconds to extend before the plume detection
        
    Returns:
    --------
    dict
        Dictionary with processed arrays
    """
    # Process all data through the parent function
    result = {}
    if processing_mode == 'all':
        # Use the optimized parent function
        return process_cell_data(data_3D, tracking_data, cell_id, idxC, add_cubes, add_time)
    
    # Get domain boundaries for creating neighbor arrays and common variables
    x_min, x_max = data_3D.x.isel(x=0).values, data_3D.x.isel(x=-1).values
    y_min, y_max = data_3D.y.isel(y=0).values, data_3D.y.isel(y=-1).values
    z_min, z_max = data_3D.z.isel(z=0).values, data_3D.z.isel(z=-1).values
    
    # Get coordinates for the specified cell
    cell_data = tracking_data[tracking_data.cell == cell_id]
    path_time = cell_data.time.values.astype('datetime64[ns]')
    path_lat = np.clip(cell_data.latitude.values, y_min, y_max)
    path_lon = np.clip(cell_data.longitude.values, x_min, x_max)
    path_alt = np.clip(cell_data.altitude.values, z_min, z_max)
    
    dt = data_3D.time.diff('time').mean().values.astype(float) * 1.0e-9
    dalt = -data_3D.z.diff('z').mean().values.astype(float) * 1000
    dlat = np.mean(np.diff(data_3D.y.values))
    dlon = np.mean(np.diff(data_3D.x.values))
    
    common_vars = {
        'x_min': x_min, 'x_max': x_max,
        'y_min': y_min, 'y_max': y_max,
        'z_min': z_min, 'z_max': z_max,
        'path_time': path_time,
        'path_lat': path_lat,
        'path_lon': path_lon,
        'path_alt': path_alt,
        'dt': dt, 'dalt': dalt, 'dlat': dlat, 'dlon': dlon
    }
    
    # Process based on mode
    if processing_mode == 'center_only':
        result['cell_center'] = process_center_track(data_3D, common_vars, idxC, add_cubes, add_time, extend_time)
    elif processing_mode == 'segmentation_only':
        result['cell_mean_segmentation'] = process_segmentation(data_3D, common_vars, add_time, extend_time)
    
    return result



def process_cell_data(data_3D, tracking_data, cell_id=1, idxC=2, add_time=True, extend_time=30):
    """
    Parent function to process cell data that loads common variables once and optimizes processing.
    
    Parameters:
    -----------
    data_3D : xarray.Dataset
        Dataset containing data variables
    tracking_data : pandas.DataFrame
        Tracking data with cell information
    cell_id : int, default=1
        Cell ID to process
    idxC : int, default=2
        Index of the central cell
    add_cubes : bool, default=False
        Whether to add neighbor cubes
    add_time : bool, default=True
        Whether to add time
    extend_time : int, default=30
        Time in seconds to extend before the plume detection
        
    Returns:
    --------
    dict
        Dictionary with processed arrays including 'cell_center' and 'cell_mean_segmentation'
    """
    # Get domain boundaries once for all processing
    x_min, x_max = data_3D.x.isel(x=0).values, data_3D.x.isel(x=-1).values
    y_min, y_max = data_3D.y.isel(y=0).values, data_3D.y.isel(y=-1).values
    z_min, z_max = data_3D.z.isel(z=0).values, data_3D.z.isel(z=-1).values
    
    # Calculate spacing variables once
    dt = data_3D.time.diff('time').mean().values.astype(float) * 1.0e-9   # in seconds 
    dalt = -data_3D.z.diff('z').mean().values.astype(float) * 1000  # in meters
    dlat = np.mean(np.diff(data_3D.y.values))
    dlon = np.mean(np.diff(data_3D.x.values))
    
    # Get coordinates for the specified cell once
    cell_data = tracking_data[tracking_data.cell == cell_id]
    path_time = cell_data.time.values.astype('datetime64[ns]')
    path_lat = np.clip(cell_data.latitude.values, y_min, y_max)
    path_lon = np.clip(cell_data.longitude.values, x_min, x_max)
    path_alt = np.clip(cell_data.altitude.values, z_min, z_max)
            
    # Extend time if needed
    if add_time and path_time.size > 10:
        extend_time_steps = int(extend_time / dt)
        path_time = np.hstack((path_time[:extend_time_steps] - np.timedelta64(extend_time, 's'), path_time))
        path_lat = np.hstack((np.repeat(path_lat[0], extend_time_steps), path_lat))
        path_lon = np.hstack((np.repeat(path_lon[0], extend_time_steps), path_lon))
        path_alt = np.hstack((np.repeat(path_alt[0], extend_time_steps), path_alt))
    
    
    # Store common variables in a dictionary to pass to processing functions
    common_vars = {
        'x_min': x_min, 'x_max': x_max,
        'y_min': y_min, 'y_max': y_max,
        'z_min': z_min, 'z_max': z_max,
        'path_time': path_time,
        'path_lat': path_lat,
        'path_lon': path_lon,
        'path_alt': path_alt,
        'dt': dt, 'dalt': dalt, 'dlat': dlat, 'dlon': dlon
    }
    
    # Process center track data
    #cell_center = process_center_track(data_3D, common_vars, idxC=idxC,  
    #                                    add_time=add_time, extend_time=extend_time)
    
    # Process segmentation data
    cell_segmentation = process_segmentation(data_3D, common_vars, add_time=add_time, extend_time=extend_time)
    
    # Combine results
    return {
        #'cell_center': cell_center,
        'cell_mean_segmentation': cell_segmentation
    }

def process_center_track(data_3D, common_vars, idxC=2, add_cubes=False, add_time=True, extend_time=30):
    """Process center track data using common variables"""
    # Extract common variables
    path_time = common_vars['path_time']
    path_lat = common_vars['path_lat']
    path_lon = common_vars['path_lon']
    path_alt = common_vars['path_alt']
    dt = common_vars['dt']
    dlat = common_vars['dlat']
    dlon = common_vars['dlon']
    x_min, x_max = common_vars['x_min'], common_vars['x_max']
    y_min, y_max = common_vars['y_min'], common_vars['y_max']
    z_min, z_max = common_vars['z_min'], common_vars['z_max']
    
    # Create copies for local modification
    time_track = path_time.copy()
    lat_track = path_lat.copy()
    lon_track = path_lon.copy()
    alt_track = path_alt.copy()

    
    # Function to handle boundary cases
    def pad_boundary(value, offset, delta, min_val, max_val):
        """Pad boundary values instead of extending beyond domain limits"""
        new_val = value + offset * delta
        if new_val < min_val:
            return min_val
        elif new_val > max_val:
            return max_val
        else:
            return new_val
    
    # Basic selection
    cell_cubes = data_3D.sel(**{'time': xr.DataArray(time_track, dims="path"),
                                'x': xr.DataArray(lon_track, dims="path"),
                                'y': xr.DataArray(lat_track, dims="path"),
                                'z': xr.DataArray(alt_track, dims="path"),
                                'method': 'nearest'})
    
    cell_cubes = cell_cubes.assign_coords({'time': xr.DataArray(time_track, dims="path"),
                                          'x': xr.DataArray(lon_track, dims="path"),
                                          'y': xr.DataArray(lat_track, dims="path"),
                                          'z': xr.DataArray(alt_track, dims="path")})
    
    # Add neighbor cubes if requested
    if add_cubes:
        dalt = common_vars['dalt']
        # Create neighbor arrays with boundary padding
        offsets = [-1, 0, 1]
        x_neighbors = np.array([[pad_boundary(lon, offset, dlon, x_min, x_max) for offset in offsets] for lon in lon_track])
        y_neighbors = np.array([[pad_boundary(lat, offset, dlat, y_min, y_max) for offset in offsets] for lat in lat_track])
        z_neighbors = np.array([[pad_boundary(alt, offset, dalt, z_min, z_max) for offset in offsets] for alt in alt_track])
        
        # Slice data with neighbors
        cell_cubes = data_3D.sel(**{'time': xr.DataArray(time_track, dims="path"),
                                    'x': xr.DataArray(x_neighbors, dims=["path", "x_neighbor"]),
                                    'y': xr.DataArray(y_neighbors, dims=["path", "y_neighbor"]),
                                    'z': xr.DataArray(z_neighbors, dims=["path", "z_neighbor"]),
                                    'method': 'nearest'})
        
        cell_cubes = cell_cubes.assign_coords({'x_neighbor': offsets, 
                                              'y_neighbor': offsets, 
                                              'z_neighbor': offsets})
        
        # Return central position
        return cell_cubes.isel(x_neighbor=idxC, y_neighbor=idxC, z_neighbor=idxC)
    
    return cell_cubes

def process_segmentation(data_3D, common_vars, add_time=True, extend_time=30):
    """Process segmentation data using common variables"""
    # Extract common variables
    path_time = common_vars['path_time']
    
    # Check if tobac_mask exists
    if 'tobac_mask' not in data_3D:
        print("Warning: 'tobac_mask' not found in data_3D. Cannot process segmentation.")
        return None
    
    # Filter by time first to reduce computation
    time_mask = xr.DataArray(np.isin(data_3D.time, path_time), dims='time')
    masked_data = data_3D.where(time_mask, drop=True)
    
    # Apply mask and compute mean
    cell_masked = masked_data.where(masked_data['tobac_mask'])
    return cell_masked.mean(dim=('x', 'y', 'z'))

# ############################################################
# END TOBAC - cell slicing
# ############################################################



# def slice_and_process_data_old(data_3D, tracking_data, cell_id=1, idxC = 2 , add_cubes=False, add_time=True, extend_time=30):
#     """
#     Slice data using slicer dictionary and create data arrays
    
#     Parameters:
#     -----------
#     data_3D : xarray.Dataset
#         Dataset containing data variables
#     tracking_data : pandas.DataFrame
#         Tracking data with cell information
#     cell_id : int, default=1
#         Cell ID to process
#     idxC : int, default=2
#         Index of the central cell
#     add_cubes : bool, default=False
#         Whether to add neighbor cubes
#     add_time : bool, default=True
#         Whether to add time
        
#     Returns:
#     --------
#     dict
#         Dictionary with processed arrays
#     """
        
#     # Get domain boundaries for creating neighbor arrays
#     x_min, x_max = data_3D.x.isel(x=0).values, data_3D.x.isel(x=-1).values
#     y_min, y_max = data_3D.y.isel(y=0).values, data_3D.y.isel(y=-1).values
#     z_min, z_max = data_3D.z.isel(z=0).values, data_3D.z.isel(z=-1).values
    
    
#     # Get coordinates for the specified cell
#     cell_data = tracking_data[tracking_data.cell == cell_id]
#     path_time = cell_data.time.values.astype('datetime64[ns]')
#     path_lat = np.clip(cell_data.latitude.values, y_min, y_max)
#     path_lon = np.clip(cell_data.longitude.values, x_min, x_max)
#     path_alt = np.clip(cell_data.altitude.values, z_min, z_max)
    
#     dt = data_3D.time.diff('time').mean().values.astype(float) * 1.0e-9   # in seconds 
#     dalt = -data_3D.z.diff('z').mean().values.astype(float) * 1000  # in meters
#     dlat = np.mean(np.diff(data_3D.y.values))
#     dlon = np.mean(np.diff(data_3D.x.values))

#     if add_time and path_time.size > 10:        
#         # 30 * 10s = 300s = 5 min
#         extend_time_steps = int(extend_time / dt)
        
#         path_time = np.hstack((path_time[:extend_time_steps] - np.timedelta64(extend_time, 's'), path_time))
#         path_lat = np.hstack((path_lat[:extend_time_steps] - extend_time_steps * dlat, path_lat))
#         path_lon = np.hstack((path_lon[:extend_time_steps] - extend_time_steps * dlon, path_lon))
#         path_alt = np.hstack((np.ones(extend_time_steps) * path_alt[0], path_alt))
            
    
#     # Create neighbor arrays with boundary handling
#     # Function to handle boundary cases
#     def pad_boundary(value, offset, delta, min_val, max_val):
#         """Pad boundary values instead of extending beyond domain limits"""
#         new_val = value + offset * delta
#         if new_val < min_val:
#             return min_val
#         elif new_val > max_val:
#             return max_val
#         else:
#             return new_val
    
#     cell_cubes = data_3D.sel(**{'time': xr.DataArray(path_time, dims="path"),
#                                 'x': xr.DataArray(path_lon, dims="path"),
#                                 'y': xr.DataArray(path_lat, dims="path"),
#                                 'z': xr.DataArray(path_alt, dims="path"),
#                                 'method': 'nearest'})
    
#     cell_cubes = cell_cubes.assign_coords({'time': xr.DataArray(path_time, dims="path"),
#                                             'x': xr.DataArray(path_lon, dims="path"),
#                                             'y': xr.DataArray(path_lat, dims="path"),
#                                             'z': xr.DataArray(path_alt, dims="path")})


    
#     if add_cubes:
#         # Create neighbor arrays with boundary padding
#         offsets = [-1, 0, 1]
#         x_neighbors = np.array([[pad_boundary(lon, offset, dlon, x_min, x_max) for offset in offsets] for lon in path_lon])
#         y_neighbors = np.array([[pad_boundary(lat, offset, dlat, y_min, y_max) for offset in offsets] for lat in path_lat])
#         z_neighbors = np.array([[pad_boundary(alt, offset, dalt, z_min, z_max) for offset in offsets] for alt in path_alt])
#         # Slice data
#         cell_cubes = data_3D.sel(**{'time': xr.DataArray(path_time, dims="path"),
#                                     'x': xr.DataArray(x_neighbors, dims=["path", "x_neighbor"]),
#                                     'y': xr.DataArray(y_neighbors, dims=["path", "y_neighbor"]),
#                                     'z': xr.DataArray(z_neighbors, dims=["path", "z_neighbor"]),
#                                     'method': 'nearest'})
#         cell_cubes = cell_cubes.assign_coords({ 'x_neighbor': offsets, 
#                                                 'y_neighbor': offsets, 
#                                                 'z_neighbor': offsets})
#         # Cell center (values at the central position)
#         cell_center = cell_cubes.isel(x_neighbor=idxC, y_neighbor=idxC, z_neighbor=idxC) if add_cubes else cell_cubes
#         #cell_mean_neighbors = cell_cubes.mean(dim=('x_neighbor', 'y_neighbor', 'z_neighbor'))
#         #cell_std_neighbors = cell_cubes.std(dim=('x_neighbor', 'y_neighbor', 'z_neighbor'))
#     else:
#         cell_center = cell_cubes
#     # Cell center (values at the central position) using tobac 3D segmentation mask
#     cell_masked = data_3D.where(data_3D['tobac_mask'])
#     cell_masked_mean = cell_masked.mean(dim=('x', 'y', 'z'))
#     #cell_masked_std = cell_masked.std(dim=('x', 'y', 'z'))
    


    
#     return {
#         'cell_center': cell_center,
#         #'cell_mean_neighbors': cell_mean_neighbors,
#         #'cell_std_neighbors': cell_std_neighbors,
#         'cell_mean_segmentation': cell_masked_mean,
#         #'cell_std_segmentation': cell_masked_std,
#         #'raw_data': cell_masked  # Include raw data for additional processing
#     }



def load_grid_data(metadata_file) -> tuple[np.ndarray | None, np.ndarray | None, np.ndarray | None, np.ndarray | None, xr.Dataset | None]:
    import json
    from pathlib import Path
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
        data_extpar = xr.open_mfdataset(glob.glob(extpar_path))
        
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
        return None, None, None, None, None



# ############################################################
# Chunking and slicing function
# ############################################################


def get_domain_bounds_from_meta(meta_file):
    import json
    from datetime import datetime
    """
    Get domain bounds from metadata.
    
    Parameters:
    -----------
    meta_file : str
        Path to metadata file

    Returns:
    --------
    dict
        {'t0': t0, 'tend': tend, 'dt': dt, 'Nt': Nt, 'init_time': init_time}
    """
    # Convert model output times to datetime objects
    with open(meta_file) as f:
        metadata = json.load(f)
        first_key = next(iter(metadata))
        nml_input_org = metadata[first_key]['INPUT_ORG']
        lmgrid = nml_input_org['lmgrid']
        hcomb = nml_input_org['sbm_par']['nc_output_hcomb']
        init_time = datetime.datetime.strptime(nml_input_org['runctl']['ydate_ini'], '%Y%m%d%H')
        t0 = float(hcomb[0])
        tend = float(hcomb[1])
        dt = float(hcomb[2])
        Nt = int((tend-t0) / dt) + 2 # +2 for the two time steps
        Nx = int(lmgrid['ie_tot'])-6 # -6 for the boundary cells
        Ny = int(lmgrid['je_tot'])-6 # -6 for the boundary cells
        Nz = int(lmgrid['ke_tot'])
        Nbin = 66

    meta_info = {'t0': t0, 'tend': tend, 'dt': dt, 'Nt': Nt, 'Nx': Nx, 
                    'Ny': Ny, 'Nz': Nz, 'Nbin': Nbin, 'init_time': init_time}
    
    return meta_info


def get_slicing_indices(meta_file, debug_mode=True):
    meta_info = get_domain_bounds_from_meta(meta_file)
    Nt = meta_info['Nt']
    Nx = meta_info['Nx']
    Ny = meta_info['Ny']
    Nz = meta_info['Nz']
    Nbin = meta_info['Nbin']
    
    print(f'DEBUG::: ORIGINAL (meta_info)   Nt: {Nt}   Nx: {Nx}   Ny: {Ny}   Nz: {Nz}   Nbin: {Nbin}')
    print('now slicing into::: ')
        
    # Create slicing based on debug mode
    center_x_idx = Nx // 2 + 2
    center_y_idx = Ny // 2 + 2
    if debug_mode:
        slicing = {
            'time': np.arange(0, 10, dtype=int), 
            'x': np.arange(max(0, center_x_idx//2-2), min(Nx, center_x_idx+3), dtype=int),
            'y': np.arange(max(0, center_y_idx//2-2), min(Ny, center_y_idx+3), dtype=int),
            'bin': np.arange(30, 50, dtype=int),
            'z': np.arange(80, 100, dtype=int)
        }
    else:
        slicing = {
            'time': np.arange(0, Nt, dtype=int),
            'x': np.arange(0, min(Nx+1, center_x_idx+5), dtype=int),
            'y': np.arange(0, min(Ny+1, center_y_idx+5), dtype=int),
            'bin': np.arange(30, 50, dtype=int),
            'z': np.arange(75, 100, dtype=int)
        }
        
    
    print(f'DEBUG::: slicing: {slicing["time"]}')
    print(f'DEBUG::: slicing: {slicing["z"]}')
    print(f'DEBUG::: slicing: {slicing["y"]}')
    print(f'DEBUG::: slicing: {slicing["x"]}')
    print(f'DEBUG::: slicing: {slicing["bin"]}')
    return slicing
    
    
def calculate_optimal_chunks(meta_file, slicing=None, debug_mode=False, tot_mem_gb=32, tgt_ck_size_mb=512):
    """
    Calculate optimal chunk sizes based on data dimensions and memory constraints.
    
    Parameters:
    -----------
    meta_file : str
        Path to metadata file
    nx, ny, nz : int
        Spatial dimensions
    ntime : int
        Number of time steps
    nbin : int
        Number of bins
    total_memory_gb : float
        Total memory per worker in GB
    target_chunk_size_mb : float
        Target chunk size in MB (100MB-1GB is recommended)
    """
    
    # Get domain dimensions, depending on slicing
    if slicing is None:
        slicing = get_slicing_indices(meta_file, debug_mode=debug_mode)
    
    
    Nx = slicing['x'].size
    Ny = slicing['y'].size
    Nz = slicing['z'].size
    Nt = slicing['time'].size
    Nbin = slicing['bin'].size

        
    # Calculate bytes per element (assuming float64)
    bytes_per_element = 8
    
    # Convert memory sizes to bytes
    total_memory_bytes = tot_mem_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    target_bytes = tgt_ck_size_mb * 1024 * 1024
    
    # Calculate total array size in bytes
    total_size = Nx * Ny * Nz * Nt * Nbin * bytes_per_element
    
    # Adjust target chunk size if it would create too many chunks for available memory
    # Aim to use at most 60% of total memory for chunks to leave room for computations
    max_memory_for_chunks = 0.6 * total_memory_bytes
    min_chunks_needed = total_size / max_memory_for_chunks
    adjusted_target_bytes = max(target_bytes, total_size / min_chunks_needed)
    
    # Calculate number of chunks needed
    total_chunks = max(1, total_size / adjusted_target_bytes)
    
    # Calculate chunk sizes for each dimension
    # Priority: time > z > y > x > bin (based on typical access patterns)
    chunks = {}
    
    # Time chunks: aim for at least 2 seconds of computation per chunk
    chunks['time'] = min(max(2, Nt // 20), Nt)  # At least 2, at most full size
    
    # Spatial chunks: try to keep aspect ratio similar to original
    total_spatial = Nx * Ny * Nz
    spatial_chunk_size = int(np.cbrt(total_spatial / total_chunks))
    
    chunks['z'] = min(max(10, Nz // 5), Nz)  # Vertical dimension often smaller
    chunks['y'] = min(max(spatial_chunk_size, Ny // 5), Ny)
    chunks['x'] = min(max(spatial_chunk_size, Nx // 5), Nx)
    
    # Bin chunks: usually smaller dimension, keep mostly whole
    chunks['bin'] = min(max(20, Nbin // 3), Nbin)
    
    # Verify chunk size is reasonable and within memory constraints
    chunk_size_bytes = (chunks['time'] * chunks['z'] * chunks['y'] * 
                       chunks['x'] * chunks['bin'] * bytes_per_element)
    chunk_size_mb = chunk_size_bytes / (1024 * 1024)
    
    # Calculate total memory needed for chunks
    n_chunks = (np.ceil(Nt / chunks['time']) * 
               np.ceil(Nz / chunks['z']) * 
               np.ceil(Ny / chunks['y']) * 
               np.ceil(Nx / chunks['x']) * 
               np.ceil(Nbin / chunks['bin']))
    total_chunks_memory_gb = (chunk_size_bytes * n_chunks) / (1024**3)
    
    if chunk_size_mb < 1:
        print("Warning: Chunk size too small (<1MB)")
    elif chunk_size_mb > 1000:
        print("Warning: Chunk size too large (>1GB)")
    elif total_chunks_memory_gb > tot_mem_gb:
        print(f"Warning: Total chunks memory ({total_chunks_memory_gb:.2f} GB) exceeds worker memory ({tot_mem_gb} GB)")
        # Adjust chunk sizes to fit in memory
        reduction_factor = np.sqrt(tot_mem_gb / total_chunks_memory_gb)
        for dim in chunks:
            chunks[dim] = max(1, int(chunks[dim] * reduction_factor))
        print(f"Adjusted chunks: {chunks}")
    else:
        print("\n  Chunk configuration:")
        chunk_size_mb = (np.prod([np.abs(chunks[dim]) for dim in chunks]) * 8) / (1024 * 1024)
        print(f"Approximate chunk size: {chunk_size_mb:.2f} MB")
        print(f"     Actual chunk size: {chunk_size_mb:.2f} MB")
        print(f"   Total chunks memory: {total_chunks_memory_gb:.2f} GB")
        print(f"      Number of chunks: {int(n_chunks)}")
        print(f"                Chunks: {chunks}")

    return chunks

def get_model_datetime_from_meta(meta_file, time_array=None):
    """
    Process model output times from metadata and create coordinate arrays.
    
    Parameters:
    -----------
    meta_file : str
        Path to metadata file
        
    Returns:
    --------
    tuple
        (model_time, model_diameter_µm, model_height_1D, model_height_3D)
    """
    from datetime import timedelta as dtdelta
    # Convert model output times to datetime objects
    meta_info = get_domain_bounds_from_meta(meta_file)
    
    if time_array is None:
        time_array = np.arange(0, meta_info['Nt']-1)
        
    # Create model time array
    base_time = meta_info['init_time'] + dtdelta(seconds=meta_info['t0'])
    model_time = [base_time + dtdelta(seconds=int(meta_info['dt'] * t)) for t in time_array]

    return model_time

def get_model_height_from_3D_data(data_3D):
    model_height_3D = data_3D.hhl.isel(time=0)
    model_height_1D = model_height_3D.mean(dim=('x', 'y'))
    return model_height_1D, model_height_3D

# ############################################################
# END Chunking and slicing function
# ############################################################
