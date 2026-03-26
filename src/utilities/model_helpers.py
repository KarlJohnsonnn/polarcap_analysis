import numpy as np
import xarray as xr
import glob
import os
import re


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

def format_model_label_as_table(metadata_entry, run_id=None):
    
    def fmt_arr(arr, factor=1, na_for_zero=True):
        """Format array with optional unit conversion"""
        arr = [arr] if not isinstance(arr, (list, np.ndarray)) else arr
        vals = ["N/A" if (na_for_zero and v == 0) else f"{v * factor:4.1e}" for v in arr]
        return "[" + ",".join(vals) + "]"
    
    # Extract parameters with defaults
    org = metadata_entry.get('INPUT_ORG', {})
    flare = org.get('flare_sbm', {})
    sbm = org.get('sbm_par', {})
    val = {'dnap_init': sbm.get('dnap_init', 0.0),
           'flare_emission': flare.get('flare_emission', 0.0),
           'dn_in': sbm.get('dn_in', [0.0]),
           'flare_dn': flare.get('flare_dn', [0.0]),
           'dp_in': sbm.get('dp_in', [0.0]),
           'flare_dp': flare.get('flare_dp', [0.0]),
           'sig_in': sbm.get('sig_in', [0.0]),
           'flare_sig': flare.get('flare_sig', [0.0]),
           'ishape': sbm.get('ishape', 0)}

    # Format all values without the ASCII frame
    return (
        f"Parameter:              Background                        Flare              ishape\n"
        f"DNAP/FPR:             {val['dnap_init']:^15.1f}                   {fmt_arr(val['flare_emission'], 1e-6):^15}    {val['ishape']:^15d}\n"
        f"DNb/DNf:            {fmt_arr(val['dn_in']):^15}           {fmt_arr(val['flare_dn']):^15}\n"
        f"DPb/DPf:             {fmt_arr(val['dp_in'], 1e9):^15}           {fmt_arr(val['flare_dp'], 1e9):^15}\n"
        f"Sigb/Sigf:            {fmt_arr(val['sig_in']):^15}           {fmt_arr(val['flare_sig']):^15}\n"
    )

    

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
    
    mean_calcs = {
        'arithmetic': lambda d, w: np.ma.sum(d * w, axis=-1) / np.ma.sum(d, axis=-1),
        'geometric': lambda d, w: np.exp(np.ma.sum(d * np.log(w), axis=-1) / np.ma.sum(d, axis=-1)),
        'median': lambda d, w: np.apply_along_axis(lambda x: w[np.searchsorted(np.ma.cumsum(x) / np.ma.sum(x), 0.5)] if np.ma.sum(x) > 0 else np.ma.masked, axis=-1, arr=d),
        'effective': lambda d, w: np.ma.sum(d * w**3, axis=-1) / np.ma.sum(d * w**2, axis=-1),
        'volume': lambda d, w: np.ma.sum(d * w**4, axis=-1) / np.ma.sum(d * w**3, axis=-1)
    }
    
    if method not in mean_calcs:
        raise ValueError(f"Method must be one of {list(mean_calcs.keys())}")
    
    means = np.ma.zeros(array.shape[:-1])
    sums = np.ma.sum(array, axis=-1)
    
    try:
        means = np.ma.where(sums > 0, mean_calcs[method](array, diameters), np.ma.masked)
    except Exception as e:
        print('ERROR Something went wrong in calculate_mean_diameter!', e)
        means.mask = True
    
    return means



def calculate_bulk_timeseries(dsm_in, lbb=[30, 50], cbb=[30, 50], var_list=None):

    if var_list is None:
        var_list = ['NW', 'NF', 'QW', 'QF', 'QV', 'QFW']

    # convert mixing raitos to number/mass concentrations per 1/cm3 and g/m3
    dsm = dsm_in[var_list] * dsm_in['RHO']
    dsm.attrs = dsm_in.attrs

    print('Convert mixing ratios to number/mass concentrations per 1/cm3 and g/m3')
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



# #########################################################################################
# #########################################################################################
# # Coordinates of Eriswil Observatory
COORDINATES_OF_ERISWIL = {
    'ruler_start': (47.05, 7.804),
    'ruler_end': (47.08, 7.90522),
    'seeding': (47.07425, 7.90522),
    'eriswil': (47.070522, 7.872991),
}

def get_closest_station_to_coordinates(station_lat, station_lon, target_lat, target_lon, verbose=False):
    distances = [ haversine_distance(target_lat, target_lon, lat, lon) for lat, lon in zip(station_lat, station_lon) ]
    closest_idx = int(np.argmin(distances))

    if verbose:
        print(f"Closest station:         {closest_idx}")
        print(f"Given Coordinates:      ({target_lat:.6f}, {target_lon:.6f})")
        print(f"Coordinates of station: ({station_lat[closest_idx]:.6f}, {station_lon[closest_idx]:.6f})")
        print(f"Distance:                {distances[closest_idx]:.2f} m")
    return closest_idx



def get_grid_cell_sizes(lat, lon):
    """Calculate average grid cell sizes using haversine formula.
    
    Args:
        lat: 2D array of latitude values
        lon: 2D array of longitude values
        
    Returns:
        tuple: Average cell dimensions (dx, dy) in meters
    """
    if len(lat.shape) == 1:
        lat_size = len(lat)
        lon_size = len(lon)
    elif len(lat.shape) == 2:
        lat_size, lon_size = lat.shape
    else:
        raise ValueError(f'lat.shape: {lat.shape} is not supported')
    cells = np.zeros((lat_size - 1, lon_size - 1, 2))
    
    # Calculate distances between adjacent grid points
    for i in range(lat_size - 1):
        for j in range(lon_size - 1):
            # Calculate N-S distance
            cells[i,j,0] = haversine_distance(lat[i,j], lon[i,j], lat[i+1,j], lon[i,j])
            # For E-W distance, use same calculation
            cells[i,j,1] = haversine_distance(lat[i,j], lon[i,j], lat[i,j], lon[i,j+1])
    # Return average cell dimensions
    return tuple(cells.mean(axis=(0,1)))



def get_flare_emission_rates(meta_data, lat, lon, model_height, verbose=False):
    """Extract run parameters and calculate flare emission rates.
    
    Args:
        meta_data: Dictionary containing ensemble metadata
        lat: 2D array of latitude values
        lon: 2D array of longitude values
        model_height: Array of model height levels in m
        
    Returns:
        tuple: (fe, fe_per_m3, fe_per_l)
            - fe: flare emission rate [1/gridcell/s]
            - fe_per_m3: flare emission rate [1/m3/s]
            - fe_per_l: flare emission rate [1/L/s]
    """
    # Load metadata and extract grid info

    nml_input_org = meta_data['INPUT_ORG']
    fheight_ind = -nml_input_org['flare_sbm']['flare_hight']
    femis_rate_per_gridcell = float(nml_input_org['flare_sbm']['flare_emission'])
    height_res = -np.diff(np.array(model_height))
    height_res = height_res[ fheight_ind - 100]
    grid_dx, grid_dy = get_grid_cell_sizes(lat, lon)
    Vcell = grid_dx * grid_dy * height_res # flare height might be wrong cause of slicing
    femis_rate_per_m3 = femis_rate_per_gridcell / Vcell          # Convert to 1/m3/s
    femis_rate_per_L = femis_rate_per_gridcell / Vcell / 1000  # Convert to 1/L/s

    if verbose:
        print(f'{"-"*95}')
        print(f'FLARE NAMELIST PARAMETERS AND GRID CELL SIZES')
        print(f'    model altitude level                         (m): {model_height}')
        print(f'    flare height index                           (-): {fheight_ind - 100:16.8e}')
        print(f'    height resolution at seeding altitude        (m): {height_res:16.8e}')
        print(f'    grid cell size at seeding location lon       (m): {grid_dx:16.8e}')
        print(f'    grid cell size at seeding location lat       (m): {grid_dy:16.8e}')
        print(f'    grid cell volume at seeding location        (m3): {Vcell:16.8e}')
        print(f'    flare emission rate per           (1/gridcell/s): {femis_rate_per_gridcell:16.8e}')
        print(f'    flare emission rate per                 (1/m3/s): {femis_rate_per_m3:16.8e}')
        print(f'    flare emission rate per                  (1/L/s): {femis_rate_per_L:16.8e}')
        print(f'{"-"*95}')
        
    return (femis_rate_per_gridcell, femis_rate_per_m3, femis_rate_per_L)

# Haversine function to compute the distance between two lat-lon points
def haversine_distance(lat1, lon1, lat2, lon2):
    '''
    Compute the distance between two lat-lon points using the Haversine formula
    
    Args:
        lat1: latitude of the first point(s) - scalar or array
        lon1: longitude of the first point(s) - scalar or array
        lat2: latitude of the second point(s) - scalar or array
        lon2: longitude of the second point(s) - scalar or array
        
    Returns:
        float or ndarray: distance between the point(s) in meters
            - If all inputs are scalars, returns a scalar distance
            - If any inputs are arrays, returns an array of distances
            - Arrays are broadcast following numpy broadcasting rules
    
    Examples:
        # Single point-to-point distance
        >>> dist = haversine_distance(47.0, 7.8, 47.1, 7.9)
        
        # Array of points to single point
        >>> lats = np.array([47.0, 47.1, 47.2])
        >>> lons = np.array([7.8, 7.9, 8.0])
        >>> dists = haversine_distance(lats, lons, 47.15, 7.95)
        
        # Pairwise distances between arrays
        >>> dists = haversine_distance(lats1, lons1, lats2, lons2)
    '''
    
    R = 6371.0e3  # Earth radius in meters
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance


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




# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def glob_lastest(nc_dir, name_temp='M_??_??_??????????????.nc', file_idx=None):
    """ Extract just the filenames for regex matching"""
    files = sorted(glob.glob(os.path.join(nc_dir, name_temp)))
    filenames = ' '.join([os.path.basename(f) for f in files])
    timestamps = sorted(set(re.findall(r'(\d{14})(?:_ref)?\.nc', filenames)))
    selected_timestamp = timestamps[file_idx] if file_idx is not None else timestamps[:]
    files_out = [f for f in files if selected_timestamp in f]
    print(f"    Found {len(files_out)} files of type {name_temp} with timestamp {selected_timestamp}")
    return files_out


def convert_units_meteogram(ds, rho):
    """Convert mixing ratios to convenient units."""
    for var in ['NINP', 'ICNC', 'NI', 'NF']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-3
        ds[var].attrs['units'] = 'L-1'

    for var in ['CDNC', 'NW']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-6
        ds[var].attrs['units'] = 'cm-3'

    for var in ['QI_Sp', 'QFtot', 'QFW', 'QF']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+0
        ds[var].attrs['units'] = 'gL-1'

    for var in ['QV_Sp', 'QC_Sp', 'QWtot', 'QW', 'QR', 'QV', 'QC']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+3
        ds[var].attrs['units'] = 'gm-3'

    return ds


def convert_units_3d(ds, rho):
    """Convert mixing ratios to convenient units."""
    for var in ['ninp', 'icnc', 'ni', 'nf']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-3
        ds[var].attrs['units'] = 'L-1'

    for var in ['cdnc', 'nw']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-6
        ds[var].attrs['units'] = 'cm-3'

    for var in ['qi', 'qfw', 'qs', 'qf']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+0
        ds[var].attrs['units'] = 'gL-1'

    for var in ['qv', 'qc', 'qw', 'qr']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+3
        ds[var].attrs['units'] = 'gm-3'

    return ds

    
# def track_plume(data_3D_filename, metadata_filename, output_path, variables_subset=None, threshold=1e-6,  chunks=None):
def track_plume(tobac_input, threshold=1e-6, debug=False,):
    import tobac
    import iris
    iris.FUTURE.date_microseconds = True
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
    
    # Statistics parameters
    statistics = {
        "mean_qi": np.mean,
        "total_qi": np.sum,
        "max_qi": np.max,
        "percentiles": (np.percentile, {"q": [95, 99]})
    }
    
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

    if debug:
        print(f'debug: tobac_input_iris: {tobac_input_iris}')
        print(f'debug: dxy: {dxy:.3f}')
        print(f'debug: dt: {dt:.3f}')
    
    # Feature detection based on precipitation field and thresholds
    features = tobac.feature_detection_threshold(
        tobac_input_iris, dxy, 
        position_threshold="weighted_abs",
        threshold=threshold, 
        # statistic=statistics, 
        vertical_coord="altitude",
        # vertical_axis=1
        )
    
    # Link features to trajectories
    tracks = tobac.linking_trackpy(
        features, 
        tobac_input_iris, 
        vertical_coord="altitude",
        method_linking="predict",
        dt=dt, dxy=dxy, v_max=100)
    
    # Segmentation
    mask, features_mask = tobac.segmentation.segmentation(
        features, 
        tobac_input_iris,
        dxy, threshold=threshold[0],
        vertical_coord='altitude'
        )
    
    
    return features, tracks, features_mask, xr.DataArray.from_iris(mask)




def define_bin_boundaries():
    """Define bin boundaries from bin edges."""
    n_bins = 67
    nmax = 2
    r_min = 1.0e-9
    rhow = 1.0e3
    fact = rhow * 4.0 / 3.0 * np.pi
    m0w = fact * r_min**3
    j0w = (nmax - 1.0) / np.log(2.0)
    mbin_edges = m0w * np.exp(np.arange(n_bins) / j0w)
    return np.cbrt(mbin_edges / fact)

def get_model_datetime_dimension(ydate_ini, t0_netcdf, tdelta_netcdf, tsize_netcdf ):
    """Get datetime from model metadata."""
    from datetime import timedelta as dtdelta, datetime
    t0 = datetime.strptime(ydate_ini, '%Y%m%d%H')
    base_time = t0 + dtdelta(seconds=t0_netcdf)
    model_time = [base_time + dtdelta(seconds=int(tdelta_netcdf * t)) for t in range(tsize_netcdf)]
    return model_time


def get_model_datetime_from_meta(org_nml, time_array):
    """Get datetime from model metadata."""
    from datetime import timedelta as dtdelta, datetime
    t0 = datetime.strptime(org_nml['runctl']['ydate_ini'], '%Y%m%d%H')
    base_time = t0 + dtdelta(seconds=org_nml['sbm_par']['nc_output_hcomb'][0])
    model_time = [base_time + dtdelta(seconds=int(org_nml['sbm_par']['nc_output_hcomb'][2] * t)) for t in time_array]
    return model_time

def make_3d_preprocessor(ncfile_cs_experiment, ncfile_extpar, nml_input_org): 
    """Return a preprocess function for xr.open_mfdataset.

    Loads extpar grid once; returned closure transforms each per-file dataset.
    """
    run_id = ncfile_cs_experiment.split('/')[-1].split('_')[1].split('.')[0]
    # Shared grid coordinates (loaded once)
    with xr.open_mfdataset(ncfile_extpar, parallel=False) as data_extpar:
        lat2D = data_extpar['lat'].values[7:-7, 7:-7]
        lon2D = data_extpar['lon'].values[7:-7, 7:-7]
    lon1D = np.linspace(lon2D.min(), lon2D.max(), lon2D.shape[1])
    lat1D = np.linspace(lat2D.min(), lat2D.max(), lat2D.shape[0])

    def _preprocess(ds):
        # Bulk integration from spectral bins
        ds['icnc'] = ds['nf'].isel(bin=slice(30, 50)).sum(dim='bin')
        ds['cdnc'] = ds['nw'].isel(bin=slice(50, None)).sum(dim='bin')
        ds['qwtot']   = ds['qw'].isel(bin=slice(30, 50)).sum(dim='bin')
        ds['qfwtot']  = ds['qfw'].isel(bin=slice(50, None)).sum(dim='bin')
        ds['nr'] = ds['nw'].isel(bin=slice(50, None)).sum(dim='bin')
        ds['ns'] = ds['nf'].isel(bin=slice(50, None)).sum(dim='bin')
        # Rename spectral bins -> diameter
        diameter_µm = define_bin_boundaries() * 1.0e6 * 2.0
        ds = ds.rename(bin='diameter')
        ds = ds.assign_coords(diameter=xr.DataArray((diameter_µm[1:] + diameter_µm[:-1]) / 2.0, dims="diameter"))
        # Model datetime from namelist metadata
        time = get_model_datetime_dimension(nml_input_org['runctl']['ydate_ini'],
                                            nml_input_org['sbm_par']['nc_output_hcomb'][0],
                                            nml_input_org['sbm_par']['nc_output_hcomb'][2],
                                            ds.time.size)
 
        # Rename raw dims -> physical names, assign coordinates
        ds = ds.rename({'x': 'longitude', 'y': 'latitude', 'z': 'altitude'})
        ds = ds.assign_coords({
            'time':        xr.DataArray(time, dims="time"),
            'longitude':   xr.DataArray(lon1D, dims="longitude"),
            'latitude':    xr.DataArray(lat1D, dims="latitude"),
            'altitude':    xr.DataArray(ds.hhl.isel(time=0).mean(dim=('longitude', 'latitude')), dims="altitude"),
            'latitude2D':  xr.DataArray(lat2D, dims=["latitude", "longitude"]),
            'longitude2D': xr.DataArray(lon2D, dims=["latitude", "longitude"]),
            'altitude3D':  xr.DataArray(ds.hhl.isel(time=0), dims=["altitude", "latitude", "longitude"]),
        })
        ds.altitude.attrs  = {'units': 'm'}
        ds.latitude.attrs  = {'units': 'deg'}
        ds.longitude.attrs = {'units': 'deg'}
        ds.time.attrs      = {'units': 'UTC'}
        ds.attrs['ncfile'] = ncfile_cs_experiment
        ds.attrs['run_id'] = run_id
        return ds

    return _preprocess

def fetch_3d_data(ncfile_3d, ncfile_extpar, nml_input_org, var_sets=['meteo'], chunks=None):
    """Load 3D dataset."""
    meteo_var = ['hhl', 'rho', 'qi', 'qc', 'qs', 'qv', 'qr', 'dz', 't', 'ut', 'vt'] if 'meteo' in var_sets else []
    spec_var = ['nf', 'nw', 'qw', 'qfw'] if 'spec' in var_sets else []
    bulk_var = ['icnc', 'cdnc', 'qwtot', 'qftot'] if 'bulk' in var_sets else []
    if chunks is None:
        chunks = {}
    data_extpar = xr.open_mfdataset(ncfile_extpar, parallel=False)
    lat2D = data_extpar['lat'].values[7:-7, 7:-7]
    lon2D = data_extpar['lon'].values[7:-7, 7:-7]
    ds = xr.open_mfdataset(ncfile_3d, chunks=chunks, parallel=True)

    if 'bulk' in var_sets:
        ds[bulk_var] = ds[spec_var].isel(bin=slice(None, None)).sum(dim='bin')
        ds['nc'] = ds['nw'].isel(bin=slice(30, 50)).sum(dim='bin')
        ds['nr'] = ds['nw'].isel(bin=slice(50, None)).sum(dim='bin')
        ds['ni'] = ds['nf'].isel(bin=slice(30, 50)).sum(dim='bin')
        ds['ns'] = ds['nf'].isel(bin=slice(50, None)).sum(dim='bin')

    if 'spec' in var_sets:
        diameter_µm = define_bin_boundaries() * 1.0e6 * 2.0
        ds = ds.rename(bin='diameter')
        ds = ds.assign_coords({
            'diameter': xr.DataArray((diameter_µm[1:] + diameter_µm[:-1]) / 2.0, dims="diameter"),
        })
    time = get_model_datetime_from_meta(nml_input_org, ds.time.values)
    ds = ds.rename({'x': 'longitude', 'y': 'latitude', 'z': 'altitude', })
    ds = ds.assign_coords({
        'time': xr.DataArray(time, dims="time"),
        'longitude': xr.DataArray(np.linspace(lon2D.min(), lon2D.max(), lon2D.shape[1]), dims="longitude"),
        'latitude': xr.DataArray(np.linspace(lat2D.min(), lat2D.max(), lat2D.shape[0]), dims="latitude"),
        'altitude': xr.DataArray(ds.hhl.isel(time=0).mean(dim=('longitude', 'latitude')), dims="altitude"),
        'latitude2D': xr.DataArray(lat2D, dims=["latitude", "longitude"]),
        'longitude2D': xr.DataArray(lon2D, dims=["latitude", "longitude"]),
        'altitude3D': xr.DataArray(ds.hhl.isel(time=0), dims=["altitude", "latitude", "longitude"]),
    })
    ds.altitude.attrs = {'units': 'm'}
    ds.latitude.attrs = {'units': 'deg'}
    ds.longitude.attrs = {'units': 'deg'}
    ds.time.attrs = {'units': 'UTC'}
    run_id = ncfile_3d.split('/')[-1].split('_')[1].split('.')[0]
    ds.attrs['ncfile'] = ncfile_3d
    ds.attrs['run_id'] = run_id
    return ds


def _median_time_step_seconds(time_values: np.ndarray) -> float:
    tv = np.asarray(time_values, dtype="datetime64[ns]")
    if tv.size < 2:
        raise ValueError("need at least two time steps to infer Δt")
    deltas = np.diff(tv.astype(np.int64))
    return float(np.median(deltas)) / 1e9


def harmonize_experiment_time_to_finest(ds_list, exp_names=None, method: str = "linear"):
    """Interpolate each dataset to one shared time axis so ensembles can be ``xr.concat``'d.

    Uses the **finest** native median Δt across members (e.g. 10 s when some files are 30 s).
    The grid spans the **overlap** of all time ranges ``[max(start), min(end)]`` with that Δt,
    so values are interpolated, not extrapolated.

    Parameters
    ----------
    ds_list
        One xarray Dataset per experiment, each with a ``time`` coordinate.
    exp_names
        Optional ids for logging (same order as ``ds_list``).
    method
        Passed to :meth:`xarray.Dataset.interp`.
    """
    if not ds_list:
        return ds_list
    times_list = [np.asarray(ds.time.values, dtype="datetime64[ns]") for ds in ds_list]
    if all(np.array_equal(times_list[0], t) for t in times_list[1:]):
        return ds_list

    dts = [_median_time_step_seconds(t) for t in times_list]
    dt_target = min(dts)
    step_ns = int(round(float(dt_target) * 1e9))

    t0 = max(t[0] for t in times_list)
    t1 = min(t[-1] for t in times_list)
    if t1 < t0:
        raise ValueError(
            "experiments have no overlapping time range; cannot build a common time axis"
        )

    t0_i = t0.astype(np.int64)
    t1_i = t1.astype(np.int64)
    n = int(np.floor((t1_i - t0_i) / step_ns)) + 1
    common = (t0_i + np.arange(n, dtype=np.int64) * step_ns).astype("datetime64[ns]")
    common = common[common <= t1]

    common_time = xr.DataArray(common, dims=("time",))
    labels = exp_names if exp_names is not None else [f"{i}" for i in range(len(ds_list))]
    print(
        "Time harmonization: median native Δt (s) → target grid Δt=%s s, n_time=%s, overlap [%s … %s]"
        % (
            step_ns / 1e9,
            common.size,
            np.datetime_as_string(common[0], unit="s"),
            np.datetime_as_string(common[-1], unit="s"),
        )
    )
    print("  per experiment:", dict(zip(labels, dts)))

    return [ds.interp(time=common_time, method=method) for ds in ds_list]


def calculate_supersaturation_ice(temperature, absolute_humidity):
    """
    Calculate supersaturation over ice, according to Murphy & Koop (2005)
    temperature [K]
    absolute_humidity [kg/m3]
    """
    epsilon = 1e-14
    R_v = 461.5  # J/(kg·K), specific gas constant for water vapour
    xlog = xr.ufuncs.log
    xexp = xr.ufuncs.exp
    A_ice = 9.550426
    B_ice = 5723.265
    C_ice = 3.53068
    D_ice = 0.00728332
    e_sat_ice = xexp(A_ice - B_ice / temperature + C_ice * xlog(temperature+epsilon) - D_ice * temperature)
    e_actual = absolute_humidity * R_v * temperature
    return (e_actual / e_sat_ice - 1.0) * 100.0

def calculate_supersaturation_water(temperature, absolute_humidity):
    """
    Calculate supersaturation over water, according to Bolton (1980)
    temperature [K]
    absolute_humidity [kg/m3]
    """
    epsilon = 1e-14
    R_v = 461.5  # J/(kg·K), specific gas constant for water vapour
    xexp = xr.ufuncs.exp
    e_0 = 611.2  # Pa, reference vapour pressure
    a_water = 17.67
    b_water = 243.5  # K
    T_celsius = temperature - 273.15
    e_sat_water = e_0 * xexp(a_water * T_celsius / (T_celsius + b_water + epsilon))
    e_actual = absolute_humidity * R_v * temperature
    return (e_actual / e_sat_water - 1.0) * 100.0


def convert_units_meteogram(ds, rho):

    # aerosol and ice number conc to L-1
    for var in ['NINP', 'ICNC', 'NI', 'NF']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-3
        ds[var].attrs['units'] = 'L-1'

    # cloud droplet number conc to cm-3
    for var in ['CDNC', 'NW']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e-6
        ds[var].attrs['units'] = 'cm-3'

    # mass of ice particles to gL-1
    for var in ['QI_Sp', 'QFtot']:
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+0
        ds[var].attrs['units'] = 'gL-1'

    # mass of cloud droplets and vapor to gm-3 
    for var in ['QV_Sp', 'QC_Sp', 'QWtot']: # note: QV stays at units kg/kg
        if var not in ds.data_vars:
            continue
        ds[var] = ds[var] * rho * 1.0e+3
        ds[var].attrs['units'] = 'gm-3'

    return ds

def fetch_meteogram_numbers(filelist):
    meteogram_nrs = [ int(f.split('/')[-1].split('.')[0].split('_')[1]) for f in filelist ]
    expermiment_nrs = [ int(f.split('/')[-1].split('.')[0].split('_')[2]) for f in filelist ]
    run_ids = [ int(f.split('/')[-1].split('.')[0].split('_')[3]) for f in filelist ]
    return meteogram_nrs, expermiment_nrs, run_ids


def fetch_meteogram_data(filelist, resolution="400m"):
    """Meteogram data given in units of mixing ratios kg/kg and 1/kg is converted
    to conveinient unit, e.g. cm-3, L-1, gcm-3."""
    from utilities import define_bin_boundaries
    
    spec_var = ['NF', 'NW', 'QW', 'QFW']
    bulk_var = ['ICNC', 'CDNC', 'QWtot', 'QFtot']
    tfr = np.arange(-30, 0, 1)
    ds_list = []
    for f in filelist:
        ds = xr.open_dataset(f, chunks={})
        for svar, bvar in zip(spec_var, bulk_var):
            if svar in ds:
                ds[bvar] = ds[svar].isel(bins=slice(30, 50)).sum(dim='bins')

        meteogram_nrs, expermiment_nrs, run_ids = fetch_meteogram_numbers([f])
        ds.attrs.update({"resolution": resolution, "exp_num": f.split('_')[-2], 'label': 'M_Eriswil'})
        
        diameter_µm = define_bin_boundaries() * 1.0e6 * 2.0
        ds = ds.rename({'bins': 'diameter', 'HMLd': 'altitude'})
        ds = ds.assign_coords(
            tfr=xr.DataArray(tfr, dims=["tfr"]),
            diameter=xr.DataArray((diameter_µm[1:] + diameter_µm[:-1]) / 2.0, dims="diameter")
        )
        ds.diameter.attrs = {'units': 'µm'}
        ds.altitude.attrs = {'units': 'm'}
        ds_list.append(ds)

    return ds_list


def tobac_5dspecs(tobac_input, parameter_features, exp_name='', output_dir='', debug_print=True):

    import tobac
    print("\n[5] Tobac cloud tracking...")
    tobac_input_iris = tobac_input.to_iris()
    
    # Calculate spatial and temporal resolution
    delta_x = 1e3 * np.mean(np.diff(tobac_input.longitude.values)) * 111.13295254925466  # 1 degree of longitude in km
    delta_y = 1e3 * np.mean(np.diff(tobac_input.latitude.values)) * 111.13295254925466  # 1 degree of latitude in km
    delta_t = np.mean(np.diff(tobac_input.time.astype('datetime64[s]')).astype(float))
    dxy, dt = tobac.get_spacings(tobac_input_iris, grid_spacing=np.max([delta_x, delta_y]), time_spacing=delta_t)
    
    # Setup output file names
    features_file      =  f'{output_dir}/{exp_name}_{tobac_input.name}_tobac_features.csv'
    tracks_file        =  f'{output_dir}/{exp_name}_{tobac_input.name}_tobac_track.csv'
    features_mask_file =  f'{output_dir}/{exp_name}_{tobac_input.name}_tobac_features_mask.csv'
    segm_mask_file     =  f'{output_dir}/{exp_name}_{tobac_input.name}_tobac_mask_xarray.nc'
    
    # Feature detection based on precipitation field and thresholds
    if "min_distance" not in parameter_features:
        parameter_features["min_distance"] = 2.0 * dxy
    # if "position_threshold" not in pkl:
    #     parameter_features["position_threshold"] = "extreme"  # weighted_abs
    # if "sigma_threshold" not in pkl:
    #     parameter_features["sigma_threshold"] = 0.5
    # if "sigma_threshold" not in pkl:
    #     parameter_features["n_min_threshold"] = 3
    # if "target" not in pkl:
    #     parameter_features["target"] = "maximum"
    # if "threshold" not in pkl:
    #     parameter_features["threshold"] = 1e-7
    # if "PBC_flag" not in pkl:
    #     parameter_features["PBC_flag"] = "hdim_2"
    # if "vertical_coord" not in pkl:
    #     parameter_features["vertical_coord"] = "altitude"
        
    if debug_print:
        print(f'debug:  delta_x: {delta_x:.3f} m    with n_x = {tobac_input.longitude.size}')
        print(f'debug:  delta_y: {delta_y:.3f} m    with n_y = {tobac_input.latitude.size}')
        print(f'debug:  delta_y: {delta_y:.3f} m    with n_z = {tobac_input.altitude.size}')
        print(f'debug:  delta_t: {delta_t:.3f} s    with n_x = {tobac_input.time.size}')
        print(f'debug:  min/max features      g/L    {tobac_input.min().values:.3e} to {tobac_input.max().values:.3e}')
        
        print(f'debug:  tobac_input_iris: {tobac_input_iris}')
        print(f'debug:  dxy: {dxy:.3f}')
        print(f'debug:  dt:  {dt:.3f}')
    
    # Tobac feature detection
    print('feature detection params:')
    print(parameter_features)
    features = tobac.feature_detection_multithreshold(tobac_input_iris, dxy, **parameter_features)
    features.to_csv(features_file)
    print("\n[6] Tobac features written to: ", features_file)
        
    # Link features to trajectories
    tracks = tobac.linking_trackpy(
        features,
        tobac_input_iris,
        # method_linking="randome",
        vertical_coord=parameter_features["vertical_coord"],
        dt=dt, dxy=dxy, v_max=100)
    tracks.to_csv(tracks_file)
    print("\n[7] Tobac tracks written to: ", tracks_file)

    # Segmentation
    mask, features_mask = tobac.segmentation.segmentation(
        features,
        tobac_input_iris,
        dxy, threshold=parameter_features["threshold"][0],
        vertical_coord=parameter_features["vertical_coord"])

    xrmask = xr.DataArray.from_iris(mask)
    features_mask.to_csv(features_mask_file)
    print("\n[8] Tobac features_mask written to: ", features_mask_file)
    
    if os.path.exists(segm_mask_file):
        print(f'    Removed {segm_mask_file}')
        os.remove(segm_mask_file)
    xrmask.to_netcdf(segm_mask_file, mode='w')
    print("\n[9] Tobac segm_mask written to: ", segm_mask_file)

    print('\nTOBAC done and EXIT')
    return tobac_input, features_file, tracks_file, features_mask_file, segm_mask_file
    
def extract_segmented_tracks(ds, tracks, fmask=None, pre_track_steps=5, **cfg):
    import pandas as pd
    from utilities import calculate_mean_diameter
    """Vectorized version with pre-tracking timesteps."""
    cells = {}
    
    for cell_id in np.unique(tracks['cell'].values):
        track = tracks.where(tracks['cell'] == cell_id, drop=True)
        
        # Add pre-tracking timesteps
        t0 = track['time'].values[0]
        t_start = t0 - np.timedelta64(pre_track_steps * 10, 's')  # Assuming 10s intervals
        pre_times = pd.date_range(t_start, t0, periods=pre_track_steps, inclusive='left')
        
        # Combine pre-track and track times
        all_times = np.concatenate([pre_times.values, track['time'].values])
        
        # For pre-track: use first tracked position
        coords = {
            'time': xr.DataArray(all_times.astype('datetime64[ns]'), dims="path"),
            'altitude': xr.DataArray(np.concatenate([[track['altitude'].values[0]]*pre_track_steps, track['altitude'].values]), dims="path"),
            'latitude': xr.DataArray(np.concatenate([[track['latitude'].values[0]]*pre_track_steps, track['latitude'].values]), dims="path"),
            'longitude': xr.DataArray(np.concatenate([[track['longitude'].values[0]]*pre_track_steps, track['longitude'].values]), dims="path")
        }

        if fmask is None:
            ds_subset = ds.sel(coords, method='nearest')
            delta_z = ds_subset.dz.values[..., np.newaxis]
            temperature = ds_subset.t
            cell_vol = cfg['delta_x'] * cfg['delta_y'] * delta_z
            cell_ds = ds_subset[['qfw', 'qw']] * cell_vol * 1e-3
            cell_ds['temperature'] = xr.DataArray(temperature, dims="path")
        else:
            ds_subset = ds.sel(time=coords['time'], method='nearest')
            delta_z = ds_subset.dz.values[..., np.newaxis]
            temperature = ds_subset.t
            cell_vol = cfg['delta_x'] * cfg['delta_y'] * delta_z
            cell_ds = ds_subset[['qfw', 'qw']] * cell_vol * 1e-3
            cell_ds = cell_ds.sum(['altitude', 'latitude', 'longitude'])
            cell_ds['temperature'] = xr.DataArray(temperature.mean(['altitude', 'latitude', 'longitude']), dims="path")

        cell_ds = cell_ds.assign_coords(coords)#.persist()

        for var, src, start, end in [('qi', 'qfw', 30, 50), ('qs', 'qfw', 50, None), 
                                      ('qc', 'qw', 30, 50), ('qr', 'qw', 50, None)]:
            _ds = cell_ds[src].isel(diameter=slice(start, end))
            cell_ds[f'mean_{var}'] = xr.DataArray(calculate_mean_diameter(_ds.values, _ds.diameter.values), dims='path')

        cells[cell_id] = cell_ds
    
    return cells

def extract_segmented_tracks_fast(ds, tracks, fmask=None, **cfg):
    from utilities import calculate_mean_diameter
    """Vectorized version for better performance."""
    cells = {}
    for cell_id in np.unique(tracks['cell'].values):
        track = tracks.where(tracks['cell'] == cell_id, drop=True)
        coords = {  'time': xr.DataArray(track['time'].values.astype('datetime64[ns]'), dims="path"),
                    'altitude': xr.DataArray(track['altitude'].values, dims="path"),
                    'latitude': xr.DataArray(track['latitude'].values, dims="path"),
                    'longitude': xr.DataArray(track['longitude'].values, dims="path")} 

        if fmask is None:
            ds_subset = ds.sel( coords, method='nearest'  )
            delta_z   = ds_subset.dz.values[..., np.newaxis]
            temperature= ds_subset.t
            cell_vol  = cfg['delta_x'] * cfg['delta_y'] * delta_z 
            cell_ds   = ds_subset[['qfw', 'qw']] * cell_vol * 1e-3  # to kg
            cell_ds['temperature'] = xr.DataArray(temperature, dims="path")

        else:
            ds_subset = ds.sel( time = coords['time'], method='nearest' )
            delta_z   = ds_subset.dz.values[..., np.newaxis]
            temperature= ds_subset.t
            cell_vol  = cfg['delta_x'] * cfg['delta_y'] * delta_z 
            cell_ds = ds_subset[['qfw', 'qw']] * cell_vol * 1e-3  # to kg
            cell_ds = cell_ds.sum(['altitude', 'latitude', 'longitude'])
            cell_ds['temperature'] = xr.DataArray( temperature.mean(['altitude', 'latitude', 'longitude']), dims="path")  

        # assing variables along path as coordinates
        cell_ds = cell_ds.assign_coords( coords )
        #cell_ds = cell_ds.persist() 

        # compute liquid and frozen population mean values 
        calc_mean_list = [('qi', 'qfw', 30, 50), 
                          ('qs', 'qfw', 50, None), 
                          ('qc', 'qw', 30, 50),  
                          ('qr', 'qw', 50, None)]

        for var, src, start, end in calc_mean_list:
            _ds = cell_ds[src].isel( diameter=slice(start, end) )
            mean_diameter = calculate_mean_diameter(_ds.values, _ds.diameter.values)
            cell_ds[f'mean_{var}'] = xr.DataArray( mean_diameter, dims='path' )

        cells[cell_id] = cell_ds

    return cells
