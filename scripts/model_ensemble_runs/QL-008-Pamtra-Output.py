
import argparse
import os

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process some integers.')
#parser.add_argument('--data_path', type=str, help='The data path')
parser.add_argument('--data_set', type=str, help='The data set identifier')
parser.add_argument('--idx_run', type=str, help='The index number of the individual run', required=False, default=1)
parser.add_argument('--vars', type=str, help='The variables to plot', required=False, default='nf,qf')
args = parser.parse_args()

data_path = os.path.join(os.getcwd(), os.pardir, 'data_model')
print(f'data_path: {os.path.abspath(data_path)}')
data_set = args.data_set
idx_run = int(args.idx_run)
plot_all_frames = True
variable_subset = args.vars.split(',') if ',' in args.vars else [args.vars]

# %matplotlib inline

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), os.pardir)))
import xarray as xr
xr.set_options(keep_attrs=True)
import numpy as np
import matplotlib as mpl
import matplotlib.colors as colors
import matplotlib.pyplot as plt
from PIL import Image
import importlib
import multiprocessing

import colormaps as cmaps 
import importlib
import glob

import pyPamtra

import tools
import utils
importlib.reload(tools) # to avoid restarting the kernel



# turn warnings off

import warnings
irgnore_list = [
    "timestamp set to now",
    "lat set to 50.938056",
    "lon set to 6.956944",
    "wind10u set to 0",
    "wind10v set to 0",
    "groundtemp set to nan",
    "sfc_salinity set to 33.0",
    "sfc_slf set to 1.0",
    "sfc_sif set to 0.0",
    "sfc_type set to -9999",
    "sfc_model set to -9999",
    "sfc_refl set to S",
    "obs_height set to [833000.0, 0.0]",
    "hydro_q set to 0",
    "hydro_reff set to 0",
    "hydro_n set to 0",
    "airturb set to nan",
    "wind_w set to nan",
    "wind_uv set to nan",
    "turb_edr set to nan",
]
for ignore in irgnore_list:
    # Filter specific warnings
    warnings.filterwarnings('ignore', message=f'{ignore}.*')

root_path = os.getcwd()
# data_path = './data_model'
obs_path = './data_obs'
plot_path = './plots'


#data_set = 'cs-eriswil__20240711_153416' # ishape=1 VERY old run
#data_set = 'cs-eriswil__20250304_140529' # fluxes (NO nf,nw,qf,qw) (ishape=1) AR=10,d (ishape=3) AR=empirical fcn, (ishape=4) AR=empirical fcn
#data_set = 'cs-eriswil__20250306_125823' # fluxes (nf,nw,qf,qw) (ishape=4) AR=empirical fcn

# ensembles with variing BG INP/CCN and flare INP/CCN
#data_set = 'cs-eriswil__20250219_100953' # even larger ensemble
# data_set = 'cs-eriswil__20250219_223004' # even larger ensemble  varying flare inp

# high time resolution data
#data_set, resolution = 'cs-eriswil__20250117_111628', 'yes' # 200x160 5sec output
#data_set, resolution = 'cs-eriswil__20250309_154959', 'yes' # 5sec 12utc 
# data_set, resolution = 'cs-eriswil__20250318_234102', 'no' # 5sec 09utc 
# data_set, resolution = 'cs-eriswil__20250318_154251', 'yes' # 5sec 12utc 

# cloudlab data
holimo_filename             = f'{obs_path}/holimo/2023-01-25/CL_20230125_1000_1140_SM058_SM060_ts1.nc'

# LACROS raw datafiles
radar_mbr7_filespath        = f'{obs_path}/mbr7/20230125/'
mwr_data_path               = f'{obs_path}/hatpro_lacros/20230125/'

# cloudnet data
mwr_hatpro_filename_cndp    = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_hatpro.nc'
radar_mira35_filename_cndp  = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_mira_c98b69a5.nc'
categorize_filename_cndp    = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_categorize-voodoo.nc'
iwc_filename_cndp           = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_iwc-Z-T-method.nc'
lwc_filename_cndp           = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_lwc-scaled-adiabatic.nc'
class_filename_cndp         = f'{obs_path}/cloudnet_dataportal/20230125/20230125_eriswil_classification-voodoo.nc'

lat_end, lon_end = 47.08, 7.90522
lat_start, lon_start = 47.05, 7.804
flare_lat, flare_lon = 47.07425, 7.90522
origin_lat, origin_lon = 47.070522, 7.872991


eriswil_height_asl = 921.0

# idx_run = 0

time_frames_plume = [
    [np.datetime64('2023-01-25T10:30:00'), np.datetime64('2023-01-25T10:42:00')],
    [np.datetime64('2023-01-25T10:55:00'), np.datetime64('2023-01-25T11:07:00')],
    [np.datetime64('2023-01-25T11:20:00'), np.datetime64('2023-01-25T11:31:00')]
    ] # standart plume time frames

# time_frames_plume = [
#     [np.datetime64('2023-01-25T09:00:00'), np.datetime64('2023-01-25T09:12:00')],
#     [np.datetime64('2023-01-25T09:25:00'), np.datetime64('2023-01-25T09:37:00')],
#     [np.datetime64('2023-01-25T09:48:00'), np.datetime64('2023-01-25T10:00:00')]
#     ] # early plume time frames for testing

time_frame_tbs = [np.datetime64('2023-01-25T10:30:00'), np.datetime64('2023-01-25T11:30:00')]
plot_time_frame = [np.datetime64("2023-01-25T09:00:00"), np.datetime64("2023-01-25T12:00:00")]
plot_time_frame = time_frame_tbs
plot_height_frame = (950, 1500)



# returns a nested dict with metadata
importlib.reload(tools)
metadata = tools.open_metadata(data_path, data_set)
resolution = tools.get_domain_resolution(metadata[next(iter(metadata))])

# list the identifiers of the ensembles
ensemble_nrs = sorted(list(metadata.keys()))
ensemble_nrs

meteogram_data_path = os.path.join(data_path, data_set, 'processed')

model_data = {}
for id in metadata.keys():
    file_name = os.path.join(meteogram_data_path, id, f'3D_{id}_meteogram_v3.nc')

    model_data[id] = xr.open_dataset(file_name)
    radius = tools.define_bin_boundaries()
    diameter_µm_bounds = radius * 2.0
    diameter_µm = (diameter_µm_bounds[1:] + diameter_µm_bounds[:-1]) / 2.0
    #model_data[id] = model_data[id].rename({'HMLd_2': 'z', 'bins': 'bin'})
    bins_coord = np.arange(len(model_data[id].bins)) 
    model_data[id] = model_data[id].assign_coords(bins=bins_coord)

model_data







# meteogram_data_path = os.path.join(data_path, data_set, 'processed')

# model_data = {}
# for id in metadata.keys():
#     file_name = os.path.join(meteogram_data_path, id, f'3D_{id}_meteogram_v3.nc')
#     try:
#         model_data[id] = xr.open_dataset(file_name)
#         radius = tools.define_bin_boundaries()
#         diameter_µm_bounds = radius * 2.0
#         diameter_µm = (diameter_µm_bounds[1:] + diameter_µm_bounds[:-1]) / 2.0
#         #model_data[id] = model_data[id].rename({'HMLd_2': 'z', 'bins': 'bin'})
#         bins_coord = np.arange(len(model_data[id].bin) + 1) 
#         model_data[id] = model_data[id].assign_coords(bins=bins_coord)
        
#         model_data[id] = model_data[id].assign_coords({
#             #'d': ('bin', diameter_µm),
#             'dbounds': ('bins', diameter_µm_bounds)
#             })
#         model_data[id] = model_data[id].load()

#     except Exception as e:
#         ensemble_nrs.remove(id)
#         print(f'Warning: Ensemble (run_id) {id} not found --> skipping: {e}')





# # returns 1D and 2D numpy arrays of lat and lon
meta_file = glob.glob(os.path.join(data_path, data_set, '*.json'))[0]
lat_1D, lon_1D, lat_2D_extpar, lon_2D_extpar, extpar = utils.load_grid_data(meta_file)





model_data[id]

model_data[id].data_vars

ensemble_nrs



run_id = ensemble_nrs[idx_run]
bin_slice = slice(31, 50)
bins_slice = slice(30, 50)
data_M = model_data[run_id]

list_height_levels = np.array(list(data_M.HMLd.values[::-1]) + [2500.])
rho = np.ma.masked_invalid(data_M['rho'].values).filled(0)
try:
    nf = np.ma.masked_invalid(data_M['nf'].isel(bin=bin_slice).values).filled(0)
    nw = np.ma.masked_invalid(data_M['nw'].isel(bin=bin_slice).values).filled(0)
    Dbound  = data_M.dbounds.isel(bins=bins_slice).values 
except:
    nf = np.ma.masked_invalid(data_M['nf'].values).filled(0)
    nw = np.ma.masked_invalid(data_M['nw'].values).filled(0)
    Dbound = diameter_µm_bounds
# #D  = np.concatenate([data_M.RGRENZ_right.values[:-1], data_M.RGRENZ_left.values[-2:]]) * 2.0 # radius to diameter

Dmean = (Dbound[:-1] + Dbound[1:]) / 2.0
nBins = Dmean.size
print(Dbound)
print(Dmean)



pam = pyPamtra.pyPamtra()

pam.df.addHydrometeor((
    "liquid",  # name 
    -99.,  # aspect ratio (NOT RELEVANT)
    1,  # liquid - ice flag
    -99.,  # density (NOT RELEVANT)
    -99.,  # mass size relation prefactor a (NOT RELEVANT)
    -99.,  # mass size relation exponent b (NOT RELEVANT)
    -99.,  # area size relation prefactor alpha (NOT RELEVANT)
    -99.,  # area size relation exponent beta (NOT RELEVANT)
    0,  # moment provided later (NOT RELEVANT)
    nBins,  # number of bins
    "fullBin",  # distribution name (NOT RELEVANT)
    -99.,  # distribution parameter 1 (NOT RELEVANT)
    -99.,  # distribution parameter 2 (NOT RELEVANT)
    -99.,  # distribution parameter 3 (NOT RELEVANT)
    -99.,  # distribution parameter 4 (NOT RELEVANT)
    -99.,  # minimum diameter (NOT RELEVANT)
    -99.,  # maximum diameter (NOT RELEVANT)
    'mie-sphere',  # scattering model
    'khvorostyanov01_drops',  # fall velocity relation
    0.0  # canting angle
))

pam.df.addHydrometeor((
    "ice",  # name 
    -99.,  # aspect ratio (NOT RELEVANT)
    -1,  # liquid - ice flag
    -99.,  # density (NOT RELEVANT)
    -99.,  # mass size relation prefactor a (NOT RELEVANT)
    -99.,  # mass size relation exponent b (NOT RELEVANT)
    -99.,  # area size relation prefactor alpha (NOT RELEVANT)
    -99.,  # area size relation exponent beta (NOT RELEVANT)
    0,  # moment provided later (NOT RELEVANT)
    nBins,  # number of bins
    "fullBin",  # distribution name (NOT RELEVANT)
    -99.,  # distribution parameter 1 (NOT RELEVANT)
    -99.,  # distribution parameter 2 (NOT RELEVANT)
    -99.,  # distribution parameter 3 (NOT RELEVANT)
    -99.,  # distribution parameter 4 (NOT RELEVANT)
    -99.,  # minimum diameter (NOT RELEVANT)
    -99.,  # maximum diameter (NOT RELEVANT)
    'ssrg-rt3',  # scattering model
    'heymsfield10_particles',  # fall velocity relation
    0  # canting angle
))

pam = pyPamtra.importer.createUsStandardProfile(
    pam, 
    hgt_lev=list_height_levels,
)

#sorted(pam.p.keys())

#pam.p['temp_lev'], pam.p['press_lev']

pam.p['wind_uv'][:] = 10
pam.p['turb_edr'][:] = 1e-4

# pam.p["hydro_q"][:] = 0.001

pam.nmlSet["passive"] = False
pam.nmlSet["randomseed"] = 0
pam.nmlSet["radar_mode"] = "spectrum"
pam.nmlSet["radar_aliasing_nyquist_interv"] = 3
pam.nmlSet["hydro_adaptive_grid"] = False
pam.nmlSet["conserve_mass_rescale_dsd"] = False
pam.nmlSet["radar_use_hildebrand"] = True
pam.nmlSet["radar_noise_distance_factor"] = -2
pam.nmlSet["hydro_fullspec"] = True

pam.set["verbose"] = 1
pam.set["pyVerbose"] = 1

pam.df.addFullSpectra()

list(pam.df.dataFullSpec.keys())


# some modifications to the model data
if 'eriswil__20250117_111628' in data_set:
    factor = 1.0e9
if 'eriswil__20250309_154959' in data_set:
    factor = 1.0e9
    
if 'eriswil__20250219_223004' in data_set:
    factor = 1.0e6
if 'eriswil__20250219_100953' in data_set:
    factor = 1.0e6

factor = 1.0e3

idx_time = min(200, len(nw) - 1) #490
dsd_nw = nw[idx_time, ::-1, :]
dsd_nf = nf[idx_time, ::-1, :]
print(dsd_nw.shape)

pam.df.dataFullSpec["d_bound_ds"][:] = Dbound
pam.df.dataFullSpec["d_ds"][:] = Dmean
pam.df.dataFullSpec["n_ds"][:, :, :, 0, :] = dsd_nw[
    np.newaxis, 
    np.newaxis,
    :, 
    :,
] * factor # 1/cm3 to 1/m3

pam.df.dataFullSpec["n_ds"][:, :, :, 1, :] = dsd_nf[
    np.newaxis, 
    np.newaxis,
    :, 
    :,
] * factor # 1/cm3 to 1/m3




pam.df.dataFullSpec["rho_ds"][:] = 1000. #rho[idx_time, np.newaxis, :, np.newaxis, np.newaxis]
pam.df.dataFullSpec["area_ds"][:] = (np.pi / 4. * pam.df.dataFullSpec["d_ds"][:]**2)
pam.df.dataFullSpec["mass_ds"][:] = (np.pi / 6. * pam.df.dataFullSpec["rho_ds"][:] * pam.df.dataFullSpec["d_ds"][:]**3)
pam.df.dataFullSpec["as_ratio"][:, :, :, 0, :] = 1.0
pam.df.dataFullSpec["as_ratio"][:, :, :, 1, :] = 5.0

frequencies = [35.5]
pam.runParallelPamtra(np.array(frequencies))
print((pam.fortError))
print('Done processing height spectrogram')



list(pam.r.keys())

pam_velocity = pam.r['radar_vel'].squeeze()
pam_height = pam.p['hgt'].squeeze()
pam_spectra = pam.r['radar_spectra'].squeeze()
Ze = pam.r['Ze'].squeeze()

plt.plot(pam.r['Ze'].squeeze(), pam_height, )
#plt.xlim(-40, 20)
plt.xscale('symlog')
plt.ylim(800, 2000)
pam.r['Ze'].shape





data3D_deff_w = (np.nansum(dsd_nw * Dmean**3, axis=1) / np.nansum(dsd_nw* Dmean**2, axis=1)) * 1.0e6 # m to µm
data3D_deff_f = (np.nansum(dsd_nf * Dmean**3, axis=1) / np.nansum(dsd_nf* Dmean**2, axis=1)) * 1.0e6 # m to µm
try:
    data3D_height = model_data[run_id].z.values[::-1]
except:
    data3D_height = model_data[run_id].HMLd_2.values[::-1]


Dmean



# Set up figure and styling
font_size = 14
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
mpl.rcParams.update({'font.size': font_size, 'xtick.labelsize': font_size, 'ytick.labelsize': font_size})

# Create figure with 3 subplots
fig, axes = plt.subplots(ncols=3, figsize=(19, 10))
ax0, ax1, ax2 = axes
dtime = model_data[run_id].time.values
cmap = tools.cmap_new_timeheight
Dmean_µm = Dmean*1.0e6  
dsd_nw_masked = np.ma.masked_less_equal(dsd_nw, 0.0) * 1/factor
dsd_nf_masked = np.ma.masked_less_equal(dsd_nf, 0.0) * 1/factor
v_lim_liq = 1.e-2, 1.e2
v_lim_ice = 1.e-6, 1.e1
v_lim_rad = -60, 0

# Plot droplet concentration
pm0 = ax0.pcolormesh(Dmean_µm, data3D_height, dsd_nw_masked,
                    norm=mcolors.LogNorm(vmin=v_lim_liq[0], vmax=v_lim_liq[1]), cmap=cmap)
plt.colorbar(pm0, label='droplet concentration [cm$^{-3}$)]', ax=ax0, extend='both')
ax0.plot(data3D_deff_w, data3D_height, c='k', lw=3)
ax0.set_xscale('log')
ax0.set_xlabel('particle size [µm]\neffective diamater (black) [µm]')
ax0.set_xlim([1.0e0, 1.0e2])

# Plot ice crystal concentration
pm1 = ax1.pcolormesh(Dmean_µm, data3D_height, dsd_nf_masked,
                    norm=mcolors.LogNorm(vmin=v_lim_ice[0], vmax=v_lim_ice[1]), cmap=cmap)
plt.colorbar(pm1, label='ice crystal concentration [cm$^{-3}$]', ax=ax1, extend='both')
ax1.plot(data3D_deff_f, data3D_height, c='k', lw=3)
ax1.set_xscale('log')
ax1.set_xlabel('particle size [µm]\neffective diamater (black) [µm]')
ax1.set_xlim([3.0e1, 1.0e3])
ax1.set_title(f'time step and utc: {idx_time}, {dtime[idx_time]}', pad=20)
ax1.set_ylabel('')

# Plot spectral reflectivity
pm2 = ax2.pcolormesh(pam_velocity, pam_height, np.fliplr(pam_spectra),
                    vmin=v_lim_rad[0], vmax=v_lim_rad[1], rasterized=True, cmap=cmap)
plt.colorbar(pm2, label='Spectral reflectivity [dB]', ax=ax2, extend='both')
ax2.set_xlim(-1.5, 1.5)
ax2.set_xlabel('Doppler velocity [m s$-1$]')
ax2.set_ylabel('')

# Common formatting for all axes
for i, iax in enumerate(axes):        
    iax.tick_params(which='both', direction='in')
    iax.minorticks_on()
    iax.set_ylim(800, 1600)
    if i == 0:
        iax.set_ylabel('Altitude [m]')
    iax.grid(True, which='major', linestyle='-', linewidth='0.5', color='black', alpha=0.5)
    iax.grid(True, which='minor', linestyle=':', linewidth='0.5', color='black', alpha=0.25)
    iax.xaxis.set_tick_params(which='both', direction='inout', top=True)

# Adjust layout and save
plt.subplots_adjust(wspace=0.3)

date_str = str(dtime[idx_time])[:19].replace('T', '_').replace(':', '').replace('-', '')
png_path = os.path.join(plot_path, '008-Pamtra-output', data_set)
os.makedirs(png_path, exist_ok=True)
file_name = f'{data_set}_{run_id}_{date_str}_PAMTRA.png'
file_path = os.path.join(png_path, file_name)
plt.savefig(file_path, dpi=300, bbox_inches='tight')
print(file_path)


try:
    tmp_file_path = os.path.join(plot_path, 'tmp', file_name)
    plt.savefig(tmp_file_path, dpi=300, bbox_inches='tight')
    print(tmp_file_path)
    print('yay')
except Exception as e:
    print(f"Error saving figure: {e}")
















from multiprocessing import Pool, Manager, cpu_count
import numpy as np
from tqdm.auto import tqdm


def process_pamtra_parallel(n_timesteps, n_processes=None):
    """
    Process PAMTRA calculations in parallel and collect results for plotting
    """
    if n_processes is None:
        n_processes = max(1, min(n_timesteps, cpu_count() - 2))  # Leave one CPU free

    with Pool(processes=n_processes) as pool:
        results = pool.map(run_pamtra, range(n_timesteps))

    return results

def merge_results(results):
    """Merge results from different timesteps"""
    # Filter out None results
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("No valid results to merge!")
        return None
    
    # Initialize merged dictionary with the same keys
    merged = {key: [] for key in valid_results[0].keys()}
    
    # Merge data from all timesteps
    for key in merged.keys():
        merged[key] = np.ma.concatenate([result[key] for result in valid_results if result is not None])
    
    return merged


# Before processing the data:
def ensure_array(data):
    if isinstance(data, (int, float)):
        return np.array([data])
    return np.array(data)

def init_pamtra():
    run_id = ensemble_nrs[idx_run]
    bin_slice = slice(31, 50)
    bins_slice = slice(30, 50)
    data_M = model_data[run_id]

    try:
        list_height_levels = np.array(list(data_M.z.values[::-1]) + [2520.])
        nf = np.ma.masked_invalid(data_M['nf'].isel(bin=bin_slice).values).filled(0)
        nw = np.ma.masked_invalid(data_M['nw'].isel(bin=bin_slice).values).filled(0)
        Dbound  = data_M.dbounds.isel(bins=bins_slice).values 
    except:
        list_height_levels = np.array(list(data_M.HMLd_2.values[::-1]) + [2520.])
        nf = np.ma.masked_invalid(data_M['nf'].values).filled(0)
        nw = np.ma.masked_invalid(data_M['nw'].values).filled(0)
        Dbound  = diameter_μm_bounds
        
    Dmean = (Dbound[:-1] + Dbound[1:]) / 2.0
    nBins = Dmean.size
    #print(Dbound)
    #print(Dmean)

    pam = pyPamtra.pyPamtra()

    pam.df.addHydrometeor((
        "liquid",  # name 
        -99.,  # aspect ratio (NOT RELEVANT)
        1,  # liquid - ice flag
        -99.,  # density (NOT RELEVANT)
        -99.,  # mass size relation prefactor a (NOT RELEVANT)
        -99.,  # mass size relation exponent b (NOT RELEVANT)
        -99.,  # area size relation prefactor alpha (NOT RELEVANT)
        -99.,  # area size relation exponent beta (NOT RELEVANT)
        0,  # moment provided later (NOT RELEVANT)
        nBins,  # number of bins
        "fullBin",  # distribution name (NOT RELEVANT)
        -99.,  # distribution parameter 1 (NOT RELEVANT)
        -99.,  # distribution parameter 2 (NOT RELEVANT)
        -99.,  # distribution parameter 3 (NOT RELEVANT)
        -99.,  # distribution parameter 4 (NOT RELEVANT)
        -99.,  # minimum diameter (NOT RELEVANT)
        -99.,  # maximum diameter (NOT RELEVANT)
        'mie-sphere',  # scattering model
        'khvorostyanov01_drops',  # fall velocity relation
        0.0  # canting angle
    ))

    pam.df.addHydrometeor((
        "ice",  # name 
        -99.,  # aspect ratio (NOT RELEVANT)
        -1,  # liquid - ice flag
        -99.,  # density (NOT RELEVANT)
        -99.,  # mass size relation prefactor a (NOT RELEVANT)
        -99.,  # mass size relation exponent b (NOT RELEVANT)
        -99.,  # area size relation prefactor alpha (NOT RELEVANT)
        -99.,  # area size relation exponent beta (NOT RELEVANT)
        0,  # moment provided later (NOT RELEVANT)
        nBins,  # number of bins
        "fullBin",  # distribution name (NOT RELEVANT)
        -99.,  # distribution parameter 1 (NOT RELEVANT)
        -99.,  # distribution parameter 2 (NOT RELEVANT)
        -99.,  # distribution parameter 3 (NOT RELEVANT)
        -99.,  # distribution parameter 4 (NOT RELEVANT)
        -99.,  # minimum diameter (NOT RELEVANT)
        -99.,  # maximum diameter (NOT RELEVANT)
        'ssrg-rt3',  # scattering model
        'heymsfield10_particles',  # fall velocity relation
        0  # canting angle
    ))

    # add a height level to the profile and reverse the order of the height levels (bottom to top) and convert to m
    pam = pyPamtra.importer.createUsStandardProfile(
        pam, 
        hgt_lev=list_height_levels
    )

    pam.p['wind_uv'][:] = 10 # try uv winds from cosmo-specs
    pam.p['turb_edr'][:] = 1e-4

    pam.nmlSet["passive"] = False
    pam.nmlSet["randomseed"] = 0
    pam.nmlSet["radar_mode"] = "spectrum"
    pam.nmlSet["radar_aliasing_nyquist_interv"] = 3
    pam.nmlSet["hydro_adaptive_grid"] = False
    pam.nmlSet["conserve_mass_rescale_dsd"] = False
    pam.nmlSet["radar_use_hildebrand"] = True
    pam.nmlSet["radar_noise_distance_factor"] = -2
    pam.nmlSet["hydro_fullspec"] = True
    pam.set["verbose"] = 0
    pam.set["pyVerbose"] = 0


    pam.df.addFullSpectra()

    pam.df.dataFullSpec["d_bound_ds"][:] = Dbound
    pam.df.dataFullSpec["d_ds"][:] = Dmean
    pam.df.dataFullSpec["rho_ds"][:] = 1000. #rho[idx_time, np.newaxis, :, np.newaxis, np.newaxis]
    pam.df.dataFullSpec["area_ds"][:] = (np.pi / 4.*pam.df.dataFullSpec["d_ds"]**2)
    pam.df.dataFullSpec["mass_ds"][:] = (np.pi / 6.*pam.df.dataFullSpec["rho_ds"] * pam.df.dataFullSpec["d_ds"]**3)
    pam.df.dataFullSpec["as_ratio"][:, :, :, 0, :] = 1.0
    pam.df.dataFullSpec["as_ratio"][:, :, :, 1, :] = 3.0

    factor = 1.0e3
    # pam_velocity = []
    # pam_height = []
    # pam_spectra = []
    # Ze = []

    NLiquid = nw[:, ::-1, :] * factor
    NFrozen = nf[:, ::-1, :] * factor
    
    return pam, NLiquid, NFrozen

def run_pamtra(pam, dsd_liq, dsd_ice):



    # initialize a blank pam_dict
    pam_var_list = ['Ze',
                    'radar_hgt',
                    'radar_spectra',
                    'radar_snr',
                    'radar_moments',
                    'radar_slopes',
                    'radar_edges',
                    'radar_quality',
                    'radar_vel',
                    'psd_area',
                    'psd_n',
                    'psd_d',
                    'psd_mass',
                    'psd_bscat',]
    pam_dict = {key: [] for key in pam_var_list}

    try:

        pam.df.dataFullSpec["n_ds"][:, :, :, 0, :] = dsd_liq[np.newaxis, np.newaxis, :, :]
        pam.df.dataFullSpec["n_ds"][:, :, :, 1, :] = dsd_ice[np.newaxis, np.newaxis, :, :]

        pam.runPamtra(np.array([35.5]))
        
        for key in pam_var_list:
            tmp_ = ensure_array(pam.r[key]).squeeze()
            if tmp_.ndim == 1:
                tmp_ = tmp_[np.newaxis, :]
            tmp_ = np.ma.masked_less_equal(tmp_, -100)
            pam_dict[key].append(tmp_)

    except Exception as e:
        print(f"Error running PAMTRA: {e}")
        return None

    return pam_dict



# convert merged_results to pam_dict
pam_var_list = ['Ze',
                'radar_hgt',
                'radar_spectra',
                'radar_snr',
                'radar_moments',
                'radar_slopes',
                'radar_edges',
                'radar_quality',
                'radar_vel',
                'psd_area',
                'psd_n',
                'psd_d',
                'psd_mass',
                'psd_bscat',]
pam_dict = {key: [] for key in pam_var_list}

# for var in pam_var_list:
#     for ts in range(len(results)):
#         pam_dict[var].append(np.array(results[ts][var]))


n_timesteps = model_data[run_id].time.size

pam, NLiquid, NFrozen = init_pamtra()

for i in tqdm(range(1500, n_timesteps)):
    results = run_pamtra(pam, NLiquid[i, :, :], NFrozen[i, :, :])
    for var in pam_var_list:
        pam_dict[var].append(np.array(results[var]).squeeze())

for var in pam_var_list:
    pam_dict[var] = np.array(pam_dict[var]).squeeze()
    pam_dict[var] = np.ma.masked_less_equal(pam_dict[var], -9999)




# load holimo data into memory
holimo_data = tools.load_holimo_data(holimo_filename)
holimo_data['Water_meanD'].values = holimo_data['Water_meanD'].values * 1.0e6
holimo_data['Ice_meanD'].values = holimo_data['Ice_meanD'].values * 1.0e6

seed_height = 1.300 # holimo_data['height_holimo'][idx_target_time]
mean_seed_height = holimo_data['instData_Height'].sel(time = slice(*time_frame_tbs)).values.mean() * 1.0e-3

print(f'mean seed height: {mean_seed_height} km')
holimo_data




#plt.style.use("seaborn-v0_8-paper")
#plt.style.use("seaborn")


 
def add_holimo_line(fig, ax):
    if isinstance(ax, mpl.axes.Axes):
        ax = [ax]
    for iax in ax:
        iax.plot(holimo_data['time'], holimo_data['instData_Height'], color='white', linestyle='-', linewidth=2.0, alpha=0.7)
        iax.plot(holimo_data['time'], holimo_data['instData_Height'], color='black', linestyle='-', linewidth=1.0, alpha=0.8)

def setup_axes(fig, axes):
    has_multiple_axes = isinstance(axes, np.ndarray)
    for i, ax in enumerate(axes.flatten() if has_multiple_axes else [axes]):
        ax.set(xlim=plot_time_frame, xlabel='Time [UTC]', title='')
        ax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
        ax.tick_params(axis='x', width=1.15, length=10, pad=5)  # Increase width and length for x-axis tick lines
        ax.tick_params(axis='y', width=1.15, length=10)  # Increase width and length for y-axis tick lines
        ax.tick_params(axis='both', which='both', direction='inout', top=True, right=True)
        ax.grid(which='both', linestyle='--', alpha=0.75, linewidth=0.5, color='black')
        ax.grid(which='minor', linestyle='--', alpha=0.55, linewidth=0.25, color='black')
        ax.xaxis.label.set_weight('semibold')
        ax.yaxis.label.set_weight('semibold')

def setup_axes_y_height(axes, ylabel, ylim):
    for ax in axes.flatten() if isinstance(axes, np.ndarray) else [axes]:
        ax.set(ylabel=ylabel, ylim=ylim)
        
        
def setup_cbars(fig, ax, label_0, label_1, pad=0.02, fontsize=12):
    cbar_0 = fig.colorbar(ax[0].get_children()[0], ax=ax[0], pad=pad, extend='both', shrink=0.99, aspect=12)
    cbar_1 = fig.colorbar(ax[1].get_children()[0], ax=ax[1], pad=pad, extend='both', shrink=0.99, aspect=12)
    cbar_0.set_label(label_0, **colorbar_kwargs(fontsize=fontsize-3))
    cbar_1.set_label(label_1, **colorbar_kwargs(fontsize=fontsize-3))
    cbar_0.ax.xaxis.set_label_coords(0.5, -0.1)
    cbar_1.ax.xaxis.set_label_coords(0.5, -0.1)




png_path = f'{plot_path}/009-Pamtra-output/{data_set}/'
print(png_path)
os.makedirs(png_path, exist_ok=True)


#pam_dict = run_pamtra(0)
import matplotlib.dates as md


colorbar_kwargs  = tools.KWM(ha='center', va='top', fontweight='semibold', fontsize=12)

fig_size_R = (7.5, 6.5)
cmap = tools.cmap_new_timeheight
fig, ax = plt.subplots(2, 1, figsize=fig_size_R)
px0 = ax[0].pcolormesh(data_M.time.values, 
                       list_height_levels[:-1], 
                       pam_dict['Ze'].T, 
                       cmap=cmap, vmin=-50, vmax=20)
px1 = ax[1].pcolormesh(data_M.time.values, 
                       list_height_levels[:-1], 
                       -pam_dict['radar_moments'][:, :, 0].T, 
                       cmap=cmap, vmin=-2.5, vmax=2.5)

setup_cbars(fig, ax, 'PAMTRA Reflectivity [dBZ]', 'PAMTRA MDV [m/s]')

# cbar2 = fig.colorbar(px2, ax=ax[2], )    
# cbar2.set_label('PAMTRA Width [m/s]')
# cbar3 = fig.colorbar(px3, ax=ax[3], )    
# cbar3.set_label('PAMTRA SKEW [-]')
# cbar4 = fig.colorbar(px3, ax=ax[4], )    
# cbar4.set_label('PAMTRA KURT [-]')
# for iax in ax:
#     iax.set_ylim((800, 2000))
#     iax.set_xlim(*plot_time_frame)
#     iax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
#     iax.tick_params(axis='x', pad=10)

setup_axes(fig, ax)
add_holimo_line(fig, ax)
setup_axes_y_height(ax, 'Height [m]', plot_height_frame)

fig.subplots_adjust(hspace=0.15, wspace=0.05, left=0.05, right=1, top=0.9, bottom=0.05)  # make space for colorbar

file_name = f'PAMTRA_Ze_MDV_{data_set}_{run_id}_{date_str}.png'
file_path = os.path.join(png_path, file_name)
plt.savefig(file_path, dpi=300, bbox_inches='tight')
print(file_path)
try:
    file_path = os.path.join(plot_path, 'tmp', file_name)
    plt.savefig(file_path, dpi=300, bbox_inches='tight')
    print(file_path)
except:
    pass





# # Main execution
# if __name__ == '__main__':
#     # Calculate number of timesteps
#     n_timesteps = len(model_data[ensemble_nrs[0]].time)
    
#     # Process data in parallel
#     print("Starting parallel processing...")
#     merged_results = process_pamtra_parallel(n_timesteps)
    
#     # Create plots using merged results
#     if merged_results is not None:
#         print("Creating plots...")
#     else:
#         print("No results to plot!")

#print(pam_dict['Ze'].min(), pam_dict['Ze'].max())

#pam_dict.keys()

#pam_dict['radar_hgt'][:,:]



    

png_path = f'{plot_path}/009-Pamtra-output/{data_set}/'
print(png_path)
os.makedirs(png_path, exist_ok=True)




import matplotlib.dates as md

plot_time = data_M.time.values 
plot_height = pam_dict['radar_hgt'][0,:]
plot_Ze = pam_dict['Ze'][:,:]
plot_mdv = pam_dict['radar_moments'][:,:,0]
fig, ax = plt.subplots(1, 2, figsize=(19, 5))

px0 = ax[0].pcolormesh(data_M.time.values, list_height_levels[:-1], pam_dict['Ze'].T, cmap=tools.cmap_new_timeheight_nofade, vmin=-50.0, vmax=0)
px1 = ax[1].pcolormesh(data_M.time.values, list_height_levels[:-1], -pam_dict['radar_moments'][:, :, 0].T, cmap='jet', vmin=-1.5, vmax=1.5)

cbar0 = fig.colorbar(px0, ax=ax[0], )    
cbar0.set_label('PAMTRA Reflectivity [dBZ]')
cbar1 = fig.colorbar(px1, ax=ax[1], )    
cbar1.set_label('PAMTRA MDV [m/s]')
# cbar2 = fig.colorbar(px2, ax=ax[2], )    
# cbar2.set_label('PAMTRA Width [m/s]')
# cbar3 = fig.colorbar(px3, ax=ax[3], )    
# cbar3.set_label('PAMTRA SKEW [-]')
# cbar4 = fig.colorbar(px3, ax=ax[4], )    
# cbar4.set_label('PAMTRA KURT [-]')
for iax in ax:
    iax.set_ylim((800, 2000))
    iax.set_xlim(*plot_time_frame)
    iax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
    iax.tick_params(axis='x', pad=10)


fig.subplots_adjust(hspace=0.15, wspace=0.05, left=0.05, right=1, top=0.9, bottom=0.05)  # make space for colorbar


filename = f'8-Ze_MDVPAMTRA_{data_set}_{run_id}_{date_str}.png'
plt.savefig(png_path + filename, dpi=300, bbox_inches='tight')
try:
    plt.savefig(os.join(plot_path, 'tmp', filename), dpi=300, bbox_inches='tight')
    print(filename)
except:
    pass












