
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

is_notebook = 'ipykernel_launcher' in sys.argv[0]

root_path = os.getcwd()
#data_path = './data_model'
# data_path = '/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/eriswil/ensemble_output/'
obs_path = './data_obs'
plot_path = './plots'


plot_time_frame = [np.datetime64('2023-01-25T10:30:00'), np.datetime64('2023-01-25T11:40:00')]
time_frame_tbs = [np.datetime64('2023-01-25T10:30:00'), np.datetime64('2023-01-25T11:30:00')]

# load holimo data into memory
holimo_filename = f'{obs_path}/holimo/2023-01-25/CL_20230125_1000_1140_SM058_SM060_ts1.nc'
holimo_data = tools.load_holimo_data(holimo_filename)

# equivilent diameter in microns
holimo_data['Water_meanD'].values = holimo_data['Water_meanD'].values * 1.0e6
holimo_data['Water_meanD'].attrs['units'] = 'µm'
holimo_data['Ice_meanD'].values = holimo_data['Ice_meanD'].values * 1.0e6
holimo_data['Ice_meanD'].attrs['units'] = 'µm'
seed_height = 1.300 # holimo_data['height_holimo'][idx_target_time]
mean_seed_height = holimo_data['instData_Height'].sel(time = slice(*time_frame_tbs)).values.mean() * 1.0e-3

print(f'mean seed height: {mean_seed_height} km')
holimo_data

print(f"Model seeding arrives at cite: {np.datetime64('2023-01-25T09:00:00') +  np.timedelta64(5286, 's')}")

flare_lat, flare_lon = 47.07425, 7.90522
origin_lat, origin_lon = 47.070522, 7.872991

dist_flare_to_obs = tools.calculate_haversine_distance(origin_lat, flare_lat, origin_lon, flare_lon)
print(f'Distance from flare to observation: {dist_flare_to_obs:.2f} km')





target_time_resolution = 30 # seconds

time_slice = np.arange(*plot_time_frame, np.timedelta64(target_time_resolution, 's'))

print(f'slicing time: {time_slice[0]} - {time_slice[-1]}')


meteogram_path_100m = f'{data_path}/cs-eriswil__20250424_151706/M_00_00_20250424.nc'
pamtra_Ze_path_100m = meteogram_path_100m.replace('/M_', '/Pamtra_').replace('.nc', f'_100m.nc') # output pamtra Ze path
model_data_100m = xr.open_dataset(meteogram_path_100m, chunks='auto')
model_data_100m.attrs['res'] = '100m'
model_data_100m = model_data_100m.assign_coords({'time': model_data_100m.time - np.timedelta64(3, 'h')})
model_data_100m = model_data_100m#.sel(time=time_slice, method='nearest')
model_data_100m


meteogram_path_400m = f'{data_path}/cs-eriswil__20240711_153416/M_00_07_20240711154727.nc'
pamtra_Ze_path_400m = meteogram_path_400m.replace('/M_', '/Pamtra_').replace('.nc', f'_400m.nc') # output pamtra Ze path
model_data_400m = xr.open_dataset(meteogram_path_400m, chunks='auto')
model_data_400m.attrs['res'] = '400m'
#model_data_400m = model_data_400m.assign_coords({'time': model_data_400m.time - np.timedelta64(3, 'h')})
model_data_400m = model_data_400m#.sel(time=time_slice, method='nearest')

model_data_400m








fig, ax = plt.subplots(2, 2, figsize=(14, 5))
model_data_100m['NW'].isel(bins=slice(30, 50)).sum(dim='bins').T.plot(ax=ax[0,0], ylim=(800, 2000))
model_data_100m['NF'].isel(bins=slice(30, 50)).sum(dim='bins').T.plot(ax=ax[0,1], ylim=(800, 2000))


model_data_400m['NW'].isel(bins=slice(30, 50)).sum(dim='bins').T.plot(ax=ax[1,0], ylim=(800, 2000))
model_data_400m['NF'].isel(bins=slice(30, 50)).sum(dim='bins').T.plot(ax=ax[1,1], ylim=(800, 2000))








def convert_mixing_ratio_to_conc(model_data):
    require_vars = ['RHO', 'NW', 'NF', 'HHLd', 'HMLd', 'RGRENZ_left', 'RGRENZ_right']
    N_conc_vars = ['NF','NW']

    pam_input_data = model_data[require_vars]
    # pam_input_data[N_conc_vars] = pam_input_data[N_conc_vars] * model_data['RHO']

    # for var in N_conc_vars:
    #     # change units of RHO from kg/m^3 to g/m^3 in attributes accordingly
    #     pam_input_data[var].attrs['units'] = '#/m^3'
        
    return pam_input_data

pam_input_data_100m = convert_mixing_ratio_to_conc(model_data_100m)
pam_input_data_400m = convert_mixing_ratio_to_conc(model_data_400m)

if is_notebook:
    pam_input_data_100m = pam_input_data_100m.isel(time=slice(  pam_input_data_100m.time.size//2 , 
                                                                pam_input_data_100m.time.size//2 + 40))
    pam_input_data_400m = pam_input_data_400m.isel(time=slice(  pam_input_data_400m.time.size//2 , 
                                                                pam_input_data_400m.time.size//2 + 40))



import numpy as np
from tqdm.auto import tqdm

# Before processing the data:
def ensure_array(data):
    if isinstance(data, (int, float)):
        return np.array([data])
    return np.array(data)

def load_data(data_M):
    data_M = data_M.compute()
    nw = np.ma.masked_invalid(data_M['NW'].values).filled(0)
    nf = np.ma.masked_invalid(data_M['NF'].values).filled(0)
    return nw, nf

def init_pamtra(data_M):
    # reverse the order of the height levels (bottom to top)
    list_height_levels = data_M.HHLd.values[::-1] 
    # radius to diameter in meter
    Dbound = 2 * np.concatenate([data_M.RGRENZ_right.values, [data_M.RGRENZ_left[-1].values]])
    Dmean = (Dbound[:-1] + Dbound[1:]) / 2.0
    nBins = Dmean.size

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
        pam, hgt_lev=list_height_levels
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

    return pam

def run_pamtra(pam, dsd_liq, dsd_ice) -> dict[str, list[np.ndarray]]:



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
            tmp_ = np.ma.masked_less_equal(tmp_, -999)
            pam_dict[key].append(tmp_)

    except Exception as e:
        print(f"Error running PAMTRA: {e}")
        print("Full traceback:")
        import traceback
        traceback.print_exc()
        return {key: None for key in pam_var_list}

    return pam_dict



model_data = model_data_400m
pam_input_data = pam_input_data_400m
pamtra_Ze_path = pamtra_Ze_path_400m



# convert merged_results to pam_dict
pam_var_list = ['Ze',
                'radar_hgt',
                # 'radar_spectra',
                # 'radar_snr',
                # 'radar_moments',
                # 'radar_slopes',
                # 'radar_edges',
                # 'radar_quality',
                # 'radar_vel',
                # 'psd_area',
                # 'psd_n',
                # 'psd_d',
                # 'psd_mass',
                # 'psd_bscat',
                ]




pam_100m = init_pamtra(pam_input_data_100m)
nw_100m, nf_100m = load_data(pam_input_data_100m)
pam_400m = init_pamtra(pam_input_data_400m)
nw_400m, nf_400m = load_data(pam_input_data_400m)
t_100m = pam_input_data_100m.time.values
t_400m = pam_input_data_400m.time.values


pams = [pam_100m, pam_400m]
nws = [nw_100m, nw_400m]
nfs = [nf_100m, nf_400m]
times = [t_100m, t_400m]

    







#plt.style.use("seaborn-v0_8-paper")
#plt.style.use("seaborn")



def add_holimo_line(fig, ax):
    for iax in ax:
        iax.plot(holimo_data['time'], holimo_data['instData_Height'], color='white', linestyle='-', linewidth=2.0, alpha=0.7)
        iax.plot(holimo_data['time'], holimo_data['instData_Height'], color='black', linestyle='-', linewidth=1.0, alpha=0.8)

def setup_axes(fig, axes):
    for i, ax in enumerate(axes.flatten()):
        ax.set(xlim=plot_time_frame, xlabel='Time [UTC]', title='')
        ax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
        ax.tick_params(axis='x', width=1.15, length=10, pad=5)  # Increase width and length for x-axis tick lines
        ax.tick_params(axis='y', width=1.15, length=10)  # Increase width and length for y-axis tick lines
        ax.tick_params(axis='both', which='both', direction='inout', top=True, right=True)
        ax.grid(which='both', linestyle='--', alpha=0.75, linewidth=0.5, color='black')
        ax.grid(which='minor', linestyle='--', alpha=0.55, linewidth=0.25, color='black')
        ax.xaxis.label.set_weight('semibold')
        ax.yaxis.label.set_weight('semibold')
        
def setup_cbars(fig, pmeshs, axes, labels, pad=0.02, fontsize=12):
    for ax, pmesh, label in zip(axes, pmeshs, labels):
        cbar = fig.colorbar(pmesh, ax=ax, pad=pad, extend='both', shrink=0.99, aspect=12)
        cbar.set_label(label, **colorbar_kwargs(fontsize=fontsize-3))
        cbar.ax.xaxis.set_label_coords(0.5, -0.1)




png_path = f'{plot_path}/009-Pamtra-Output/'
print(png_path)
os.makedirs(png_path, exist_ok=True)


#pam_dict = run_pamtra(0)
import matplotlib.dates as md


colorbar_kwargs  = tools.KWM(ha='center', va='top', fontweight='semibold', fontsize=12)#






lwrite_ncfile = not os.path.exists(pamtra_Ze_path) or True
print(f'writing pamtra Ze to {pamtra_Ze_path}')



fac = 1
nws = np.array(nws)*fac
nfs = np.array(nfs)*fac

# start pamtra processing and save Ze to netcdf
resses = ['100m', '400m']
if lwrite_ncfile:
    
    data_Ze_list = []
    
    for pam, nw, nf, res, t in zip(pams, nws, nfs, resses, times):
        
        pam_dict = {'Ze': [], 'radar_hgt': []}
        
        
        for i in tqdm(range(nw.shape[0])):
            results = run_pamtra(pam, nw[i, :, :], nf[i, :, :])
            for var in pam_dict.keys():
                pam_dict[var].append(np.array(results[var]).squeeze())

        for var in pam_dict.keys():
            pam_dict[var] = np.array(pam_dict[var]).squeeze()
            pam_dict[var] = np.ma.masked_less_equal(pam_dict[var], -9999)
        
        
        xrdata_time = xr.DataArray(t,  dims='time')
        xrdata_height = xr.DataArray(pam_dict['radar_hgt'][0, :], dims='height')
        xrdata_Ze = xr.DataArray(pam_dict['Ze'], dims=['time', 'height'], coords={'time': xrdata_time, 'height': xrdata_height})
        xrdata_Ze.attrs = {'units': 'dBZ', 'long_name': 'PAMTRA Reflectivity Factor'}

         
        print(f'\nmin: {np.nanmin(xrdata_Ze)}')
        print(f'\nmax: {np.nanmax(xrdata_Ze)}')
        print(f'\nmean: {np.nanmean(xrdata_Ze)}')
        print(f'\nstd: {np.nanstd(xrdata_Ze)}')
        print(f'\nmedian: {np.nanmedian(xrdata_Ze)}')
        print(f'\nquantile 0.1: {np.nanpercentile(xrdata_Ze, 10)}')
        print(f'\nquantile 0.9: {np.nanpercentile(xrdata_Ze, 90)}')
        #xrdataset.to_netcdf(pamtra_Ze_path, mode='w')
        
        data_Ze_list.append(xrdata_Ze)
        
        
    xrdataset = xr.Dataset({f'Ze_{res}': value for res, value in zip(resses, data_Ze_list)})
        
        


if lwrite_ncfile:
    
    data_Ze_list = []
    
    for res, xrdata_Ze in xrdataset.items():
    
        plot_time = xrdata_Ze.time.values
        plot_height = xrdata_Ze.height.values
        plot_Ze = xrdata_Ze.values.T

        print(plot_Ze.shape)
        print('nanmin, nanmax', np.nanmin(plot_Ze), np.nanmax(plot_Ze))
        print('mean, std', np.nanmean(plot_Ze), np.nanstd(plot_Ze))
        print('median, 10%, 90%', np.nanmedian(plot_Ze), np.nanpercentile(plot_Ze, 10), np.nanpercentile(plot_Ze, 90))


        fig_size_R = (7.5, 3.25)
        cmap = tools.cmap_new_timeheight

        fig, ax = plt.subplots(1, 1, figsize=fig_size_R)


        pm = ax.pcolormesh(plot_time, 
                            plot_height, 
                            np.flip(plot_Ze, axis=0), 
                            cmap=cmap, vmin=-50, vmax=00)
                
        cbar = fig.colorbar(pm, ax=ax, pad=0.05, extend='both', shrink=0.99, aspect=12)
        cbar.set_label('PAMTRA Reflectivity [dBZ]', **colorbar_kwargs(fontsize=12-3))
        cbar.ax.xaxis.set_label_coords(0.5, -0.1)#
        
                

        axes = np.array([ax])
        setup_axes(fig, axes)
        add_holimo_line(fig, axes)
        ax.set(ylabel='Height [m]', ylim=(900, 1500), title=res)

        fig.subplots_adjust(hspace=0.15, wspace=0.05, left=0.05, right=1, top=0.9, bottom=0.05)  # make space for colorbar

        file_name = f"PAMTRA_Ze_MDV_{res}.png"
        file_path = os.path.join(png_path, file_name)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        print(file_path)
        try:
            file_path = os.path.join(plot_path, 'tmp', file_name)
            plt.savefig(file_path, dpi=300, bbox_inches='tight')
            print(file_path)
        except:
            pass















