from utilities.slurm import (
    allocate_resources,
    calculate_optimal_scaling,
)

from utilities.model_helpers import (
    calculate_supersaturation_ice, 
    calculate_supersaturation_water,
    calculate_bulk_timeseries,
    get_closest_station_to_coordinates,
    COORDINATES_OF_ERISWIL,
    format_list_chunks,
    format_dict_chunks,
    haversine_distance,
    get_flare_emission_rates,
    format_model_table,
    format_model_label_as_table,
    calculate_mean_diameter,
    get_model_datetime_from_meta,
    define_bin_boundaries,
    fetch_3d_data,
    convert_units_meteogram,
    convert_units_3d,
    glob_lastest,
    track_plume,
    tobac_5dspecs,
    fetch_meteogram_data,
    extract_segmented_tracks_fast,
    make_3d_preprocessor,

)

from utilities.holimo_helpers import (
    load_and_prepare_holimo,
    rebin_timeseries,
    rebin_logspace_bins,
    time_frames_plume,
    time_frame_tbs,
)

from utilities.namelist_metadata import (
    MetadataManager,
    update_dataset_metadata,
    get_variable_attrs,
    get_process_groups,
    metadata_manager,
)
from utilities.plotting import (
    make_pastel,
    create_new_jet,
    create_new_jet2,
    create_new_jet3,
    create_fade_cmap,
    plot_2d_model_and_holimo_bulk_timeseries,
    make_bulk_figure,
    setup_bulk_axes,
    plot_1d_model_bulk_ts,
    new_jet,
    new_jet2,
    new_fjet,
    new_fjet2,
    new_jet3,
    new_fjet3,
    plot_holimo_bulk_ts,
    add_grouped_legends,
    print_reduction_history,
    plot_3d_col_wrap,
    fmt_title,
    get_extpar_data,
    find_nearest_grid_point,
    get_unique_meteogram_locations,
    print_meteogram_list,
    logscale_FacetGrid,
    set_name_tick_params,
    add_ruler,

)

from utilities.style_profiles import (
    BASE_STYLE,
    STYLE_TIMESERIES,
    STYLE_2D,
    STYLE_HIST,
    STYLE_REGISTRY,
    get_style,
    use_style,
)
# from utilities.namelist_metadata import (
#     MetadataManager, 
#     update_dataset_metadata, 
#     get_variable_attrs, 
#     get_process_groups, 
#     metadata_manager,
# )

# from utilities.tools import (
#     save_fig, 
#     smooth, 
#     convert_to_gif, 
#     convert_to_video, 
#     rebin_data_with_time,
#     calculate_mean_diameter,
#     load_grid_data, 
#     open_metadata, 
#     open_3D_data, 
#     time2delta_t, 
#     calculate_haversine_distance,
#     get_grid_cell_sizes, 
#     get_domain_resolution,
#     format_model_label, 
#     display_pops_seeding_image, 
#     cmap_new_timeheight, 
#     cmap_new_timeheight_nofade, 
#     load_holimo_data,
# )

# from utilities.utils_meteogram import (
#     create_height_matrix, 
#     add_stations_height_to_ds,
#     plot_cosmo_specs_meteogram, 
#     calculate_bulk_timeseries, 
#     get_closest_station_to_coordinates,
#     convert_mixing_ratio, 
#     convert_number_concentration, 
#     get_station_coords_from_cfg, 
#     load_meteogram_data, 
#     calculate_bulk_timeseries, 
#     format_list_chunks, 
#     format_dict_chunks, 
#     pcolormesh_stations_height_data,
# )

# from utilities.init_common import (
#     init_analysis,
# )

# from utilities.plot_bulk_timeseries import (
#     make_bulk_figure, 
#     setup_bulk_axes, 
#     plot_1d_model_bulk_ts,
#     plot_holimo_bulk_ts,
#     add_grouped_legends,
# )

# from utilities.holimo_helpers import (
#     load_holimo_dataset, 
#     prepare_holimo_quicklook, 
#     summarize_holimo, 
#     load_and_prepare_holimo,
#     interpolate_timeseries,
#     rebin_timeseries,
#     rebin_logspace_bins,
#     print_reduction_history,
#     check_rebinned_data_structure,
# )
