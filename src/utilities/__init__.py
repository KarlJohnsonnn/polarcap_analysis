from utilities.compute_fabric import (
    allocate_resources,
    calculate_optimal_scaling,
    is_server,
    in_slurm_allocation,
    recommend_target_chunk_mb,
    auto_chunk_dataset,
    describe_chunk_plan,
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

try:
    from utilities.holimo_helpers import (
        load_and_prepare_holimo,
        rebin_timeseries,
        rebin_logspace_bins,
        time_frames_plume,
        time_frame_tbs,
        prepare_holimo_for_overlay,
    )
except ImportError:
    pass  # holimo requires colormaps; processing_chain scripts work without it

from utilities.namelist_metadata import (
    MetadataManager,
    update_dataset_metadata,
    get_variable_attrs,
    get_process_groups,
    metadata_manager,
)
from utilities.process_budget_data import (
    discover_candidate_datasets,
    open_dataset_auto,
    make_synthetic_rates,
    load_process_budget_data,
    stn_label,
    select_rates_for_range,
)

try:
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
        ENERGY_PROCESSES,
        HEAT_RELEASE_PROCESSES,
        HEAT_CONSUME_PROCESSES,
        get_style,
        use_style,
        format_elapsed_minutes_tick,
        MM,
        SINGLE_COL_IN,
        FULL_COL_IN,
        MAX_H_IN,
        PUBLICATION_RCPARAMS,
        apply_publication_style,
        save_fig,
        PROC_COLORS,
        PROC_HATCH,
        proc_color,
        proc_hatch,
        build_fixed_legend,
    )
    from utilities.plume_path_plot import (
        _assign_elapsed_time,
        _prepare_da,
        plot_plume_path_sum,
        build_common_xlim,
        diagnostics_table,
        compute_holimo_elapsed_anchors,
    )
    from utilities.cloud_field_overview import (
        DEFAULT_WINDOW_SPECS_MIN,
        build_cloud_phase_budget_tables,
        default_cloud_field_overview_output,
        default_cloud_phase_budget_outputs,
        load_cloud_field_overview_context,
        render_cloud_field_overview,
        save_cloud_phase_budget_tables,
        save_cloud_field_overview,
    )
    from utilities.plume_lagrangian import (
        DEFAULT_RUNS as DEFAULT_PLUME_RUNS,
        build_ensemble_mean_datasets,
        hist_profile,
        load_plume_lagrangian_context,
        median_diameter,
        peak_indices,
        plume_lagrangian_output,
        render_plume_lagrangian_figure,
        save_plume_lagrangian_figure,
    )
    from utilities.plume_loader import load_plume_path_runs
    from utilities.psd_waterfall import (
        DEFAULT_PLOT_KINDS as DEFAULT_PSD_WATERFALL_KINDS,
        DEFAULT_RUNS as DEFAULT_PSD_WATERFALL_RUNS,
        build_psd_stats_dataframe,
        build_holimo_obs_series,
        load_psd_waterfall_context,
        plot_psd_waterfall,
        prepare_psd_waterfall_data,
        prepared_to_latex_table,
        render_all_psd_waterfall_cases,
        render_psd_waterfall_case,
        save_latex_table,
        save_psd_stats_csv,
        waterfall_output_root,
    )
except ImportError:
    pass  # plotting/colormaps optional; processing_chain works without them

from utilities.processing_metadata import (
    find_repo_root,
    git_head,
    provenance_attrs,
    add_provenance_to_dataset,
    normalize_attrs_for_zarr,
)
from utilities.processing_paths import get_runs_root, resolve_ensemble_output
from utilities.tracking_pipeline import (
    prep_tobac_input,
    RunContext,
    discover_3d_runs,
    run_tobac_tracking,
    extract_segmented_tracks_paths,
    run_plume_path_extraction,
    DEFAULT_TRACER_SPECS,
    DEFAULT_EXTRACTION_TYPES,
)
from utilities.process_rates import (
    PHYSICS_GROUPS,
    PROCESS_PLOT_ORDER,
    build_proc_vars,
    build_rates,
    build_spectral_rates,
    build_rates_dataset,
    build_rates_for_experiments,
    tendency_to_rate,
    get_process_display_name,
    panel_process_values,
    panel_concentration_profile,
    ridge_process_values,
    ridge_concentration_profile,
    merge_liq_ice_net,
    normalize_net_stacks,
)

from utilities.data_slicer import (
    normalize_slice_dict,
    slice_dataset,
    slice_dataset_to_zarr,
)

from utilities.meteogram_io import (
    discover_meteogram_files,
    get_max_timesteps,
    get_variable_names,
    build_meteogram_zarr,
    add_coords_and_metadata,
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
