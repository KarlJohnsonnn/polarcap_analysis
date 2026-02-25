import dask
from dask_jobqueue import SLURMCluster
from dask.distributed import Client



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



def allocate_resources( n_cpu=16, 
                        n_jobs=1, 
                        m=0, 
                        n_threads_per_process=1, 
                        port='7777', 
                        part='compute', 
                        walltime="02:00:00", 
                        account='bb1376', 
                        python='/home/b/b382237/.conda/envs/pcpaper_env/bin/python', 
                        name='dask_cluster'  ):
    
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
    cluster = SLURMCluster( name=name,
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
                            python=python
                        )

    if N_nodes > 1:
        cluster.scale(  N_nodes  )

    print(  cluster.job_script()  )
    print(  len(cluster.scheduler.workers)  )

    client = Client(  cluster  )

        # Print dashboard addresses
    dashboard_address = cluster.scheduler_address
    remote_dashboard = f"http://{dashboard_address.split('//')[-1].split(':')[0]}:{port}"
    print(  f"Remote dashboard address: {remote_dashboard}" )
    print(  f"Local dashboard address: http://localhost:{port}" )


    return cluster, client