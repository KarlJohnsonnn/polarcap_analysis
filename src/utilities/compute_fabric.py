"""Machine-aware compute utilities for local, laptop, and HPC workflows.

This module centralizes:
- Runtime environment detection (`is_server`, SLURM checks)
- Dask chunk-size heuristics (`auto_chunk_dataset`)
- SLURM-backed Dask cluster helpers (`allocate_resources`)
- Setting up Dask dashboard
"""

from __future__ import annotations

import math
import os
import platform
from typing import Iterable

import dask
import xarray as xr

try:
    import psutil
except Exception:  # pragma: no cover
    psutil = None

try:
    from dask.distributed import Client, get_client
except Exception:  # pragma: no cover
    Client = None
    get_client = None

try:
    from dask_jobqueue import SLURMCluster
except Exception:  # pragma: no cover
    SLURMCluster = None


def is_server() -> bool:
    """Return True when running in server/HPC-like environments."""
    if os.getenv("JUPYTERHUB_API_URL") or os.getenv("JUPYTERHUB_USER"):
        return True
    if os.getenv("SLURM_JOB_ID"):
        return True
    return platform.system() != "Darwin"


def in_slurm_allocation() -> bool:
    """Return True when the current process is inside an active SLURM job."""
    return bool(os.getenv("SLURM_JOB_ID"))


def _local_available_memory_bytes() -> int:
    """Return available physical memory in bytes, with a safe 8 GB fallback."""
    if psutil is not None:
        return int(psutil.virtual_memory().available)
    if hasattr(os, "sysconf") and "SC_PAGE_SIZE" in os.sysconf_names and "SC_PHYS_PAGES" in os.sysconf_names:
        return int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    return int(8 * 1024**3)


def _dask_worker_memory_bytes() -> int | None:
    """Return the smallest per-worker memory limit from the active Dask cluster.

    Returns ``None`` when no distributed client is reachable or no memory
    limits are configured, so callers can fall back to local memory.
    """
    if get_client is None:
        return None
    try:
        info = get_client().scheduler_info()
    except Exception:
        return None
    workers = info.get("workers", {})
    if not workers:
        return None
    limits = [int(w.get("memory_limit", 0)) for w in workers.values() if int(w.get("memory_limit", 0)) > 0]
    return min(limits) if limits else None


def recommend_target_chunk_mb(
    min_chunk_mb: int = 64,
    max_chunk_mb: int = 512,
    memory_fraction: float = 0.12,
) -> int:
    """Return a chunk target (MB) clamped to ``[min_chunk_mb, max_chunk_mb]``.

    Queries the active Dask cluster's per-worker memory limit first; falls back
    to the local machine's available RAM when no cluster is reachable.

    Parameters
    ----------
    min_chunk_mb:
        Lower bound on the returned chunk size.
    max_chunk_mb:
        Upper bound on the returned chunk size.
    memory_fraction:
        Fraction of total available memory to target per chunk (0.01–0.8).
    """
    if min_chunk_mb <= 0 or max_chunk_mb <= 0:
        raise ValueError("Chunk bounds must be positive.")
    if min_chunk_mb > max_chunk_mb:
        raise ValueError("min_chunk_mb must be <= max_chunk_mb.")
    if not (0.01 <= memory_fraction <= 0.8):
        raise ValueError("memory_fraction must be between 0.01 and 0.8.")

    memory_bytes = _dask_worker_memory_bytes() or _local_available_memory_bytes()
    target_mb = int((memory_bytes * memory_fraction) / (1024**2))
    return max(min_chunk_mb, min(max_chunk_mb, target_mb))


def _pick_reference_var(ds: xr.Dataset) -> xr.DataArray:
    """Return the data variable with the most dimensions and largest size."""
    if not ds.data_vars:
        raise ValueError("Dataset has no data variables.")
    return max(ds.data_vars.values(), key=lambda da: (da.ndim, da.size))


def _balanced_chunk_sizes(
    dim_sizes: dict[str, int],
    target_chunk_bytes: int,
    itemsize: int,
    prefer_dims: Iterable[str],
) -> dict[str, int]:
    """Return chunk sizes per dimension clamped to ``[1, dim_sizes[d]]``.

    Dimensions are grown in priority order (``prefer_dims`` first, then the
    remainder) by repeatedly doubling each chunk size until the target element
    count would be exceeded.  After the doubling pass the leading preferred
    dimension is widened linearly to consume any remaining budget.

    Parameters
    ----------
    dim_sizes:
        Mapping of dimension name → full extent.
    target_chunk_bytes:
        Desired uncompressed chunk size in bytes.
    itemsize:
        Bytes per array element (e.g. 4 for float32, 8 for float64).
    prefer_dims:
        Dimension names to grow first; dims absent from ``dim_sizes`` are ignored.
    """
    dims = [d for d in prefer_dims if d in dim_sizes] + [d for d in dim_sizes if d not in set(prefer_dims)]
    chunk = {d: 1 for d in dims}
    max_sizes = {d: max(1, int(dim_sizes[d])) for d in dims}
    target_elems = max(1, int(target_chunk_bytes // max(1, itemsize)))
    current_elems = 1

    while True:
        grew = False
        for d in dims:
            if chunk[d] >= max_sizes[d]:
                continue
            proposed = min(max_sizes[d], chunk[d] * 2)
            new_elems = (current_elems // chunk[d]) * proposed
            if new_elems <= target_elems:
                chunk[d] = proposed
                current_elems = new_elems
                grew = True
        if not grew:
            break

    if dims:
        d0 = dims[0]
        if chunk[d0] < max_sizes[d0]:
            remaining = max(1, target_elems // max(1, current_elems // chunk[d0]))
            chunk[d0] = min(max_sizes[d0], max(chunk[d0], int(remaining)))

    return {d: int(max(1, min(max_sizes[d], v))) for d, v in chunk.items()}


def auto_chunk_dataset(
    ds: xr.Dataset,
    *,
    target_chunk_mb: int | None = None,
    min_chunk_mb: int = 64,
    max_chunk_mb: int = 512,
    memory_fraction: float = 0.12,
    prefer_dims: tuple[str, ...] = ("time", "altitude", "latitude", "longitude", "diameter"),
) -> tuple[xr.Dataset, dict[str, int]]:
    """Rechunk a dataset with machine-aware balanced chunk sizes.

    Selects the reference variable (largest-dimensioned), derives a byte budget
    via ``recommend_target_chunk_mb``, and delegates to ``_balanced_chunk_sizes``
    to compute per-dimension chunk sizes that respect ``prefer_dims`` priority.
    Returns the rechunked dataset together with the chunk dictionary applied.

    Parameters
    ----------
    ds:
        Input dataset to rechunk.
    target_chunk_mb:
        Override the automatic chunk-size estimate (MB); derived from available
        memory when ``None``.
    min_chunk_mb:
        Passed to ``recommend_target_chunk_mb`` when ``target_chunk_mb`` is not set.
    max_chunk_mb:
        Passed to ``recommend_target_chunk_mb`` when ``target_chunk_mb`` is not set.
    memory_fraction:
        Passed to ``recommend_target_chunk_mb`` when ``target_chunk_mb`` is not set.
    prefer_dims:
        Dimensions to fill first during chunk-size allocation.
    """
    ref = _pick_reference_var(ds)
    itemsize = max(1, int(ref.dtype.itemsize))
    target_mb = target_chunk_mb or recommend_target_chunk_mb(
        min_chunk_mb=min_chunk_mb,
        max_chunk_mb=max_chunk_mb,
        memory_fraction=memory_fraction,
    )
    target_bytes = int(target_mb * 1024**2)
    chunk_dict = _balanced_chunk_sizes(
        dim_sizes=dict(ds.sizes),
        target_chunk_bytes=target_bytes,
        itemsize=itemsize,
        prefer_dims=prefer_dims,
    )
    return ds.chunk(chunk_dict), chunk_dict


def describe_chunk_plan(ds: xr.Dataset, chunk_dict: dict[str, int]) -> str:
    """Return a single-line summary of chunk size (MB), count, and dims.

    Parameters
    ----------
    ds:
        Dataset whose dimension sizes are used for the calculation.
    chunk_dict:
        Chunk sizes per dimension as returned by ``auto_chunk_dataset``.
    """
    ref = _pick_reference_var(ds)
    elems = 1
    for d in ref.dims:
        elems *= chunk_dict.get(d, ds.sizes[d])
    chunk_mb = elems * max(1, int(ref.dtype.itemsize)) / (1024**2)
    n_chunks = 1
    for d in ref.dims:
        n_chunks *= math.ceil(ds.sizes[d] / chunk_dict.get(d, ds.sizes[d]))
    dims_txt = ", ".join(f"{d}={chunk_dict.get(d, ds.sizes[d])}" for d in ref.dims)
    return f"chunk ~{chunk_mb:.1f} MB, ~{n_chunks} chunks for '{ref.name}'; dims: {dims_txt}"


def calculate_optimal_scaling(
    n_time_steps: int,
    n_experiments: int,
    n_stations: int,
    debug_mode: bool = False,
) -> tuple[int, int, float, int, str]:
    """Return ``(n_nodes, n_cpu, memory_gb, n_workers, walltime)`` for SLURM.

    Scales resources in four tiers based on
    ``n_time_steps × n_experiments × n_stations``, with additional boosts for
    large experiment counts (>50) and long time series (>1000 steps).
    Prints a summary of the workload analysis and chosen settings.

    Parameters
    ----------
    n_time_steps:
        Number of time steps in the workload.
    n_experiments:
        Number of parallel experiments/ensemble members.
    n_stations:
        Number of station or grid-point locations.
    debug_mode:
        If ``True``, return minimal single-node settings for quick testing.
    """
    if debug_mode:
        return 1, 64, 32, 2, "00:10:00"

    total_workload = n_time_steps * n_experiments * n_stations
    base_cpu, base_memory, base_workers, base_walltime = 128, 64.0, 2, "02:00:00"

    if total_workload < 1e5:
        n_nodes, n_cpu, memory, workers, walltime = 1, base_cpu, base_memory, base_workers, base_walltime
    elif total_workload < 1e6:
        n_nodes, n_cpu, memory, workers, walltime = 2, base_cpu * 2, base_memory * 2, base_workers * 2, "06:00:00"
    elif total_workload < 1e7:
        n_nodes, n_cpu, memory, workers, walltime = 4, base_cpu * 2, base_memory * 3, base_workers * 4, "07:00:00"
    else:
        n_nodes, n_cpu, memory, workers, walltime = 8, base_cpu * 2, base_memory * 4, base_workers * 6, "08:00:00"

    if n_experiments > 50:
        workers = min(workers * 2, 32)
    if n_time_steps > 1000:
        memory = min(memory * 1.5, 512)

    print("Workload analysis:")
    print(f"  - Time steps: {n_time_steps}")
    print(f"  - Experiments: {n_experiments}")
    print(f"  - Stations: {n_stations}")
    print(f"  - Total workload estimate: {total_workload}")
    print("Optimal scaling:")
    print(f"  - Nodes: {n_nodes}")
    print(f"  - CPU per node: {n_cpu}")
    print(f"  - Memory per node: {memory}GB")
    print(f"  - Scale up workers: {workers}")
    print(f"  - Walltime: {walltime}")
    return n_nodes, n_cpu, memory, workers, walltime


def allocate_resources(
    n_cpu: int = 16,
    n_jobs: int = 1,
    m: int = 0,
    n_threads_per_process: int = 1,
    port: str = "7777",
    part: str = "compute",
    walltime: str = "02:00:00",
    account: str = "bb1376",
    python: str = "/home/b/b382237/.conda/envs/pcpaper_env/bin/python",
    name: str = "dask_cluster",
) -> tuple:
    """Return ``(cluster, client)`` for a SLURM-backed Dask cluster.

    Configures a ``SLURMCluster`` with memory-management settings tuned for
    large array workloads (spill/target disabled, terminate at 95 %), submits
    ``n_jobs`` SLURM nodes, and prints SSH port-forwarding instructions for the
    Dask dashboard.

    Parameters
    ----------
    n_cpu:
        CPUs (cores) per SLURM node; also used as memory in GB when ``m=0``.
    n_jobs:
        Number of SLURM nodes / workers to request.
    m:
        Memory per node in GB. Defaults to ``n_cpu`` GB when 0.
    n_threads_per_process:
        OMP/MKL/BLAS thread count per Dask worker process.
    port:
        Dask dashboard port, forwarded via SSH tunnel.
    part:
        SLURM partition (queue) name.
    walltime:
        SLURM walltime string (``"HH:MM:SS"``).
    account:
        SLURM billing account.
    python:
        Absolute path to the Python interpreter used by Dask workers.
    name:
        Dask cluster/job name visible in SLURM and the dashboard.
    """
    if SLURMCluster is None or Client is None:
        raise ImportError("SLURMCluster/Client unavailable. Install dask-jobqueue and dask[distributed].")
    if n_threads_per_process <= 0:
        raise ValueError("n_threads_per_process must be >= 1.")

    cores_per_node = n_cpu
    processes_per_node = max(1, n_cpu // n_threads_per_process)
    n_nodes = n_jobs
    memory_per_node_gb = n_cpu if m == 0 else m

    dask.config.set(
        {
            "distributed.worker.memory.target": False,
            "distributed.worker.memory.spill": False,
            "distributed.worker.memory.terminate": 0.95,
            "array.slicing.split_large_chunks": True,
            "distributed.scheduler.worker-saturation": 0.95,
            "distributed.scheduler.worker-memory-limit": 0.95,
        }
    )

    cluster = SLURMCluster(
        name=name,
        cores=cores_per_node,
        processes=processes_per_node,
        n_workers=n_nodes,
        memory=f"{memory_per_node_gb}GB",
        account=account,
        queue=part,
        walltime=walltime,
        scheduler_options={"dashboard_address": f":{port}"},
        job_extra_directives=[
            "--output=./logs/%j.out",
            "--error=./logs/%j.err",
            "--propagate=STACK",
        ],
        job_script_prologue=[
            "source ~/.bashrc",
            "conda activate pcpaper_env",
            f"export OMP_NUM_THREADS={n_threads_per_process}",
            f"export MKL_NUM_THREADS={n_threads_per_process}",
            f"export OPENBLAS_NUM_THREADS={n_threads_per_process}",
            f"export VECLIB_MAXIMUM_THREADS={n_threads_per_process}",
            f"export NUMEXPR_NUM_THREADS={n_threads_per_process}",
            "ulimit -s unlimited",
            "ulimit -c 0",
        ],
        python=python,
    )

    if n_nodes > 1:
        cluster.scale(n_nodes)

    print(cluster.job_script())
    print(len(cluster.scheduler.workers))

    client = Client(cluster)
    dashboard_address = cluster.scheduler_address
    remote_dashboard = f"http://{dashboard_address.split('//')[-1].split(':')[0]}:{port}"
    print(f"Remote dashboard address: {remote_dashboard}")
    print(f"Setup ssh port forwarding: ssh -L {port}:{dashboard_address.split('//')[-1].split(':')[0]}:{port} username@levante.dkrz.de")
    print(f"Local dashboard address: http://localhost:{port}")
    return cluster, client


__all__ = [
    "is_server",
    "in_slurm_allocation",
    "recommend_target_chunk_mb",
    "auto_chunk_dataset",
    "describe_chunk_plan",
    "calculate_optimal_scaling",
    "allocate_resources",
]
