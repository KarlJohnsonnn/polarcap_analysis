"""Machine-aware compute utilities for local, laptop, and HPC workflows.

This module centralizes:
- Runtime environment detection (`is_server`, SLURM checks)
- Dask chunk-size heuristics (`auto_chunk_dataset`)
- SLURM-backed Dask cluster helpers (`allocate_resources`)
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
    if psutil is not None:
        return int(psutil.virtual_memory().available)
    if hasattr(os, "sysconf") and "SC_PAGE_SIZE" in os.sysconf_names and "SC_PHYS_PAGES" in os.sysconf_names:
        return int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    return int(8 * 1024**3)


def _dask_worker_memory_bytes() -> int | None:
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
    """Estimate a robust chunk target (MB) from worker/local memory."""
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
    if not ds.data_vars:
        raise ValueError("Dataset has no data variables.")
    return max(ds.data_vars.values(), key=lambda da: (da.ndim, da.size))


def _balanced_chunk_sizes(
    dim_sizes: dict[str, int],
    target_chunk_bytes: int,
    itemsize: int,
    prefer_dims: Iterable[str],
) -> dict[str, int]:
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
    """Rechunk dataset with machine-aware default chunk sizes."""
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
    """Return a short human-readable summary of a chunk strategy."""
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
    """Estimate SLURM scaling settings from workload dimensions."""
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
):
    """Create and return `(cluster, client)` for a SLURM-backed Dask cluster.

    Parameters keep backward compatibility with existing notebooks/scripts.
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
