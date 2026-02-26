# Compute Fabric Utilities

Machine-specific compute helpers are centralized in `utilities.compute_fabric`.

This module unifies:

- runtime detection (`is_server`, `in_slurm_allocation`)
- chunk planning (`recommend_target_chunk_mb`, `auto_chunk_dataset`, `describe_chunk_plan`)
- SLURM + Dask orchestration (`calculate_optimal_scaling`, `allocate_resources`)

Legacy modules `utilities.slurm`, `utilities.runtime_env`, and `utilities.chunking` are kept as compatibility wrappers.

## API Overview

### Environment and Scheduler Context

- `is_server() -> bool`
  - True in JupyterHub/SLURM/server-like environments.
- `in_slurm_allocation() -> bool`
  - True when `SLURM_JOB_ID` is present (active allocation).

### Chunking and Memory Heuristics

- `recommend_target_chunk_mb(min_chunk_mb=64, max_chunk_mb=512, memory_fraction=0.12) -> int`
  - Uses Dask worker memory if a client exists; otherwise local memory.
- `auto_chunk_dataset(ds, ...) -> (ds_chunked, chunk_dict)`
  - Returns a rechunked dataset and chosen chunk sizes.
- `describe_chunk_plan(ds, chunk_dict) -> str`
  - Returns a concise summary of estimated chunk size/count.

### SLURM Scaling Helpers

- `calculate_optimal_scaling(n_time_steps, n_experiments, n_stations, debug_mode=False)`
  - Returns `(n_nodes, n_cpu, memory_gb, scale_workers, walltime)`.
- `allocate_resources(...)`
  - Creates `SLURMCluster` + `Client`, prints dashboard URLs, and returns `(cluster, client)`.

## Fast Diagnostic Commands

Use these before deciding local vs cluster mode:

- `env | rg '^SLURM'`
- `hostname`
- `squeue -u "$USER"`

If no `SLURM_*` variables are present, you are typically not inside a compute allocation.

## Recommended Workflow

1. Start local with Dask progress bars/dashboard and profile bottlenecks.
2. Keep raw high-volume data lazy.
3. Materialize reused reduced fields once (`.load()`).
4. If workload stays heavy (many runs/repeats), switch to SLURM allocation.

## Chunking Example

```python
from utilities import auto_chunk_dataset, describe_chunk_plan

ds, chunks = auto_chunk_dataset(ds, min_chunk_mb=64, max_chunk_mb=512, memory_fraction=0.12)
print(describe_chunk_plan(ds, chunks))
```

## SLURM Example

```python
from utilities import calculate_optimal_scaling, allocate_resources

n_nodes, n_cpu, mem_gb, _, walltime = calculate_optimal_scaling(
    n_time_steps=300,
    n_experiments=8,
    n_stations=3,
)
cluster, client = allocate_resources(
    n_cpu=n_cpu,
    n_jobs=n_nodes,
    m=int(mem_gb),
    walltime=walltime,
    part="compute",
)
```

## Practical Performance Notes

- Chunking is heuristic, not globally optimal.
- Very small chunks (for example `time=1`) can create scheduler overhead.
- Use larger chunks while keeping memory pressure stable.
- Cache reduced intermediates (Zarr/NetCDF) when iterating on plots.
- SLURM startup overhead can dominate small workloads; local mode is often faster for quick iterations.
