# Compute Fabric Utilities

## HPC Resource Usage Philosophy

HPC users usually follow one rule: **use only as many resources as the data and goal actually require**. But how should you know how much resources you need?

1. Start with the smallest faithful version of the problem (reduced domain/time/diagnostics).
2. Keep data lazy and chunked until you have reduced it.
3. Compute only what you need for the next step (for example a histogram, not the full raw volume).
4. Measure bottlenecks first; optimize second.
5. Increase resources only after measurement shows a real need.

This keeps workflows fast, stable, and fair for everyone sharing the cluster.

### Plain-language meaning of common HPC terms

- **Data movement**: copying large arrays between disk, RAM, workers, or nodes. This is often slower than math itself.
- **Lazy array**: data is not loaded yet; Python stores a plan of operations first.
- **Chunking**: splitting big arrays into smaller blocks so processing fits memory.
- **Materialize**: actually load/compute the array now (for example with `.compute()` or `.values`).
- **Scale up**: request more CPUs, RAM, or nodes.
- **Profile**: measure where runtime is spent (CPU, memory, I/O, scheduler overhead).

### Resource reduction checklist (recommended order)

Before asking for bigger jobs:

1. Increase stride/downsampling.
2. Reduce histogram bin count.
3. Process one run at a time.
4. Save reduced intermediates to NetCDF/Zarr and reuse them.
5. Switch to `float32` where scientific accuracy allows.

### Why this matters in practice

- Large jobs can fail from memory pressure even when CPU usage looks low.
- Oversized allocations can waste queue time and reduce cluster availability for others.
- A measured, reduced workflow is usually easier to debug, rerun, and reproduce.

In short: **scale by evidence, not by guesswork**.



## User Manual: Working with Large NetCDF Files (1.6 TB class)

This section is a step-by-step operating guide for large `xarray`/`dask` workflows.

### 1) Scope and risk

For datasets with dimensions like `time=229, z=100, y=146, x=186, bin=66`:

- one `float64` variable with `time,z,y,x` is about `4.6 GiB`
- one `float64` variable with `time,z,y,x,bin` is about `305 GiB`

Implication: full in-memory operations are unsafe unless you have very large allocations.

### 2) Pre-run checklist

Before running expensive cells read the following checklist. A detailed explanation for each item is provided below.

1. Keep the dataset lazy (do not eagerly load full variables).
2. Select only required variables.
3. Cast diagnostic fields to `float32` when scientific precision allows.
4. Define a downsampling stride for diagnostics.
5. Open the Dask dashboard.

### 3) Required coding pattern

Use this pattern as default for diagnostics and figure generation:

```python
mod = ds[['qfw', 'qw', 'qv', 'qc']].astype('float32')
sample = mod.isel(
    time=slice(None, None, 8),
    altitude=slice(None, None, 4),
    latitude=slice(None, None, 8),
    longitude=slice(None, None, 8),
)
```

#### Prohibited pattern (common crash source)

```python
mod = ds[['qfw', 'qw', 'qv', 'qc']].persist()
arr = mod['qfw'].values
```

Do not use `.values` on large lazy arrays before heavy reduction.

### 4) Histogram workflow for large files

1. Estimate bin edges from sampled data.
2. Compute histograms on sampled data first.
3. Compute only reduced intermediates.
4. Save reduced outputs for reuse, best if in Zarr format.

Example:

```python
import dask                                       # pip install dask
from dask.diagnostics import ProgressBar          # pip install dask-diagnostics
from xhistogram.xarray import histogram as xhist  # pip install xhistogram
# use only 1/2 of the datas resolution, where slice(start_index, end_index, step) in this example the value of 2 is the stride 
small = mod.isel(
    time=slice(None, None, 2),
    altitude=slice(None, None, 2),
    latitude=slice(None, None, 2),
    longitude=slice(None, None, 2),
)
h = xhist(small['qfw'], small['qw'], bins=[xbins, ybins])
delayed = h.to_zarr(path='histogram.zarr', compute=False)
with ProgressBar():
    result = dask.compute(delayed)
```

### 5) Resource escalation procedure

Follow this order before requesting more memory/workers:

1. Increase stride/downsampling.
2. Reduce histogram bins (example: `96 -> 64 -> 48`).
3. Process one model simulation run at a time.
4. Cache reduced products to NetCDF/Zarr.
5. Scale cluster resources only if full-resolution output is required.

### 6) Dashboard operation guide

Monitor these panels during execution:

- **Task Stream**: confirms tasks are being completed.
- **Worker Memory**: detects memory pressure and spill behavior.
- **Progress**: confirms percent completion advances.
- **Profile**: identifies dominant compute hotspots.

Interpretation:

- Steady progress with stable memory: execution is healthy, even if runtime is several minutes.
- Flat progress + repeated near-limit memory/spill: stop and increase stride/reduce bins.

## Helper Functions for Local and HPC Workflows

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

## Generic Dataset Slicer

For local condensed-data workflows, use the generic slicer helpers in `utilities.data_slicer`.

### Supported slicing keys

- `time`
- `lat` aliases to `latitude` or `y`
- `lon` aliases to `longitude` or `x`
- `alt` aliases to `altitude` or `z`
- `diameter` aliases to `diameter` or `bin`

### Supported values

- scalar (single point/index)
- tuple `(start, end)` for bounded range
- `slice(start, end, step)`
- explicit list of values/indices

### Example: slice in-memory

```python
from utilities import slice_dataset

slice_cfg = {
    "time": ("2023-01-25T10:35:00", "2023-01-25T11:35:00"),
    "lat": (47.03, 47.11),
    "lon": (7.80, 7.92),
    "alt": (1000.0, 2200.0),
    "diameter": (1.0, 2000.0),
}

ds_slice, meta = slice_dataset(ds, slice_cfg, strict=True)
print(meta["shape_before"])
print(meta["shape_after"])
print(meta["effective_bounds"])
```

### Example: write intermediate Zarr

```python
from utilities import slice_dataset_to_zarr

ds_slice, meta = slice_dataset_to_zarr(
    ds,
    slice_cfg,
    out_path="data/processed/intermediate/local_slice.zarr",
    overwrite=True,
)
print(meta["zarr_path"])
```
