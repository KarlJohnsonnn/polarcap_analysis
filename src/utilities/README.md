# Utilities library for Polarcap analysis

## HPC Resource Usage Philosophy

**Use only as many resources as the data and goal actually require.**

1. Start with the smallest faithful version of the problem (reduced domain/time/diagnostics).
2. Keep data lazy and chunked until you have reduced it.
3. Compute only what you need for the next step (e.g. a histogram, not the full raw volume).
4. Measure bottlenecks first; optimize second.
5. Increase resources only after measurement shows a real need.

In short: **scale by evidence, not by guesswork**.

### Glossary

| Term | Meaning |
|---|---|
| **Data movement** | Copying large arrays between disk, RAM, workers, or nodes. Often slower than math itself. |
| **Lazy array** | Data is not loaded yet; Python stores a plan of operations first. |
| **Chunking** | Splitting big arrays into smaller blocks so processing fits memory. |
| **Materialize** | Actually load/compute the array now (`.compute()` or `.values`). |
| **Scale up** | Request more CPUs, RAM, or nodes. |
| **Profile** | Measure where runtime is spent (CPU, memory, I/O, scheduler overhead). |

### Resource escalation checklist (follow in order)

Before requesting more memory or workers:

1. Increase stride / downsampling.
2. Reduce histogram bin count (e.g. `96 -> 64 -> 48`).
3. Process one model run at a time.
4. Cache reduced intermediates to NetCDF/Zarr and reuse them.
5. Switch to `float32` where scientific accuracy allows.
6. Scale cluster resources only if full-resolution output is required.

## Working with Large NetCDF Files (1.6 TB class)

Step-by-step operating guide for large `xarray`/`dask` workflows.

### Scope and risk

For datasets with dimensions like `time=229, z=100, y=146, x=186, bin=66`:

- one `float64` variable with `time,z,y,x` is about `4.6 GiB`
- one `float64` variable with `time,z,y,x,bin` is about `305 GiB`

Full in-memory operations are unsafe unless you have very large allocations.

### Required coding pattern

Select only needed variables, cast to `float32`, and downsample before computing:

```python
mod = ds[['qfw', 'qw', 'qv', 'qc']].astype('float32')
sample = mod.isel(
    time=slice(None, None, 8),
    altitude=slice(None, None, 4),
    latitude=slice(None, None, 8),
    longitude=slice(None, None, 8),
)
```

**Prohibited** -- do not call `.values` on large lazy arrays before heavy reduction:

```python
mod = ds[['qfw', 'qw', 'qv', 'qc']].persist()
arr = mod['qfw'].values  # will likely crash
```

### Histogram workflow

1. Estimate bin edges from sampled data.
2. Compute histograms on sampled data first.
3. Save reduced outputs to Zarr for reuse.

```python
import dask
from dask.diagnostics import ProgressBar
from xhistogram.xarray import histogram as xhist

# stride=2: half resolution on every axis
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

### Dask dashboard panels

| Panel | What to watch |
|---|---|
| **Task Stream** | Tasks are being completed. |
| **Worker Memory** | Memory pressure and spill behavior. |
| **Progress** | Percent completion advances. |
| **Profile** | Dominant compute hotspots. |

- Steady progress + stable memory = healthy execution.
- Flat progress + near-limit memory/spill = stop, increase stride or reduce bins.

## API Reference

All compute helpers live in `utilities.compute_fabric`.
Legacy wrappers `utilities.slurm`, `utilities.runtime_env`, and `utilities.chunking` are kept for backward compatibility.

### Environment detection

- `is_server() -> bool` -- True in JupyterHub/SLURM/server-like environments.
- `in_slurm_allocation() -> bool` -- True when `SLURM_JOB_ID` is present.

Quick shell check:

```bash
env | rg '^SLURM'
hostname
squeue -u "$USER"
```

### Chunking and memory heuristics

- `recommend_target_chunk_mb(min_chunk_mb=64, max_chunk_mb=512, memory_fraction=0.12) -> int`
  Uses Dask worker memory if a client exists; otherwise local memory.
- `auto_chunk_dataset(ds, ...) -> (ds_chunked, chunk_dict)`
  Returns a rechunked dataset and chosen chunk sizes.
- `describe_chunk_plan(ds, chunk_dict) -> str`
  Concise summary of estimated chunk size/count.

```python
from utilities import auto_chunk_dataset, describe_chunk_plan

ds, chunks = auto_chunk_dataset(ds, min_chunk_mb=64, max_chunk_mb=512, memory_fraction=0.12)
print(describe_chunk_plan(ds, chunks))
```

### SLURM scaling

- `calculate_optimal_scaling(n_time_steps, n_experiments, n_stations, debug_mode=False)`
  Returns `(n_nodes, n_cpu, memory_gb, scale_workers, walltime)`.
- `allocate_resources(...)`
  Creates `SLURMCluster` + `Client`, prints dashboard URLs, returns `(cluster, client)`.

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

### Dataset slicer

Generic slicer helpers in `utilities.data_slicer` for local condensed-data workflows.

**Supported keys and aliases:**

| Key | Resolves to |
|---|---|
| `time` | `time` |
| `lat` | `latitude` or `y` |
| `lon` | `longitude` or `x` |
| `alt` | `altitude` or `z` |
| `diameter` | `diameter` or `bin` |

**Supported value types:** scalar, tuple `(start, end)`, `slice(start, end, step)`, or explicit list.

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
print(meta["shape_before"], meta["shape_after"], meta["effective_bounds"])
```

Write a sliced intermediate to Zarr:

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

## Practical Tips

- Start local with Dask progress bars/dashboard; switch to SLURM only when workload stays heavy.
- SLURM startup overhead can dominate small workloads -- local mode is often faster for quick iterations.
- Chunking is heuristic, not globally optimal. Very small chunks (e.g. `time=1`) create scheduler overhead.
- Cache reduced intermediates (Zarr/NetCDF) when iterating on plots.
- Large jobs can fail from memory pressure even when CPU usage looks low.
- Oversized allocations waste queue time and reduce cluster availability for others.
