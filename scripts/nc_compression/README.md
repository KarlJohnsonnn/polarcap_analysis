# NetCDF compression and HSM archive

- `compress.sh`: list/compress/extract `M_*.nc` and `3D_*.nc`
- `archive.sh`: list `*.nc.zst` or archive one file via `slk archive -vv`
- `run_compess_and_archive.sh`: submits compression+archive Slurm arrays
- `archive2tape`: short wrapper command

Before archiving on Levante, run `module load slk` and `slk login`.

DKRZ docs:

- [Archivals to tape](https://docs.dkrz.de/doc/datastorage/hsm/archivals.html#)
- [Getting Started with slk](https://docs.dkrz.de/doc/datastorage/hsm/getting_started.html)

## Enable `archive2tape` from anywhere

```bash
export POLARCAP_ROOT=/path/to/polarcap_analysis
export PATH="$POLARCAP_ROOT/scripts/nc_compression:$PATH"
```

## Command

Compress and archive NetCDF files from one source directory to one HSM tape.
```bash
archive2tape [source_dir] <compressed_name>
```

Compress and archive NetCDF files from multiple source directories to one HSM tape.

```bash
cd /path/to/ensemble_output
for d in ./cs-eriswil__*
do
    [[ -d "$d" ]] || continue
    archive2tape "$d" "${d}.tar.zst"
done
```

## Workflow sketch

```mermaid
%%{init: { "theme": "neutral", "flowchart": { "curve": "linear", "nodeSpacing": 30, "rankSpacing": 40, "padding": 8, "useMaxWidth": true } }}%%
flowchart TD
    subgraph STEP0["0. Setup"]
        direction LR
        CMD["archive2tape<br/>source_dir compressed_name"]:::command
        ENV["define env vars<br/>GRAVEYARD · HSM_ROOT"]:::setup
        NAME["derive run_name<br/>from compressed_name"]:::meta
        CMD --> ENV --> NAME
    end

    subgraph STEP1["1. Compress to graveyard"]
        direction LR
        SRC["source_dir<br/>M_*.nc + 3D_*.nc"]:::data
        CSUB["submit compression array<br/>COMPRESS_JOBS"]:::step
        CWORK["worker action<br/>compress.sh per file"]:::meta
        GRV["$GRAVEYARD/run_name/<br/>*.nc.zst files"]:::store
        SRC --> CSUB --> CWORK --> GRV
    end

    subgraph STEP2["2. Send to tape archive"]
        direction LR
        DEP["wait for compression<br/>afterok dependency"]:::gate
        ASUB["submit archive array<br/>ARCHIVE_JOBS"]:::step
        AWORK["worker action<br/>archive.sh + slk archive -vv"]:::meta
        HSM["$HSM_ROOT/run_name/<br/>tape archive"]:::store
        DEP --> ASUB --> AWORK --> HSM
    end

    subgraph STEP3["3. Result"]
        direction LR
        DONE["compressed files archived<br/>to HSM tape"]:::result
        LOG["job ids, manifests, and<br/>.slurm logs for monitoring"]:::meta
        DONE --> LOG
    end

    STEP0 --> STEP1 --> STEP2 --> STEP3

    classDef command fill:#2f3640,stroke:#2f3640,color:#ffffff,stroke-width:2px
    classDef setup fill:#eaf2ff,stroke:#6b8fd6,color:#1f2937,stroke-width:2px
    classDef step fill:#eaf2ff,stroke:#6b8fd6,color:#1f2937,stroke-width:2px
    classDef gate fill:#eaf2ff,stroke:#6b8fd6,color:#1f2937,stroke-width:2px
    classDef data fill:#eef7ee,stroke:#6da36d,color:#1f2937,stroke-width:2px
    classDef store fill:#eef7ee,stroke:#6da36d,color:#1f2937,stroke-width:2px
    classDef result fill:#eef7ee,stroke:#6da36d,color:#1f2937,stroke-width:2px
    classDef meta fill:#f5f6f7,stroke:#c7ccd1,color:#4b5563,stroke-width:1px

    style STEP0 fill:#fafafa,stroke:#d9dde2,stroke-width:1px
    style STEP1 fill:#fafafa,stroke:#d9dde2,stroke-width:1px
    style STEP2 fill:#fafafa,stroke:#d9dde2,stroke-width:1px
    style STEP3 fill:#fafafa,stroke:#d9dde2,stroke-width:1px
```



**Legend**

- **Dark gray**: command entrypoint (`archive2tape`)
- **Blue**: active processing steps (setup, submit jobs, dependency wait)
- **Green**: data and storage states (source, graveyard, HSM)
- **Light gray**: metadata and monitoring details
- **Pale stage container**: grouped workflow stage

Example:

```bash
cd /path/to/ensemble_output
archive2tape ./cs-eriswil__20260318_153631 cs-eriswil__20260318_153631.tar.zst
```

Behavior:

1. Create run dir in `$GRAVEYARD`: `cs-eriswil__YYYYMMDD_HHMMSS`
2. Compress NetCDF files directly into `$GRAVEYARD/<run_name>/`
3. Archive compressed files to `$HSM_ROOT/<run_name>/`

## Key environmental variables

- `GRAVEYARD` (temporary storage of compressed files, default: `/scratch/b/<user_name>/path/to/ensemble_output`)
- `HSM_ROOT` (HSM storage of archived files, default: `/arch/<project_name>/path/to/cosmo_specs/ensemble_output`)
- `COMPRESS_JOBS` (number of parallel compression jobs, default: `8`)
- `ARCHIVE_JOBS` (number of parallel archive jobs, default: `2`)
- `OVERWRITE=1` (overwrite existing compressed files)
- `RETRY=1` (retry archive if it fails) and `RETRY_DELAY=60` (delay between retries in seconds)
- `LOG_DIR` (optional; default: `./.slurm/<run_name>_<timestamp>/` in the directory where `archive2tape` is executed)

Note: `<user_name>` (e.g. `b382237`) and `<project_name>` (e.g. `bb1262`) are placeholders for your actual user and project names.

## Printed output variables

- `RUN_NAME=...` (derived from the compressed file name)
- `COMPRESSED_DIR=...` (path to the compressed files)
- `HSM_NAMESPACE=...` (path to the HSM archive)
- `COMPRESS_JOB_ID=...` (job ID for the compression job)
- `ARCHIVE_JOB_ID=...` (job ID for the archive job)

