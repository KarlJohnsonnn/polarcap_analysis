# IPython Startup (One-Time Setup)

Install once to make PolarCAP runtime helpers available in every new notebook kernel.

## Install

```bash
$ scripts/ipython_startup/install.sh
```

## Uninstall

```bash
$ scripts/ipython_startup/uninstall.sh
```

## What it does

- Writes `~/.ipython/profile_default/startup/10-polarcap-startup.py`
- Prepends this repo's `src` directory to `sys.path`
- Loads local runtime helpers from `src/polarcap_runtime.py`
- Exposes `is_server()` globally in notebook kernels

## Optional override

Set `POLARCAP_REPO_ROOT` if your repo location changes:

```bash
export POLARCAP_REPO_ROOT="/path/to/polarcap_analysis"
```
