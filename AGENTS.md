# AGENTS.md

## Cursor Cloud specific instructions

This repo bundles three largely independent products (see `README.md`):

- **`polarcap_analysis` (main)** — a file-based scientific data pipeline + Jupyter notebooks for COSMO-SPECS / PolarCAP cloud-seeding analysis. It is a **batch pipeline, not a persistent server**, and uses **no database**.
- **`web/cs_data_viewer` (QuicklookBrowser)** — a local media viewer served by Python's stdlib `http.server`.
- **`kinesis-world`** — a standalone static typing-trainer web app (no build, no backend); explicitly unrelated to the research repo.

### Environment / dependencies

- Python 3.12 is the interpreter here; `requirements.txt` pins `numpy>=1.26,<2`.
- `python3-venv` is **not installable** in this VM (no apt candidate). Do not rely on `python3 -m venv`. Install into the user/system site instead: `pip3 install --break-system-packages -r requirements.txt` (this is what the startup update script runs).
- The full science stack (`xarray`, `pandas`, `netCDF4`, `dask`, `tobac`, `iris`) is **not** in `requirements.txt` and must be installed manually only if you actually run the processing chain (see `scripts/processing_chain/README.md`).

### Running the pipeline (main product) — caveats

- The processing chain (`scripts/processing_chain/run_chain.py`, LV1–LV3) and notebooks require **external raw COSMO-SPECS model data** (`CS_RUNS_DIR` / `POLARCAP_OUTPUT_ROOT`) that is **not present in this repo**, and normally target the DKRZ Levante HPC + Slurm. These cannot run end-to-end here without that data.
- `scripts/ipython_startup/install.sh` is **interactive** (prompts for `CS_RUNS_DIR`) and **exits non-zero if left empty** — do not call it from non-interactive automation. It only bootstraps notebook `sys.path`; skip it unless running notebooks with real data.

### Running the web tools (fully runnable here)

- QuicklookBrowser: `python3 web/cs_data_viewer/generate_manifest.py` then start `python3 web/cs_data_viewer/serve_quicklookbrowser.py --host 127.0.0.1 --port 8000` (the `./qlb` wrapper does both but also tries to open a browser). App URL: `http://127.0.0.1:8000/web/cs_data_viewer/cosmo-viewer.html`; health check: `GET /__quicklookbrowser/status`. It indexes media under `output/`, `notebooks/output/`, `scripts/processing_chain/output/`.
- Kinesis World: `cd kinesis-world && python3 -m http.server 8765`, then open `http://localhost:8765/`. Progress persists in browser `localStorage`, so quests may already show as completed on a returning session.

### Lint / tests

- There are **no automated tests** and no lint config beyond `pyrightconfig.json` (Pyright type-checking scope only). Validate Python changes with `python3 -c "import ..."` / running the relevant script.
