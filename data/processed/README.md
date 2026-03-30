# Processed Data

`data/processed/` is the canonical local view of processed LV1, LV2, LV2b, and LV3 artifacts.

Policy:

- Keep any existing local products in place.
- Fill missing stage files with symlinks to the preferred source root on `/work` or `/scratch`.
- Avoid duplicate copies when the same stage already exists locally or remotely.
- Record inventory and gap reports in `output/tables/registry/processed_sync_inventory.csv` and `output/tables/registry/processed_sync_report.md`.

The preferred source order is:

1. `/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_50x40x100/ensemble_output`
2. `/work/bb1262/user/schimmel/cosmo-specs-torch/cosmo-specs-runs/RUN_ERISWILL_200x160x100/ensemble_output`
3. `/scratch/b/b382237/schimmel/cosmo-specs-runs/ensemble_output`

Use `python scripts/processing_chain/sync_processed_root.py` to refresh this view.
