"""Resolve figure caption by filename or by figure-type prefix. Used by gallery.ipynb."""
from pathlib import Path
import yaml

FIGURE_TYPE_PREFIXES = (
    "figure13_psd_alt_time_number",
    "figure13_psd_alt_time_mass",
    "figure12_plume_path",
    "cloud_field_overview",
    "spectral_waterfall",
    "pamtra_mira35_quicklook",
)


def load_captions(captions_file: Path) -> dict:
    with open(captions_file) as f:
        return yaml.safe_load(f) or {}


def caption_for(name: str, captions: dict) -> str:
    raw = (captions.get(name) or captions.get(name.replace(" ", "_")) or "").strip()
    if raw:
        return raw
    stem = Path(name).stem
    for prefix in FIGURE_TYPE_PREFIXES:
        if stem.startswith(prefix) or name.startswith(prefix):
            raw = (captions.get(prefix) or "").strip()
            if raw:
                return raw
    return "(no caption)"
