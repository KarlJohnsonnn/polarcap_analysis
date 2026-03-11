import sys
from pathlib import Path
import numpy as np

src_dir = next(p / "src" for p in (Path.cwd(), *Path.cwd().parents)
               if (p / "src" / "polarcap_runtime.py").is_file())
sys.path.insert(0, str(src_dir))

from utilities import load_plume_path_runs
datasets = load_plume_path_runs(processed_root=Path("../../data/processed"), kinds=("integrated",))
ds = list(datasets.values())[0]["integrated"]

vars_to_check = ['nf', 'nw', 'qf', 'qfw', 'qw', 'qi', 'qc', 'icnc', 'cdnc', 'rho', 't', 'pp', 'p0', 'wt']
for v in vars_to_check:
    if v in ds.data_vars:
        print(f"{v}: min={float(ds[v].min()):.3e}, max={float(ds[v].max()):.3e}, attrs={ds[v].attrs}")
    else:
        print(f"{v} not found")
