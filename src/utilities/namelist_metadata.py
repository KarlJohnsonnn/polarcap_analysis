from matplotlib import cm
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from pathlib import Path
import numpy as np
import json
import sys
import os
import os.path as osp

sys.path.append(
    "/work/bb1262/user/schimmel/cosmo-specs-torch/PaperCode/polarcap1/utils"
)


"""
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Example usage:
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
```
    import xarray as xr
    from utilities.namelist_metadata import update_dataset_metadata, metadata_manager

    # Load your dataset
    ds = xr.open_dataset('your_data.nc')

    # Update with metadata - one simple call!
    ds = update_dataset_metadata(ds)

    # Now all variables have proper attributes for plotting
    print(ds.qv.attrs)  # {'long_name': 'Water vapor mixing ratio', 'units': 'kg/kg', ...}

    # Access colormaps for plotting
    cmap = metadata_manager.colormaps['coolwarm']

    # Get tendency attributes for positive/negative variants
    pos_attrs = metadata_manager.get_variable_attrs('SUM_P_CONDN')
    neg_attrs = metadata_manager.get_variable_attrs('SUM_N_CONDN')

    # Get process groups for analysis
    groups = metadata_manager.get_process_groups()
```

"""


def _find_utils_dir():
    """Automatically find the utils directory relative to this file."""
    current_file = Path(__file__).resolve()

    # If this file is already in utils/ or utilities/, return its parent
    if current_file.parent.name in ("utils", "utilities"):
        return current_file.parent

    # Otherwise, look for utils/ in the current directory tree
    current_dir = current_file.parent
    while current_dir != current_dir.parent:  # Stop at filesystem root
        utils_path = current_dir / "utils"
        if utils_path.exists() and utils_path.is_dir():
            return utils_path
        current_dir = current_dir.parent

    # If not found, assume it's in the same directory as this file
    return current_file.parent


class MetadataManager:
    """Clean, pythonic metadata manager for dataset attributes."""

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = _find_utils_dir() / "metadata_config.json"

        self.config_path = Path(config_path)
        self._load_config()
        self._setup_colormaps()

    def _load_config(self):
        """Load configuration from JSON file."""
        with open(self.config_path, "r") as f:
            self.config = json.load(f)

    def _setup_colormaps(self):
        """Create custom colormaps for plotting."""
        n = 512
        midpoint = n // 2
        cmap = cm.get_cmap("coolwarm")
        colors = cmap(np.linspace(0, 1, n))

        # Create white transition colormap
        cmap_bwr = LinearSegmentedColormap.from_list(
            "bw", [colors[midpoint - 1], "white", colors[midpoint + 1]]
        )
        colors_bwr = cmap_bwr(np.linspace(0, 1, 16))
        colors = np.vstack([colors[:midpoint], colors_bwr, colors[midpoint:]])

        # Store colormaps
        self.cmap_cw = ListedColormap(colors)
        midpoint = len(colors) // 2
        self.cmap_lcw = ListedColormap(colors[:midpoint:-1])
        self.cmap_ucw = ListedColormap(colors[midpoint:])

    def _find_tendency_attrs(self, base_varname):
        """Find tendency attributes by searching through all process groups."""
        if "tendencies" not in self.config:
            return {}

        # Search through all tendency process groups
        for process_group, variables in self.config["tendencies"].items():
            if base_varname in variables:
                return variables[base_varname].copy()

        return {}

    def _get_tendency_attrs(self, varname, tendency_type="net"):
        """Get attributes for tendency variables with pos/neg variants."""
        # Remove SUM_ prefix and P_/N_ variants to get base variable name
        base_varname = varname.replace("SUM_", "").replace("P_", "").replace("N_", "")
        base_attrs = self._find_tendency_attrs(base_varname)

        if not base_attrs:
            return {}

        # Set colormap based on tendency type
        if tendency_type == "pos" or varname.startswith("SUM_P_"):
            base_attrs["cmap"] = self.cmap_ucw.colors
            base_attrs["vlim"] = [0.0, max(0.0, base_attrs.get("vlim", [0, 1])[1])]
        elif tendency_type == "neg" or varname.startswith("SUM_N_"):
            base_attrs["cmap"] = self.cmap_lcw.colors
            base_attrs["vlim"] = [min(0.0, base_attrs.get("vlim", [-1, 0])[0]), 0.0]
        else:
            base_attrs["cmap"] = self.cmap_cw.colors

        return base_attrs

    def get_variable_attrs(self, varname):
        """Get attributes for any variable type."""
        # Check tendency variables first
        if varname.startswith("SUM_"):
            return self._get_tendency_attrs(varname)

        # Check standard variables - handle case-insensitive matching
        varname_upper = varname.upper()
        for category in ["SPECTRAL", "BULK", "ENVIRONMENT"]:
            if category in self.config["variables"]:
                if varname_upper in self.config["variables"][category]:
                    return self.config["variables"][category][varname_upper].copy()

        return {}

    def update_dataset_attrs(self, dataset):
        """Update dataset with variable attributes."""
        for var_name in dataset.data_vars:
            attrs = self.get_variable_attrs(var_name)
            if attrs:
                dataset[var_name].attrs.update(attrs)

        return dataset

    def get_process_groups(self):
        """Get process groups for tendency analysis."""
        return self.config.get("process_groups", {})

    @property
    def colormaps(self):
        """Access to colormaps for plotting."""
        return {
            "coolwarm": self.cmap_cw.colors,
            "lower_coolwarm": self.cmap_lcw.colors,
            "upper_coolwarm": self.cmap_ucw.colors,
        }


# Global instance for easy access
metadata_manager = MetadataManager()

# Convenience functions for backward compatibility


def update_dataset_metadata(dataset):
    """Update dataset with metadata attributes."""
    return metadata_manager.update_dataset_attrs(dataset)


def get_variable_attrs(varname):
    """Get attributes for a specific variable."""
    return metadata_manager.get_variable_attrs(varname)


def get_process_groups():
    """Get process groups for analysis."""
    return metadata_manager.get_process_groups()
