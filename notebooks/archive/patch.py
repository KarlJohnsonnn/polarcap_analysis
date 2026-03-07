import json
import numpy as np

def patch_nb(filename):
    with open(filename, 'r') as f:
        nb = json.load(f)
    
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source = "".join(cell['source'])
            
            # Patch 01
            if "mean_diam[:, c] = calculate_mean_diameter(nf_ma[:, c, :], diameters)" in source:
                new_source = """# Mean ice diameter per cell per timestep (arithmetic mean, weighted by number)
nf_vals = nf.values
mean_diam = np.full((ds.sizes['time'], ds.sizes['cell']), np.nan)
for c in range(ds.sizes['cell']):
    nf_ma = np.ma.masked_less_equal(nf_vals[:, c, :], 0)
    try:
        mask = nf_vals[:, c, :] > 0
        num = np.sum(nf_vals[:, c, :] * diameters * mask, axis=1)
        den = np.sum(nf_vals[:, c, :] * mask, axis=1)
        mean_diam[:, c] = np.where(den > 0, num / den, np.nan)
    except Exception as e:
        print(f"Error calculating mean diam: {e}")

print(f"Mean diameter range: {np.nanmin(mean_diam):.1f} - {np.nanmax(mean_diam):.1f} µm")"""
                
                # We need to replace just that block
                old_block = """# Mean ice diameter per cell per timestep (arithmetic mean, weighted by number)
nf_vals = nf.values
nf_ma = np.ma.masked_less_equal(nf_vals, 0)
mean_diam = np.zeros((ds.sizes['time'], ds.sizes['cell']))
for c in range(ds.sizes['cell']):
    mean_diam[:, c] = calculate_mean_diameter(nf_ma[:, c, :], diameters)

mean_diam = np.ma.masked_invalid(mean_diam)
print(f"Mean diameter range: {np.nanmin(mean_diam):.1f} – {np.nanmax(mean_diam):.1f} µm")"""
                
                if old_block in source:
                    source = source.replace(old_block, new_source)
                    cell['source'] = [line + '\n' if i < len(source.split('\n'))-1 else line for i, line in enumerate(source.split('\n'))]

            # Clear outputs
            cell['outputs'] = []
            cell['execution_count'] = None
            
    with open(filename, 'w') as f:
        json.dump(nb, f, indent=1)

patch_nb('01-growth-rate-sensitivity.ipynb')
print("Patched 01")
