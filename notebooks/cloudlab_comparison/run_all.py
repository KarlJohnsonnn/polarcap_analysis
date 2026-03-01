import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os

nbs = [
    '01-growth-rate-sensitivity.ipynb',
    '02-lagrangian-icnc-lwc-comparison.ipynb',
    '03-inf-model-vs-field.ipynb'
]

ep = ExecutePreprocessor(timeout=600, kernel_name='python3')

for nb_file in nbs:
    print(f"Executing {nb_file}...")
    try:
        with open(nb_file) as f:
            nb = nbformat.read(f, as_version=4)
        ep.preprocess(nb, {'metadata': {'path': '.'}})
        with open(nb_file, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        print(f"  -> Success")
    except Exception as e:
        print(f"  -> Error: {e}")
