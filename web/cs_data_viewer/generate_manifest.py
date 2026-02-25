#!/usr/bin/env python3
"""
Generate manifest.json for cosmo-specs data viewer.
Run this from your data root directory (e.g. /path/to/pngs/).

Usage:
    python generate_manifest.py
    python generate_manifest.py /path/to/pngs/
"""

import json
import os
import pathlib
import sys

SUPPORTED = {'.png', '.jpg', '.jpeg', '.gif', '.mp4', '.webm', '.svg'}
VIDEO_EXT = {'.mp4', '.webm'}

def generate(root='.'):
    root = pathlib.Path(root)
    files = []

    for p in sorted(root.rglob('*')):
        if p.suffix.lower() not in SUPPORTED:
            continue
        rel = p.relative_to(root)
        ext = p.suffix[1:].lower()
        ftype = 'video' if p.suffix.lower() in VIDEO_EXT else 'image'
        folder = str(rel.parent) if str(rel.parent) != '.' else ''

        files.append({
            'path': str(rel),
            'name': p.name,
            'folder': folder,
            'type': ftype,
            'ext': ext,
        })

    out = root / 'manifest.json'
    with open(out, 'w') as f:
        json.dump({'files': files}, f, indent=1)

    print(f"✓ Indexed {len(files)} files → {out}")

    # Summary
    folders = sorted(set(f['folder'] for f in files if f['folder']))
    print(f"  Folders: {len(folders)}")
    for folder in folders[:20]:
        count = sum(1 for f in files if f['folder'] == folder)
        print(f"    {folder}/ ({count})")
    if len(folders) > 20:
        print(f"    ... and {len(folders) - 20} more")

if __name__ == '__main__':
    target = sys.argv[1] if len(sys.argv) > 1 else '.'
    generate(target)
