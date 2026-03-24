"""Compare COSMO-SPECS experiment blocks inside ``<cs_run>.json`` (ensemble metadata).

Typical layout: top-level keys are experiment ids; each value holds ``INPUT_ORG`` (namelist-like
nested dicts), ``domain``, job metadata, etc. Use :func:`print_ensemble_config_diff` in notebooks
or CLIs to list keys that differ across members.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence, TextIO

__all__ = ["flatten_nested", "ensemble_config_differences", "print_ensemble_config_diff"]


def flatten_nested(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Dot-path keys for nested dicts; leaf values unchanged (lists/scalars kept as-is)."""
    if not isinstance(obj, dict):
        return {prefix: obj} if prefix else {"": obj}
    flat: dict[str, Any] = {}
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict):
            flat.update(flatten_nested(v, key))
        else:
            flat[key] = v
    return flat


def _display_value(v: Any) -> str:
    if isinstance(v, (dict, list, tuple)):
        try:
            return json.dumps(v, sort_keys=True, default=str)
        except TypeError:
            return repr(v)
    return repr(v)


def _subtree(meta_exp: Mapping[str, Any], roots: Sequence[str]) -> dict[str, Any]:
    if not roots:
        return dict(meta_exp)
    out: dict[str, Any] = {}
    for r in roots:
        if r in meta_exp:
            out[r] = meta_exp[r]
    return out


def ensemble_config_differences(
    meta: Mapping[str, Any],
    experiment_keys: Sequence[str],
    *,
    subtrees: Sequence[str] = ("INPUT_ORG",),
) -> list[tuple[str, dict[str, Any]]]:
    """
    Return [(dot_path, {exp_id: value}), ...] for leaves that are not identical across experiments.

    *subtrees* limits comparison to those top-level keys under each experiment (default COSMO input).
    Use ``subtrees=()`` to compare the full per-experiment record (still flattened).
    """
    keys = list(experiment_keys)
    if not keys:
        return []

    flats: dict[str, dict[str, Any]] = {}
    for eid in keys:
        block = meta.get(eid)
        if not isinstance(block, dict):
            continue
        sub = _subtree(block, subtrees)
        flats[eid] = flatten_nested(sub)

    all_paths: set[str] = set()
    for fd in flats.values():
        all_paths |= set(fd.keys())
    all_paths_sorted = sorted(all_paths)

    differing: list[tuple[str, dict[str, Any]]] = []
    for path in all_paths_sorted:
        by_exp: dict[str, Any] = {}
        for eid in keys:
            if eid not in flats:
                by_exp[eid] = _MISSING
            else:
                by_exp[eid] = flats[eid].get(path, _MISSING)
        norms = {_display_value(v) if v is not _MISSING else "<missing>" for v in by_exp.values()}
        if len(norms) > 1:
            differing.append((path, by_exp))
    return differing


_MISSING = object()


def print_ensemble_config_diff(
    meta: Mapping[str, Any],
    experiment_keys: Sequence[str] | None = None,
    *,
    subtrees: Sequence[str] = ("INPUT_ORG",),
    title: str = "Ensemble config differences (COSMO-SPECS / INPUT_ORG)",
    stream: TextIO | None = None,
) -> None:
    """Print human-readable differing keys across *experiment_keys* (default: all dict top-level keys in *meta*)."""
    import sys

    out = stream or sys.stdout
    if experiment_keys is None:
        experiment_keys = [k for k, v in meta.items() if isinstance(v, dict)]

    diffs = ensemble_config_differences(meta, experiment_keys, subtrees=subtrees)
    print(title, file=out)
    print(f"experiments ({len(experiment_keys)}): {', '.join(experiment_keys)}", file=out)
    if not diffs:
        print("(no differences in selected subtrees)", file=out)
        return
    for path, by_exp in diffs:
        print(f"\n{path}", file=out)
        for eid in experiment_keys:
            v = by_exp.get(eid, _MISSING)
            disp = "<missing>" if v is _MISSING else _display_value(v)
            print(f"  {eid}: {disp}", file=out)
