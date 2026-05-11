"""Compare COSMO-SPECS experiment blocks inside ``<cs_run>.json`` (ensemble metadata).

Typical layout: top-level keys are experiment ids; each value holds ``INPUT_ORG`` (namelist-like
nested dicts), ``domain``, job metadata, etc.

- :func:`print_ensemble_config_diff` — text listing of differing leaves.
- :func:`create_ensemble_diff_table` — pandas table (flare rows, reference column, mandatory paths such as ishape/domain, then differing keys).
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence, TextIO

# Full flattened paths under each experiment block (see *_flats_for_keys).
_DEFAULT_ALWAYS_COLUMNS: tuple[str, ...] = (
    "INPUT_ORG.sbm_par.ishape",
    "domain",
)

__all__ = [
    "ensemble_config_differences",
    "print_ensemble_config_diff",
    "create_ensemble_diff_table",
]

_MISSING = object()


def _flatten_nested(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Dot-path keys for nested dicts; leaf values unchanged (lists/scalars kept as-is)."""
    if not isinstance(obj, dict):
        return {prefix: obj} if prefix else {"": obj}
    flat: dict[str, Any] = {}
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict):
            flat.update(_flatten_nested(v, key))
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


def _truncate_cell(text: str, max_col_width: int | None) -> str:
    if max_col_width is None or len(text) <= max_col_width:
        return text
    return text[: max_col_width - 3] + "..."


def _subtree(meta_exp: Mapping[str, Any], roots: Sequence[str]) -> dict[str, Any]:
    if not roots:
        return dict(meta_exp)
    out: dict[str, Any] = {}
    for r in roots:
        if r in meta_exp:
            out[r] = meta_exp[r]
    return out


def _flats_for_keys(
    meta: Mapping[str, Any],
    experiment_keys: Sequence[str],
    subtrees: Sequence[str],
) -> dict[str, dict[str, Any]]:
    flats: dict[str, dict[str, Any]] = {}
    for eid in experiment_keys:
        block = meta.get(eid)
        if not isinstance(block, dict):
            continue
        sub = _subtree(block, subtrees)
        flats[eid] = _flatten_nested(sub)
    return flats


def _by_exp_for_path(
    flats: Mapping[str, Mapping[str, Any]],
    experiment_keys: Sequence[str],
    path: str,
) -> dict[str, Any]:
    by_exp: dict[str, Any] = {}
    for eid in experiment_keys:
        if eid not in flats:
            by_exp[eid] = _MISSING
        else:
            by_exp[eid] = flats[eid].get(path, _MISSING)
    return by_exp


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

    flats = _flats_for_keys(meta, keys, subtrees)

    all_paths: set[str] = set()
    for fd in flats.values():
        all_paths |= set(fd.keys())
    all_paths_sorted = sorted(all_paths)

    differing: list[tuple[str, dict[str, Any]]] = []
    for path in all_paths_sorted:
        by_exp = _by_exp_for_path(flats, keys, path)
        norms = {_display_value(v) if v is not _MISSING else "<missing>" for v in by_exp.values()}
        if len(norms) > 1:
            differing.append((path, by_exp))
    return differing


def _flare_order_and_refs(pair_rows: Sequence[tuple[Any, ...]]) -> tuple[list[str], dict[str, str]]:
    """First-seen flare order; last pairing wins if same flare appears twice."""
    flare_order: list[str] = []
    ref_for_flare: dict[str, str] = {}
    for tup in pair_rows:
        if len(tup) < 2:
            continue
        fexp, rexp = str(tup[0]), str(tup[1])
        ref_for_flare[fexp] = rexp
        if fexp not in flare_order:
            flare_order.append(fexp)
    return flare_order, ref_for_flare


def _strip_path_label(path: str, strip_column_prefix: str | None) -> str:
    if strip_column_prefix and path.startswith(strip_column_prefix):
        return path[len(strip_column_prefix) :]
    return path


def create_ensemble_diff_table(
    meta: Mapping[str, Any],
    experiment_keys: Sequence[str] | None = None,
    *,
    pair_rows: Sequence[tuple[Any, ...]] | None = None,
    subtrees: Sequence[str] = ("INPUT_ORG",),
    always_include_paths: Sequence[str] = _DEFAULT_ALWAYS_COLUMNS,
    title: str = "Ensemble config differences (COSMO-SPECS / INPUT_ORG)",
    strip_column_prefix: str | None = "INPUT_ORG.",
    max_col_width: int | None = 48,
):
    """Rows: flare runs if *pair_rows* set, else every *experiment_keys*.

    Columns: ``flare_run``, ``reference_run``, then *always_include_paths* (even when identical
    across runs), then any other paths that differ within *subtrees*. Values shown per row are for
    ``flare_run``. Requires pandas.
    """
    import pandas as pd

    if experiment_keys is None:
        experiment_keys = [k for k, v in meta.items() if isinstance(v, dict)]

    keys = list(experiment_keys)
    diffs = ensemble_config_differences(meta, keys, subtrees=subtrees)

    flatten_roots = tuple(dict.fromkeys((*subtrees, *(p.split(".")[0] for p in always_include_paths))))
    flats = _flats_for_keys(meta, keys, flatten_roots)

    columns_paths: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for path in always_include_paths:
        if path in seen:
            continue
        columns_paths.append((path, _by_exp_for_path(flats, keys, path)))
        seen.add(path)
    for path, by_exp in diffs:
        if path in seen:
            continue
        columns_paths.append((path, by_exp))
        seen.add(path)

    print(title)
    print(f"experiments in diff ({len(keys)}): {', '.join(keys)}")
    if not columns_paths:
        print("(no columns to show)")
        return None

    col_labels = [_strip_path_label(path, strip_column_prefix) for path, _ in columns_paths]

    if pair_rows is not None:
        row_ids, ref_for_flare = _flare_order_and_refs(pair_rows)
    else:
        row_ids, ref_for_flare = keys, {}

    rows: list[list[Any]] = []
    for flare_run in row_ids:
        reference_run = ref_for_flare.get(flare_run, pd.NA) if pair_rows is not None else pd.NA
        row: list[Any] = [flare_run, reference_run]
        for _, by_exp in columns_paths:
            v = by_exp.get(flare_run, _MISSING)
            cell = "<missing>" if v is _MISSING else _display_value(v)
            row.append(_truncate_cell(cell, max_col_width))
        rows.append(row)

    cols = ["flare_run", "reference_run", *col_labels]
    return pd.DataFrame(rows, columns=pd.Index(cols))


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
