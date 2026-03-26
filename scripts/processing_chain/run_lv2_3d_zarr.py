#!/usr/bin/env python3
"""
LV2: 3D NetCDFs -> single normalized Zarr store.

Usage:
  python run_lv2_3d_zarr.py -r cs-eriswil__20260318_153631
  python run_lv2_3d_zarr.py -r cs-eriswil__20260318_153631 --limit-experiments 2 --overwrite
  python run_lv2_3d_zarr.py -r cs-eriswil__20260318_153631 --out /path/to/processed --extpar /path/to/extPar_Eriswil_200x160.nc
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_src = _script_dir.parent.parent / "src"
_utilities = _src / "utilities"
for candidate in (_utilities, _src):
    if candidate.is_dir() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from processing_paths import expand_path, get_output_root, get_runs_root, resolve_ensemble_output


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LV2: 3D NetCDF ensemble -> normalized Zarr")
    p.add_argument(
        "-r",
        "--cs-run",
        required=True,
        help="Run ID (e.g. cs-eriswil__YYYYMMDD_HHMMSS)",
    )
    p.add_argument(
        "--root",
        default=get_runs_root(),
        help="Root containing RUN_ERISWILL_*x100, an ensemble_output dir, or a direct <cs_run> dir",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Output root; writes to <out>/<cs_run>/lv2_3d/ (default mirrors ensemble_output when possible)",
    )
    p.add_argument(
        "--extpar",
        default=None,
        help="Optional explicit extpar file; auto-detected from run metadata when omitted",
    )
    p.add_argument(
        "--domain-xy",
        default=None,
        help="Optional domain override like 200x160; otherwise inferred from run JSON",
    )
    p.add_argument(
        "--limit-experiments",
        type=int,
        default=None,
        help="Only use the first N experiments, useful for smoke tests",
    )
    p.add_argument(
        "--var-sets",
        nargs="+",
        choices=("meteo", "spec", "bulk"),
        default=None,
        help="Optional variable subset; omit to preserve the full native variable set",
    )
    p.add_argument(
        "--target-time-step-seconds",
        type=float,
        default=10.0,
        help="Output time step in seconds (default: 10)",
    )
    p.add_argument(
        "--time-method",
        choices=("linear", "nearest"),
        default="linear",
        help="Interpolation method for time harmonization",
    )
    p.add_argument(
        "--target-chunk-mb",
        type=int,
        default=None,
        help="Override automatic target chunk size in MB",
    )
    p.add_argument("--min-chunk-mb", type=int, default=64, help="Lower chunk-size bound in MB")
    p.add_argument("--max-chunk-mb", type=int, default=512, help="Upper chunk-size bound in MB")
    p.add_argument(
        "--memory-fraction",
        type=float,
        default=0.12,
        help="Fraction of worker memory to target per chunk when auto-chunking",
    )
    p.add_argument(
        "--compression-level",
        type=int,
        default=3,
        help="Blosc zstd compression level for data variables",
    )
    p.add_argument("--overwrite", action="store_true", help="Overwrite an existing Zarr store")
    return p.parse_args()


def _resolve_run_dir(cs_run: str, root: str | None) -> tuple[Path, str | None]:
    runs_root = get_runs_root(root)
    if runs_root:
        ens_root = resolve_ensemble_output(runs_root, cs_run=cs_run)
        if ens_root is not None:
            run_dir = Path(ens_root) / cs_run
            if run_dir.is_dir():
                return run_dir.resolve(), runs_root

        root_path = Path(expand_path(runs_root)).resolve()
        flat_run_dir = root_path / cs_run
        if flat_run_dir.is_dir():
            return flat_run_dir, runs_root
        if root_path.name == cs_run and root_path.is_dir():
            return root_path, str(root_path.parent)

    raise FileNotFoundError(
        "Set CS_RUNS_DIR or pass --root pointing to a runs root, ensemble_output dir, or direct run dir."
    )


def _resolve_meta_file(run_dir: Path, cs_run: str) -> Path:
    exact = run_dir / f"{cs_run}.json"
    if exact.is_file():
        return exact

    json_files = sorted(run_dir.glob("*.json"))
    if len(json_files) == 1:
        return json_files[0]
    if not json_files:
        raise FileNotFoundError(f"No run JSON found in {run_dir}")
    raise FileNotFoundError(f"Multiple JSON files found in {run_dir}; pass a canonical {cs_run}.json")


def _resolve_extpar_file(run_dir: Path, domain_xy: str, explicit_extpar: str | None) -> Path:
    if explicit_extpar:
        candidate = Path(expand_path(explicit_extpar)).resolve()
        if not candidate.is_file():
            raise FileNotFoundError(f"Extpar file not found: {candidate}")
        return candidate

    extpar_name = f"extPar_Eriswil_{domain_xy}.nc"
    candidates: list[Path] = []
    seen: set[Path] = set()
    for parent in [run_dir, *run_dir.parents]:
        candidate = parent / "COS_in" / extpar_name
        if candidate not in seen:
            candidates.append(candidate)
            seen.add(candidate)
        if parent.name == "ensemble_output":
            alt = parent.parent / "COS_in" / extpar_name
            if alt not in seen:
                candidates.append(alt)
                seen.add(alt)

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    raise FileNotFoundError(
        f"Could not auto-detect {extpar_name}; pass --extpar explicitly."
    )


def _resolve_output_base(
    run_dir: Path,
    runs_root: str | None,
    cs_run: str,
    out: str | None,
) -> Path:
    if out:
        return Path(get_output_root(out, runs_root=runs_root, cs_run=cs_run)).resolve()
    if run_dir.parent.name == "ensemble_output":
        return run_dir.parent.resolve()
    return Path(get_output_root(None, runs_root=runs_root, cs_run=cs_run)).resolve()


def _zarr_name(cs_run: str, n_experiments: int, dt_seconds: float, is_subset: bool) -> str:
    dt_label = f"{int(dt_seconds)}s" if float(dt_seconds).is_integer() else f"{dt_seconds:g}s"
    subset = "_subset" if is_subset else ""
    return f"3D_{cs_run}_nExp{n_experiments}_dt{dt_label}{subset}.zarr"


def main() -> None:
    args = parse_args()
    from processing_metadata import provenance_attrs
    from three_d_zarr import build_3d_zarr, discover_3d_files, infer_domain_xy, load_run_metadata

    if not re.match(r"^cs-eriswil__\d{8}_\d{6}$", args.cs_run):
        raise ValueError(f"Invalid cs_run format: {args.cs_run}")
    if args.limit_experiments is not None and args.limit_experiments <= 0:
        raise ValueError("--limit-experiments must be positive.")

    run_dir, runs_root = _resolve_run_dir(args.cs_run, args.root)
    meta_file = _resolve_meta_file(run_dir, args.cs_run)
    meta = load_run_metadata(str(meta_file))
    domain_xy = args.domain_xy or infer_domain_xy(meta)
    extpar_file = _resolve_extpar_file(run_dir, domain_xy, args.extpar)

    file_dict = discover_3d_files(str(run_dir))
    if not file_dict:
        print(f"No 3D_*.nc files in {run_dir}", file=sys.stderr)
        sys.exit(1)

    total_experiments = len(file_dict)
    if args.limit_experiments is not None:
        file_dict = dict(list(file_dict.items())[: args.limit_experiments])

    out_base = _resolve_output_base(run_dir, runs_root, args.cs_run, args.out)
    out_root = out_base / args.cs_run / "lv2_3d"
    out_root.mkdir(parents=True, exist_ok=True)
    zarr_path = out_root / _zarr_name(
        args.cs_run,
        n_experiments=len(file_dict),
        dt_seconds=args.target_time_step_seconds,
        is_subset=len(file_dict) < total_experiments,
    )

    if zarr_path.exists() and not args.overwrite:
        print(f"Zarr exists, skip (use --overwrite to rebuild): {zarr_path}")
        return

    var_sets = tuple(dict.fromkeys(args.var_sets)) if args.var_sets else None
    global_attrs = provenance_attrs(
        stage="lv2",
        processing_level="LV2",
        title="PolarCAP 3D ensemble Zarr",
        summary=(
            "3D COSMO-SPECS experiment files normalized to physical coordinates, "
            "interpolated to a shared time grid, and concatenated along expname."
        ),
        source_code_path=str(Path(__file__).resolve()),
        source_notebook_or_script="scripts/processing_chain/run_lv2_3d_zarr.py",
        input_files=list(file_dict.values()),
        cs_run=args.cs_run,
        domain=domain_xy,
    )
    if len(file_dict) > 50:
        global_attrs["input_files"] = f"{len(file_dict)} files (see manifest)"
    global_attrs["extpar_file"] = str(extpar_file)
    global_attrs["target_time_step_seconds"] = args.target_time_step_seconds
    global_attrs["time_interp_method"] = args.time_method
    global_attrs["var_sets"] = ",".join(var_sets) if var_sets else "native_all"
    global_attrs["rebuild_required_for_new_experiments"] = True
    if len(file_dict) < total_experiments:
        global_attrs["subset_build"] = True

    print(f"Run dir : {run_dir}")
    print(f"Meta    : {meta_file}")
    print(f"Extpar  : {extpar_file}")
    print(f"Inputs  : {len(file_dict)} / {total_experiments} experiments")
    print(f"Vars    : {global_attrs['var_sets']}")

    result = build_3d_zarr(
        file_dict,
        str(zarr_path),
        meta_file=str(meta_file),
        extpar_file=str(extpar_file),
        var_sets=var_sets,
        target_time_step_seconds=args.target_time_step_seconds,
        time_interp_method=args.time_method,
        target_chunk_mb=args.target_chunk_mb,
        min_chunk_mb=args.min_chunk_mb,
        max_chunk_mb=args.max_chunk_mb,
        memory_fraction=args.memory_fraction,
        compression_level=args.compression_level,
        global_attrs=global_attrs,
        overwrite=args.overwrite,
    )

    compress_sh = _script_dir.parent / "nc_compression" / "compress.sh"
    print("\nValidation OK")
    print(f"  Zarr     : {result['zarr_path']}")
    print(f"  Manifest : {result['manifest_path']}")
    print(f"  Sizes    : {result['validation']['sizes']}")
    print(f"  dt (s)   : {result['validation']['time_step_seconds']}")
    print("\nCleanup after validation")
    print(f"  Optional archive originals: {compress_sh} compress {run_dir} /path/to/archive/{args.cs_run}")
    print(f"  Delete originals after archive: rm {run_dir}/3D_*.nc")
    print(
        f"  Rebuild later with new experiments: python {Path(__file__).name} -r {args.cs_run} --overwrite"
    )


if __name__ == "__main__":
    main()
