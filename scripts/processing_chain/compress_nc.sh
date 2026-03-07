#!/usr/bin/env bash
# Compress M_*.nc and 3D_*.nc with tar.zst (zstd level 9).
# Supports compress and extract with optional progress bar (pv).

set -euo pipefail

NC_GLOB_METE="M_*.nc"
NC_GLOB_3D="3D_*.nc"

usage() {
    cat <<'EOF'
Compress M_*.nc and 3D_*.nc with tar.zst (zstd level 9).
Supports compress and extract with optional progress bar (pv).

Usage:
  compress_nc.sh compress [dir] [archive]
  compress_nc.sh extract <archive> [dir]

Examples:
  compress_nc.sh compress .                              # -> nc_YYYYMMDD_HHMMSS.tar.zst
  compress_nc.sh compress . my_run.tar.zst               # custom archive name
  compress_nc.sh compress . test_datall_cs-eriswil__20260304_110254.tar.zst
  compress_nc.sh extract my_run.tar.zst .                # extract to current dir
  compress_nc.sh extract test_datall_cs-eriswil__20260304_110254.tar.zst /tmp/out
  compress_nc.sh --overwrite compress . my_run.tar.zst   # overwrite existing archive
  compress_nc.sh -f extract my_run.tar.zst /tmp/out      # overwrite existing files in dir

Options:
  -h, --help       show this help
  -f, --overwrite  overwrite existing archive (compress) or files (extract)

Env (for large data / SLURM):
  PV_INTERVAL=1    pv update interval (seconds); use 1 for steady progress on big archives
  Use compress_nc_slurm.sh to queue compression on Levante.
EOF
}

# Total bytes of M_*.nc and 3D_*.nc in dir (nullglob: only existing files; ls with no args lists ., so use set --)
_get_total_bytes() {
    local dir="$1"
    (cd "$dir" && shopt -s nullglob && set -- ${NC_GLOB_METE} ${NC_GLOB_3D} && \
        [ $# -eq 0 ] && echo 0 || ls -lq "$@" 2>/dev/null | awk '{s+=$5} END {print s+0}')
}

# Human-readable size (portable fallback when numfmt missing)
_fmt_size() {
    local b="$1"
    command -v numfmt &>/dev/null && numfmt --to=iec-i --suffix=B "$b" || \
        awk -v b="$b" 'BEGIN{if(b>=1073741824)printf "%.1f GiB",b/1073741824;else if(b>=1048576)printf "%.1f MiB",b/1048576;else if(b>=1024)printf "%.1f KiB",b/1024;else printf "%d B",b}'
}

# Count matching files (nullglob: only existing files; ls with no args lists ., so use set --)
_count_files() {
    local dir="$1"
    (cd "$dir" && shopt -s nullglob && set -- ${NC_GLOB_METE} ${NC_GLOB_3D} && echo $#)
}

cmd_compress() {
    local dir="${1:-.}" archive="${2:-}"
    [[ -z "$archive" ]] && archive="nc_$(date +%Y%m%d_%H%M%S).tar.zst"
    local archive_path="$archive"
    [[ "$archive" != /* ]] && archive_path="$dir/$archive"
    [[ -f "$archive_path" ]] && [[ -z "${OVERWRITE:-}" ]] && \
        { echo "Archive exists: $archive_path (use --overwrite)" >&2; exit 1; }
    local n=$(_count_files "$dir") total=$(_get_total_bytes "$dir")
    [[ "$n" -eq 0 ]] && { echo "No M_*.nc or 3D_*.nc in $dir" >&2; exit 1; }
    echo "Compressing $n files ($(_fmt_size "$total")) -> $archive"
    local zstd_opts=(-T0 -9 -o "$archive")
    [[ -n "${OVERWRITE:-}" ]] && zstd_opts=(-T0 -9 -f -o "$archive")
    local pv_opts="-s $total"
    [[ -n "${PV_INTERVAL:-}" ]] && pv_opts="$pv_opts -i ${PV_INTERVAL}"
    # Build file list in main shell (avoids pipeline subshell glob expansion issues with paths containing spaces)
    workdir=$(cd "$dir" && pwd)
    saved_cwd=$(pwd)
    cd "$workdir" || { echo "Cannot cd to $dir" >&2; exit 1; }
    shopt -s nullglob
    files=(${NC_GLOB_METE} ${NC_GLOB_3D})
    [[ ${#files[@]} -eq 0 ]] && { cd "$saved_cwd"; echo "No files to archive in $workdir" >&2; exit 1; }
    tar -cf - "${files[@]}" | \
        (command -v pv &>/dev/null && [[ "$total" -gt 0 ]] && pv $pv_opts || cat) | zstd "${zstd_opts[@]}"
    cd "$saved_cwd"
    echo "Done: $archive"
}

cmd_extract() {
    local archive="$1" dir="${2:-.}"
    [[ ! -f "$archive" ]] && { echo "Archive not found: $archive" >&2; exit 1; }
    local size=$(stat -f%z "$archive" 2>/dev/null || stat -c%s "$archive" 2>/dev/null || echo 0)
    echo "Extracting $archive -> $dir"
    mkdir -p "$dir"
    local k=""
    [[ -z "${OVERWRITE:-}" ]] && k="-k"
    (command -v pv &>/dev/null && [[ "$size" -gt 0 ]] && pv ${PV_INTERVAL:+-i "$PV_INTERVAL"} "$archive" || cat "$archive") | zstd -T0 -d | tar $k -xf - -C "$dir"
    echo "Done."
}

main() {
    while [[ "${1:-}" == "-f" || "${1:-}" == "--overwrite" ]]; do OVERWRITE=1; shift; done
    local cmd="${1:-}"
    shift || true
    [[ "$cmd" == "-h" || "$cmd" == "--help" ]] && { usage; exit 0; }
    case "$cmd" in
        compress) cmd_compress "${1:-.}" "${2:-}" ;;
        extract)  [[ -z "${1:-}" ]] && { usage; exit 1; }; cmd_extract "$1" "${2:-.}" ;;
        *)        usage; exit 1 ;;
    esac
}

main "$@"

