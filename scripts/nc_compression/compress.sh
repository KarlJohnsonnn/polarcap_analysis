#!/usr/bin/env bash
set -euo pipefail

GLOBS=(M_*.nc 3D_*.nc Meteogram_*.zarr)

usage() {
    cat <<'EOF'
Usage:
  compress.sh list [dir]
  compress.sh compress <file_or_dir> [out]
  compress.sh extract <file_or_dir> [out]
EOF
}

dir_abs() { cd "$1" && pwd -P; }
file_size() { stat -c%s "$1" 2>/dev/null || stat -f%z "$1" 2>/dev/null || echo 0; }
threads() { printf '%s\n' "${SLURM_CPUS_PER_TASK:-0}"; }
is_zarr_dir() { [[ -d "$1" && "$1" == *.zarr ]]; }
dir_size() { du -sb "$1" 2>/dev/null | awk '{print $1}' || awk -v k="$(du -sk "$1" | awk '{print $1}')" 'BEGIN{print k*1024}'; }

list_nc() {
    local dir; dir="$(dir_abs "${1:-.}")" || exit 1
    (
        cd "$dir"
        shopt -s nullglob
        local files=() glob
        for glob in "${GLOBS[@]}"; do files+=($glob); done
        ((${#files[@]})) && printf '%s\n' "${files[@]/#/$dir/}"
    )
}

list_zst() {
    local dir; dir="$(dir_abs "${1:-.}")" || exit 1
    (
        cd "$dir"
        shopt -s nullglob
        local files=(*.nc.zst *.tar.zst)
        ((${#files[@]})) && printf '%s\n' "${files[@]/#/$dir/}"
    )
}

compressed_path() {
    local src="$1" out="${2:-}"
    local suffix=".zst"
    [[ -d "$src" ]] && suffix=".tar.zst"
    if [[ -z "$out" ]]; then printf '%s%s\n' "$src" "$suffix"; return; fi
    if [[ -d "$out" || "$out" == */ || "$out" != *.zst ]]; then
        mkdir -p "$out"
        printf '%s/%s%s\n' "$(dir_abs "$out")" "$(basename "$src")" "$suffix"
        return
    fi
    mkdir -p "$(dirname "$out")"
    printf '%s/%s\n' "$(dir_abs "$(dirname "$out")")" "$(basename "$out")"
}

extracted_path() {
    local src="$1" out="${2:-}" base
    base="$(basename "${src%.zst}")"
    if [[ -z "$out" ]]; then printf '%s\n' "${src%.zst}"; return; fi
    if [[ -d "$out" || "$out" == */ || "$out" != *.nc ]]; then
        mkdir -p "$out"
        printf '%s/%s\n' "$(dir_abs "$out")" "$base"
        return
    fi
    mkdir -p "$(dirname "$out")"
    printf '%s/%s\n' "$(dir_abs "$(dirname "$out")")" "$(basename "$out")"
}

compress_one() {
    local src="$1" out="${2:-}" dst bytes pv=()
    src="$(dir_abs "$(dirname "$src")")/$(basename "$src")"
    [[ -f "$src" || -d "$src" ]] || { echo "Missing input: $1" >&2; exit 1; }
    dst="$(compressed_path "$src" "$out")"
    [[ ! -e "$dst" || -n "${OVERWRITE:-}" ]] || { echo "Exists: $dst" >&2; exit 1; }
    if [[ -d "$src" ]]; then
        bytes="$(dir_size "$src")"
    else
        bytes="$(file_size "$src")"
    fi
    [[ -n "${PV_INTERVAL:-}" ]] && pv=(-i "$PV_INTERVAL")
    if [[ -d "$src" ]]; then
        local parent name
        parent="$(dirname "$src")"
        name="$(basename "$src")"
        if command -v pv >/dev/null 2>&1 && [[ "$bytes" -gt 0 ]]; then
            tar -C "$parent" -cf - "$name" | pv -s "$bytes" "${pv[@]}" | zstd -T"$(threads)" -9 ${OVERWRITE:+-f} -o "$dst"
        else
            tar -C "$parent" -cf - "$name" | zstd -T"$(threads)" -9 ${OVERWRITE:+-f} -o "$dst"
        fi
    elif command -v pv >/dev/null 2>&1 && [[ "$bytes" -gt 0 ]]; then
        pv -s "$bytes" "${pv[@]}" "$src" | zstd -T"$(threads)" -9 ${OVERWRITE:+-f} -o "$dst"
    else
        zstd -T"$(threads)" -9 -k ${OVERWRITE:+-f} -o "$dst" "$src"
    fi
}

extract_one() {
    local src="$1" out="${2:-}" dst bytes pv=()
    src="$(dir_abs "$(dirname "$src")")/$(basename "$src")"
    [[ -f "$src" ]] || { echo "Missing file: $1" >&2; exit 1; }
    bytes="$(file_size "$src")"
    [[ -n "${PV_INTERVAL:-}" ]] && pv=(-i "$PV_INTERVAL")
    if [[ "$src" == *.tar.zst ]]; then
        local target="${out:-.}"
        mkdir -p "$target"
        if command -v pv >/dev/null 2>&1 && [[ "$bytes" -gt 0 ]]; then
            pv -s "$bytes" "${pv[@]}" "$src" | zstd -T"$(threads)" -d | tar -xf - -C "$target"
        else
            zstd -T"$(threads)" -d -c "$src" | tar -xf - -C "$target"
        fi
    else
        dst="$(extracted_path "$src" "$out")"
        [[ ! -e "$dst" || -n "${OVERWRITE:-}" ]] || { echo "Exists: $dst" >&2; exit 1; }
        if command -v pv >/dev/null 2>&1 && [[ "$bytes" -gt 0 ]]; then
            pv -s "$bytes" "${pv[@]}" "$src" | zstd -T"$(threads)" -d ${OVERWRITE:+-f} -o "$dst"
        else
            zstd -T"$(threads)" -d -k ${OVERWRITE:+-f} -o "$dst" "$src"
        fi
    fi
}

run_many() {
    local lister="$1" action="$2" dir="$3" out="${4:-$3}" label="$5" file
    mapfile -t files < <("$lister" "$dir")
    [[ ${#files[@]} -gt 0 ]] || { echo "No $label in $dir" >&2; exit 1; }
    mkdir -p "$out"
    for file in "${files[@]}"; do "$action" "$file" "$out"; done
}

while [[ "${1:-}" == "-f" || "${1:-}" == "--overwrite" ]]; do OVERWRITE=1; shift; done
case "${1:-}" in
    list) list_nc "${2:-.}" ;;
    compress)
        [[ -n "${2:-}" ]] || { usage; exit 1; }
        if [[ -d "$2" ]] && ! is_zarr_dir "$2"; then
            run_many list_nc compress_one "$2" "${3:-$2}" 'M_*.nc or 3D_*.nc or Meteogram_*.zarr'
        else
            compress_one "$2" "${3:-}"
        fi
        ;;
    extract)
        [[ -n "${2:-}" ]] || { usage; exit 1; }
        if [[ -d "$2" ]]; then
            run_many list_zst extract_one "$2" "${3:-$2}" '*.nc.zst or *.tar.zst files'
        else
            extract_one "$2" "${3:-}"
        fi
        ;;
    *) usage; exit 1 ;;
esac
