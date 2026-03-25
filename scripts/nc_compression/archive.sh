#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  archive.sh list [dir]
  archive.sh archive <file> <hsm_namespace>
EOF
}

dir_abs() { cd "$1" && pwd -P; }

list_zst() {
    local dir; dir="$(dir_abs "${1:-.}")" || exit 1
    (
        cd "$dir"
        shopt -s nullglob
        local files=(*.nc.zst)
        ((${#files[@]})) && printf '%s\n' "${files[@]/#/$dir/}"
    )
}

load_slk() {
    [[ -f /sw/etc/profile.levante ]] && . /sw/etc/profile.levante
    command -v module >/dev/null 2>&1 && module load slk >/dev/null 2>&1 || true
    command -v slk >/dev/null 2>&1 || { echo "slk not found" >&2; exit 1; }
    [[ -f "${HOME}/.slk/config.json" ]] || { echo "Run 'module load slk && slk login' first." >&2; exit 1; }
}

archive_one() {
    local src ns code
    src="$(dir_abs "$(dirname "$1")")/$(basename "$1")"
    ns="${2%/}/"
    [[ -f "$src" ]] || { echo "Missing file: $1" >&2; exit 1; }
    load_slk
    set +e
    slk archive -vv "$src" "$ns"
    code=$?
    set -e
    if [[ "$code" -ne 0 && "${RETRY:-0}" == 1 ]]; then
        echo "WARNING: slk archive failed for '$src' (exit $code). Retrying in ${RETRY_DELAY:-60}s..." >&2
        sleep "${RETRY_DELAY:-60}"
        set +e
        slk archive -vv "$src" "$ns"
        code=$?
        set -e
    fi
    if [[ "$code" -ne 0 ]]; then
        echo "WARNING: archive failed for '$src' -> '$ns' (exit $code)." >&2
    fi
    return "$code"
}

case "${1:-}" in
    list) list_zst "${2:-.}" ;;
    archive) [[ -n "${2:-}" && -n "${3:-}" ]] || { usage; exit 1; }; archive_one "$2" "$3" ;;
    *) usage; exit 1 ;;
esac
